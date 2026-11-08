package com.vitallink.readings.alerting.evaluator;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.ActiveEpisode;
import com.vitallink.readings.domain.AlertChange;
import com.vitallink.readings.domain.AlertDecisions;
import com.vitallink.readings.domain.AuditEntry;
import com.vitallink.readings.domain.BreachWithReading;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.EpisodePlan;
import com.vitallink.readings.domain.EpisodeRow;
import com.vitallink.readings.domain.OutboxEvent;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.messaging.AlertsRaisedV1;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.ports.AlertEpisodeRepository;
import com.vitallink.readings.ports.AuditWriter;
import com.vitallink.readings.ports.OutboxWriter;
import com.vitallink.readings.ports.ThresholdProvider;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Per-event evaluation (pattern 2): one transaction per readings.ingested event. Thresholds and strategies run
 * before the transaction; inside it each breach locks its episode, opens or folds it, records its occurrence and,
 * for a new episode or an upgrade only, appends an alerts.raised outbox row. Every alert state change (raise,
 * upgrade, reopen) writes an audit_log row in the same transaction (rule 9); a repeat breach changes no state.
 */
public class AlertEvaluator {

    private static final Logger log = LoggerFactory.getLogger(AlertEvaluator.class);

    static final String AUDIT_ACTOR = "alert-evaluator";

    private final ThresholdProvider thresholds;
    private final MeasureEvaluators evaluators;
    private final AlertDecisions decisions;
    private final AlertEpisodeRepository episodes;
    private final OutboxWriter outbox;
    private final AuditWriter audit;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final UuidV7 ids;
    private final ObjectMapper json;
    private final MeterRegistry meters;
    private final Timer latency;

    public AlertEvaluator(ThresholdProvider thresholds, MeasureEvaluators evaluators, AlertDecisions decisions,
                          AlertEpisodeRepository episodes, OutboxWriter outbox, AuditWriter audit,
                          TransactionTemplate tx, Clock clock, UuidV7 ids, ObjectMapper json, MeterRegistry meters) {
        this.thresholds = thresholds;
        this.evaluators = evaluators;
        this.decisions = decisions;
        this.episodes = episodes;
        this.outbox = outbox;
        this.audit = audit;
        this.tx = tx;
        this.clock = clock;
        this.ids = ids;
        this.json = json;
        this.meters = meters;
        this.latency = Timer.builder("vitallink.evaluator.latency")
                .description("readings.ingested event evaluated and committed")
                .register(meters);
    }

    /**
     * Evaluates one event's readings (all for one patient) and returns the changes announced. Safe to repeat: an
     * evaluated reading is skipped, and a concurrent duplicate rolls the transaction back (guard 4).
     *
     * @param eventId the readings.ingested event_id; recorded as the audit request id
     */
    public List<AlertChange> evaluate(UUID eventId, long patientId, List<Reading> readings) {
        return latency.record(() -> {
            List<BreachWithReading> breaches = breaches(patientId, readings);
            if (breaches.isEmpty()) {
                return List.<AlertChange>of();
            }
            try {
                return commit(eventId, breaches);
            } catch (DuplicateKeyException race) {
                // Another consumer (rebalance overlap) committed the same occurrence or opened the same episode first.
                // PostgreSQL aborted our transaction, so replay it once from the start: the early-exit check skips
                // what is already recorded and the episode is now found and locked. A second failure goes to the
                // container's retry policy.
                log.info("Evaluation replayed after unique violation patient_id={}", patientId);
                return commit(eventId, breaches);
            }
        });
    }

    private List<BreachWithReading> breaches(long patientId, List<Reading> readings) {
        EffectiveThresholds effective = thresholds.forPatients(List.of(patientId));
        return readings.stream()
                .sorted(Comparator.comparing(Reading::measuredAt))   // episode first/last seen follow device time
                .flatMap(r -> evaluators.evaluate(r, effective).stream()
                        .map(b -> new BreachWithReading(r.patientId(), r.deviceId(), r.readingUuid(), r.measuredAt(), b)))
                .toList();
    }

    private List<AlertChange> commit(UUID eventId, List<BreachWithReading> breaches) {
        List<AlertChange> changes = tx.execute(status -> {
            List<AlertChange> announced = new ArrayList<>();
            for (BreachWithReading b : breaches) {
                apply(eventId, b).ifPresent(announced::add);
            }
            outbox.append(announced.stream().map(this::toOutbox).toList());
            return announced;
        });
        changes.forEach(c -> meters.counter(c.reason() == AlertChange.Reason.RAISED
                        ? "vitallink.alerts.raised" : "vitallink.alerts.upgraded", "severity", c.severity().name())
                .increment());
        return changes;
    }

    private Optional<AlertChange> apply(UUID eventId, BreachWithReading b) {
        if (episodes.occurrenceExists(b)) {
            return Optional.empty();   // evaluated before: no count change, no event
        }
        Optional<ActiveEpisode> current = episodes.lockActiveEpisode(b.patientId(), b.measure());
        EpisodePlan plan = decisions.plan(b, current);
        EpisodeRow row = switch (plan) {
            case EpisodePlan.Open open -> episodes.openEpisode(b, open);
            case EpisodePlan.Fold fold -> episodes.foldEpisode(b, fold);
        };
        episodes.insertOccurrence(row.alertId(), b);
        auditStateChanges(eventId, b.patientId(), row.alertId(), plan);
        return plan.reason().map(reason -> new AlertChange(row.alertId(), b.patientId(), row.coachId(), b.measure(),
                b.breach().observedValue(), plan.severity(), reason, b.breach().band().thresholdId(),
                row.occurrenceCnt(), plan.late()));
    }

    private void auditStateChanges(UUID eventId, long patientId, long alertId, EpisodePlan plan) {
        switch (plan) {
            case EpisodePlan.Open open -> audit(eventId, patientId, alertId, "RAISE_ALERT");
            case EpisodePlan.Fold fold -> {
                if (fold.upgraded()) {
                    audit(eventId, patientId, alertId, "UPGRADE_ALERT");
                }
                if (fold.reopened()) {
                    audit(eventId, patientId, alertId, "REOPEN_ALERT");
                }
            }
        }
    }

    private void audit(UUID eventId, long patientId, long alertId, String action) {
        audit.record(new AuditEntry(ids.next(), clock.instant(), AuditEntry.ActorType.SERVICE, AUDIT_ACTOR, action,
                AuditEntry.Outcome.ALLOWED, patientId, "alert/" + alertId, eventId));
    }

    private OutboxEvent toOutbox(AlertChange change) {
        AlertsRaisedV1 payload = AlertsRaisedV1.of(ids.next(), change);
        try {
            return new OutboxEvent(payload.eventId(), AlertsRaisedV1.topicFor(change.severity()),
                    AlertsRaisedV1.EVENT_TYPE, String.valueOf(change.patientId()), AlertsRaisedV1.SCHEMA_VERSION,
                    json.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("alerts.raised payload not serializable alert_id=" + change.alertId(), e);
        }
    }
}
