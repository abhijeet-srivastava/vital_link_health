package com.vitallink.readings.alerting.evaluator;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.domain.AlertChange;
import com.vitallink.readings.domain.AlertDecisions;
import com.vitallink.readings.domain.AlertOccurrence;
import com.vitallink.readings.domain.AuditEntry;
import com.vitallink.readings.domain.BreachWithReading;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.EpisodeGroupPlan;
import com.vitallink.readings.domain.EpisodeKey;
import com.vitallink.readings.domain.EpisodeRow;
import com.vitallink.readings.domain.EpisodeUpdate;
import com.vitallink.readings.domain.LockedEpisode;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.ports.AlertBatchRepository;
import com.vitallink.readings.ports.AuditWriter;
import com.vitallink.readings.ports.OutboxWriter;
import com.vitallink.readings.ports.ThresholdProvider;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Batched evaluation (pattern 3, templates/java/BatchAlertEvaluator.java): one transaction for a whole poll, with
 * set-based statements for the occurrence pre-check, the episode lock, inserts, updates, audit and outbox rows.
 * Breaches for one (patient, measure) are folded in memory with the per-event decision function, so the database
 * sees one row per episode and the announcements match the per-event path. Correctness never depends on this path:
 * any failure rolls the batch back and {@link #evaluateWithFallback} replays it per event on {@link AlertEvaluator}.
 */
public class BatchAlertEvaluator {

    private static final Logger log = LoggerFactory.getLogger(BatchAlertEvaluator.class);

    /** One readings.ingested event: all readings of one patient. */
    public record Event(UUID eventId, long patientId, List<Reading> readings) {
    }

    private record Committed(List<AlertChange> changes, int breaches) {
    }

    private final ThresholdProvider thresholds;
    private final MeasureEvaluators evaluators;
    private final AlertDecisions decisions;
    private final AlertBatchRepository alerts;
    private final OutboxWriter outbox;
    private final AuditWriter audit;
    private final TransactionTemplate tx;
    private final AlertRecords records;
    private final AlertEvaluator perEvent;
    private final MeterRegistry meters;
    private final DistributionSummary batchSize;
    private final DistributionSummary breachesPerCommit;
    private final Timer commitLatency;

    public BatchAlertEvaluator(ThresholdProvider thresholds, MeasureEvaluators evaluators, AlertDecisions decisions,
                               AlertBatchRepository alerts, OutboxWriter outbox, AuditWriter audit,
                               TransactionTemplate tx, AlertRecords records, AlertEvaluator perEvent,
                               MeterRegistry meters) {
        this.thresholds = thresholds;
        this.evaluators = evaluators;
        this.decisions = decisions;
        this.alerts = alerts;
        this.outbox = outbox;
        this.audit = audit;
        this.tx = tx;
        this.records = records;
        this.perEvent = perEvent;
        this.meters = meters;
        this.batchSize = DistributionSummary.builder("vitallink.evaluator.batch.size")
                .description("readings.ingested events per batch").register(meters);
        this.breachesPerCommit = DistributionSummary.builder("vitallink.evaluator.batch.breaches")
                .description("breaches written per batch commit").register(meters);
        this.commitLatency = Timer.builder("vitallink.evaluator.batch.commit")
                .description("batch transaction, from first statement to commit").register(meters);
    }

    /**
     * The batch in one transaction; on any failure, each event again in its own transaction on the per-event path.
     * Offsets may be committed once this returns.
     *
     * @return events that failed on the per-event path as well; the caller retries them elsewhere
     */
    public List<Event> evaluateWithFallback(List<Event> events) {
        if (events.isEmpty()) {
            return List.of();
        }
        try {
            evaluateBatch(events);
            return List.of();
        } catch (DuplicateKeyException alreadySeenOrRace) {
            // Replay or rebalance overlap: an occurrence or episode was committed by someone else, or the same
            // reading appears twice in this poll. PostgreSQL aborted the transaction; nothing of it was kept.
            fallback("unique_violation", events.size(), alreadySeenOrRace);
        } catch (RuntimeException failed) {
            // One bad event (e.g. no PRIMARY coach) must not hold back the rest of the poll.
            fallback("failure", events.size(), failed);
        }
        List<Event> failed = new ArrayList<>();
        for (Event event : events) {
            try {
                perEvent.evaluate(event.eventId(), event.patientId(), event.readings());
            } catch (RuntimeException e) {
                log.warn("Per-event evaluation failed patient_id={} event_id={} error={}", event.patientId(),
                        event.eventId(), e.getClass().getSimpleName());
                failed.add(event);
            }
        }
        return failed;
    }

    /** One transaction for the whole batch. A unique violation rolls it back and propagates. */
    public List<AlertChange> evaluateBatch(List<Event> events) {
        batchSize.record(events.size());
        // Thresholds once for every patient in the batch, outside the transaction.
        EffectiveThresholds effective = thresholds.forPatients(
                events.stream().map(Event::patientId).distinct().toList());
        // Identity, not equals: a reading delivered twice gives equal breaches, each with its own event id.
        Map<BreachWithReading, UUID> eventOf = new IdentityHashMap<>();
        List<BreachWithReading> breaches = new ArrayList<>();
        for (Event event : events) {
            for (BreachWithReading b : AlertEvaluator.breaches(evaluators, effective, event.readings())) {
                breaches.add(b);
                eventOf.put(b, event.eventId());
            }
        }
        if (breaches.isEmpty()) {
            return List.of();
        }
        Timer.Sample sample = Timer.start(meters);
        Committed committed = tx.execute(status -> evaluateInCurrentTx(breaches, eventOf));
        sample.stop(commitLatency);
        breachesPerCommit.record(committed.breaches());
        committed.changes().forEach(c -> meters.counter(c.reason() == AlertChange.Reason.RAISED
                        ? "vitallink.alerts.raised" : "vitallink.alerts.upgraded", "severity", c.severity().name())
                .increment());
        return committed.changes();
    }

    private Committed evaluateInCurrentTx(List<BreachWithReading> breaches, Map<BreachWithReading, UUID> eventOf) {
        // 1. Guard 4 pre-check: drop breaches already evaluated (one query). The PK is the guarantee.
        List<BreachWithReading> fresh = alerts.withoutExistingOccurrences(breaches);
        if (fresh.isEmpty()) {
            return new Committed(List.of(), 0);
        }

        // 2. Guard 3: lock every unresolved episode the batch touches (one SELECT ... FOR UPDATE).
        Map<EpisodeKey, List<BreachWithReading>> groups = fresh.stream()
                .collect(Collectors.groupingBy(EpisodeKey::of, LinkedHashMap::new, Collectors.toList()));
        Map<EpisodeKey, LockedEpisode> locked = alerts.lockActiveEpisodes(groups.keySet());

        // 3. Fold each (patient, measure) in memory: new episode, repeat, or upgrade (with reopen).
        List<EpisodeGroupPlan> plans = groups.entrySet().stream()
                .map(g -> decisions.planEpisode(g.getValue(),
                        Optional.ofNullable(locked.get(g.getKey())).map(LockedEpisode::episode)))
                .toList();

        // 4. One row per episode. Unique violations propagate -> rollback -> per-event fallback.
        List<EpisodeGroupPlan> opening = plans.stream().filter(EpisodeGroupPlan::opensEpisode).toList();
        Map<EpisodeKey, EpisodeRow> created = opening.isEmpty() ? Map.of() : alerts.insertEpisodes(opening);
        List<EpisodeUpdate> updates = plans.stream().filter(p -> !p.opensEpisode())
                .map(p -> new EpisodeUpdate(locked.get(p.key()).episode().alertId(), p))
                .toList();
        if (!updates.isEmpty()) {
            alerts.updateEpisodes(updates);
        }

        // 5. Occurrences, audit rows and announcements step by step, as the per-event path would write them.
        List<AlertOccurrence> occurrences = new ArrayList<>(fresh.size());
        List<AuditEntry> audits = new ArrayList<>();
        List<AlertChange> changes = new ArrayList<>();
        for (EpisodeGroupPlan plan : plans) {
            LockedEpisode existing = locked.get(plan.key());
            long alertId = plan.opensEpisode() ? created.get(plan.key()).alertId() : existing.episode().alertId();
            long coachId = plan.opensEpisode() ? created.get(plan.key()).coachId() : existing.coachId();
            int count = plan.opensEpisode() ? 0 : existing.occurrenceCnt();
            for (EpisodeGroupPlan.Step step : plan.steps()) {
                BreachWithReading b = step.breach();
                int occurrenceCnt = ++count;
                occurrences.add(new AlertOccurrence(alertId, b));
                audits.addAll(records.audits(eventOf.get(b), b.patientId(), alertId, step.plan()));
                step.plan().reason().ifPresent(reason -> changes.add(new AlertChange(alertId, b.patientId(), coachId,
                        b.measure(), b.breach().observedValue(), step.plan().severity(), reason,
                        b.breach().band().thresholdId(), occurrenceCnt, step.plan().late())));
            }
        }
        alerts.insertOccurrences(occurrences);   // PK (device_id, reading_uuid, measure_code, measured_at)
        audit.recordAll(audits);

        // 6. Only new episodes and upgrades are announced; CRITICAL goes to the critical lane.
        outbox.append(changes.stream().map(records::outbox).toList());
        return new Committed(changes, fresh.size());
    }

    private void fallback(String reason, int events, RuntimeException cause) {
        meters.counter("vitallink.evaluator.batch.fallbacks", "reason", reason).increment();
        log.info("Batch rolled back, replaying per event reason={} events={} error={}", reason, events,
                cause.getClass().getSimpleName());
    }
}
