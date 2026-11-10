package com.vitallink.readings.alerting.evaluator;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.AlertChange;
import com.vitallink.readings.domain.AuditEntry;
import com.vitallink.readings.domain.EpisodePlan;
import com.vitallink.readings.domain.OutboxEvent;
import com.vitallink.readings.messaging.AlertsRaisedV1;
import com.vitallink.readings.messaging.UuidV7;

/**
 * The rows an evaluation writes besides the episode: the alerts.raised outbox row per announced change and the
 * audit_log rows per state change (D27). Shared by the per-event and batch paths so both write the same rows.
 */
public class AlertRecords {

    static final String AUDIT_ACTOR = "alert-evaluator";

    private final Clock clock;
    private final UuidV7 ids;
    private final ObjectMapper json;

    public AlertRecords(Clock clock, UuidV7 ids, ObjectMapper json) {
        this.clock = clock;
        this.ids = ids;
        this.json = json;
    }

    /** RAISE_ALERT, UPGRADE_ALERT, REOPEN_ALERT; a repeat breach changes no state and is not audited. */
    List<AuditEntry> audits(UUID eventId, long patientId, long alertId, EpisodePlan plan) {
        List<String> actions = new ArrayList<>();
        switch (plan) {
            case EpisodePlan.Open open -> actions.add("RAISE_ALERT");
            case EpisodePlan.Fold fold -> {
                if (fold.upgraded()) {
                    actions.add("UPGRADE_ALERT");
                }
                if (fold.reopened()) {
                    actions.add("REOPEN_ALERT");
                }
            }
        }
        return actions.stream()
                .map(action -> new AuditEntry(ids.next(), clock.instant(), AuditEntry.ActorType.SERVICE, AUDIT_ACTOR,
                        action, AuditEntry.Outcome.ALLOWED, patientId, "alert/" + alertId, eventId))
                .toList();
    }

    /** CRITICAL goes to the critical lane topic. */
    OutboxEvent outbox(AlertChange change) {
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
