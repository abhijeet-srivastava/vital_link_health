package com.vitallink.readings.alerts;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.AuditEntry;
import com.vitallink.readings.domain.OutboxEvent;
import com.vitallink.readings.domain.StateChange;
import com.vitallink.readings.messaging.AlertStateChangedV1;
import com.vitallink.readings.messaging.UuidV7;

/**
 * The rows every alert state change writes in its own transaction: one alert.state_changed outbox row (rule 2) and
 * one audit_log row (rule 9). Shared by coach actions, escalation and reassignment so all three write the same rows.
 */
public class StateChangeRecords {

    private final Clock clock;
    private final UuidV7 ids;
    private final ObjectMapper json;

    public StateChangeRecords(Clock clock, UuidV7 ids, ObjectMapper json) {
        this.clock = clock;
        this.ids = ids;
        this.json = json;
    }

    public record Rows(List<OutboxEvent> events, List<AuditEntry> audits) {
    }

    /**
     * @param requestId the caller's request id for the audit rows; null for system actors, whose audit row then
     *                  carries the change's event_id (as the evaluator does, D27)
     */
    public Rows rows(List<StateChange> changes, UUID requestId) {
        List<OutboxEvent> events = new ArrayList<>(changes.size());
        List<AuditEntry> audits = new ArrayList<>(changes.size());
        for (StateChange c : changes) {
            UUID eventId = ids.next();
            events.add(outbox(eventId, c));
            audits.add(new AuditEntry(ids.next(), clock.instant(), c.actorType(), c.actorId(), c.auditAction(),
                    AuditEntry.Outcome.ALLOWED, c.alert().patientId(), "alert/" + c.alert().alertId(),
                    requestId != null ? requestId : eventId));
        }
        return new Rows(events, audits);
    }

    private OutboxEvent outbox(UUID eventId, StateChange c) {
        AlertStateChangedV1 payload = AlertStateChangedV1.of(eventId, c);
        try {
            return new OutboxEvent(eventId, AlertStateChangedV1.TOPIC, AlertStateChangedV1.EVENT_TYPE,
                    String.valueOf(c.alert().patientId()), AlertStateChangedV1.SCHEMA_VERSION,
                    json.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("alert.state_changed payload not serializable alert_id="
                    + c.alert().alertId(), e);
        }
    }
}
