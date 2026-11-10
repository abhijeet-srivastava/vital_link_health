package com.vitallink.readings.messaging;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.StateChange;

/**
 * {@code alert.state_changed} payload, schema_version 1 (references/architecture.md): ACKNOWLEDGED, RESOLVED,
 * ESCALATED or REASSIGNED. IDs and values only; the resolution note never travels. Key = patient_id.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AlertStateChangedV1(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("type") String type,
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("alert_id") long alertId,
        @JsonProperty("patient_id") long patientId,
        @JsonProperty("coach_id") long coachId,
        @JsonProperty("previous_coach_id") Long previousCoachId,
        @JsonProperty("measure") String measure,
        @JsonProperty("severity") String severity,
        @JsonProperty("from") String from,
        @JsonProperty("to") String to,
        @JsonProperty("reason") String reason,
        @JsonProperty("actor_type") String actorType,
        @JsonProperty("actor_id") String actorId,
        @JsonProperty("alert_version") int alertVersion) {

    public static final String EVENT_TYPE = "alert.state_changed";
    public static final String TOPIC = KafkaTopics.ALERT_STATE_CHANGED;
    public static final int SCHEMA_VERSION = 1;

    public static AlertStateChangedV1 of(UUID eventId, StateChange c) {
        Alert a = c.alert();
        return new AlertStateChangedV1(eventId, EVENT_TYPE, SCHEMA_VERSION, a.alertId(), a.patientId(), a.coachId(),
                c.previousCoachId(), a.measure().name(), a.severity().name(), c.from().name(), a.status().name(),
                c.reason().name(), c.actorType().name(), c.actorId(), a.version());
    }
}
