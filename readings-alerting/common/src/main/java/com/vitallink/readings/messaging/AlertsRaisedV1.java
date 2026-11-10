package com.vitallink.readings.messaging;

import java.math.BigDecimal;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vitallink.readings.domain.AlertChange;
import com.vitallink.readings.domain.Severity;

/**
 * {@code alerts.raised} payload, schema_version 1 (references/architecture.md): a new episode (RAISED) or a severity
 * upgrade (SEVERITY_UPGRADED). IDs and values only. CRITICAL travels on {@code alerts.raised.critical}, same shape.
 */
public record AlertsRaisedV1(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("type") String type,
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("alert_id") long alertId,
        @JsonProperty("patient_id") long patientId,
        @JsonProperty("coach_id") long coachId,
        @JsonProperty("measure") String measure,
        @JsonProperty("observed_value") BigDecimal observedValue,
        @JsonProperty("severity") String severity,
        @JsonProperty("reason") String reason,
        @JsonProperty("threshold_id") long thresholdId,
        @JsonProperty("occurrence_cnt") int occurrenceCnt,
        @JsonProperty("is_late") boolean late) {

    public static final String EVENT_TYPE = "alerts.raised";
    public static final int SCHEMA_VERSION = 1;

    public static AlertsRaisedV1 of(UUID eventId, AlertChange c) {
        return new AlertsRaisedV1(eventId, EVENT_TYPE, SCHEMA_VERSION, c.alertId(), c.patientId(), c.coachId(),
                c.measure().name(), c.observedValue(), c.severity().name(), c.reason().name(), c.thresholdId(),
                c.occurrenceCnt(), c.late());
    }

    /** The critical lane has its own topic, notifier group and provider quota. */
    public static String topicFor(Severity severity) {
        return severity == Severity.CRITICAL ? KafkaTopics.ALERTS_RAISED_CRITICAL : KafkaTopics.ALERTS_RAISED;
    }
}
