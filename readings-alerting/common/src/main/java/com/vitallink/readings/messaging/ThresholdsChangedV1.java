package com.vitallink.readings.messaging;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vitallink.readings.domain.ThresholdChange;

/**
 * {@code thresholds.changed} payload, schema_version 1 (references/architecture.md): a patient's override bands for
 * one measure were replaced. IDs only; consumers evict caches and reload the bands from the DB. Key = patient_id.
 */
public record ThresholdsChangedV1(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("patient_id") long patientId,
        @JsonProperty("measure_code") int measureCode,
        @JsonProperty("closed_threshold_ids") List<Long> closedThresholdIds,
        @JsonProperty("threshold_ids") List<Long> thresholdIds) {

    public static final String EVENT_TYPE = "thresholds.changed";
    public static final String TOPIC = KafkaTopics.THRESHOLDS_CHANGED;
    public static final int SCHEMA_VERSION = 1;

    public static ThresholdsChangedV1 of(UUID eventId, ThresholdChange c) {
        return new ThresholdsChangedV1(eventId, SCHEMA_VERSION, c.patientId(), c.measure().code(), c.closedIds(),
                c.insertedIds());
    }
}
