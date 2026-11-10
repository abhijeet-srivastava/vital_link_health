package com.vitallink.readings.messaging;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vitallink.readings.domain.Reading;

/**
 * {@code readings.ingested} payload, schema_version 1 (references/architecture.md). IDs and values only, never
 * names, contact details or free text. Names are explicit so app-level Jackson settings cannot change the wire format.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReadingsIngestedV1(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("type") String type,
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("patient_id") long patientId,
        @JsonProperty("device_id") long deviceId,
        @JsonProperty("readings") List<Item> readings) {

    public static final String TOPIC = "readings.ingested";
    public static final String EVENT_TYPE = "readings.ingested";
    public static final int SCHEMA_VERSION = 1;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Item(
            @JsonProperty("reading_uuid") UUID readingUuid,
            @JsonProperty("reading_type") String readingType,
            @JsonProperty("measured_at") String measuredAt,
            @JsonProperty("device_seq") Long deviceSeq,
            @JsonProperty("systolic") Integer systolic,
            @JsonProperty("diastolic") Integer diastolic,
            @JsonProperty("pulse") Integer pulse,
            @JsonProperty("value") BigDecimal value) {
    }

    public static ReadingsIngestedV1 of(UUID eventId, long patientId, long deviceId, List<Reading> accepted) {
        List<Item> items = accepted.stream()
                .map(r -> new Item(r.readingUuid(), r.type().name(), r.measuredAt().toString(), r.deviceSeq(),
                        r.systolic(), r.diastolic(), r.pulse(), r.valueNum()))
                .toList();
        return new ReadingsIngestedV1(eventId, EVENT_TYPE, SCHEMA_VERSION, patientId, deviceId, items);
    }
}
