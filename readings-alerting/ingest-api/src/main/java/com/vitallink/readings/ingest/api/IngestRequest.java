package com.vitallink.readings.ingest.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** POST /v1/devices/{deviceId}/readings body (api-contracts.md); snake_case on the wire. */
public record IngestRequest(Long patientId, List<Item> readings) {

    public record Item(UUID readingId, Long deviceSeq, String type, Instant measuredAt,
                       Map<String, BigDecimal> values, String unit) {
    }
}
