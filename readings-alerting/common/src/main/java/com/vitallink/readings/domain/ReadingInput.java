package com.vitallink.readings.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** One reading as the device sent it, before validation. Any field may be missing. */
public record ReadingInput(
        UUID readingId,
        Long deviceSeq,
        String type,
        Instant measuredAt,
        Map<String, BigDecimal> values,
        String unit) {
}
