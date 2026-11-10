package com.vitallink.readings.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * One stored measurement (a row of {@code reading}). Values are canonical: measuredAt truncated to microseconds,
 * valueNum at scale 2, so what is stored equals what was hashed.
 */
public record Reading(
        long deviceId,
        UUID readingUuid,
        Instant measuredAt,
        long patientId,
        ReadingType type,
        Long deviceSeq,
        Integer systolic,
        Integer diastolic,
        Integer pulse,
        BigDecimal valueNum,
        long payloadHash) {

    public ReadingKey key() {
        return new ReadingKey(readingUuid, measuredAt);
    }
}
