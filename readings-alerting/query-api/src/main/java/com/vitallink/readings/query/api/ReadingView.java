package com.vitallink.readings.query.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;

/** A reading as a coach sees it; the payload hash is an integrity detail and stays internal. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReadingView(UUID readingId, long deviceId, ReadingType type, Instant measuredAt, Long deviceSeq,
                          Integer systolic, Integer diastolic, Integer pulse, BigDecimal value) {

    public static ReadingView of(Reading r) {
        return new ReadingView(r.readingUuid(), r.deviceId(), r.type(), r.measuredAt(), r.deviceSeq(), r.systolic(),
                r.diastolic(), r.pulse(), r.valueNum());
    }
}
