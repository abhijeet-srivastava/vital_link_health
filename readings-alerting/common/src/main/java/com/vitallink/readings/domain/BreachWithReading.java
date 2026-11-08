package com.vitallink.readings.domain;

import java.time.Instant;
import java.util.UUID;

/** A breach and the reading it came from; (deviceId, readingUuid, measure, measuredAt) is the alert_occurrence PK. */
public record BreachWithReading(long patientId, long deviceId, UUID readingUuid, Instant measuredAt, Breach breach) {

    public Measure measure() {
        return breach.measure();
    }
}
