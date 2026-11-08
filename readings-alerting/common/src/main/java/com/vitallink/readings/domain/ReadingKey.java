package com.vitallink.readings.domain;

import java.time.Instant;
import java.util.UUID;

/** Natural key of a reading within one device: reading PK minus device_id. measuredAt is microsecond precision. */
public record ReadingKey(UUID readingUuid, Instant measuredAt) {
}
