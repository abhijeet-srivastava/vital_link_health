package com.vitallink.readings.domain;

import java.time.Instant;
import java.util.UUID;

/** Keyset position in a reading range: the last row of the previous page, in (measured_at DESC, reading_uuid) order. */
public record Cursor(Instant measuredAt, UUID readingUuid) {

    public static Cursor after(Reading last) {
        return new Cursor(last.measuredAt(), last.readingUuid());
    }
}
