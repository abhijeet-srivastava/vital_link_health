package com.vitallink.readings.messaging;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.UUID;

/**
 * RFC 9562 UUIDv7: 48-bit Unix millis, then random bits. Time-ordered, so index inserts stay local; ids from the
 * same millisecond are not ordered among themselves, and nothing may rely on commit order (pattern 7).
 */
public final class UuidV7 {

    private final Clock clock;
    private final SecureRandom random = new SecureRandom();

    public UuidV7(Clock clock) {
        this.clock = clock;
    }

    public UUID next() {
        long millis = clock.millis();
        long msb = (millis << 16) | 0x7000L | (random.nextInt() & 0x0FFFL);
        long lsb = (random.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb);
    }
}
