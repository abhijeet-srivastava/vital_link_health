package com.vitallink.readings.alerting.notify;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Token bucket for one provider, in-process per pod (configure the rate per pod). A share of the capacity is reserved
 * for CRITICAL: non-critical sends may only take tokens above the reserve, CRITICAL may take every token. PENDING rows
 * that get no token simply stay PENDING and drain at the provider rate.
 */
public class ProviderRateLimiter {

    private final Clock clock;
    private final double capacity;
    private final double refillPerSecond;
    private final double reserved;
    private double tokens;
    private Instant lastRefill;

    public ProviderRateLimiter(Clock clock, int capacity, double refillPerSecond, double criticalReservedShare) {
        if (criticalReservedShare < 0 || criticalReservedShare > 1) {
            throw new IllegalArgumentException("critical reserved share must be within [0, 1]");
        }
        this.clock = clock;
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.reserved = capacity * criticalReservedShare;
        this.tokens = capacity;
        this.lastRefill = clock.instant();
    }

    /** Takes up to {@code wanted} tokens and returns how many were granted (possibly zero). */
    public synchronized int acquire(int wanted, boolean critical) {
        refill();
        double floor = critical ? 0 : reserved;
        int granted = (int) Math.min(wanted, Math.max(0, Math.floor(tokens - floor + 1e-9)));
        tokens -= granted;
        return granted;
    }

    /** Returns tokens taken but not used (fewer rows were due than granted). */
    public synchronized void release(int unused) {
        tokens = Math.min(capacity, tokens + unused);
    }

    private void refill() {
        Instant now = clock.instant();
        if (now.isAfter(lastRefill)) {
            double seconds = Duration.between(lastRefill, now).toNanos() / 1e9;
            tokens = Math.min(capacity, tokens + seconds * refillPerSecond);
            lastRefill = now;
        }
    }
}
