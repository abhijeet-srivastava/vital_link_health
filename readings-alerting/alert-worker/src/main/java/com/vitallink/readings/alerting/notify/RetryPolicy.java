package com.vitallink.readings.alerting.notify;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Durable retry for a failed delivery: the row stays PENDING and is due again at {@code next_attempt_at}; after
 * {@code maxAttempts} failed calls it is FAILED. Delay after the n-th failure = min(base * 2^(n-1), cap), plus up to
 * {@code jitter} of that delay at random so a provider outage does not end in a synchronised retry burst.
 */
public record RetryPolicy(int maxAttempts, Duration base, Duration cap, double jitter) {

    public boolean exhausted(int failedAttempts) {
        return failedAttempts >= maxAttempts;
    }

    public Instant nextAttempt(Instant now, int failedAttempts) {
        long baseMs = base.toMillis();
        long delay = Math.min(cap.toMillis(), baseMs << Math.min(30, Math.max(0, failedAttempts - 1)));
        long extra = jitter > 0 ? (long) (ThreadLocalRandom.current().nextDouble() * jitter * delay) : 0;
        return now.plusMillis(delay + extra);
    }
}
