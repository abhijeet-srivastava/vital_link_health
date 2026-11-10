package com.vitallink.readings.ingest.backpressure;

import java.time.Duration;

/** The device used up its token bucket: 429 with Retry-After = time until the next token. */
public class RateLimitedException extends RuntimeException {

    private final Duration retryAfter;

    public RateLimitedException(Duration retryAfter) {
        super("Device request rate over its limit");
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
