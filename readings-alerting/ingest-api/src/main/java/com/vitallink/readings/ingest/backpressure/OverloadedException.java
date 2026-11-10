package com.vitallink.readings.ingest.backpressure;

import java.time.Duration;

/** Ingest is shedding load: 503 with a jittered Retry-After. Devices buffer and retry, so shedding is safe. */
public class OverloadedException extends RuntimeException {

    public enum Reason { IN_FLIGHT, OUTBOX_BACKLOG }

    private final Reason reason;
    private final Duration retryAfter;

    public OverloadedException(Reason reason, Duration retryAfter) {
        super(reason == Reason.IN_FLIGHT ? "Too many ingest requests in progress" : "Event backlog is over its limit");
        this.reason = reason;
        this.retryAfter = retryAfter;
    }

    public Reason reason() {
        return reason;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
