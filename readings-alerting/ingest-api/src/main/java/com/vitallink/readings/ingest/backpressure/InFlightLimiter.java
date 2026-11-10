package com.vitallink.readings.ingest.backpressure;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.web.servlet.HandlerInterceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Sheds ingest with 503 before the body is read or the DB is touched: when the outbox backlog is over its limit, or
 * when {@code maxInFlight} requests (each ending in the ingest transaction) are already running on this pod. The
 * permit is held for the whole request and released in afterCompletion, which runs only if preHandle returned true.
 */
public class InFlightLimiter implements HandlerInterceptor {

    private final Semaphore permits;
    private final OutboxBacklogMonitor backlog;
    private final long retryAfterMinSeconds;
    private final long retryAfterMaxSeconds;

    public InFlightLimiter(OutboxBacklogMonitor backlog, BackpressureProperties props) {
        this.permits = new Semaphore(props.maxInFlight());
        this.backlog = backlog;
        this.retryAfterMinSeconds = props.retryAfterMin().toSeconds();
        this.retryAfterMaxSeconds = Math.max(retryAfterMinSeconds, props.retryAfterMax().toSeconds());
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (backlog.overloaded()) {
            throw new OverloadedException(OverloadedException.Reason.OUTBOX_BACKLOG, jitteredRetryAfter());
        }
        if (!permits.tryAcquire()) {
            throw new OverloadedException(OverloadedException.Reason.IN_FLIGHT, jitteredRetryAfter());
        }
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler,
                                Exception ex) {
        permits.release();
    }

    private Duration jitteredRetryAfter() {
        return Duration.ofSeconds(ThreadLocalRandom.current().nextLong(retryAfterMinSeconds, retryAfterMaxSeconds + 1));
    }
}
