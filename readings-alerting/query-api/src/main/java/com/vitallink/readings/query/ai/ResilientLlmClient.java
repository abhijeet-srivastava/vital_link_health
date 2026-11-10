package com.vitallink.readings.query.ai;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vitallink.readings.domain.Prompt;
import com.vitallink.readings.ports.LlmClient;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;

/**
 * Guards around any LlmClient (security-phi.md, AI features): circuit breaker, rate limiter, daily token budget, then
 * the call with a hard timeout (at most 2 s). Each guard that refuses fails the future at once with an
 * {@link LlmUnavailableException} and makes no call; timeouts and errors count against the circuit. On timeout the
 * provider's future is cancelled so the adapter can abort the request instead of leaving it open. Callers fall back
 * to rules on any failure. The budget and the rate limiter are per pod (as the notifier's token bucket, D31).
 */
public class ResilientLlmClient implements LlmClient {

    public static final String BREAKER = "llm";

    private static final Logger log = LoggerFactory.getLogger(ResilientLlmClient.class);

    public enum Reason {
        CIRCUIT_OPEN,
        RATE_LIMITED,
        BUDGET_EXCEEDED,
        TIMEOUT,
        ERROR
    }

    public static class LlmUnavailableException extends RuntimeException {

        private final Reason reason;

        LlmUnavailableException(Reason reason, Throwable cause) {
            super("LLM unavailable: " + reason, cause, false, false);
            this.reason = reason;
        }

        public Reason reason() {
            return reason;
        }
    }

    private final LlmClient delegate;
    private final CircuitBreaker breaker;
    private final RateLimiter limiter;
    private final DailyTokenBudget budget;
    private final Duration maxTimeout;

    public ResilientLlmClient(LlmClient delegate, CircuitBreaker breaker, RateLimiter limiter, long dailyTokenBudget,
                              Duration maxTimeout, Clock clock) {
        this.delegate = delegate;
        this.breaker = breaker;
        this.limiter = limiter;
        this.budget = new DailyTokenBudget(dailyTokenBudget, clock);
        this.maxTimeout = maxTimeout;
    }

    @Override
    public CompletableFuture<String> complete(Prompt prompt, Duration timeout) {
        Duration limit = timeout.compareTo(maxTimeout) < 0 ? timeout : maxTimeout;
        if (!breaker.tryAcquirePermission()) {
            return unavailable(Reason.CIRCUIT_OPEN, null);
        }
        if (!limiter.acquirePermission()) {
            breaker.releasePermission();
            return unavailable(Reason.RATE_LIMITED, null);
        }
        if (!budget.tryCharge(prompt.estimatedTokens())) {
            breaker.releasePermission();
            return unavailable(Reason.BUDGET_EXCEEDED, null);
        }
        long start = breaker.getCurrentTimestamp();
        CompletableFuture<String> call;
        try {
            call = delegate.complete(prompt, limit);
        } catch (RuntimeException e) {
            call = CompletableFuture.failedFuture(e);
        }
        CompletableFuture<String> provider = call;
        // The timeout runs on a copy so the provider's future is never completed by this class, only cancelled.
        return call.copy().orTimeout(limit.toMillis(), TimeUnit.MILLISECONDS).handle((text, error) -> {
            long elapsed = breaker.getCurrentTimestamp() - start;
            if (error == null) {
                breaker.onSuccess(elapsed, breaker.getTimestampUnit());
                budget.charge(text == null ? 0 : text.length() / 4 + 1);
                return text;
            }
            Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause()
                    : error;
            boolean timedOut = cause instanceof TimeoutException;
            if (timedOut) {
                provider.cancel(true);   // abort the request; runs before the caller's join() returns
            }
            breaker.onError(elapsed, breaker.getTimestampUnit(), cause);
            throw new LlmUnavailableException(timedOut ? Reason.TIMEOUT : Reason.ERROR, cause);
        });
    }

    private static CompletableFuture<String> unavailable(Reason reason, Throwable cause) {
        return CompletableFuture.failedFuture(new LlmUnavailableException(reason, cause));
    }

    /**
     * Tokens spent today (UTC) on this pod: prompt estimate charged before the call, the answer after. At the limit
     * every summary is rule-based until midnight UTC; cost per day is bounded by limit x pods x price per token.
     */
    static final class DailyTokenBudget {

        private final long limit;
        private final Clock clock;
        private LocalDate day;
        private long used;

        DailyTokenBudget(long limit, Clock clock) {
            this.limit = limit;
            this.clock = clock;
        }

        synchronized boolean tryCharge(long tokens) {
            roll();
            if (used + tokens > limit) {
                return false;
            }
            used += tokens;
            if (used * 10 >= limit * 9 && (used - tokens) * 10 < limit * 9) {
                log.warn("LLM daily token budget 90% used limit={}", limit);
            }
            return true;
        }

        synchronized void charge(long tokens) {
            roll();
            used += tokens;
        }

        private void roll() {
            LocalDate today = LocalDate.ofInstant(clock.instant(), ZoneOffset.UTC);
            if (!today.equals(day)) {
                day = today;
                used = 0;
            }
        }
    }
}
