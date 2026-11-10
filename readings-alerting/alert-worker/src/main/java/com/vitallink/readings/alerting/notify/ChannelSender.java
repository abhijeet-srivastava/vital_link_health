package com.vitallink.readings.alerting.notify;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.domain.Channel;
import com.vitallink.readings.domain.NotificationRow;
import com.vitallink.readings.domain.PendingNotification;
import com.vitallink.readings.ports.DeliveryProvider;
import com.vitallink.readings.ports.NotificationRepository;
import com.vitallink.readings.ports.NotificationSender;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Guard 6 / pattern 6: claims due PENDING rows with SKIP LOCKED, calls the provider with notification_id as the
 * idempotency key, and writes the outcome under {@code status = 'PENDING'} in the same transaction, so two senders
 * never claim the same row and a crash before the commit only repeats a call the provider deduplicates.
 *
 * <ul>
 *   <li>CRITICAL rows are claimed first and may use the whole token bucket; other rows only the share above the
 *       CRITICAL reserve. Lane {@code CRITICAL_ONLY} (the critical notifier pods) claims CRITICAL rows only.</li>
 *   <li>Each call has a timeout and a circuit breaker per provider. A failure counts an attempt and backs off through
 *       next_attempt_at (durable: survives a restart); after max attempts the row is FAILED. An open circuit makes no
 *       call and counts no attempt: the row is pushed back by the breaker's wait.</li>
 *   <li>Fallback (D33): when a channel with a fallback (PUSH -> SMS) has its circuit open, a row for the fallback
 *       channel is recorded in the same transaction, under the same delivery key (trigger event, coach), so it is
 *       written once however often the circuit is found open, and not at all when the change already has one (an
 *       escalation). The original row stays PENDING and is still delivered once the circuit closes. Channels are
 *       drained in enum order, so PUSH runs before SMS and the fallback goes out in the same tick.</li>
 * </ul>
 * The transaction holds the claimed rows while the provider is called: batches stay small and calls are bounded by
 * the timeout.
 */
public class ChannelSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(ChannelSender.class);

    public enum Lane {
        ALL,
        CRITICAL_ONLY
    }

    /** One provider with its quota and fault handling. */
    public record Delivery(DeliveryProvider provider, ProviderRateLimiter limiter, CircuitBreaker breaker,
                           TimeLimiter timeLimiter, Duration openWait, Channel fallback) {

        public static Delivery of(DeliveryProvider provider, ProviderRateLimiter limiter, Duration timeout) {
            Duration openWait = Duration.ofSeconds(30);
            CircuitBreaker breaker = CircuitBreaker.of(provider.name(), CircuitBreakerConfig.custom()
                    .slidingWindowSize(20)
                    .minimumNumberOfCalls(10)
                    .failureRateThreshold(50)
                    .waitDurationInOpenState(openWait)
                    .build());
            TimeLimiter timeLimiter = TimeLimiter.of(provider.name(), TimeLimiterConfig.custom()
                    .timeoutDuration(timeout).cancelRunningFuture(true).build());
            return new Delivery(provider, limiter, breaker, timeLimiter, openWait, null);
        }

        /** Records a row for {@code channel} when this provider's circuit is open. */
        public Delivery withFallback(Channel channel) {
            return new Delivery(provider, limiter, breaker, timeLimiter, openWait, channel);
        }
    }

    private enum Outcome {
        SENT,
        RETRY,
        FAILED,
        NOT_CALLED
    }

    private final Lane lane;
    private final Map<Channel, Delivery> deliveries;
    private final NotificationRepository notifications;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final RetryPolicy retry;
    private final int batchSize;
    private final MeterRegistry meters;
    private final ExecutorService calls = Executors.newVirtualThreadPerTaskExecutor();

    public ChannelSender(Lane lane, Map<Channel, Delivery> deliveries, NotificationRepository notifications,
                         TransactionTemplate tx, Clock clock, RetryPolicy retry, int batchSize, MeterRegistry meters) {
        this.lane = lane;
        this.deliveries = Collections.unmodifiableMap(new EnumMap<>(deliveries));
        this.notifications = notifications;
        this.tx = tx;
        this.clock = clock;
        this.retry = retry;
        this.batchSize = batchSize;
        this.meters = meters;
    }

    /** Drains every channel: full batches are followed by another one. Returns the rows handled this tick. */
    @Scheduled(initialDelayString = "${vitallink.notifier.sender-poll-interval}",
            fixedDelayString = "${vitallink.notifier.sender-poll-interval}")
    public int run() {
        int total = 0;
        for (Channel channel : deliveries.keySet()) {
            int batch;
            do {
                batch = sendDue(channel, batchSize);
                total += batch;
            } while (batch == batchSize);
        }
        return total;
    }

    /** One transaction: CRITICAL rows first, then (lane ALL) the others with what the bucket allows them. */
    @Override
    public int sendDue(Channel channel, int limit) {
        Delivery delivery = deliveries.get(channel);
        if (delivery == null) {
            throw new IllegalArgumentException("No provider for channel " + channel);
        }
        Integer handled = tx.execute(status -> {
            int critical = claimAndSend(delivery, channel, true, limit);
            int other = lane == Lane.ALL ? claimAndSend(delivery, channel, false, limit - critical) : 0;
            return critical + other;
        });
        return handled == null ? 0 : handled;
    }

    private int claimAndSend(Delivery delivery, Channel channel, boolean critical, int limit) {
        int granted = limit <= 0 ? 0 : delivery.limiter().acquire(limit, critical);
        if (granted == 0) {
            return 0;
        }
        List<PendingNotification> claimed = notifications.claimDue(channel, critical, granted, clock.instant());
        delivery.limiter().release(granted - claimed.size());
        for (PendingNotification n : claimed) {
            Outcome outcome = send(delivery, n);
            counter(outcome, channel).increment();
        }
        return claimed.size();
    }

    private Outcome send(Delivery delivery, PendingNotification n) {
        try {
            delivery.breaker().executeCallable(() -> delivery.timeLimiter().executeFutureSupplier(
                    () -> CompletableFuture.runAsync(() -> delivery.provider().deliver(n), calls)));
            notifications.markSent(n.notificationId(), clock.instant());
            return Outcome.SENT;
        } catch (CallNotPermittedException open) {
            delivery.limiter().release(1);   // no call made: the token goes back
            if (delivery.fallback() != null) {
                int recorded = notifications.insertAll(List.of(new NotificationRow(n.triggerEventId(), n.alertId(),
                        n.coachId(), delivery.fallback(), n.reason(), n.severity())), clock.instant());
                if (recorded > 0) {
                    Counter.builder("vitallink.notifications.fallback")
                            .description("fallback rows recorded because the channel's circuit was open")
                            .tag("from", n.channel().name()).tag("to", delivery.fallback().name())
                            .register(meters).increment();
                }
            }
            notifications.reschedule(n.notificationId(), clock.instant().plus(delivery.openWait()));
            return Outcome.NOT_CALLED;
        } catch (Exception e) {
            int failed = n.attempts() + 1;
            if (retry.exhausted(failed)) {
                notifications.markFailed(n.notificationId());
                log.warn("Notification failed after max attempts notification_id={} alert_id={} channel={} "
                                + "attempts={} error={}", n.notificationId(), n.alertId(), n.channel(), failed,
                        e.getClass().getName());
                return Outcome.FAILED;
            }
            notifications.markRetry(n.notificationId(), retry.nextAttempt(clock.instant(), failed));
            return Outcome.RETRY;
        }
    }

    private Counter counter(Outcome outcome, Channel channel) {
        String name = switch (outcome) {
            case SENT -> "vitallink.notifications.sent";
            case RETRY -> "vitallink.notifications.retried";
            case FAILED -> "vitallink.notifications.failed";
            case NOT_CALLED -> "vitallink.notifications.circuit_open";
        };
        return Counter.builder(name).tag("channel", channel.name()).register(meters);
    }
}
