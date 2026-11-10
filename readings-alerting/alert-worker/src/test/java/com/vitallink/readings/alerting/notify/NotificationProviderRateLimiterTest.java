package com.vitallink.readings.alerting.notify;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.alerting.state.MutableClock;

class NotificationProviderRateLimiterTest {

    final MutableClock clock = new MutableClock(Instant.parse("2026-11-08T06:00:00Z"));

    @Test
    void non_critical_cannot_take_the_reserved_share_but_critical_can() {
        ProviderRateLimiter limiter = new ProviderRateLimiter(clock, 10, 5, 0.2);

        assertThat(limiter.acquire(10, false)).isEqualTo(8);
        assertThat(limiter.acquire(10, false)).isZero();
        assertThat(limiter.acquire(10, true)).isEqualTo(2);
        assertThat(limiter.acquire(1, true)).isZero();
    }

    @Test
    void tokens_refill_with_time_up_to_capacity() {
        ProviderRateLimiter limiter = new ProviderRateLimiter(clock, 10, 5, 0.2);
        limiter.acquire(10, true);

        clock.advance(Duration.ofSeconds(1));
        assertThat(limiter.acquire(10, true)).isEqualTo(5);
        clock.advance(Duration.ofHours(1));
        assertThat(limiter.acquire(100, true)).isEqualTo(10);
    }

    @Test
    void released_tokens_return_to_the_bucket() {
        ProviderRateLimiter limiter = new ProviderRateLimiter(clock, 10, 0, 0.2);
        assertThat(limiter.acquire(8, false)).isEqualTo(8);

        limiter.release(5);
        assertThat(limiter.acquire(10, false)).isEqualTo(5);
    }
}
