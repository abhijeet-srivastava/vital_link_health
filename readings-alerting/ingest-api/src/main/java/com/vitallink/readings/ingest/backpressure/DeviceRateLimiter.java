package com.vitallink.readings.ingest.backpressure;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.web.servlet.HandlerInterceptor;

import com.vitallink.readings.domain.DeviceIdentity;
import com.vitallink.readings.ingest.auth.DeviceAuthInterceptor;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Per-device token bucket in Redis ({@code rate:{deviceId}}), checked after authentication and before the body is
 * read: over the bucket is 429 with Retry-After. Redis is never required for correctness, so a Redis error or an open
 * breaker lets the request through (fail open); the in-flight limit still protects the database.
 */
public class DeviceRateLimiter implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(DeviceRateLimiter.class);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> TAKE_TOKEN =
            RedisScript.of(new ClassPathResource("lua/device_token_bucket.lua"), List.class);

    private final StringRedisTemplate redis;
    private final CircuitBreaker breaker;
    private final String burst;
    private final String perMinute;

    public DeviceRateLimiter(StringRedisTemplate redis, CircuitBreakerRegistry breakers,
                             BackpressureProperties props) {
        this.redis = redis;
        this.breaker = breakers.circuitBreaker("device-rate-limiter");
        this.burst = String.valueOf(props.deviceBurst());
        this.perMinute = String.valueOf(props.deviceRequestsPerMinute());
    }

    static String key(long deviceId) {
        return "rate:" + deviceId;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!(request.getAttribute(DeviceAuthInterceptor.DEVICE_ATTRIBUTE) instanceof DeviceIdentity device)) {
            return true;
        }
        List<?> verdict;
        try {
            verdict = breaker.executeSupplier(() -> redis.execute(TAKE_TOKEN, List.of(key(device.deviceId())),
                    burst, perMinute));
        } catch (CallNotPermittedException e) {
            return true;   // breaker open: Redis known to be down, skip without waiting on a timeout
        } catch (RuntimeException e) {
            log.warn("Device rate limit skipped device_id={} error={}", device.deviceId(), e.getClass().getSimpleName());
            return true;
        }
        if (verdict == null || ((Number) verdict.get(0)).longValue() == 1) {
            return true;
        }
        long waitMillis = ((Number) verdict.get(1)).longValue();
        throw new RateLimitedException(Duration.ofSeconds(Math.max(1, (waitMillis + 999) / 1000)));
    }
}
