package com.vitallink.readings.cache;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.ports.LatestReadingCache;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * latest:{patientId}:{TYPE} as a hash {m: measured_at epoch micros, v: reading JSON}. ingest-api updates it after
 * commit and query-api backfills it after a DB fallback; both go through the same compare-and-set, so neither can
 * replace a newer value with an older one. Best effort (rule 4): failures are counted by a circuit breaker and
 * swallowed; readers fall back to the DB. Writers and readers must share the ObjectMapper naming (snake_case).
 */
public class RedisLatestReadingCache implements LatestReadingCache {

    private static final Logger log = LoggerFactory.getLogger(RedisLatestReadingCache.class);

    private static final RedisScript<Long> COMPARE_AND_SET =
            RedisScript.of(new ClassPathResource("lua/latest_cas.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final CircuitBreaker breaker;
    private final String ttlSeconds;

    public RedisLatestReadingCache(StringRedisTemplate redis, ObjectMapper json, CircuitBreakerRegistry breakers,
                                   Duration ttl) {
        this.redis = redis;
        this.json = json;
        this.breaker = breakers.circuitBreaker("latest-reading-cache");
        this.ttlSeconds = String.valueOf(ttl.toSeconds());
    }

    @Override
    public void putIfNewer(long patientId, ReadingType type, Reading reading) {
        try {
            String value = json.writeValueAsString(reading);
            breaker.executeRunnable(() -> redis.execute(COMPARE_AND_SET, List.of(CacheKeys.latest(patientId, type)),
                    String.valueOf(micros(reading.measuredAt())), value, ttlSeconds));
        } catch (JsonProcessingException | RuntimeException e) {
            // Class name only: driver messages can echo command arguments, which hold reading values.
            log.warn("Latest-reading cache update skipped patient_id={} reading_type={} error={}",
                    patientId, type, e.getClass().getSimpleName());
        }
    }

    @Override
    public Optional<Reading> get(long patientId, ReadingType type) {
        try {
            Object v = breaker.executeSupplier(() -> redis.opsForHash().get(CacheKeys.latest(patientId, type), "v"));
            return v == null ? Optional.empty() : Optional.of(json.readValue(v.toString(), Reading.class));
        } catch (JsonProcessingException | RuntimeException e) {
            log.warn("Latest-reading cache read skipped patient_id={} reading_type={} error={}",
                    patientId, type, e.getClass().getSimpleName());
            return Optional.empty();
        }
    }

    private static long micros(Instant t) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, t);
    }
}
