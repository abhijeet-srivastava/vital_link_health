package com.vitallink.readings.ingest.config;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.cache.RedisLatestReadingCache;
import com.vitallink.readings.domain.ReadingValidator;
import com.vitallink.readings.ingest.auth.CachingDeviceDirectory;
import com.vitallink.readings.ingest.auth.HmacDeviceAuthenticator;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.persistence.JdbcAuditWriter;
import com.vitallink.readings.persistence.JdbcDeviceDirectory;
import com.vitallink.readings.persistence.JdbcMeasureCatalog;
import com.vitallink.readings.persistence.JdbcOutboxWriter;
import com.vitallink.readings.persistence.JdbcReadingRepository;
import com.vitallink.readings.ports.AuditWriter;
import com.vitallink.readings.ports.DeviceAuthenticator;
import com.vitallink.readings.ports.DeviceDirectory;
import com.vitallink.readings.ports.LatestReadingCache;
import com.vitallink.readings.ports.MeasureCatalog;
import com.vitallink.readings.ports.OutboxWriter;
import com.vitallink.readings.ports.ReadingRepository;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/** Wires the common adapters this deployable uses; the adapters carry no stereotypes so other apps don't get them. */
@Configuration
@EnableConfigurationProperties(IngestProperties.class)
public class IngestConfig {

    /** Credential cache TTL: also the revocation latency until the Redis dev:{keyId} tier exists. */
    static final Duration CREDENTIAL_TTL = Duration.ofMinutes(5);
    static final long CREDENTIAL_CACHE_SIZE = 100_000;

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    UuidV7 uuidV7(Clock clock) {
        return new UuidV7(clock);
    }

    @Bean
    ReadingRepository readingRepository(JdbcClient jdbc) {
        return new JdbcReadingRepository(jdbc);
    }

    @Bean
    OutboxWriter outboxWriter(JdbcClient jdbc) {
        return new JdbcOutboxWriter(jdbc);
    }

    @Bean
    AuditWriter auditWriter(JdbcClient jdbc) {
        return new JdbcAuditWriter(jdbc);
    }

    @Bean
    MeasureCatalog measureCatalog(JdbcClient jdbc) {
        return new JdbcMeasureCatalog(jdbc);
    }

    /** Runs credential cache loads off the request thread, outside the cache's map lock (see CachingDeviceDirectory). */
    @Bean(destroyMethod = "close")
    ExecutorService credentialLoader() {
        return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("credential-loader-", 0).factory());
    }

    @Bean
    DeviceDirectory deviceDirectory(JdbcClient jdbc, ExecutorService credentialLoader) {
        return new CachingDeviceDirectory(new JdbcDeviceDirectory(jdbc), CREDENTIAL_TTL, CREDENTIAL_CACHE_SIZE,
                credentialLoader);
    }

    @Bean
    DeviceAuthenticator deviceAuthenticator(DeviceDirectory directory, Clock clock,
                                            @Value("${vitallink.device-keys.pepper}") String pepper) {
        if (pepper.isBlank()) {
            throw new IllegalStateException("vitallink.device-keys.pepper (VITALLINK_DEVICE_KEY_PEPPER) is not set");
        }
        return new HmacDeviceAuthenticator(directory, pepper.getBytes(StandardCharsets.UTF_8), clock);
    }

    @Bean
    ReadingValidator readingValidator(IngestProperties props) {
        return new ReadingValidator(props.maxFutureSkew(), props.rejectOlderThan());
    }

    @Bean
    LatestReadingCache latestReadingCache(StringRedisTemplate redis, ObjectMapper json,
                                          CircuitBreakerRegistry breakers, IngestProperties props) {
        return new RedisLatestReadingCache(redis, json, breakers, props.latestTtl());
    }

    /** Breakers for best-effort calls after commit (latest-reading cache). */
    @Bean
    CircuitBreakerRegistry circuitBreakerRegistry() {
        return CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .slidingWindowSize(20)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .build());
    }
}
