package com.vitallink.readings.query.config;

import java.time.Clock;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.alerts.DefaultAlertService;
import com.vitallink.readings.alerts.ReassignmentService;
import com.vitallink.readings.alerts.StateChangeRecords;
import com.vitallink.readings.cache.RedisLatestReadingCache;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.persistence.JdbcAlertQueueRepository;
import com.vitallink.readings.persistence.JdbcAlertStateRepository;
import com.vitallink.readings.persistence.JdbcAuditWriter;
import com.vitallink.readings.persistence.JdbcCareAssignmentRepository;
import com.vitallink.readings.persistence.JdbcMeasureCatalog;
import com.vitallink.readings.persistence.JdbcOutboxWriter;
import com.vitallink.readings.persistence.JdbcReadingQueryRepository;
import com.vitallink.readings.persistence.JdbcThresholdRepository;
import com.vitallink.readings.ports.AlertQueueRepository;
import com.vitallink.readings.ports.AlertService;
import com.vitallink.readings.ports.AuditWriter;
import com.vitallink.readings.ports.LatestReadingCache;
import com.vitallink.readings.ports.MeasureCatalog;
import com.vitallink.readings.ports.OutboxWriter;
import com.vitallink.readings.ports.ReadingQueryRepository;
import com.vitallink.readings.ports.ThresholdRepository;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Wires the common adapters onto the right pool: reads on the replica, coach writes and audit on the primary (both
 * as vitallink_query, under row-level security), panel changes on the admin pool (vitallink_service).
 */
@Configuration
@EnableConfigurationProperties(QueryProperties.class)
class QueryConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    UuidV7 uuidV7(Clock clock) {
        return new UuidV7(clock);
    }

    @Bean
    ReadingQueryRepository readingQueryRepository(@Qualifier("replicaJdbcClient") JdbcClient replica) {
        return new JdbcReadingQueryRepository(replica);
    }

    @Bean
    AlertQueueRepository alertQueueRepository(@Qualifier("replicaJdbcClient") JdbcClient replica) {
        return new JdbcAlertQueueRepository(replica);
    }

    @Bean
    ThresholdRepository thresholdRepository(JdbcClient jdbc) {
        return new JdbcThresholdRepository(jdbc);
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

    @Bean
    StateChangeRecords stateChangeRecords(Clock clock, UuidV7 ids, ObjectMapper json) {
        return new StateChangeRecords(clock, ids, json);
    }

    @Bean
    AlertService alertService(JdbcClient jdbc, OutboxWriter outbox, AuditWriter audit, TransactionTemplate tx,
                              StateChangeRecords records, Clock clock) {
        return new DefaultAlertService(new JdbcAlertStateRepository(jdbc), outbox, audit, tx, records, clock);
    }

    @Bean
    ReassignmentService reassignmentService(@Qualifier("adminJdbcClient") JdbcClient admin,
                                            @Qualifier("adminTransactionTemplate") TransactionTemplate adminTx,
                                            StateChangeRecords records, Clock clock) {
        return new ReassignmentService(new JdbcCareAssignmentRepository(admin), new JdbcAlertStateRepository(admin),
                new JdbcOutboxWriter(admin), new JdbcAuditWriter(admin), adminTx, records, clock);
    }

    @Bean
    LatestReadingCache latestReadingCache(StringRedisTemplate redis, ObjectMapper json,
                                          CircuitBreakerRegistry breakers, QueryProperties props) {
        return new RedisLatestReadingCache(redis, json, breakers, props.latestTtl());
    }

    /** Breaker for the latest-reading cache: an open circuit goes straight to the DB. */
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
