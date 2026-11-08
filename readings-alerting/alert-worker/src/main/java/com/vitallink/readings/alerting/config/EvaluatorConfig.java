package com.vitallink.readings.alerting.config;

import java.time.Clock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.alerting.cache.CachingThresholdProvider;
import com.vitallink.readings.alerting.evaluator.AlertEvaluator;
import com.vitallink.readings.alerting.evaluator.MeasureEvaluators;
import com.vitallink.readings.alerting.evaluator.ReadingsIngestedListener;
import com.vitallink.readings.domain.AlertDecisions;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.persistence.DbThresholdProvider;
import com.vitallink.readings.persistence.JdbcAlertEpisodeRepository;
import com.vitallink.readings.persistence.JdbcAuditWriter;
import com.vitallink.readings.persistence.JdbcOutboxWriter;

import io.micrometer.core.instrument.MeterRegistry;

/** Per-event alert evaluator ({@code evaluator} profile): readings.ingested -> alert episodes -> alerting outbox. */
@Configuration
@Profile("evaluator")
@EnableConfigurationProperties(AlertingProperties.class)
class EvaluatorConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    UuidV7 uuidV7(Clock clock) {
        return new UuidV7(clock);
    }

    @Bean
    AlertDecisions alertDecisions(Clock clock, AlertingProperties props) {
        return new AlertDecisions(clock, props.reopenOnCritical(), props.escalationWindow(), props.lateAfter());
    }

    @Bean
    CachingThresholdProvider thresholdProvider(JdbcClient jdbc, Clock clock, StringRedisTemplate redis,
                                               AlertingProperties props) {
        return new CachingThresholdProvider(new DbThresholdProvider(jdbc, clock, props.multiProgramPolicy()), redis,
                props.thresholdCacheTtl(), props.thresholdCacheSize());
    }

    @Bean
    AlertEvaluator alertEvaluator(CachingThresholdProvider thresholds, AlertDecisions decisions, JdbcClient jdbc,
                                  TransactionTemplate tx, Clock clock, UuidV7 ids, ObjectMapper json,
                                  MeterRegistry meters) {
        return new AlertEvaluator(thresholds, MeasureEvaluators.standard(), decisions,
                new JdbcAlertEpisodeRepository(jdbc), new JdbcOutboxWriter(jdbc), new JdbcAuditWriter(jdbc), tx, clock,
                ids, json, meters);
    }

    @Bean
    ReadingsIngestedListener readingsIngestedListener(AlertEvaluator evaluator, ObjectMapper json,
                                                      MeterRegistry meters) {
        return new ReadingsIngestedListener(evaluator, json, meters);
    }
}
