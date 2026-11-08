package com.vitallink.readings.alerting.config;

import java.time.Clock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.alerting.cache.CachingThresholdProvider;
import com.vitallink.readings.alerting.cache.ThresholdsChangedListener;
import com.vitallink.readings.alerting.evaluator.AlertEvaluator;
import com.vitallink.readings.alerting.evaluator.AlertRecords;
import com.vitallink.readings.alerting.evaluator.BatchAlertEvaluator;
import com.vitallink.readings.alerting.evaluator.EvaluatorDeadLetters;
import com.vitallink.readings.alerting.evaluator.MeasureEvaluators;
import com.vitallink.readings.alerting.evaluator.ReadingsIngestedBatchListener;
import com.vitallink.readings.alerting.evaluator.ReadingsIngestedListener;
import com.vitallink.readings.domain.AlertDecisions;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.persistence.DbThresholdProvider;
import com.vitallink.readings.persistence.JdbcAlertBatchRepository;
import com.vitallink.readings.persistence.JdbcAlertEpisodeRepository;
import com.vitallink.readings.persistence.JdbcAuditWriter;
import com.vitallink.readings.persistence.JdbcOutboxWriter;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Alert evaluator ({@code evaluator} profile): readings.ingested -> alert episodes -> alerting outbox. Batched on the
 * main topic, per event on the fallback and retry paths.
 */
@Configuration
@Profile("evaluator")
@EnableConfigurationProperties(AlertingProperties.class)
class EvaluatorConfig {

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
    AlertRecords alertRecords(Clock clock, UuidV7 ids, ObjectMapper json) {
        return new AlertRecords(clock, ids, json);
    }

    @Bean
    AlertEvaluator alertEvaluator(CachingThresholdProvider thresholds, AlertDecisions decisions, JdbcClient jdbc,
                                  TransactionTemplate tx, AlertRecords records, MeterRegistry meters) {
        return new AlertEvaluator(thresholds, MeasureEvaluators.standard(), decisions,
                new JdbcAlertEpisodeRepository(jdbc), new JdbcOutboxWriter(jdbc), new JdbcAuditWriter(jdbc), tx,
                records, meters);
    }

    @Bean
    BatchAlertEvaluator batchAlertEvaluator(CachingThresholdProvider thresholds, AlertDecisions decisions,
                                            JdbcClient jdbc, TransactionTemplate tx, AlertRecords records,
                                            AlertEvaluator perEvent, MeterRegistry meters) {
        return new BatchAlertEvaluator(thresholds, MeasureEvaluators.standard(), decisions,
                new JdbcAlertBatchRepository(jdbc), new JdbcOutboxWriter(jdbc), new JdbcAuditWriter(jdbc), tx,
                records, perEvent, meters);
    }

    @Bean
    ReadingsIngestedBatchListener readingsIngestedBatchListener(BatchAlertEvaluator evaluator, ObjectMapper json,
                                                                KafkaTemplate<String, String> dlqTemplate,
                                                                EvaluatorDeadLetters deadLetters,
                                                                AlertingProperties props, MeterRegistry meters) {
        return new ReadingsIngestedBatchListener(evaluator, json, dlqTemplate, deadLetters, props.sendTimeout(),
                meters);
    }

    @Bean
    ReadingsIngestedListener readingsIngestedRetryListener(AlertEvaluator evaluator, ObjectMapper json,
                                                           MeterRegistry meters) {
        return new ReadingsIngestedListener(evaluator, json, meters);
    }

    @Bean
    ThresholdsChangedListener thresholdsChangedListener(CachingThresholdProvider thresholds) {
        return new ThresholdsChangedListener(thresholds);
    }
}
