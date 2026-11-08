package com.vitallink.readings.alerting.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.vitallink.readings.domain.MultiProgramPolicy;

/**
 * {@code vitallink.alerting.*}; defaults live in application.yml.
 *
 * @param reopenOnCritical      open clinical decision: an ACKNOWLEDGED alert rising to CRITICAL returns to OPEN
 * @param multiProgramPolicy    open clinical decision: several programs without an override
 * @param escalationWindow      OPEN + CRITICAL to escalation (global until per-program windows exist)
 * @param lateAfter             older breaches still alert, flagged is_late
 * @param thresholdCacheTtl     L1 and Redis TTL for effective thresholds (jittered +10%)
 * @param thresholdCacheSize    L1 patients held per pod
 * @param maxRetries            attempts on readings.ingested.retry before readings.ingested.dlq
 * @param retryInterval         delay before each retry attempt (fixed, so one retry topic)
 * @param listenerConcurrency   consumer threads per pod
 * @param batchMaxRecords       readings.ingested events per batch transaction (max.poll.records)
 * @param batchMaxWait          longest the broker holds a fetch to fill a batch (fetch.max.wait.ms)
 * @param batchMinBytes         fetch size that ends the wait early (fetch.min.bytes)
 * @param sendTimeout           broker ack wait when moving a failed event to the retry topic
 */
@ConfigurationProperties("vitallink.alerting")
public record AlertingProperties(boolean reopenOnCritical, MultiProgramPolicy multiProgramPolicy,
                                 Duration escalationWindow, Duration lateAfter, Duration thresholdCacheTtl,
                                 long thresholdCacheSize, int maxRetries, Duration retryInterval,
                                 int listenerConcurrency, int batchMaxRecords, Duration batchMaxWait,
                                 int batchMinBytes, Duration sendTimeout) {
}
