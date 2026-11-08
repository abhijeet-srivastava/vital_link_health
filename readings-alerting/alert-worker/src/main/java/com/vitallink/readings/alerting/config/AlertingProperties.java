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
 */
@ConfigurationProperties("vitallink.alerting")
public record AlertingProperties(boolean reopenOnCritical, MultiProgramPolicy multiProgramPolicy,
                                 Duration escalationWindow, Duration lateAfter, Duration thresholdCacheTtl,
                                 long thresholdCacheSize, int maxRetries, Duration retryInterval,
                                 int listenerConcurrency) {
}
