package com.vitallink.readings.alerting.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code vitallink.notifier.*} ({@code notifier} and {@code notifier-critical} profiles); defaults live in
 * application.yml.
 *
 * @param digestEnabled       open clinical decision: honour notification_policy DAILY_DIGEST for non-critical alerts
 * @param listenerConcurrency consumer threads per container per pod
 * @param maxRetries          redeliveries of a failing record before {@code <topic>.dlq}
 * @param retryInterval       delay between redeliveries
 * @param senderPollInterval  delay between sender ticks; bounds how late a due PUSH/SMS goes out
 * @param batchSize           rows claimed per sender transaction (held while the provider is called)
 * @param maxAttempts         provider calls before a row is FAILED
 * @param backoffBase         delay after the first failed call; doubles per failure
 * @param backoffCap          longest delay between calls
 * @param backoffJitter       random extra delay, as a fraction of the delay
 * @param providerTimeout     per call
 * @param push                push provider quota (per pod)
 * @param sms                 SMS provider quota (per pod)
 */
@ConfigurationProperties("vitallink.notifier")
public record NotifierProperties(boolean digestEnabled, int listenerConcurrency, int maxRetries,
                                 Duration retryInterval, Duration senderPollInterval, int batchSize, int maxAttempts,
                                 Duration backoffBase, Duration backoffCap, double backoffJitter,
                                 Duration providerTimeout, Quota push, Quota sms) {

    /**
     * @param capacity              bucket size (burst)
     * @param refillPerSecond       sustained sends per second
     * @param criticalReservedShare share of the bucket only CRITICAL may use
     */
    public record Quota(int capacity, double refillPerSecond, double criticalReservedShare) {
    }
}
