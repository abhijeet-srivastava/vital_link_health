package com.vitallink.readings.alerting.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code vitallink.relay.*}; defaults live in application.yml.
 *
 * @param pollInterval pause after a batch smaller than 500 (a full batch is followed at once by the next)
 * @param ackTimeout   longest wait for broker acks before the batch is rolled back
 * @param maxBackoff   cap of the exponential backoff after failed batches (e.g. Kafka down)
 */
@ConfigurationProperties("vitallink.relay")
public record RelayProperties(Duration pollInterval, Duration ackTimeout, Duration maxBackoff) {
}
