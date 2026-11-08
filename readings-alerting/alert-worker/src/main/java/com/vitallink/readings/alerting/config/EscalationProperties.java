package com.vitallink.readings.alerting.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code vitallink.escalation.*} ({@code scheduler} profile); defaults live in application.yml. The window itself is
 * {@code vitallink.alerting.escalation-window}: it is applied where escalate_at is set, and this job only reads it.
 *
 * @param pollInterval delay between escalation ticks; bounds how late past escalate_at an alert escalates
 * @param batchSize    alerts claimed per transaction
 */
@ConfigurationProperties("vitallink.escalation")
public record EscalationProperties(Duration pollInterval, int batchSize) {
}
