package com.vitallink.readings.alerting.partitions;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Scheduled jobs run only on the {@code scheduler} profile (escalation joins in stage 6). */
@Configuration
@Profile("scheduler")
@EnableScheduling
class SchedulingConfig {
}
