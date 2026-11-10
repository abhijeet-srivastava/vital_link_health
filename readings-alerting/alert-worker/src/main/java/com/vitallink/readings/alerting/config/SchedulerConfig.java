package com.vitallink.readings.alerting.config;

import java.time.Clock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.alerting.escalation.EscalationJob;
import com.vitallink.readings.alerts.DefaultEscalationScheduler;
import com.vitallink.readings.alerts.StateChangeRecords;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.persistence.JdbcAlertStateRepository;
import com.vitallink.readings.persistence.JdbcAuditWriter;
import com.vitallink.readings.persistence.JdbcOutboxWriter;

import io.micrometer.core.instrument.MeterRegistry;

/** Escalation scheduler ({@code scheduler} profile): OPEN CRITICAL past escalate_at -> ESCALATED, once. */
@Configuration
@Profile("scheduler")
@EnableConfigurationProperties(EscalationProperties.class)
class SchedulerConfig {

    @Bean
    EscalationJob escalationJob(JdbcClient jdbc, TransactionTemplate tx, Clock clock, UuidV7 ids, ObjectMapper json,
                                EscalationProperties props, MeterRegistry meters) {
        DefaultEscalationScheduler scheduler = new DefaultEscalationScheduler(new JdbcAlertStateRepository(jdbc),
                new JdbcOutboxWriter(jdbc), new JdbcAuditWriter(jdbc), tx, new StateChangeRecords(clock, ids, json));
        return new EscalationJob(scheduler, clock, props.batchSize(), meters);
    }
}
