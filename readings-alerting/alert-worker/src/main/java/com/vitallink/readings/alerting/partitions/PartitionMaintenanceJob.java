package com.vitallink.readings.alerting.partitions;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Keeps pg_partman daily partitions 7 days ahead and applies 90-day retention for reading, alert_occurrence and
 * audit_log. The DDL runs inside {@code partman_maint.run()} (SECURITY DEFINER), so this job needs only EXECUTE.
 * Safe on several replicas: pg_partman serialises maintenance with an advisory lock.
 */
@Component
@Profile("scheduler")
public class PartitionMaintenanceJob {

    private static final Logger log = LoggerFactory.getLogger(PartitionMaintenanceJob.class);

    private final JdbcClient jdbc;

    public PartitionMaintenanceJob(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Returns the DEFAULT partitions holding rows (a missing day partition or a skewed device clock). */
    @Scheduled(initialDelayString = "PT0S", fixedDelayString = "${vitallink.partitions.maintenance-interval:PT1H}")
    public List<String> run() {
        List<String> nonEmptyDefaults = jdbc.sql("SELECT * FROM partman_maint.run()").query(String.class).list();
        if (nonEmptyDefaults.isEmpty()) {
            log.info("Partition maintenance done");
        } else {
            log.warn("Partition maintenance done; rows in DEFAULT partitions {}", nonEmptyDefaults);
        }
        return nonEmptyDefaults;
    }
}
