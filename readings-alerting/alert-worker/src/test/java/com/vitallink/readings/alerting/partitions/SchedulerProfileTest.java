package com.vitallink.readings.alerting.partitions;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.scheduling.config.ScheduledTaskHolder;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

@SpringBootTest
@ActiveProfiles("scheduler")
class SchedulerProfileTest extends SpringIntegrationTestBase {

    @Autowired
    ScheduledTaskHolder scheduledTasks;

    @Test
    void scheduler_profile_schedules_partition_maintenance() {
        assertThat(scheduledTasks.getScheduledTasks())
                .extracting(task -> task.getTask().getRunnable().toString())
                .anyMatch(name -> name.contains("PartitionMaintenanceJob.run"));
    }
}
