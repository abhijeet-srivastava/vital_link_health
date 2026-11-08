package com.vitallink.readings.alerting.evaluator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.messaging.KafkaTopics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Default batch settings (500 records, 200 ms, fetch.min.bytes far above one event): a lone CRITICAL on an idle
 * partition must not wait for the batch to fill. The broker answers after fetch.max.wait.ms with what it has.
 */
@SpringBootTest
@ActiveProfiles("evaluator")
class QuietPartitionEvaluatorTest extends EvaluatorTestSupport {

    /** Critical alert visible < 5 s p95 after ingest (performance-and-scaling.md SLO). */
    static final Duration CRITICAL_SLO = Duration.ofSeconds(5);

    @Autowired
    MeterRegistry meters;

    @Test
    void quiet_partition_still_commits_single_critical_breach_within_slo() throws Exception {
        givenPatient(8901, 41, 1);
        givenSystolicBands(1);
        publish(event(8901, bp(8901, 120)));   // warm-up: the group has joined and the partitions are idle
        awaitConsumed();
        DistributionSummary sizes = meters.find("vitallink.evaluator.batch.size").summary();
        long batchesBefore = sizes.count();
        double eventsBefore = sizes.totalAmount();

        publish(event(8901, bp(8901, 186)));

        await().atMost(CRITICAL_SLO).until(() -> alerts(8901).size() == 1);
        assertThat(alerts(8901)).singleElement().satisfies(a -> {
            assertThat(a.severity()).isEqualTo("CRITICAL");
            assertThat(a.escalateAt()).isNotNull();
        });
        assertThat(alertEvents()).singleElement().extracting(OutboxRow::topic)
                .isEqualTo(KafkaTopics.ALERTS_RAISED_CRITICAL);
        assertThat(sizes.count() - batchesBefore).as("committed as its own batch").isEqualTo(1);
        assertThat(sizes.totalAmount() - eventsBefore).as("a batch of one event").isEqualTo(1);
        assertThat(meters.find("vitallink.evaluator.batch.commit").timer().count()).isPositive();
    }
}
