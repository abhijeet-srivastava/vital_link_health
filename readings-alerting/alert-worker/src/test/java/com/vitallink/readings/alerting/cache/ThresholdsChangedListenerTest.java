package com.vitallink.readings.alerting.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.ThresholdBand;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

@SpringBootTest
@ActiveProfiles("evaluator")
class ThresholdsChangedListenerTest extends SpringIntegrationTestBase {

    @Autowired
    CachingThresholdProvider thresholds;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Test
    void thresholds_changed_evicts_cached_bands_for_that_patient() throws Exception {
        jdbc.sql("INSERT INTO enrollment (patient_id, program_id, valid_from) VALUES (7701, 1, now() - interval '1 day')")
                .update();
        jdbc.sql("""
                        INSERT INTO threshold_rule (program_id, measure_code, operator, limit_value, severity, created_by)
                        VALUES (1, ?, 'GT', 160, 'HIGH', 'test')
                        """)
                .param(Measure.SYSTOLIC.code()).update();
        assertThat(limits(7701)).containsExactly(160);   // cached in L1 and Redis now
        jdbc.sql("""
                        INSERT INTO threshold_rule (patient_id, measure_code, operator, limit_value, severity, created_by)
                        VALUES (7701, ?, 'GT', 150, 'HIGH', 'test')
                        """)
                .param(Measure.SYSTOLIC.code()).update();
        assertThat(limits(7701)).as("still served from cache").containsExactly(160);
        // Latest-offset consumer: publish only once it holds its partitions.
        await().atMost(Duration.ofSeconds(30)).until(() -> !listeners.getListenerContainer(ThresholdsChangedListener.ID)
                .getAssignedPartitions().isEmpty());

        kafka.send(new ProducerRecord<>(KafkaTopics.THRESHOLDS_CHANGED, "7701", "{\"patient_id\":7701}")).get();

        await().atMost(Duration.ofSeconds(30)).until(() -> limits(7701).equals(List.of(150)));
    }

    List<Integer> limits(long patientId) {
        return thresholds.forPatients(List.of(patientId)).bandsFor(patientId, Measure.SYSTOLIC).stream()
                .map(b -> b.limit().intValue()).toList();
    }
}
