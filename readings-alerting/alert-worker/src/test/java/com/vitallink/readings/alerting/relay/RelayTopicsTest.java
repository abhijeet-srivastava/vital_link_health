package com.vitallink.readings.alerting.relay;

import static com.vitallink.readings.alerting.relay.RelayTestSupport.endOffsets;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.header;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.insertOutbox;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.outboxCount;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.readFrom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.messaging.EventHeaders;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

import io.micrometer.core.instrument.MeterRegistry;

/** Topics are created by alert-worker on startup; the relay profile wires and runs the relay. */
@SpringBootTest
@ActiveProfiles("relay")
class RelayTopicsTest extends SpringIntegrationTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    MeterRegistry meters;

    @Test
    void topics_have_the_agreed_partition_counts() throws Exception {
        Map<String, Integer> expected = Map.of(
                "readings.ingested", 128,
                "alerts.raised", 32,
                "alerts.raised.critical", 16,
                "alert.state_changed", 32,
                "thresholds.changed", 8,
                "device.binding_changed", 8);

        Map<String, Integer> actual;
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap()))) {
            List<String> names = expected.keySet().stream().flatMap(t -> List.of(t, t + ".dlq").stream()).toList();
            actual = admin.describeTopics(names).allTopicNames().get().values().stream()
                    .collect(Collectors.toMap(TopicDescription::name, d -> d.partitions().size()));
        }

        expected.forEach((topic, partitions) -> {
            assertThat(actual).containsEntry(topic, partitions);
            assertThat(actual).containsEntry(topic + ".dlq", partitions);
        });
    }

    @Test
    void relay_profile_starts_relay_and_publishes_an_outbox_row() {
        Map<TopicPartition, Long> start = endOffsets(kafkaBootstrap(), KafkaTopics.READINGS_INGESTED);
        UUID eventId = insertOutbox(jdbc, KafkaTopics.READINGS_INGESTED, "2", Instant.now());

        await().atMost(Duration.ofSeconds(30)).until(() -> outboxCount(jdbc) == 0);

        List<ConsumerRecord<String, String>> records = readFrom(kafkaBootstrap(), start);
        assertThat(records).extracting(ConsumerRecord::key).containsExactly("2");
        assertThat(header(records.getFirst(), EventHeaders.EVENT_ID)).isEqualTo(eventId.toString());
        assertThat(meters.find("vitallink.outbox.backlog").gauge()).isNotNull();
        assertThat(meters.find("vitallink.outbox.oldest.age").gauge()).isNotNull();
    }
}
