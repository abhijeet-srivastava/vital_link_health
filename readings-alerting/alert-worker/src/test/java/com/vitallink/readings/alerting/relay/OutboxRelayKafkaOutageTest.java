package com.vitallink.readings.alerting.relay;

import static com.vitallink.readings.alerting.relay.RelayTestSupport.asServiceRole;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.endOffsets;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.header;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.insertOutbox;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.outboxCount;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.producer;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.readFrom;
import static com.vitallink.readings.alerting.relay.RelayTestSupport.relay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.messaging.EventHeaders;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.messaging.OutboxRelay;
import com.vitallink.readings.testsupport.IntegrationTestBase;

/** Kafka outage (architecture.md, partial outages): ingest keeps writing, the outbox grows, then drains in order. */
class OutboxRelayKafkaOutageTest extends IntegrationTestBase {

    final JdbcClient owner = JdbcClient.create(dataSource());

    @BeforeAll
    static void topics() throws Exception {
        RelayTestSupport.createTopics(kafkaBootstrap());
    }

    @Test
    void kafka_down_outbox_grows_and_drains_in_order_when_it_returns() throws Exception {
        Map<TopicPartition, Long> start = endOffsets(kafkaBootstrap(), KafkaTopics.READINGS_INGESTED);
        List<String> keys = List.of("1", "2", "3");
        Map<String, List<String>> expectedPerKey = new LinkedHashMap<>();
        keys.forEach(k -> expectedPerKey.put(k, new ArrayList<>()));
        Instant t0 = Instant.now();
        int seq = 0;

        // Short client timeouts so a tick against a dead broker fails fast instead of hanging the test.
        Map<String, Object> fastFail = Map.of(
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_500,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_000);
        try (Producer<String, String> producer = producer(kafkaBootstrap(), fastFail)) {
            OutboxRelay relay = relay(asServiceRole(dataSource()), producer, Duration.ofSeconds(5));

            for (; seq < 30; seq++) {
                String key = keys.get(seq % keys.size());
                expectedPerKey.get(key).add(insertOutbox(owner, KafkaTopics.READINGS_INGESTED, key,
                        t0.plusMillis(seq)).toString());
            }
            pauseKafka();
            try {
                assertThatThrownBy(relay::publishBatch).isInstanceOf(RuntimeException.class);
                assertThat(outboxCount(owner)).isEqualTo(30);

                // Ingest keeps committing while Kafka is down: the outbox grows.
                for (; seq < 60; seq++) {
                    String key = keys.get(seq % keys.size());
                    expectedPerKey.get(key).add(insertOutbox(owner, KafkaTopics.READINGS_INGESTED, key,
                            t0.plusMillis(seq)).toString());
                }
                assertThat(outboxCount(owner)).isEqualTo(60);
            } finally {
                unpauseKafka();
            }

            drain(relay);
        }

        assertThat(outboxCount(owner)).isZero();
        // At-least-once: a request from the failed tick may still land after the broker resumes, so duplicates are
        // allowed; per key, the first delivery of each event follows created_at order.
        Map<String, LinkedHashSet<String>> firstSeenPerKey = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> r : readFrom(kafkaBootstrap(), start)) {
            firstSeenPerKey.computeIfAbsent(r.key(), k -> new LinkedHashSet<>()).add(header(r, EventHeaders.EVENT_ID));
        }
        expectedPerKey.forEach((key, ids) ->
                assertThat(new ArrayList<>(firstSeenPerKey.get(key))).as("order for key %s", key).isEqualTo(ids));
    }

    /** Retries ticks until the outbox is empty, as the relay runner does with backoff. */
    private void drain(OutboxRelay relay) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (outboxCount(owner) > 0) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("outbox not drained within 60s");
            }
            try {
                relay.publishBatch();
            } catch (RuntimeException brokerStillRecovering) {
                // next tick retries the same rows with the same event_ids
            }
        }
    }

    /** docker pause keeps the container and its mapped port, unlike stop/start. */
    private static void pauseKafka() {
        KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
    }

    private static void unpauseKafka() {
        KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
    }
}
