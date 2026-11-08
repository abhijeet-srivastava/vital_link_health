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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.messaging.EventHeaders;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.messaging.OutboxRelay;
import com.vitallink.readings.testsupport.IntegrationTestBase;

/** Guard 2 / pattern 7: SKIP LOCKED claim, publish with event_id header, delete only after the broker ack. */
class OutboxRelayTest extends IntegrationTestBase {

    static final Duration ACK_TIMEOUT = Duration.ofSeconds(30);

    final JdbcClient owner = JdbcClient.create(dataSource());
    final DataSource service = asServiceRole(dataSource());

    @BeforeAll
    static void topics() throws Exception {
        RelayTestSupport.createTopics(kafkaBootstrap());
    }

    @Test
    void outbox_row_is_published_keyed_by_patient_with_event_id_header_and_deleted() {
        Map<TopicPartition, Long> start = endOffsets(kafkaBootstrap(), KafkaTopics.READINGS_INGESTED);
        UUID eventId = insertOutbox(owner, KafkaTopics.READINGS_INGESTED, "8812", Instant.now());

        try (Producer<String, String> producer = producer(kafkaBootstrap(), Map.of())) {
            assertThat(relay(service, producer, ACK_TIMEOUT).publishBatch()).isEqualTo(1);
        }

        assertThat(outboxCount(owner)).isZero();
        List<ConsumerRecord<String, String>> records = readFrom(kafkaBootstrap(), start);
        assertThat(records).hasSize(1);
        ConsumerRecord<String, String> record = records.getFirst();
        assertThat(record.key()).isEqualTo("8812");
        assertThat(header(record, EventHeaders.EVENT_ID)).isEqualTo(eventId.toString());
        assertThat(header(record, EventHeaders.EVENT_TYPE)).isEqualTo(KafkaTopics.READINGS_INGESTED);
        assertThat(header(record, EventHeaders.SCHEMA_VERSION)).isEqualTo("1");
        assertThat(record.value()).contains(eventId.toString());
    }

    @Test
    void two_relays_running_together_publish_each_row_once() throws Exception {
        int rows = 2_000;
        Map<TopicPartition, Long> start = endOffsets(kafkaBootstrap(), KafkaTopics.READINGS_INGESTED);
        Set<String> inserted = new HashSet<>();
        Instant t0 = Instant.now();
        for (int i = 0; i < rows; i++) {
            inserted.add(insertOutbox(owner, KafkaTopics.READINGS_INGESTED, String.valueOf(i % 50),
                    t0.plusMillis(i)).toString());
        }

        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (Producer<String, String> pa = producer(kafkaBootstrap(), Map.of());
             Producer<String, String> pb = producer(kafkaBootstrap(), Map.of())) {
            List<Future<Integer>> published = new ArrayList<>();
            for (OutboxRelay relay : List.of(relay(service, pa, ACK_TIMEOUT), relay(service, pb, ACK_TIMEOUT))) {
                published.add(pool.submit(() -> {
                    go.await();
                    int total = 0;
                    for (int n; (n = relay.publishBatch()) > 0; ) {
                        total += n;
                    }
                    return total;
                }));
            }
            go.countDown();
            int a = published.get(0).get();
            int b = published.get(1).get();
            assertThat(a + b).isEqualTo(rows);
        } finally {
            pool.shutdownNow();
        }

        assertThat(outboxCount(owner)).isZero();
        List<ConsumerRecord<String, String>> records = readFrom(kafkaBootstrap(), start);
        List<String> eventIds = records.stream().map(r -> header(r, EventHeaders.EVENT_ID)).toList();
        assertThat(eventIds).hasSize(rows).doesNotHaveDuplicates();
        assertThat(new HashSet<>(eventIds)).isEqualTo(inserted);
    }

    @Test
    void crash_after_publish_before_delete_republishes_same_event_id() {
        Map<TopicPartition, Long> start = endOffsets(kafkaBootstrap(), KafkaTopics.READINGS_INGESTED);
        UUID eventId = insertOutbox(owner, KafkaTopics.READINGS_INGESTED, "8812", Instant.now());

        // Relay A: the broker acks the record, then the process "dies" before the DELETE commits.
        try (Producer<String, String> real = producer(kafkaBootstrap(), Map.of())) {
            OutboxRelay crashing = relay(service, crashAfterFlush(real), ACK_TIMEOUT);
            assertThatThrownBy(crashing::publishBatch).isInstanceOf(SimulatedCrash.class);
        }
        assertThat(outboxCount(owner)).isEqualTo(1);

        // Relay B (fresh producer, as after a restart) picks the row up again.
        try (Producer<String, String> producer = producer(kafkaBootstrap(), Map.of())) {
            assertThat(relay(service, producer, ACK_TIMEOUT).publishBatch()).isEqualTo(1);
        }

        assertThat(outboxCount(owner)).isZero();
        List<ConsumerRecord<String, String>> records = readFrom(kafkaBootstrap(), start);
        assertThat(records).hasSize(2);
        assertThat(records).extracting(r -> header(r, EventHeaders.EVENT_ID)).containsOnly(eventId.toString());
        assertThat(records).extracting(ConsumerRecord::key).containsOnly("8812");
        assertThat(records).extracting(ConsumerRecord::partition).containsOnly(records.getFirst().partition());
    }

    /** flush() returns only when every sent record is acked; throwing right after it is a crash before DELETE. */
    @SuppressWarnings("unchecked")
    static Producer<String, String> crashAfterFlush(Producer<String, String> real) {
        return (Producer<String, String>) Proxy.newProxyInstance(Producer.class.getClassLoader(),
                new Class<?>[] {Producer.class}, (proxy, method, args) -> {
                    Object result;
                    try {
                        result = method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (method.getName().equals("flush")) {
                        throw new SimulatedCrash();
                    }
                    return result;
                });
    }

    static final class SimulatedCrash extends RuntimeException {
        SimulatedCrash() {
            super("simulated crash after publish, before delete");
        }
    }
}
