package com.vitallink.readings.alerting.relay;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.messaging.OutboxRelay;
import com.vitallink.readings.messaging.UuidV7;

/** Topics, outbox rows, relays and an offset-bounded topic reader for the relay tests. */
final class RelayTestSupport {

    private static final UuidV7 IDS = new UuidV7(Clock.systemUTC());

    private RelayTestSupport() {
    }

    static void createTopics(String bootstrap) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> missing = KafkaTopics.all().stream()
                    .filter(t -> !existing.contains(t.name()))
                    .map(t -> new NewTopic(t.name(), t.partitions(), (short) 1))
                    .toList();
            admin.createTopics(missing).all().get();
        }
    }

    /** The relay's DB connections run as the least-privilege service role, as in compose. */
    static DataSource asServiceRole(DataSource owner) {
        return new DelegatingDataSource(owner) {
            @Override
            public Connection getConnection() throws SQLException {
                Connection c = super.getConnection();
                try (Statement st = c.createStatement()) {
                    st.execute("SET ROLE vitallink_service");
                }
                return c;
            }
        };
    }

    static Producer<String, String> producer(String bootstrap, Map<String, Object> extra) {
        Map<String, Object> base = new HashMap<>(extra);
        base.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return new KafkaProducer<>(OutboxRelay.producerConfig(base));
    }

    static OutboxRelay relay(DataSource ds, Producer<String, String> producer, Duration ackTimeout) {
        return new OutboxRelay(JdbcClient.create(ds), new TransactionTemplate(new DataSourceTransactionManager(ds)),
                producer, ackTimeout);
    }

    static UUID insertOutbox(JdbcClient owner, String topic, String key, Instant createdAt) {
        UUID id = IDS.next();
        owner.sql("""
                        INSERT INTO outbox_event (event_id, topic, event_type, aggregate_key, payload, created_at)
                        VALUES (?, ?, ?, ?, ?::jsonb, ?)
                        """)
                .params(id, topic, topic, key, "{\"event_id\":\"" + id + "\",\"patient_id\":" + key + "}",
                        OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC))
                .update();
        return id;
    }

    static int outboxCount(JdbcClient owner) {
        return owner.sql("SELECT count(*) FROM outbox_event").query(Integer.class).single();
    }

    static Map<TopicPartition, Long> endOffsets(String bootstrap, String topic) {
        try (KafkaConsumer<String, String> consumer = consumer(bootstrap)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            return consumer.endOffsets(partitions);
        }
    }

    /**
     * Everything written to the topic since {@code start}, read up to the current end offsets. Call after the outbox
     * is empty: rows are deleted only after acks=all, so every published record is already below the end offset.
     */
    static List<ConsumerRecord<String, String>> readFrom(String bootstrap, Map<TopicPartition, Long> start) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = consumer(bootstrap)) {
            consumer.assign(start.keySet());
            start.forEach(consumer::seek);
            Map<TopicPartition, Long> end = consumer.endOffsets(start.keySet());
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (end.entrySet().stream().anyMatch(e -> consumer.position(e.getKey()) < e.getValue())) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Topic not read to its end offsets within 30s");
                }
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
        }
        return records;
    }

    static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static KafkaConsumer<String, String> consumer(String bootstrap) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }
}
