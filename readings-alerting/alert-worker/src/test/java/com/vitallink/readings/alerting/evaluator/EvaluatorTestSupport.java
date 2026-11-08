package com.vitallink.readings.alerting.evaluator;

import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.EventHeaders;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.messaging.ReadingsIngestedV1;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

/**
 * Drives the evaluator through real Kafka: events are produced to readings.ingested exactly as the relay writes them
 * (key = patient_id, event_id header). The relay profile is off, so alerting outbox rows stay put for assertions.
 */
abstract class EvaluatorTestSupport extends SpringIntegrationTestBase {

    static final String GROUP = ReadingsIngestedListener.GROUP_ID;
    static final Duration WAIT = Duration.ofSeconds(30);
    static final UuidV7 IDS = new UuidV7(java.time.Clock.systemUTC());

    @Autowired
    JdbcClient jdbc;

    @Autowired
    KafkaTemplate<String, String> kafka;

    final ObjectMapper json = new ObjectMapper();

    // ----- given -----

    void givenPatient(long patientId, long coachId, long programId) {
        givenEnrollment(patientId, programId);
        jdbc.sql("""
                        INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
                        VALUES (?, ?, 'PRIMARY', timestamptz '2026-01-01T00:00Z')
                        """)
                .params(patientId, coachId).update();
    }

    void givenEnrollment(long patientId, long programId) {
        jdbc.sql("INSERT INTO enrollment (patient_id, program_id, valid_from) VALUES (?, ?, timestamptz '2026-01-01T00:00Z')")
                .params(patientId, programId).update();
    }

    long givenProgramBand(long programId, Measure measure, String operator, int limit, Severity severity) {
        return jdbc.sql("""
                        INSERT INTO threshold_rule (program_id, measure_code, operator, limit_value, severity, created_by)
                        VALUES (?, ?, ?, ?, ?, 'test') RETURNING threshold_id
                        """)
                .params(programId, measure.code(), operator, limit, severity.name())
                .query(Long.class).single();
    }

    /** Hypertension-like systolic bands: GT 160 HIGH, GT 180 CRITICAL, LT 90 CRITICAL. */
    void givenSystolicBands(long programId) {
        givenProgramBand(programId, Measure.SYSTOLIC, "GT", 160, Severity.HIGH);
        givenProgramBand(programId, Measure.SYSTOLIC, "GT", 180, Severity.CRITICAL);
        givenProgramBand(programId, Measure.SYSTOLIC, "LT", 90, Severity.CRITICAL);
    }

    // ----- when -----

    static Reading bp(long patientId, int systolic, Instant measuredAt) {
        return new Reading(patientId, UUID.randomUUID(), measuredAt.truncatedTo(ChronoUnit.MICROS), patientId,
                ReadingType.BLOOD_PRESSURE, 1L, systolic, 80, 70, null, 0L);
    }

    static Reading bp(long patientId, int systolic) {
        return bp(patientId, systolic, Instant.now());
    }

    ProducerRecord<String, String> event(long patientId, Reading... readings) throws Exception {
        UUID eventId = IDS.next();
        ReadingsIngestedV1 payload = ReadingsIngestedV1.of(eventId, patientId, readings[0].deviceId(), List.of(readings));
        return record(String.valueOf(patientId), eventId, json.writeValueAsString(payload));
    }

    static ProducerRecord<String, String> record(String key, UUID eventId, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaTopics.READINGS_INGESTED, key, value);
        record.headers().add(EventHeaders.EVENT_ID, eventId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(EventHeaders.EVENT_TYPE, ReadingsIngestedV1.EVENT_TYPE.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    void publish(ProducerRecord<String, String> record) throws Exception {
        kafka.send(record).get();
    }

    static UUID eventId(ProducerRecord<String, String> record) {
        return UUID.fromString(new String(record.headers().lastHeader(EventHeaders.EVENT_ID).value(),
                StandardCharsets.UTF_8));
    }

    /**
     * Every record published so far has been processed and committed by the evaluator group, on the main topic and
     * on the retry topic (a failing record leaves the main partition at once and waits on the retry topic).
     */
    void awaitConsumed() {
        await().atMost(WAIT).until(this::groupCaughtUp);
    }

    /** A patient id whose records land on the same readings.ingested partition as {@code patientId}'s. */
    static long samePartitionAs(long patientId) {
        int target = partitionOf(patientId);
        for (long candidate = patientId + 1; ; candidate++) {
            if (partitionOf(candidate) == target) {
                return candidate;
            }
        }
    }

    static int partitionOf(long patientId) {
        int partitions = KafkaTopics.all().stream().filter(t -> t.name().equals(KafkaTopics.READINGS_INGESTED))
                .findFirst().orElseThrow().partitions();
        return Utils.toPositive(Utils.murmur2(String.valueOf(patientId).getBytes(StandardCharsets.UTF_8))) % partitions;
    }

    /** The retry-topic consumer runs in its own group: the main group id plus the retry suffix. */
    private boolean groupCaughtUp() throws Exception {
        String retry = KafkaTopics.retry(KafkaTopics.READINGS_INGESTED);
        return caughtUp(GROUP, KafkaTopics.READINGS_INGESTED) && caughtUp(GROUP + ".retry", retry);
    }

    private boolean caughtUp(String group, String topic) throws Exception {
        try (Admin admin = admin()) {
            Set<TopicPartition> partitions = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic)
                    .partitions().stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toSet());
            Map<TopicPartition, Long> ends = admin.listOffsets(partitions.stream()
                            .collect(Collectors.toMap(p -> p, p -> OffsetSpec.latest())))
                    .all().get().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            return ends.entrySet().stream()
                    .filter(e -> e.getValue() > 0)
                    .allMatch(e -> committed.get(e.getKey()) != null
                            && committed.get(e.getKey()).offset() >= e.getValue());
        }
    }

    // ----- then -----

    record AlertRow(long alertId, long coachId, String severity, String status, int occurrenceCnt, Instant escalateAt,
                    boolean late, int version) {
    }

    List<AlertRow> alerts(long patientId) {
        return jdbc.sql("""
                        SELECT alert_id, coach_id, severity, status, occurrence_cnt, escalate_at, is_late, version
                        FROM alert WHERE patient_id = ? ORDER BY alert_id
                        """)
                .param(patientId)
                .query((rs, n) -> new AlertRow(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4),
                        rs.getInt(5), rs.getTimestamp(6) == null ? null : rs.getTimestamp(6).toInstant(),
                        rs.getBoolean(7), rs.getInt(8)))
                .list();
    }

    record OutboxRow(String topic, String eventType, String aggregateKey, Map<String, Object> payload) {
    }

    @SuppressWarnings("unchecked")
    List<OutboxRow> alertEvents() {
        return jdbc.sql("""
                        SELECT topic, event_type, aggregate_key, payload::text FROM outbox_event
                        WHERE event_type = 'alerts.raised' ORDER BY created_at, event_id
                        """)
                .query((rs, n) -> {
                    try {
                        return new OutboxRow(rs.getString(1), rs.getString(2), rs.getString(3),
                                json.readValue(rs.getString(4), Map.class));
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .list();
    }

    record AuditRow(String actorType, String actorId, String action, String outcome, Long patientId, String resource,
                    UUID requestId) {
    }

    List<AuditRow> audit(long patientId) {
        return jdbc.sql("""
                        SELECT actor_type, actor_id, action, outcome, patient_id, resource, request_id
                        FROM audit_log WHERE patient_id = ? ORDER BY at, audit_id
                        """)
                .param(patientId)
                .query((rs, n) -> new AuditRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getLong(5), rs.getString(6), rs.getObject(7, UUID.class)))
                .list();
    }

    List<String> auditActions(long patientId) {
        return audit(patientId).stream().map(AuditRow::action).toList();
    }

    int occurrences(long alertId) {
        return jdbc.sql("SELECT count(*) FROM alert_occurrence WHERE alert_id = ?").param(alertId)
                .query(Integer.class).single();
    }

    int count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Integer.class).single();
    }

    /** Records on readings.ingested.dlq matching the predicate, read from the beginning. */
    List<ConsumerRecord<String, String>> awaitDeadLetters(Predicate<ConsumerRecord<String, String>> match) {
        String dlq = KafkaTopics.dlq(KafkaTopics.READINGS_INGESTED);
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false))) {
            List<TopicPartition> partitions = consumer.partitionsFor(dlq).stream()
                    .map(p -> new TopicPartition(dlq, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            await().atMost(WAIT).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (match.test(r)) {
                        found.add(r);
                    }
                });
                return !found.isEmpty();
            });
        }
        return found;
    }

    static Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap()));
    }

    static BigDecimal decimal(Object value) {
        return new BigDecimal(value.toString());
    }
}
