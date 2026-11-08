package com.vitallink.readings.alerting.notify;

import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.AlertStateChangedV1;
import com.vitallink.readings.messaging.AlertsRaisedV1;
import com.vitallink.readings.messaging.EventHeaders;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

/**
 * Drives the notifier through real Kafka: events are produced to alerts.raised(.critical) and alert.state_changed
 * exactly as the relay writes them (key = patient_id, event_id header). Assertions read the notification table.
 */
abstract class NotifierTestSupport extends SpringIntegrationTestBase {

    static final Duration WAIT = Duration.ofSeconds(30);
    static final UuidV7 IDS = new UuidV7(java.time.Clock.systemUTC());
    static final long COACH = 41;
    static final long BACKUP_COACH = 42;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    KafkaTemplate<String, String> kafka;

    final ObjectMapper json = new ObjectMapper();

    // ----- given -----

    void givenCareTeam(long patientId, long primary, Long backup) {
        jdbc.sql("""
                        INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
                        VALUES (?, ?, 'PRIMARY', timestamptz '2026-01-01T00:00Z')
                        """)
                .params(patientId, primary).update();
        if (backup != null) {
            jdbc.sql("""
                            INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
                            VALUES (?, ?, 'BACKUP', timestamptz '2026-01-01T00:00Z')
                            """)
                    .params(patientId, backup).update();
        }
    }

    long givenAlert(long patientId, Severity severity, AlertStatus status) {
        boolean timer = status == AlertStatus.OPEN && severity == Severity.CRITICAL;
        return jdbc.sql("""
                        INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, escalate_at,
                                           first_seen_at, last_seen_at)
                        VALUES (?, ?, ?, ?, ?, CASE WHEN ? THEN now() + interval '15 minutes' END, now(), now())
                        RETURNING alert_id
                        """)
                .params(patientId, Measure.SYSTOLIC.code(), COACH, severity.name(), status.name(), timer)
                .query(Long.class).single();
    }

    // ----- when -----

    ProducerRecord<String, String> raised(long alertId, long patientId, Severity severity, String reason)
            throws Exception {
        UUID eventId = IDS.next();
        AlertsRaisedV1 payload = new AlertsRaisedV1(eventId, AlertsRaisedV1.EVENT_TYPE, AlertsRaisedV1.SCHEMA_VERSION,
                alertId, patientId, COACH, Measure.SYSTOLIC.name(), new BigDecimal("186"), severity.name(), reason,
                7, 1, false);
        return record(AlertsRaisedV1.topicFor(severity), patientId, eventId, AlertsRaisedV1.EVENT_TYPE,
                json.writeValueAsString(payload));
    }

    ProducerRecord<String, String> stateChanged(long alertId, long patientId, String from, String to, String reason)
            throws Exception {
        UUID eventId = IDS.next();
        AlertStateChangedV1 payload = new AlertStateChangedV1(eventId, AlertStateChangedV1.EVENT_TYPE,
                AlertStateChangedV1.SCHEMA_VERSION, alertId, patientId, COACH, null, Measure.SYSTOLIC.name(),
                Severity.CRITICAL.name(), from, to, reason, "SERVICE", "escalation-scheduler", 1);
        return record(AlertStateChangedV1.TOPIC, patientId, eventId, AlertStateChangedV1.EVENT_TYPE,
                json.writeValueAsString(payload));
    }

    static ProducerRecord<String, String> record(String topic, long patientId, UUID eventId, String type,
                                                 String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(patientId), value);
        record.headers().add(EventHeaders.EVENT_ID, eventId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(EventHeaders.EVENT_TYPE, type.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    void publish(ProducerRecord<String, String> record) throws Exception {
        kafka.send(record).get();
    }

    /** Publishes every outbox row as the relay would (key, event_id header) and deletes it. */
    void relayOutbox() throws Exception {
        List<Map<String, Object>> rows = jdbc.sql("""
                        SELECT event_id, topic, event_type, aggregate_key, payload::text AS payload FROM outbox_event
                        ORDER BY created_at, event_id
                        """)
                .query().listOfRows();
        for (Map<String, Object> r : rows) {
            ProducerRecord<String, String> record = new ProducerRecord<>((String) r.get("topic"),
                    (String) r.get("aggregate_key"), (String) r.get("payload"));
            record.headers().add(EventHeaders.EVENT_ID, r.get("event_id").toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add(EventHeaders.EVENT_TYPE,
                    ((String) r.get("event_type")).getBytes(StandardCharsets.UTF_8));
            publish(record);
            jdbc.sql("DELETE FROM outbox_event WHERE event_id = ?").param(r.get("event_id")).update();
        }
    }

    /** Both notifier groups have committed every record published so far (alert.state_changed: critical group). */
    void awaitConsumed() {
        await().atMost(WAIT).until(() ->
                lag(AlertEventsListener.GROUP_ID, KafkaTopics.ALERTS_RAISED) == 0
                        && lag(CriticalAlertEventsListener.GROUP_ID, KafkaTopics.ALERT_STATE_CHANGED) == 0
                        && lag(CriticalAlertEventsListener.GROUP_ID, KafkaTopics.ALERTS_RAISED_CRITICAL) == 0);
    }

    /** Every partition of the notifier topics has a leader (topics are created at context start). */
    static void awaitTopicsReady() {
        List<String> topics = List.of(KafkaTopics.ALERTS_RAISED, KafkaTopics.ALERTS_RAISED_CRITICAL,
                KafkaTopics.ALERT_STATE_CHANGED);
        await().atMost(WAIT).until(() -> {
            try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap()))) {
                return admin.describeTopics(topics).allTopicNames().get().values().stream()
                        .flatMap(t -> t.partitions().stream())
                        .allMatch(p -> p.leader() != null && !p.leader().isEmpty());
            }
        });
    }

    /** Records on the topic not yet committed by the group (a group with no commits lags by every record). */
    static long lag(String group, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap()))) {
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
                    .mapToLong(e -> {
                        OffsetAndMetadata c = committed.get(e.getKey());
                        return e.getValue() - (c == null ? 0 : c.offset());
                    })
                    .sum();
        }
    }

    /** Partitions of the topic on which the group has a committed offset. */
    static long committedPartitions(String group, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap()))) {
            return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().keySet().stream()
                    .filter(tp -> tp.topic().equals(topic))
                    .count();
        }
    }

    // ----- then -----

    record NotificationView(long alertId, long coachId, String channel, String reason, String severity, String status,
                            UUID triggerEventId) {
    }

    List<NotificationView> notifications() {
        return jdbc.sql("""
                        SELECT alert_id, coach_id, channel, reason, severity, status, trigger_event_id
                        FROM notification ORDER BY notification_id
                        """)
                .query((rs, n) -> new NotificationView(rs.getLong(1), rs.getLong(2), rs.getString(3),
                        rs.getString(4), rs.getString(5), rs.getString(6), rs.getObject(7, UUID.class)))
                .list();
    }

    int count(String sql, Object... params) {
        return jdbc.sql(sql).params(params).query(Integer.class).single();
    }
}
