package com.vitallink.readings.alerting.notify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.KafkaTopics;

/**
 * D13 / D32 critical lane: alerts.raised.critical and alert.state_changed are consumed by group notifier-critical and
 * senders claim CRITICAL rows first, so a CRITICAL raise and an escalation are notified while alerts.raised holds a
 * backlog. The non-critical group is paused while the backlog is
 * built so the backlog is certain to be there when the CRITICAL arrives (a saturated lane, made deterministic).
 */
@SpringBootTest(properties = "vitallink.notifier.sender-poll-interval=PT0.2S")
@ActiveProfiles({"notifier", "notifier-critical"})
class CriticalLaneBacklogTest extends NotifierTestSupport {

    static final int BACKLOG = 10_000;
    static final long CRITICAL_PATIENT = 1;
    static final long ESCALATED_PATIENT = 3;

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Test
    void critical_alert_notified_while_non_critical_topic_has_backlog() throws Exception {
        awaitTopicsReady();
        MessageListenerContainer nonCritical = listeners.getListenerContainer(AlertEventsListener.GROUP_ID);
        nonCritical.pause();
        List<Long> backlogAlerts = jdbc.sql("""
                        INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, first_seen_at,
                                           last_seen_at)
                        SELECT p, 1, 7, 'HIGH', 'OPEN', now(), now() FROM generate_series(100000, 100000 + ? - 1) p
                        RETURNING alert_id
                        """)
                .param(BACKLOG).query(Long.class).list();
        // Bulk producer for the backlog only: the context's template is the idempotent DLQ producer, whose
        // sequence tracking can hold a partition after the NOT_LEADER retries a burst gets right after topic creation.
        try (Producer<String, String> bulk = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
                ProducerConfig.LINGER_MS_CONFIG, 5))) {
            List<Future<RecordMetadata>> sends = new ArrayList<>(BACKLOG);
            for (int i = 0; i < BACKLOG; i++) {
                sends.add(bulk.send(raised(backlogAlerts.get(i), 100000 + i, Severity.HIGH, "RAISED")));
            }
            bulk.flush();
            for (Future<RecordMetadata> send : sends) {
                send.get();
            }
        }
        // >= : the plain producer may write a retried batch twice; guard 5 absorbs the duplicates downstream.
        assertThat(lag(AlertEventsListener.GROUP_ID, KafkaTopics.ALERTS_RAISED)).isGreaterThanOrEqualTo(BACKLOG);

        givenCareTeam(CRITICAL_PATIENT, COACH, BACKUP_COACH);
        long critical = givenAlert(CRITICAL_PATIENT, Severity.CRITICAL, AlertStatus.OPEN);
        publish(raised(critical, CRITICAL_PATIENT, Severity.CRITICAL, "RAISED"));
        givenCareTeam(ESCALATED_PATIENT, COACH, BACKUP_COACH);
        long escalated = givenAlert(ESCALATED_PATIENT, Severity.CRITICAL, AlertStatus.ESCALATED);
        publish(stateChanged(escalated, ESCALATED_PATIENT, "OPEN", "ESCALATED", "ESCALATED"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(count("""
                        SELECT count(*) FROM notification
                        WHERE alert_id = ? AND coach_id = ? AND status = 'SENT' AND channel IN ('QUEUE', 'PUSH')
                        """, critical, COACH)).isEqualTo(2));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(count("""
                        SELECT count(*) FROM notification
                        WHERE alert_id = ? AND reason = 'ESCALATED' AND status = 'SENT' AND coach_id IN (?, ?)
                        """, escalated, COACH, BACKUP_COACH))
                .as("primary and backup: QUEUE, PUSH, SMS").isEqualTo(6));
        assertThat(lag(AlertEventsListener.GROUP_ID, KafkaTopics.ALERTS_RAISED))
                .as("the non-critical backlog is still there").isGreaterThanOrEqualTo(BACKLOG);
        assertThat(committedPartitions(AlertEventsListener.GROUP_ID, KafkaTopics.ALERT_STATE_CHANGED))
                .as("group notifier no longer consumes alert.state_changed").isZero();

        nonCritical.resume();
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> assertThat(count(
                "SELECT count(*) FROM notification WHERE severity = 'HIGH' AND channel = 'QUEUE'"))
                .isEqualTo(BACKLOG));
    }
}
