package com.vitallink.readings.alerting.notify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.alerts.DefaultAlertService;
import com.vitallink.readings.alerts.DefaultEscalationScheduler;
import com.vitallink.readings.alerts.StateChangeRecords;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.persistence.JdbcAlertStateRepository;
import com.vitallink.readings.persistence.JdbcAuditWriter;
import com.vitallink.readings.persistence.JdbcOutboxWriter;

/** Guard 5: one notification per (trigger event, coach, channel); a replay adds nothing, a real change notifies. */
@SpringBootTest(properties = "vitallink.notifier.sender-poll-interval=PT0.2S")
@ActiveProfiles({"notifier", "notifier-critical"})
class NotificationIdempotencyTest extends NotifierTestSupport {

    static final long PATIENT = 8812;

    @Autowired
    TransactionTemplate tx;

    @Test
    void same_event_consumed_twice_gives_one_notification_per_coach_and_channel() throws Exception {
        givenCareTeam(PATIENT, COACH, BACKUP_COACH);
        long alertId = givenAlert(PATIENT, Severity.CRITICAL, AlertStatus.OPEN);
        ProducerRecord<String, String> raised = raised(alertId, PATIENT, Severity.CRITICAL, "RAISED");
        ProducerRecord<String, String> escalated = stateChanged(alertId, PATIENT, "OPEN", "ESCALATED", "ESCALATED");
        ProducerRecord<String, String> acknowledged =
                stateChanged(alertId, PATIENT, "ESCALATED", "ACKNOWLEDGED", "ACKNOWLEDGED");

        publish(raised);
        publish(raised);           // relay re-publish or Kafka redelivery: same event_id
        publish(escalated);
        publish(escalated);
        publish(acknowledged);     // no notification for an acknowledgement
        awaitConsumed();

        assertThat(notifications())
                .extracting(NotificationView::reason, NotificationView::coachId, NotificationView::channel)
                .containsExactlyInAnyOrder(
                        tuple("RAISED", COACH, "QUEUE"),
                        tuple("RAISED", COACH, "PUSH"),
                        tuple("ESCALATED", COACH, "QUEUE"),
                        tuple("ESCALATED", COACH, "PUSH"),
                        tuple("ESCALATED", COACH, "SMS"),
                        tuple("ESCALATED", BACKUP_COACH, "QUEUE"),
                        tuple("ESCALATED", BACKUP_COACH, "PUSH"),
                        tuple("ESCALATED", BACKUP_COACH, "SMS"));
        assertThat(notifications()).filteredOn(n -> n.channel().equals("QUEUE"))
                .allSatisfy(n -> assertThat(n.status()).as("QUEUE rows are SENT at once").isEqualTo("SENT"));
        await().atMost(WAIT).untilAsserted(() -> assertThat(notifications())
                .allSatisfy(n -> assertThat(n.status()).isEqualTo("SENT")));
    }

    @Test
    void second_escalation_after_reopen_notifies_again() throws Exception {
        givenCareTeam(PATIENT, COACH, BACKUP_COACH);
        long alertId = givenAlert(PATIENT, Severity.CRITICAL, AlertStatus.OPEN);
        Clock clock = Clock.systemUTC();
        StateChangeRecords records = new StateChangeRecords(clock, new UuidV7(clock), new ObjectMapper());
        JdbcAlertStateRepository alerts = new JdbcAlertStateRepository(jdbc);
        DefaultEscalationScheduler scheduler = new DefaultEscalationScheduler(alerts, new JdbcOutboxWriter(jdbc),
                new JdbcAuditWriter(jdbc), tx, records);
        DefaultAlertService coach = new DefaultAlertService(alerts, new JdbcOutboxWriter(jdbc),
                new JdbcAuditWriter(jdbc), tx, records, clock);
        Instant pastWindow = Instant.now().plus(Duration.ofMinutes(16));

        assertThat(scheduler.escalateDue(pastWindow, 10)).isEqualTo(1);
        coach.acknowledge(alertId, COACH, 1, UUID.randomUUID());
        // The reopen as the evaluator writes it (ACKNOWLEDGED -> OPEN, timer restarted). Done in SQL: the evaluator
        // reopens only on a rise *to* CRITICAL, which an already-CRITICAL episode cannot make.
        jdbc.sql("""
                        UPDATE alert SET status = 'OPEN', escalate_at = now() + interval '15 minutes',
                                         version = version + 1
                        WHERE alert_id = ? AND status = 'ACKNOWLEDGED'
                        """)
                .param(alertId).update();
        assertThat(scheduler.escalateDue(pastWindow, 10)).isEqualTo(1);
        relayOutbox();             // ESCALATED, ACKNOWLEDGED, ESCALATED: three distinct event ids
        awaitConsumed();

        assertThat(notifications()).allSatisfy(n -> assertThat(n.reason()).isEqualTo("ESCALATED"));
        assertThat(notifications().stream().map(NotificationView::triggerEventId).distinct()).hasSize(2);
        assertThat(notifications()).hasSize(2 * 2 * 3);   // two escalations x (primary + backup) x QUEUE/PUSH/SMS
        assertThat(count("SELECT count(*) FROM notification WHERE coach_id = ? AND channel = 'SMS'", BACKUP_COACH))
                .as("the backup coach is paged once per escalation").isEqualTo(2);
    }
}
