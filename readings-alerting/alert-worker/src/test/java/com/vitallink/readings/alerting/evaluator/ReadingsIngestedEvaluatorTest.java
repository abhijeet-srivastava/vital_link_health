package com.vitallink.readings.alerting.evaluator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.KafkaTopics;

import io.micrometer.core.instrument.MeterRegistry;

@SpringBootTest(properties = {
        "vitallink.alerting.max-retries=3",
        "vitallink.alerting.retry-interval=PT1S"})
@ActiveProfiles("evaluator")
class ReadingsIngestedEvaluatorTest extends EvaluatorTestSupport {

    static final long PROGRAM = 1;
    static final long COACH = 41;

    @Autowired
    MeterRegistry meters;

    @Test
    void readings_ingested_delivered_twice_gives_one_alert_count_one_and_one_event() throws Exception {
        givenPatient(8812, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);
        ProducerRecord<String, String> event = event(8812, bp(8812, 170));

        publish(event);
        publish(event);   // relay re-publish or Kafka redelivery: same event_id, same readings
        awaitConsumed();

        List<AlertRow> alerts = alerts(8812);
        assertThat(alerts).singleElement().satisfies(a -> {
            assertThat(a.occurrenceCnt()).isEqualTo(1);
            assertThat(a.severity()).isEqualTo("HIGH");
            assertThat(a.status()).isEqualTo("OPEN");
            assertThat(a.coachId()).isEqualTo(COACH);
            assertThat(a.escalateAt()).as("no timer below CRITICAL").isNull();
        });
        assertThat(occurrences(alerts.get(0).alertId())).isEqualTo(1);
        assertThat(alertEvents()).singleElement().satisfies(e -> {
            assertThat(e.topic()).isEqualTo(KafkaTopics.ALERTS_RAISED);
            assertThat(e.aggregateKey()).isEqualTo("8812");
            assertThat(e.payload()).containsEntry("reason", "RAISED")
                    .containsEntry("severity", "HIGH")
                    .containsEntry("measure", "SYSTOLIC")
                    .containsEntry("patient_id", 8812)
                    .containsEntry("coach_id", (int) COACH)
                    .containsEntry("occurrence_cnt", 1)
                    .containsEntry("schema_version", 1)
                    .containsKeys("event_id", "alert_id", "threshold_id", "observed_value", "is_late");
        });
        assertThat(audit(8812)).singleElement().isEqualTo(new AuditRow("SERVICE", "alert-evaluator", "RAISE_ALERT",
                "ALLOWED", 8812L, "alert/" + alerts.get(0).alertId(), eventId(event)));
    }

    @Test
    void two_breaches_same_patient_measure_give_one_alert_with_count_two() throws Exception {
        givenPatient(8813, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);

        publish(event(8813, bp(8813, 170, Instant.now().minusSeconds(60))));
        publish(event(8813, bp(8813, 175)));
        awaitConsumed();

        assertThat(alerts(8813)).singleElement().satisfies(a -> {
            assertThat(a.occurrenceCnt()).isEqualTo(2);
            assertThat(a.severity()).isEqualTo("HIGH");
            assertThat(occurrences(a.alertId())).isEqualTo(2);
        });
        assertThat(alertEvents()).as("a repeat breach announces nothing").hasSize(1);
        assertThat(auditActions(8813)).as("a repeat breach changes no state").containsExactly("RAISE_ALERT");
    }

    @Test
    void high_then_critical_upgrades_severity_in_place_with_one_upgrade_event_and_escalate_at() throws Exception {
        givenPatient(8814, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);
        publish(event(8814, bp(8814, 170, Instant.now().minusSeconds(60))));
        awaitConsumed();
        long alertId = alerts(8814).get(0).alertId();

        Instant before = Instant.now();
        publish(event(8814, bp(8814, 186)));
        awaitConsumed();
        Instant after = Instant.now();

        assertThat(alerts(8814)).singleElement().satisfies(a -> {
            assertThat(a.alertId()).as("upgraded in place").isEqualTo(alertId);
            assertThat(a.severity()).isEqualTo("CRITICAL");
            assertThat(a.status()).isEqualTo("OPEN");
            assertThat(a.occurrenceCnt()).isEqualTo(2);
            assertThat(a.escalateAt()).isBetween(before.plus(Duration.ofMinutes(15)).minusSeconds(1),
                    after.plus(Duration.ofMinutes(15)));
        });
        List<OutboxRow> events = alertEvents();
        assertThat(events).extracting(e -> e.payload().get("reason")).containsExactly("RAISED", "SEVERITY_UPGRADED");
        assertThat(events.get(1).topic()).isEqualTo(KafkaTopics.ALERTS_RAISED_CRITICAL);
        assertThat(events.get(1).payload()).containsEntry("occurrence_cnt", 2).containsEntry("severity", "CRITICAL");
        assertThat(auditActions(8814)).containsExactly("RAISE_ALERT", "UPGRADE_ALERT");
    }

    @Test
    void acknowledged_alert_rising_to_critical_returns_to_open() throws Exception {
        givenPatient(8815, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);
        publish(event(8815, bp(8815, 170, Instant.now().minusSeconds(60))));
        awaitConsumed();
        // Stage 6 owns the acknowledge path; set the state the coach would leave behind.
        jdbc.sql("""
                        UPDATE alert SET status = 'ACKNOWLEDGED', acknowledged_by = ?, acknowledged_at = now(),
                                         version = version + 1
                        WHERE patient_id = ?
                        """)
                .params(COACH, 8815).update();

        publish(event(8815, bp(8815, 190)));
        awaitConsumed();

        assertThat(alerts(8815)).singleElement().satisfies(a -> {
            assertThat(a.status()).isEqualTo("OPEN");
            assertThat(a.severity()).isEqualTo("CRITICAL");
            assertThat(a.escalateAt()).as("escalation restarts").isNotNull();
        });
        assertThat(alertEvents()).extracting(e -> e.payload().get("reason"))
                .containsExactly("RAISED", "SEVERITY_UPGRADED");
        assertThat(auditActions(8815)).containsExactly("RAISE_ALERT", "UPGRADE_ALERT", "REOPEN_ALERT");
    }

    @Test
    void critical_goes_to_critical_topic() throws Exception {
        givenPatient(8816, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);

        publish(event(8816, bp(8816, 186)));
        awaitConsumed();

        assertThat(alerts(8816)).singleElement().satisfies(a -> {
            assertThat(a.severity()).isEqualTo("CRITICAL");
            assertThat(a.escalateAt()).isNotNull();
        });
        assertThat(alertEvents()).singleElement().satisfies(e -> {
            assertThat(e.topic()).isEqualTo(KafkaTopics.ALERTS_RAISED_CRITICAL);
            assertThat(e.eventType()).isEqualTo("alerts.raised");
        });
    }

    @Test
    void value_inside_every_band_creates_nothing() throws Exception {
        givenPatient(8817, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);

        publish(event(8817, bp(8817, 120), bp(8817, 160)));
        awaitConsumed();

        assertThat(count("alert")).isZero();
        assertThat(count("alert_occurrence")).isZero();
        assertThat(count("outbox_event")).isZero();
        assertThat(count("audit_log")).isZero();
    }

    @Test
    void normal_reading_after_breach_does_not_resolve_alert() throws Exception {
        givenPatient(8818, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);
        publish(event(8818, bp(8818, 186, Instant.now().minusSeconds(60))));
        awaitConsumed();

        publish(event(8818, bp(8818, 118)));
        awaitConsumed();

        assertThat(alerts(8818)).singleElement().satisfies(a -> {
            assertThat(a.status()).isEqualTo("OPEN");
            assertThat(a.occurrenceCnt()).isEqualTo(1);
        });
        assertThat(alertEvents()).hasSize(1);
    }

    @Test
    void multi_program_patient_gets_most_conservative_default() throws Exception {
        givenPatient(8819, COACH, 1);
        givenEnrollment(8819, 2);
        givenProgramBand(1, Measure.SYSTOLIC, "GT", 180, Severity.CRITICAL);
        givenProgramBand(2, Measure.SYSTOLIC, "GT", 150, Severity.HIGH);

        publish(event(8819, bp(8819, 155)));
        awaitConsumed();

        assertThat(alerts(8819)).singleElement().extracting(AlertRow::severity).isEqualTo("HIGH");
    }

    @Test
    void malformed_event_goes_straight_to_dlq_and_does_not_block_the_partition() throws Exception {
        givenPatient(8820, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);
        UUID poisonId = UUID.randomUUID();

        publish(record("8820", poisonId, "{not json"));
        publish(event(8820, bp(8820, 186)));   // same key, same partition, behind the poison record
        awaitConsumed();

        List<ConsumerRecord<String, String>> dead = awaitDeadLetters(r -> hasEventId(r, poisonId));
        assertThat(dead).singleElement().satisfies(r -> assertThat(r.key()).isEqualTo("8820"));
        assertThat(alerts(8820)).hasSize(1);
    }

    @Test
    void poison_message_goes_to_dlq_after_n_retries() throws Exception {
        givenEnrollment(8821, PROGRAM);   // no PRIMARY coach: alert.coach_id cannot be filled
        givenSystolicBands(PROGRAM);
        double failuresBefore = failures();
        ProducerRecord<String, String> event = event(8821, bp(8821, 186));
        UUID eventId = eventId(event);

        publish(event);

        assertThat(awaitDeadLetters(r -> hasEventId(r, eventId))).hasSize(1);
        awaitConsumed();
        assertThat(failures() - failuresBefore).as("first attempt + 3 retries").isEqualTo(4);
        assertThat(count("alert")).isZero();
        assertThat(count("outbox_event")).isZero();
        assertThat(count("audit_log")).as("rolled back with the alert").isZero();
    }

    @Test
    void failing_event_does_not_hold_back_critical_for_another_patient_on_its_partition() throws Exception {
        long failing = 8830;
        long other = samePartitionAs(failing);
        givenEnrollment(failing, PROGRAM);   // no PRIMARY coach: fails every attempt
        givenPatient(other, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);
        double failuresBefore = failures();
        ProducerRecord<String, String> poison = event(failing, bp(failing, 186));

        publish(poison);
        publish(event(other, bp(other, 186)));   // behind the failing record on the same partition

        await().atMost(WAIT).until(() -> alerts(other).size() == 1);
        assertThat(failures() - failuresBefore).as("evaluated while the failing record still waits for retries")
                .isLessThan(4);
        assertThat(awaitDeadLetters(r -> hasEventId(r, eventId(poison)))).hasSize(1);
        assertThat(alerts(other)).singleElement().extracting(AlertRow::severity).isEqualTo("CRITICAL");
    }

    @Test
    void failed_event_is_redelivered_until_it_commits() throws Exception {
        givenEnrollment(8822, PROGRAM);
        givenSystolicBands(PROGRAM);
        double failuresBefore = failures();

        publish(event(8822, bp(8822, 186)));
        await().atMost(WAIT).until(() -> failures() > failuresBefore);
        // The care-team projection catches up while the event waits in the retry backoff.
        jdbc.sql("INSERT INTO care_assignment (patient_id, coach_id, role, valid_from) VALUES (8822, ?, 'PRIMARY', now())")
                .param(COACH).update();
        awaitConsumed();

        assertThat(alerts(8822)).singleElement().satisfies(a -> assertThat(a.occurrenceCnt()).isEqualTo(1));
        assertThat(alertEvents()).hasSize(1);
    }

    @Test
    void late_reading_alerts_with_late_flag() throws Exception {
        givenPatient(8823, COACH, PROGRAM);
        givenSystolicBands(PROGRAM);

        publish(event(8823, bp(8823, 170, Instant.now().minus(Duration.ofDays(8)))));
        awaitConsumed();

        assertThat(alerts(8823)).singleElement().satisfies(a -> assertThat(a.late()).isTrue());
        assertThat(alertEvents()).singleElement().satisfies(e -> assertThat(e.payload()).containsEntry("is_late", true));
    }

    double failures() {
        var counter = meters.find("vitallink.evaluator.failures").counter();
        return counter == null ? 0 : counter.count();
    }

    static boolean hasEventId(ConsumerRecord<String, String> r, UUID eventId) {
        var header = r.headers().lastHeader("event_id");
        return header != null && eventId.toString().equals(new String(header.value(), StandardCharsets.UTF_8));
    }
}
