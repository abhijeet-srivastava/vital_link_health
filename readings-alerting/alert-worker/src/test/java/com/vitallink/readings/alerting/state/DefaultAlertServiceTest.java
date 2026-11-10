package com.vitallink.readings.alerting.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.vitallink.readings.alerts.DefaultAlertService;
import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.AlertConflict;
import com.vitallink.readings.domain.AlertNotFoundException;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.ResolutionNoteRequiredException;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.KafkaTopics;

/** Guard 8 / pattern 4: version match and legal source states in one UPDATE; zero rows means 409 + current state. */
class DefaultAlertServiceTest extends AlertStateTestSupport {

    @Test
    void two_acknowledges_same_version_one_succeeds_one_conflicts_with_current_state() throws Exception {
        long alertId = openCritical(PATIENT);
        DefaultAlertService alerts = alertService();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Alert>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            for (long coach : new long[] {COACH, OTHER_COACH}) {
                results.add(pool.submit(gated(start, () -> alerts.acknowledge(alertId, coach, 0, UUID.randomUUID()))));
            }
            start.countDown();
        }

        List<Alert> won = new ArrayList<>();
        List<AlertConflict> lost = new ArrayList<>();
        for (Future<Alert> f : results) {
            try {
                won.add(f.get());
            } catch (ExecutionException e) {
                lost.add((AlertConflict) e.getCause());
            }
        }
        assertThat(won).hasSize(1);
        assertThat(lost).hasSize(1);
        Alert winner = won.getFirst();
        assertThat(winner.status()).isEqualTo(AlertStatus.ACKNOWLEDGED);
        assertThat(winner.version()).isEqualTo(1);
        assertThat(winner.escalateAt()).isNull();
        assertThat(lost.getFirst().current()).isEqualTo(winner);   // the loser sees the state the winner committed
        assertThat(audits("ACK_ALERT")).hasSize(1);
        assertThat(stateChangedEvents()).hasSize(1);
    }

    @Test
    void acknowledge_clears_escalate_at_so_alert_cannot_escalate() {
        long alertId = openCritical(PATIENT);

        alertService().acknowledge(alertId, COACH, 0, UUID.randomUUID());
        clock.advance(Duration.ofHours(1));

        assertThat(scheduler().escalateDue(clock.instant(), 100)).isZero();
        assertThat(alertRow(alertId))
                .containsEntry("status", "ACKNOWLEDGED")
                .containsEntry("version", 1)
                .containsEntry("escalate_at", null);
        assertThat(stateChangedEvents()).extracting(e -> e.get("to").asText()).containsExactly("ACKNOWLEDGED");
    }

    @Test
    void resolve_critical_without_note_is_rejected() {
        long alertId = openCritical(PATIENT);
        DefaultAlertService alerts = alertService();

        for (String blank : new String[] {null, "", "   "}) {
            assertThatThrownBy(() -> alerts.resolve(alertId, COACH, 0, blank, UUID.randomUUID()))
                    .isInstanceOf(ResolutionNoteRequiredException.class)
                    .hasMessageNotContaining("null");
        }
        assertThat(alertRow(alertId)).containsEntry("status", "OPEN").containsEntry("version", 0);
        assertThat(stateChangedEvents()).isEmpty();
        assertThat(audits()).isEmpty();

        Alert resolved = alerts.resolve(alertId, COACH, 0, "Rechecked by phone, BP back in range", UUID.randomUUID());

        assertThat(resolved.status()).isEqualTo(AlertStatus.RESOLVED);
        assertThat(resolved.escalateAt()).isNull();
        assertThat(alertRow(alertId)).containsEntry("resolution_note", "Rechecked by phone, BP back in range");
    }

    @Test
    void resolve_non_critical_without_note_is_allowed() {
        long alertId = insertAlert(PATIENT, Measure.SYSTOLIC, Severity.HIGH, AlertStatus.OPEN, null);

        Alert resolved = alertService().resolve(alertId, COACH, 0, null, UUID.randomUUID());

        assertThat(resolved.status()).isEqualTo(AlertStatus.RESOLVED);
        assertThat(resolved.resolvedBy()).isEqualTo(COACH);
    }

    @Test
    void every_state_change_writes_audit_and_state_changed_event() {
        long alertId = openCritical(PATIENT);
        UUID ackRequest = UUID.randomUUID();
        UUID resolveRequest = UUID.randomUUID();
        clock.advance(WINDOW);

        assertThat(scheduler().escalateDue(clock.instant(), 100)).isEqualTo(1);
        alertService().acknowledge(alertId, OTHER_COACH, 1, ackRequest);   // backup coach acknowledges ESCALATED
        alertService().resolve(alertId, OTHER_COACH, 2, "free text that must stay out of events", resolveRequest);

        List<JsonNode> events = stateChangedEvents(alertId);
        assertThat(events).extracting(e -> e.get("from").asText() + ">" + e.get("to").asText())
                .containsExactly("OPEN>ESCALATED", "ESCALATED>ACKNOWLEDGED", "ACKNOWLEDGED>RESOLVED");
        assertThat(events).extracting(e -> e.get("reason").asText())
                .containsExactly("ESCALATED", "ACKNOWLEDGED", "RESOLVED");
        assertThat(events).extracting(e -> e.get("alert_version").asInt()).containsExactly(1, 2, 3);
        assertThat(events).extracting(e -> e.get("actor_type").asText() + ":" + e.get("actor_id").asText())
                .containsExactly("SERVICE:escalation-scheduler", "COACH:" + OTHER_COACH, "COACH:" + OTHER_COACH);
        assertThat(events).allSatisfy(e -> {
            assertThat(e.get("type").asText()).isEqualTo("alert.state_changed");
            assertThat(e.get("schema_version").asInt()).isEqualTo(1);
            assertThat(e.get("patient_id").asLong()).isEqualTo(PATIENT);
            assertThat(e.get("severity").asText()).isEqualTo("CRITICAL");
            assertThat(e.toString()).doesNotContain("free text").doesNotContain("note");
        });
        assertThat(outboxTopics()).containsOnly(KafkaTopics.ALERT_STATE_CHANGED);

        assertThat(audits()).extracting(a -> a.get("action"))
                .containsExactlyInAnyOrder("ESCALATE_ALERT", "ACK_ALERT", "RESOLVE_ALERT");
        assertThat(audits()).allSatisfy(a -> {
            assertThat(a.get("outcome")).isEqualTo("ALLOWED");
            assertThat(a.get("patient_id")).isEqualTo(PATIENT);
            assertThat(a.get("resource")).isEqualTo("alert/" + alertId);
        });
        assertThat(audits("ACK_ALERT").getFirst()).containsEntry("request_id", ackRequest)
                .containsEntry("actor_type", "COACH").containsEntry("actor_id", String.valueOf(OTHER_COACH));
        assertThat(audits("RESOLVE_ALERT").getFirst()).containsEntry("request_id", resolveRequest);
    }

    @Test
    void acknowledge_from_resolved_conflicts() {
        long alertId = insertAlert(PATIENT, Measure.SYSTOLIC, Severity.HIGH, AlertStatus.RESOLVED, null);

        assertThatThrownBy(() -> alertService().acknowledge(alertId, COACH, 0, UUID.randomUUID()))
                .isInstanceOfSatisfying(AlertConflict.class,
                        c -> assertThat(c.current().status()).isEqualTo(AlertStatus.RESOLVED));
        assertThat(stateChangedEvents()).isEmpty();
    }

    @Test
    void acknowledge_unknown_alert_is_not_found() {
        assertThatThrownBy(() -> alertService().acknowledge(999_999, COACH, 0, UUID.randomUUID()))
                .isInstanceOf(AlertNotFoundException.class);
    }

    static <T> Callable<T> gated(CountDownLatch start, Callable<T> body) {
        return () -> {
            start.await();
            return body.call();
        };
    }
}
