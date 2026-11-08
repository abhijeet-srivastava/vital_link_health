package com.vitallink.readings.alerting.state;

import static com.vitallink.readings.alerting.state.DefaultAlertServiceTest.gated;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.alerting.escalation.EscalationJob;
import com.vitallink.readings.alerts.DefaultAlertService;
import com.vitallink.readings.alerts.DefaultEscalationScheduler;
import com.vitallink.readings.domain.AlertConflict;
import com.vitallink.readings.domain.AlertStatus;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** Guard 7 / pattern 5: SKIP LOCKED claim + status = 'OPEN' guard, escalate_at cleared in the same UPDATE. */
class EscalationJobTest extends AlertStateTestSupport {

    @Test
    void critical_not_acknowledged_by_escalate_at_is_escalated_once_with_three_schedulers() throws Exception {
        int due = 50;
        List<Long> dueIds = new ArrayList<>();
        for (int i = 0; i < due; i++) {
            dueIds.add(openCritical(PATIENT + i));
        }
        long notYetDue = openCritical(PATIENT + due);
        owner.sql("UPDATE alert SET escalate_at = escalate_at + interval '1 hour' WHERE alert_id = ?")
                .param(notYetDue).update();
        clock.advance(WINDOW);

        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> runs = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(3)) {
            for (int i = 0; i < 3; i++) {
                EscalationJob job = new EscalationJob(scheduler(), clock, 7, meters);   // small batches: several loops
                runs.add(pool.submit(gated(start, job::run)));
            }
            start.countDown();
        }

        int escalated = 0;
        for (Future<Integer> run : runs) {
            escalated += run.get();
        }
        assertThat(escalated).isEqualTo(due);
        for (long id : dueIds) {
            assertThat(alertRow(id))
                    .containsEntry("status", "ESCALATED")
                    .containsEntry("version", 1)
                    .containsEntry("escalate_at", null);
            assertThat(stateChangedEvents(id)).hasSize(1);
        }
        assertThat(stateChangedEvents()).hasSize(due);
        assertThat(audits("ESCALATE_ALERT")).hasSize(due);
        assertThat(alertRow(notYetDue)).containsEntry("status", "OPEN").containsEntry("version", 0);
        assertThat(meters.counter("vitallink.alerts.escalated").count()).isEqualTo(due);

        assertThat(new EscalationJob(scheduler(), clock, 7, meters).run()).isZero();   // a later tick finds nothing
    }

    @Test
    void concurrent_ack_and_escalation_exactly_one_wins() throws Exception {
        DefaultAlertService alerts = alertService();
        DefaultEscalationScheduler scheduler = scheduler();
        for (int round = 0; round < 20; round++) {
            clock.set(T0);
            long alertId = openCritical(PATIENT + round);
            clock.advance(WINDOW.plusSeconds(1));
            CountDownLatch start = new CountDownLatch(1);
            Future<?> ack;
            Future<Integer> escalate;
            try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
                ack = pool.submit(gated(start, () -> alerts.acknowledge(alertId, COACH, 0, UUID.randomUUID())));
                escalate = pool.submit(gated(start, () -> scheduler.escalateDue(clock.instant(), 100)));
                start.countDown();
            }

            boolean ackWon;
            try {
                ack.get();
                ackWon = true;
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(AlertConflict.class);
                assertThat(((AlertConflict) e.getCause()).current().status()).isEqualTo(AlertStatus.ESCALATED);
                ackWon = false;
            }
            assertThat(escalate.get()).as("round %d", round).isEqualTo(ackWon ? 0 : 1);
            assertThat(alertRow(alertId))
                    .containsEntry("status", ackWon ? "ACKNOWLEDGED" : "ESCALATED")
                    .containsEntry("version", 1)
                    .containsEntry("escalate_at", null);
            assertThat(stateChangedEvents(alertId)).as("round %d", round).hasSize(1);
        }
    }

    @Test
    void alert_is_not_escalated_before_escalate_at() {
        long alertId = openCritical(PATIENT);
        clock.advance(WINDOW.minus(Duration.ofSeconds(1)));

        assertThat(scheduler().escalateDue(clock.instant(), 100)).isZero();
        assertThat(alertRow(alertId)).containsEntry("status", "OPEN");

        clock.advance(Duration.ofSeconds(1));   // escalate_at <= now: due exactly at the deadline
        assertThat(scheduler().escalateDue(clock.instant(), 100)).isEqualTo(1);
    }
}
