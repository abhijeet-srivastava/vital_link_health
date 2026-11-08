package com.vitallink.readings.alerting.notify;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.domain.AlertEvent;
import com.vitallink.readings.domain.NotificationReason;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.persistence.JdbcNotificationRepository;
import com.vitallink.readings.testsupport.IntegrationTestBase;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Open clinical decision: daily digests stay off unless the flag is on and a clinician set DAILY_DIGEST for the
 * patient. CRITICAL is never digested. Also: recipients come from care_assignment when the event is recorded.
 */
class NotificationDigestTest extends IntegrationTestBase {

    static final long PATIENT = 8812;

    final JdbcClient jdbc = JdbcClient.create(dataSource());

    @Test
    void digest_flag_off_by_default_gives_queue_only_even_with_daily_digest_policy() {
        long alertId = givenDigestPatientAndAlert();

        recorder(false).record(event(alertId, Severity.HIGH));

        assertThat(channels()).containsExactly("QUEUE");
    }

    @Test
    void digest_flag_on_adds_digest_for_non_critical_and_never_for_critical() {
        long alertId = givenDigestPatientAndAlert();
        NotificationRecorder recorder = recorder(true);

        recorder.record(event(alertId, Severity.HIGH));
        assertThat(channels()).containsExactlyInAnyOrder("QUEUE", "DIGEST");

        jdbc.sql("DELETE FROM notification").update();
        recorder.record(event(alertId, Severity.CRITICAL));
        assertThat(channels()).containsExactlyInAnyOrder("QUEUE", "PUSH");
    }

    @Test
    void recipient_is_the_primary_at_record_time_with_event_coach_as_fallback() {
        long alertId = givenDigestPatientAndAlert();
        jdbc.sql("UPDATE care_assignment SET coach_id = 43 WHERE patient_id = ?").param(PATIENT).update();
        recorder(false).record(event(alertId, Severity.HIGH));
        assertThat(coaches()).containsExactly(43L);

        jdbc.sql("DELETE FROM care_assignment").update();
        jdbc.sql("DELETE FROM notification").update();
        recorder(false).record(event(alertId, Severity.CRITICAL));
        assertThat(coaches()).as("no open PRIMARY: the alert's owner, never dropped").containsOnly(41L);
    }

    long givenDigestPatientAndAlert() {
        jdbc.sql("""
                        INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
                        VALUES (?, 41, 'PRIMARY', timestamptz '2026-01-01T00:00Z')
                        """)
                .param(PATIENT).update();
        jdbc.sql("INSERT INTO notification_policy (patient_id, non_critical, set_by) VALUES (?, 'DAILY_DIGEST', 900)")
                .param(PATIENT).update();
        return jdbc.sql("""
                        INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, first_seen_at,
                                           last_seen_at)
                        VALUES (?, 1, 41, 'HIGH', 'OPEN', now(), now()) RETURNING alert_id
                        """)
                .param(PATIENT).query(Long.class).single();
    }

    NotificationRecorder recorder(boolean digestEnabled) {
        return new NotificationRecorder(new JdbcNotificationRepository(jdbc),
                new TransactionTemplate(new DataSourceTransactionManager(dataSource())), Clock.systemUTC(),
                digestEnabled, new SimpleMeterRegistry());
    }

    static AlertEvent event(long alertId, Severity severity) {
        return new AlertEvent(UUID.randomUUID(), alertId, PATIENT, 41, severity, NotificationReason.RAISED);
    }

    List<String> channels() {
        return jdbc.sql("SELECT channel FROM notification").query(String.class).list();
    }

    List<Long> coaches() {
        return jdbc.sql("SELECT coach_id FROM notification").query(Long.class).list();
    }
}
