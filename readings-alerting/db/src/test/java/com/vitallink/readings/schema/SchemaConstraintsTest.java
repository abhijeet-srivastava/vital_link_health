package com.vitallink.readings.schema;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.vitallink.readings.testsupport.IntegrationTestBase;

/** The business-rule guards in references/schema.sql, proven against the migrated database. */
class SchemaConstraintsTest extends IntegrationTestBase {

    // SQLSTATEs: https://www.postgresql.org/docs/16/errcodes-appendix.html
    static final String UNIQUE_VIOLATION = "23505";
    static final String CHECK_VIOLATION = "23514";
    static final String EXCLUSION_VIOLATION = "23P01";
    static final String INSUFFICIENT_PRIVILEGE = "42501";

    @BeforeEach
    void baseRows() throws SQLException {
        exec("""
                INSERT INTO device (device_id, serial_no, device_type) VALUES (1, 'SN-1', 'BP_CUFF');
                INSERT INTO threshold_rule (threshold_id, program_id, measure_code, operator, limit_value, severity, created_by)
                VALUES (1, 1, 1, 'GT', 180, 'CRITICAL', 'test');
                """);
    }

    @Test
    void overlapping_device_bindings_are_rejected() throws SQLException {
        exec("""
                INSERT INTO device_binding (device_id, patient_id, valid_from, valid_to)
                VALUES (1, 100, '2026-01-01T00:00Z', '2026-06-01T00:00Z')
                """);

        assertSqlState(EXCLUSION_VIOLATION, """
                INSERT INTO device_binding (device_id, patient_id, valid_from)
                VALUES (1, 200, '2026-05-01T00:00Z')
                """);
        assertThatCode(() -> exec("""
                INSERT INTO device_binding (device_id, patient_id, valid_from)
                VALUES (1, 200, '2026-06-01T00:00Z')
                """)).as("a binding starting exactly when the previous one ends is a rebinding, not an overlap")
                .doesNotThrowAnyException();
    }

    @Test
    void second_unresolved_episode_for_same_patient_and_measure_is_rejected() throws SQLException {
        String episode = """
                INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, first_seen_at, last_seen_at, acknowledged_at)
                VALUES (100, 1, 7, 'HIGH', '%s', now(), now(), %s)
                """;
        exec(episode.formatted("OPEN", "NULL"));

        assertSqlState(UNIQUE_VIOLATION, episode.formatted("OPEN", "NULL"));
        assertSqlState(UNIQUE_VIOLATION, episode.formatted("ACKNOWLEDGED", "now()"));
    }

    @Test
    void escalate_at_on_acknowledged_alert_is_rejected() {
        assertSqlState(CHECK_VIOLATION, """
                INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, acknowledged_at,
                                   escalate_at, first_seen_at, last_seen_at)
                VALUES (100, 1, 7, 'CRITICAL', 'ACKNOWLEDGED', now(), now() + interval '15 minutes', now(), now())
                """);
    }

    @Test
    void escalate_at_on_open_non_critical_alert_is_rejected() {
        assertSqlState(CHECK_VIOLATION, """
                INSERT INTO alert (patient_id, measure_code, coach_id, severity, escalate_at, first_seen_at, last_seen_at)
                VALUES (100, 1, 7, 'HIGH', now() + interval '15 minutes', now(), now())
                """);
    }

    @Test
    void acknowledging_without_clearing_escalate_at_is_rejected() throws SQLException {
        exec("""
                INSERT INTO alert (alert_id, patient_id, measure_code, coach_id, severity, escalate_at, first_seen_at, last_seen_at)
                VALUES (1, 100, 1, 7, 'CRITICAL', now() + interval '15 minutes', now(), now())
                """);

        assertSqlState(CHECK_VIOLATION, """
                UPDATE alert SET status = 'ACKNOWLEDGED', acknowledged_at = now() WHERE alert_id = 1
                """);
    }

    @Test
    void critical_rule_with_confirm_n_above_one_is_rejected() {
        assertSqlState(CHECK_VIOLATION, """
                INSERT INTO threshold_rule (program_id, measure_code, operator, limit_value, severity,
                                            confirm_n, confirm_window, created_by)
                VALUES (2, 1, 'GT', 180, 'CRITICAL', 2, interval '1 hour', 'test')
                """);
    }

    @Test
    void duplicate_alert_occurrence_key_is_rejected() throws SQLException {
        exec("""
                INSERT INTO alert (alert_id, patient_id, measure_code, coach_id, severity, first_seen_at, last_seen_at)
                VALUES (1, 100, 1, 7, 'CRITICAL', now(), now())
                """);
        String occurrence = """
                INSERT INTO alert_occurrence (device_id, reading_uuid, measure_code, measured_at, alert_id,
                                              observed_value, threshold_id, severity)
                VALUES (1, '0190f1d2-0000-7000-8000-000000000001', 1, date_trunc('hour', now()), 1, %s, 1, 'CRITICAL')
                """;
        exec(occurrence.formatted("185"));

        assertSqlState(UNIQUE_VIOLATION, occurrence.formatted("190"));
    }

    @Test
    void alert_occurrence_with_unknown_severity_is_rejected() throws SQLException {
        exec("""
                INSERT INTO alert (alert_id, patient_id, measure_code, coach_id, severity, first_seen_at, last_seen_at)
                VALUES (1, 100, 1, 7, 'CRITICAL', now(), now())
                """);

        assertSqlState(CHECK_VIOLATION, """
                INSERT INTO alert_occurrence (device_id, reading_uuid, measure_code, measured_at, alert_id,
                                              observed_value, threshold_id, severity)
                VALUES (1, '0190f1d2-0000-7000-8000-000000000001', 1, date_trunc('hour', now()), 1, 185, 1, 'LOW')
                """);
    }

    @Test
    void notification_with_unknown_severity_is_rejected() throws SQLException {
        exec("""
                INSERT INTO alert (alert_id, patient_id, measure_code, coach_id, severity, first_seen_at, last_seen_at)
                VALUES (1, 100, 1, 7, 'CRITICAL', now(), now())
                """);

        assertSqlState(CHECK_VIOLATION, """
                INSERT INTO notification (trigger_event_id, alert_id, coach_id, channel, reason, severity)
                VALUES ('0190f1d2-0000-7000-8000-000000000004', 1, 7, 'QUEUE', 'RAISED', 'critical')
                """);
    }

    @Test
    void overlapping_primary_coach_assignments_are_rejected() throws SQLException {
        // Both rows are closed, so ux_primary_coach (current rows only) cannot catch this; the EXCLUDE does.
        exec("""
                INSERT INTO care_assignment (patient_id, coach_id, role, valid_from, valid_to)
                VALUES (100, 1, 'PRIMARY', '2026-01-01T00:00Z', '2026-06-01T00:00Z')
                """);

        assertSqlState(EXCLUSION_VIOLATION, """
                INSERT INTO care_assignment (patient_id, coach_id, role, valid_from, valid_to)
                VALUES (100, 2, 'PRIMARY', '2026-03-01T00:00Z', '2026-09-01T00:00Z')
                """);
    }

    @Test
    void primary_coach_handover_at_the_same_instant_is_accepted() throws SQLException {
        exec("""
                INSERT INTO care_assignment (patient_id, coach_id, role, valid_from, valid_to)
                VALUES (100, 1, 'PRIMARY', '2026-01-01T00:00Z', '2026-06-01T00:00Z')
                """);

        assertThatCode(() -> exec("""
                INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
                VALUES (100, 2, 'PRIMARY', '2026-06-01T00:00Z')
                """)).doesNotThrowAnyException();
    }

    @Test
    void backup_coach_alongside_primary_is_accepted() throws SQLException {
        exec("""
                INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
                VALUES (100, 1, 'PRIMARY', '2026-01-01T00:00Z')
                """);

        assertThatCode(() -> exec("""
                INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
                VALUES (100, 2, 'BACKUP', '2026-01-01T00:00Z')
                """)).doesNotThrowAnyException();
    }

    @Test
    void service_role_cannot_update_or_delete_audit_log() throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("SET ROLE vitallink_service");
            st.execute("""
                    INSERT INTO audit_log (audit_id, actor_type, actor_id, action, outcome, patient_id, resource, request_id)
                    VALUES ('0190f1d2-0000-7000-8000-000000000002', 'COACH', '7', 'READ_READINGS', 'ALLOWED', 100,
                            'patient/100/readings', '0190f1d2-0000-7000-8000-000000000003')
                    """);

            assertSqlState(st, INSUFFICIENT_PRIVILEGE, "UPDATE audit_log SET outcome = 'DENIED'");
            assertSqlState(st, INSUFFICIENT_PRIVILEGE, "DELETE FROM audit_log");
            assertSqlState(st, INSUFFICIENT_PRIVILEGE, "TRUNCATE audit_log");
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static void assertSqlState(String sqlState, String sql) {
        assertThatThrownBy(() -> exec(sql))
                .isInstanceOf(SQLException.class)
                .extracting(e -> ((SQLException) e).getSQLState())
                .isEqualTo(sqlState);
    }

    private static void assertSqlState(Statement st, String sqlState, String sql) {
        assertThatThrownBy(() -> st.execute(sql))
                .isInstanceOf(SQLException.class)
                .extracting(e -> ((SQLException) e).getSQLState())
                .isEqualTo(sqlState);
    }
}
