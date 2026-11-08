package com.vitallink.readings.persistence;

import static com.vitallink.readings.persistence.JdbcReadingRepository.utc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.AuditEntry;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.domain.StateChange;
import com.vitallink.readings.ports.AlertStateRepository;

/**
 * Patterns 4 and 5 in SQL. Every transition is one statement whose WHERE holds the guard (version, legal source
 * states); zero rows means the guard did not match. Coach actions and reassignment lock the row in a CTE first so
 * the statement can also return the status it moved from; the CTE's FOR UPDATE re-checks the guard against the
 * latest committed row, so a concurrent change makes it match nothing rather than act on stale state.
 */
public class JdbcAlertStateRepository implements AlertStateRepository {

    static final String ESCALATION_ACTOR = "escalation-scheduler";
    static final String REASSIGNMENT_ACTOR = "care-team";

    private static final String COLUMNS = """
            a.alert_id, a.patient_id, a.measure_code, a.coach_id, a.severity, a.status, a.is_late, a.escalate_at,
            a.first_seen_at, a.last_seen_at, a.occurrence_cnt, a.version, a.created_at, a.acknowledged_by,
            a.acknowledged_at, a.resolved_by, a.resolved_at""";

    private final JdbcClient jdbc;

    public JdbcAlertStateRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<StateChange> acknowledge(long alertId, long coachId, int expectedVersion, Instant now) {
        // Clearing escalate_at here is what makes an acknowledged alert unable to escalate (guard 8).
        return jdbc.sql("""
                        WITH cur AS (
                            SELECT alert_id, status FROM alert
                            WHERE alert_id = ? AND version = ? AND status IN ('OPEN', 'ESCALATED')
                            FOR UPDATE)
                        UPDATE alert a
                        SET status = 'ACKNOWLEDGED', acknowledged_by = ?, acknowledged_at = ?, escalate_at = NULL,
                            version = a.version + 1
                        FROM cur
                        WHERE a.alert_id = cur.alert_id AND a.version = ? AND a.status IN ('OPEN', 'ESCALATED')
                        RETURNING %s, cur.status AS from_status
                        """.formatted(COLUMNS))
                .params(alertId, expectedVersion, coachId, utc(now), expectedVersion)
                .query((rs, n) -> new StateChange(alert(rs), AlertStatus.valueOf(rs.getString("from_status")),
                        StateChange.Reason.ACKNOWLEDGED, AuditEntry.ActorType.COACH, String.valueOf(coachId), null))
                .optional();
    }

    @Override
    public Optional<StateChange> resolve(long alertId, long coachId, int expectedVersion, String note, Instant now) {
        // The note rule sits in the guard, so a severity upgrade to CRITICAL racing this resolve cannot slip past it.
        return jdbc.sql("""
                        WITH cur AS (
                            SELECT alert_id, status FROM alert
                            WHERE alert_id = ? AND version = ? AND status <> 'RESOLVED'
                              AND (severity <> 'CRITICAL' OR ?::text IS NOT NULL)
                            FOR UPDATE)
                        UPDATE alert a
                        SET status = 'RESOLVED', resolved_by = ?, resolved_at = ?, resolution_note = ?,
                            escalate_at = NULL, version = a.version + 1
                        FROM cur
                        WHERE a.alert_id = cur.alert_id AND a.version = ? AND a.status <> 'RESOLVED'
                        RETURNING %s, cur.status AS from_status
                        """.formatted(COLUMNS))
                .params(alertId, expectedVersion, note, coachId, utc(now), note, expectedVersion)
                .query((rs, n) -> new StateChange(alert(rs), AlertStatus.valueOf(rs.getString("from_status")),
                        StateChange.Reason.RESOLVED, AuditEntry.ActorType.COACH, String.valueOf(coachId), null))
                .optional();
    }

    @Override
    public List<StateChange> escalateDue(Instant now, int limit) {
        // Guard 7: rows another scheduler (or a coach) holds are skipped, not waited for; status = 'OPEN' and the
        // escalate_at re-check under the lock keep an alert acknowledged meanwhile from escalating.
        return jdbc.sql("""
                        UPDATE alert a
                        SET status = 'ESCALATED', escalate_at = NULL, version = a.version + 1
                        WHERE a.alert_id IN (SELECT alert_id FROM alert
                                             WHERE escalate_at <= ?
                                             ORDER BY escalate_at
                                             LIMIT ?
                                             FOR UPDATE SKIP LOCKED)
                          AND a.status = 'OPEN'
                        RETURNING %s
                        """.formatted(COLUMNS))
                .params(utc(now), limit)
                .query((rs, n) -> new StateChange(alert(rs), AlertStatus.OPEN, StateChange.Reason.ESCALATED,
                        AuditEntry.ActorType.SERVICE, ESCALATION_ACTOR, null))
                .list();
    }

    @Override
    public List<StateChange> reassignUnresolved(long patientId, long newCoachId) {
        // Status and escalate_at are untouched: a running escalation timer keeps running for the new owner.
        return jdbc.sql("""
                        WITH cur AS (
                            SELECT alert_id, coach_id FROM alert
                            WHERE patient_id = ? AND status <> 'RESOLVED' AND coach_id <> ?
                            ORDER BY alert_id
                            FOR UPDATE)
                        UPDATE alert a
                        SET coach_id = ?, version = a.version + 1
                        FROM cur
                        WHERE a.alert_id = cur.alert_id AND a.status <> 'RESOLVED'
                        RETURNING %s, cur.coach_id AS previous_coach_id
                        """.formatted(COLUMNS))
                .params(patientId, newCoachId, newCoachId)
                .query((rs, n) -> {
                    Alert alert = alert(rs);
                    return new StateChange(alert, alert.status(), StateChange.Reason.REASSIGNED,
                            AuditEntry.ActorType.SERVICE, REASSIGNMENT_ACTOR, rs.getLong("previous_coach_id"));
                })
                .list();
    }

    @Override
    public Optional<Alert> find(long alertId) {
        return jdbc.sql("SELECT %s FROM alert a WHERE a.alert_id = ?".formatted(COLUMNS))
                .param(alertId)
                .query((rs, n) -> alert(rs))
                .optional();
    }

    private static Alert alert(ResultSet rs) throws SQLException {
        return new Alert(rs.getLong("alert_id"), rs.getLong("patient_id"), Measure.fromCode(rs.getInt("measure_code")),
                rs.getLong("coach_id"), Severity.valueOf(rs.getString("severity")),
                AlertStatus.valueOf(rs.getString("status")), rs.getBoolean("is_late"), instant(rs, "escalate_at"),
                instant(rs, "first_seen_at"), instant(rs, "last_seen_at"), rs.getInt("occurrence_cnt"),
                rs.getInt("version"), instant(rs, "created_at"), rs.getObject("acknowledged_by", Long.class),
                instant(rs, "acknowledged_at"), rs.getObject("resolved_by", Long.class), instant(rs, "resolved_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime t = rs.getObject(column, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }
}
