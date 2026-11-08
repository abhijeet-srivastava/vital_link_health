package com.vitallink.readings.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.ActiveEpisode;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.BreachWithReading;
import com.vitallink.readings.domain.EpisodePlan;
import com.vitallink.readings.domain.EpisodeRow;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.NoPrimaryCoachException;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.ports.AlertEpisodeRepository;

/** Pattern 2 in SQL. The database decides: occurrence PK (guard 4) and ux_alert_active_episode (guard 3). */
public class JdbcAlertEpisodeRepository implements AlertEpisodeRepository {

    private final JdbcClient jdbc;

    public JdbcAlertEpisodeRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean occurrenceExists(BreachWithReading b) {
        return jdbc.sql("""
                        SELECT EXISTS (SELECT 1 FROM alert_occurrence
                                       WHERE device_id = ? AND reading_uuid = ? AND measure_code = ? AND measured_at = ?)
                        """)
                .params(b.deviceId(), b.readingUuid(), b.measure().code(), utc(b.measuredAt()))
                .query(Boolean.class).single();
    }

    @Override
    public Optional<ActiveEpisode> lockActiveEpisode(long patientId, Measure measure) {
        return jdbc.sql("""
                        SELECT alert_id, status, severity, is_late FROM alert
                        WHERE patient_id = ? AND measure_code = ? AND status <> 'RESOLVED'
                        FOR UPDATE
                        """)
                .params(patientId, measure.code())
                .query((rs, n) -> new ActiveEpisode(rs.getLong("alert_id"), AlertStatus.valueOf(rs.getString("status")),
                        Severity.valueOf(rs.getString("severity")), rs.getBoolean("is_late")))
                .optional();
    }

    @Override
    public EpisodeRow openEpisode(BreachWithReading b, EpisodePlan.Open plan) {
        // Owner = current PRIMARY coach (ux_primary_coach); no row means no coach, and no alert is inserted.
        return jdbc.sql("""
                        INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, is_late, escalate_at,
                                           first_seen_at, last_seen_at)
                        SELECT ?::bigint, ?::smallint, ca.coach_id, ?, 'OPEN', ?::boolean, ?::timestamptz, ?::timestamptz,
                               ?::timestamptz
                        FROM care_assignment ca
                        WHERE ca.patient_id = ? AND ca.role = 'PRIMARY' AND ca.valid_to IS NULL
                        RETURNING alert_id, coach_id, occurrence_cnt
                        """)
                .params(b.patientId(), b.measure().code(), plan.severity().name(), plan.late(),
                        utc(plan.escalateAt()), utc(b.measuredAt()), utc(b.measuredAt()), b.patientId())
                .query(JdbcAlertEpisodeRepository::episodeRow)
                .optional()
                .orElseThrow(() -> new NoPrimaryCoachException(b.patientId()));
    }

    @Override
    public EpisodeRow foldEpisode(BreachWithReading b, EpisodePlan.Fold plan) {
        if (!plan.upgraded()) {
            // Unindexed columns only, so the repeat breach stays a HOT update.
            return jdbc.sql("""
                            UPDATE alert
                            SET occurrence_cnt = occurrence_cnt + 1,
                                first_seen_at = LEAST(first_seen_at, ?), last_seen_at = GREATEST(last_seen_at, ?),
                                is_late = ?, version = version + 1
                            WHERE alert_id = ?
                            RETURNING alert_id, coach_id, occurrence_cnt
                            """)
                    .params(utc(b.measuredAt()), utc(b.measuredAt()), plan.late(), plan.alertId())
                    .query(JdbcAlertEpisodeRepository::episodeRow).single();
        }
        return jdbc.sql("""
                        UPDATE alert
                        SET occurrence_cnt = occurrence_cnt + 1,
                            first_seen_at = LEAST(first_seen_at, ?), last_seen_at = GREATEST(last_seen_at, ?),
                            is_late = ?, severity = ?, status = ?, escalate_at = ?, version = version + 1
                        WHERE alert_id = ?
                        RETURNING alert_id, coach_id, occurrence_cnt
                        """)
                .params(utc(b.measuredAt()), utc(b.measuredAt()), plan.late(), plan.severity().name(),
                        plan.status().name(), utc(plan.escalateAt()), plan.alertId())
                .query(JdbcAlertEpisodeRepository::episodeRow).single();
    }

    @Override
    public void insertOccurrence(long alertId, BreachWithReading b) {
        jdbc.sql("""
                        INSERT INTO alert_occurrence (device_id, reading_uuid, measure_code, measured_at, alert_id,
                                                      observed_value, threshold_id, severity)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """)
                .params(b.deviceId(), b.readingUuid(), b.measure().code(), utc(b.measuredAt()), alertId,
                        b.breach().observedValue(), b.breach().band().thresholdId(), b.breach().severity().name())
                .update();
    }

    private static EpisodeRow episodeRow(ResultSet rs, int n) throws SQLException {
        return new EpisodeRow(rs.getLong("alert_id"), rs.getLong("coach_id"), rs.getInt("occurrence_cnt"));
    }

    private static OffsetDateTime utc(Instant t) {
        return t == null ? null : t.atOffset(ZoneOffset.UTC);
    }
}
