package com.vitallink.readings.persistence;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.ReadingRollup;
import com.vitallink.readings.ports.ReadingRollupRepository;

/**
 * reading_rollup: hourly aggregates per patient and measure. A refresh recomputes whole hours from reading, so running
 * it twice, or on two pods at once, writes the same rows (the primary key is the idempotency key); unchanged rows are
 * not rewritten. Readings are split into measures with reading_type_measure (D22); PULSE comes from reading.pulse for
 * both blood-pressure and pulse-ox rows.
 */
public class JdbcReadingRollupRepository implements ReadingRollupRepository {

    private final JdbcClient jdbc;

    public JdbcReadingRollupRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public int refresh(Instant since) {
        // Whole hours only: starting mid-hour would overwrite an hour's row with part of its readings.
        return jdbc.sql("""
                        INSERT INTO reading_rollup AS r (patient_id, measure_code, hour_start, n, min_value, max_value,
                                                         avg_value, updated_at)
                        SELECT patient_id, measure_code, hour_start, count(*), min(v), max(v), round(avg(v), 2), now()
                        FROM (SELECT rd.patient_id, m.measure_code, date_trunc('hour', rd.measured_at, 'UTC') AS hour_start,
                                     CASE m.measure_code WHEN 1 THEN rd.systolic WHEN 2 THEN rd.diastolic
                                                         WHEN 3 THEN rd.pulse ELSE rd.value_num END::numeric(8,2) AS v
                              FROM reading rd JOIN reading_type_measure m ON m.reading_type = rd.reading_type
                              WHERE rd.measured_at >= date_trunc('hour', ?, 'UTC')) x
                        WHERE v IS NOT NULL
                        GROUP BY patient_id, measure_code, hour_start
                        ON CONFLICT (patient_id, measure_code, hour_start) DO UPDATE
                        SET n = EXCLUDED.n, min_value = EXCLUDED.min_value, max_value = EXCLUDED.max_value,
                            avg_value = EXCLUDED.avg_value, updated_at = EXCLUDED.updated_at
                        WHERE (r.n, r.min_value, r.max_value, r.avg_value)
                              IS DISTINCT FROM (EXCLUDED.n, EXCLUDED.min_value, EXCLUDED.max_value, EXCLUDED.avg_value)
                        """)
                .param(since.atOffset(ZoneOffset.UTC))
                .update();
    }

    @Override
    public List<ReadingRollup> findSince(long patientId, Instant since) {
        return jdbc.sql("""
                        SELECT measure_code, hour_start, n, min_value, max_value, avg_value
                        FROM reading_rollup
                        WHERE patient_id = ? AND hour_start >= date_trunc('hour', ?, 'UTC')
                        ORDER BY measure_code, hour_start
                        """)
                .params(patientId, since.atOffset(ZoneOffset.UTC))
                .query((rs, n) -> new ReadingRollup(Measure.fromCode(rs.getShort("measure_code")),
                        rs.getObject("hour_start", OffsetDateTime.class).toInstant(), rs.getInt("n"),
                        rs.getBigDecimal("min_value"), rs.getBigDecimal("max_value"), rs.getBigDecimal("avg_value")))
                .list();
    }
}
