package com.vitallink.readings.persistence;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.AlertDecisions;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.MultiProgramPolicy;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.domain.ThresholdBand;
import com.vitallink.readings.ports.ThresholdProvider;

/**
 * One query for all requested patients: their current overrides plus the current defaults of every program they are
 * enrolled in now. Precedence and the multi-program policy are applied by {@link AlertDecisions#effectiveBands}.
 */
public class DbThresholdProvider implements ThresholdProvider {

    private final JdbcClient jdbc;
    private final Clock clock;
    private final MultiProgramPolicy policy;

    public DbThresholdProvider(JdbcClient jdbc, Clock clock, MultiProgramPolicy policy) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.policy = policy;
    }

    private record Row(long patientId, Long programId, ThresholdBand band) {
    }

    @Override
    public EffectiveThresholds forPatients(List<Long> patientIds) {
        if (patientIds.isEmpty()) {
            return new EffectiveThresholds(Map.of());
        }
        OffsetDateTime now = OffsetDateTime.now(clock).withOffsetSameInstant(ZoneOffset.UTC);
        // Program rows come in enrollment order (FIRST_ENROLLED depends on it); override rows have no program.
        List<Row> rows = jdbc.sql("""
                        SELECT tr.patient_id, NULL::bigint AS program_id, NULL::timestamptz AS enrolled_from,
                               tr.threshold_id, tr.measure_code, tr.operator, tr.limit_value, tr.severity
                        FROM threshold_rule tr
                        WHERE tr.patient_id = ANY(?::bigint[]) AND tr.valid_to IS NULL
                        UNION ALL
                        SELECT e.patient_id, e.program_id, e.valid_from,
                               tr.threshold_id, tr.measure_code, tr.operator, tr.limit_value, tr.severity
                        FROM enrollment e
                        JOIN threshold_rule tr ON tr.program_id = e.program_id AND tr.valid_to IS NULL
                        WHERE e.patient_id = ANY(?::bigint[])
                          AND e.valid_from <= ? AND (e.valid_to IS NULL OR e.valid_to > ?)
                        ORDER BY 1, 3 NULLS FIRST, 2, 4
                        """)
                .params(patientIds.toArray(Long[]::new), patientIds.toArray(Long[]::new), now, now)
                .query((rs, n) -> new Row(rs.getLong("patient_id"), (Long) rs.getObject("program_id"),
                        new ThresholdBand(rs.getLong("threshold_id"), Measure.fromCode(rs.getInt("measure_code")),
                                ThresholdBand.Operator.valueOf(rs.getString("operator")),
                                rs.getBigDecimal("limit_value"), Severity.valueOf(rs.getString("severity")))))
                .list();

        Map<Long, List<ThresholdBand>> overrides = new HashMap<>();
        Map<Long, LinkedHashMap<Long, List<ThresholdBand>>> programs = new HashMap<>();
        for (Row r : rows) {
            if (r.programId() == null) {
                overrides.computeIfAbsent(r.patientId(), p -> new ArrayList<>()).add(r.band());
            } else {
                programs.computeIfAbsent(r.patientId(), p -> new LinkedHashMap<>())
                        .computeIfAbsent(r.programId(), p -> new ArrayList<>()).add(r.band());
            }
        }
        Map<Long, Map<Measure, List<ThresholdBand>>> byPatient = new HashMap<>();
        for (Long patientId : patientIds) {   // every requested patient gets an entry, so "no bands" can be cached
            byPatient.put(patientId, AlertDecisions.effectiveBands(
                    overrides.getOrDefault(patientId, List.of()),
                    List.copyOf(programs.getOrDefault(patientId, new LinkedHashMap<>()).values()),
                    policy));
        }
        return new EffectiveThresholds(byPatient);
    }
}
