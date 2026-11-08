package com.vitallink.readings.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.NewBand;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.domain.ThresholdBand;
import com.vitallink.readings.domain.ThresholdChange;
import com.vitallink.readings.ports.ThresholdRepository;

/**
 * Threshold rules are immutable versions: a change closes the open rows (valid_to) and inserts new ones, so every
 * alert_occurrence keeps pointing at the exact rule that fired. ux_rule_patient allows one open row per (patient,
 * measure, operator, severity). Callers run this in a SERIALIZABLE transaction: two concurrent replacements with
 * different bands would otherwise both insert and leave a merged set open.
 */
public class JdbcThresholdRepository implements ThresholdRepository {

    private static final Comparator<NewBand> ORDER = Comparator.comparing((NewBand b) -> b.operator())
            .thenComparing(NewBand::severity).thenComparing(NewBand::limit);

    private final JdbcClient jdbc;

    public JdbcThresholdRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<ThresholdChange> replacePatientBands(long patientId, Measure measure, List<NewBand> bands,
                                                         String createdBy, Instant now) {
        List<NewBand> open = jdbc.sql("""
                        SELECT operator, limit_value, severity FROM threshold_rule
                        WHERE patient_id = ? AND measure_code = ? AND valid_to IS NULL
                        """)
                .params(patientId, measure.code())
                .query((rs, n) -> new NewBand(ThresholdBand.Operator.valueOf(rs.getString("operator")),
                        rs.getBigDecimal("limit_value"), Severity.valueOf(rs.getString("severity"))))
                .list();
        if (sameBands(open, bands)) {
            return Optional.empty();
        }
        List<Long> closed = jdbc.sql("""
                        UPDATE threshold_rule SET valid_to = ?
                        WHERE patient_id = ? AND measure_code = ? AND valid_to IS NULL
                        RETURNING threshold_id
                        """)
                .params(JdbcReadingRepository.utc(now), patientId, measure.code())
                .query(Long.class)
                .list();
        List<Long> inserted = bands.isEmpty() ? List.of() : jdbc.sql("""
                        INSERT INTO threshold_rule (patient_id, measure_code, operator, limit_value, severity, valid_from,
                                                    created_by)
                        SELECT ?, ?, op, lim, sev, ?, ?
                        FROM unnest(?::text[], ?::numeric[], ?::text[]) AS b(op, lim, sev)
                        RETURNING threshold_id
                        """)
                .params(patientId, measure.code(), JdbcReadingRepository.utc(now), createdBy,
                        bands.stream().map(b -> b.operator().name()).toArray(String[]::new),
                        bands.stream().map(b -> b.limit().toPlainString()).toArray(String[]::new),
                        bands.stream().map(b -> b.severity().name()).toArray(String[]::new))
                .query(Long.class)
                .list();
        return Optional.of(new ThresholdChange(patientId, measure, closed, inserted));
    }

    private static boolean sameBands(List<NewBand> a, List<NewBand> b) {
        if (a.size() != b.size()) {
            return false;
        }
        List<NewBand> x = a.stream().sorted(ORDER).toList();
        List<NewBand> y = b.stream().sorted(ORDER).toList();
        for (int i = 0; i < x.size(); i++) {
            NewBand l = x.get(i);
            NewBand r = y.get(i);
            if (l.operator() != r.operator() || l.severity() != r.severity() || !sameValue(l.limit(), r.limit())) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameValue(BigDecimal l, BigDecimal r) {
        return l.compareTo(r) == 0;
    }
}
