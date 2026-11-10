package com.vitallink.readings.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Page;
import com.vitallink.readings.domain.QueueCursor;
import com.vitallink.readings.ports.AlertQueueRepository;

/**
 * Coach queue on the partial index ix_alert_queue (coach_id, severity_rank, created_at, alert_id) WHERE status <>
 * 'RESOLVED': most severe first, then oldest, alert_id as the unique tie-breaker (D21), keyset by row comparison.
 */
public class JdbcAlertQueueRepository implements AlertQueueRepository {

    private final JdbcClient jdbc;

    public JdbcAlertQueueRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Page<Alert> queue(long coachId, Optional<AlertStatus> status, QueueCursor after, int limit) {
        if (status.filter(s -> s == AlertStatus.RESOLVED).isPresent()) {
            throw new IllegalArgumentException("The queue holds unresolved alerts only");
        }
        // Fixed SQL fragments only (no "? IS NULL OR ..."), so every variant keeps an index range scan.
        StringBuilder where = new StringBuilder("a.coach_id = ? AND a.status <> 'RESOLVED'");
        List<Object> params = new ArrayList<>();
        params.add(coachId);
        status.ifPresent(s -> {
            where.append(" AND a.status = ?");
            params.add(s.name());
        });
        if (after != null) {
            where.append(" AND (a.severity_rank, a.created_at, a.alert_id) > (?, ?, ?)");
            params.add(after.severityRank());
            params.add(JdbcReadingRepository.utc(after.createdAt()));
            params.add(after.alertId());
        }
        params.add(limit + 1);
        List<Alert> rows = jdbc.sql("""
                        SELECT %s FROM alert a
                        WHERE %s
                        ORDER BY a.severity_rank, a.created_at, a.alert_id
                        LIMIT ?
                        """.formatted(JdbcAlertStateRepository.COLUMNS, where))
                .params(params)
                .query((rs, n) -> JdbcAlertStateRepository.alert(rs))
                .list();
        return Page.of(rows, limit);
    }
}
