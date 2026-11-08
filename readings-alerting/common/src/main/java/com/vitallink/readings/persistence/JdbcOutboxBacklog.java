package com.vitallink.readings.persistence;

import java.time.Duration;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.OutboxBacklog;

/**
 * Samples the outbox backlog for ingest backpressure and the relay gauges. The count stops at {@code rowCap + 1} and
 * the age reads min(created_at) through ix_outbox_created, so a sample stays cheap during a long Kafka outage.
 */
public class JdbcOutboxBacklog {

    private final JdbcClient jdbc;

    public JdbcOutboxBacklog(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public OutboxBacklog sample(long rowCap) {
        return jdbc.sql("""
                        SELECT (SELECT count(*) FROM (SELECT 1 FROM outbox_event LIMIT ?) b) AS backlog_rows,
                               coalesce(extract(epoch FROM now() - (SELECT min(created_at) FROM outbox_event)), 0)
                                   AS oldest_age_seconds
                        """)
                .param(rowCap + 1)
                .query((rs, i) -> new OutboxBacklog(rs.getLong("backlog_rows"),
                        Duration.ofMillis(Math.round(rs.getDouble("oldest_age_seconds") * 1000))))
                .single();
    }
}
