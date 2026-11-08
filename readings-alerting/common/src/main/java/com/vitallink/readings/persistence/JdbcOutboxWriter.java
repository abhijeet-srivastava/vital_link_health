package com.vitallink.readings.persistence;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.OutboxEvent;
import com.vitallink.readings.ports.OutboxWriter;

/** Transactional outbox (rule 2): rows are written in the caller's transaction and published later by the relay. */
public class JdbcOutboxWriter implements OutboxWriter {

    private final JdbcClient jdbc;

    public JdbcOutboxWriter(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void append(List<OutboxEvent> events) {
        for (OutboxEvent e : events) {
            jdbc.sql("""
                            INSERT INTO outbox_event (event_id, topic, event_type, aggregate_key, schema_version, payload)
                            VALUES (?, ?, ?, ?, ?, ?::jsonb)
                            """)
                    .params(e.eventId(), e.topic(), e.eventType(), e.aggregateKey(), e.schemaVersion(), e.payloadJson())
                    .update();
        }
    }
}
