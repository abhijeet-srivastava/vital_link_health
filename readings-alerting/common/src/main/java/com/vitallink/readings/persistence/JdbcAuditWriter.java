package com.vitallink.readings.persistence;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.AuditEntry;
import com.vitallink.readings.ports.AuditWriter;

public class JdbcAuditWriter implements AuditWriter {

    private final JdbcClient jdbc;

    public JdbcAuditWriter(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void record(AuditEntry e) {
        jdbc.sql("""
                        INSERT INTO audit_log (audit_id, at, actor_type, actor_id, action, outcome, patient_id, resource,
                                               request_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """)
                .params(e.auditId(), JdbcReadingRepository.utc(e.at()), e.actorType().name(), e.actorId(), e.action(),
                        e.outcome().name(), e.patientId(), e.resource(), e.requestId())
                .update();
    }
}
