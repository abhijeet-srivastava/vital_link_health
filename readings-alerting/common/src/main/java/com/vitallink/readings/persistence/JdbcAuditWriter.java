package com.vitallink.readings.persistence;

import java.util.List;

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

    @Override
    public void recordAll(List<AuditEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        jdbc.sql("""
                        INSERT INTO audit_log (audit_id, at, actor_type, actor_id, action, outcome, patient_id, resource,
                                               request_id)
                        SELECT * FROM unnest(?::uuid[], ?::timestamptz[], ?::text[], ?::text[], ?::text[], ?::text[],
                                             ?::bigint[], ?::text[], ?::uuid[])
                        """)
                .params(entries.stream().map(e -> e.auditId().toString()).toArray(String[]::new),
                        entries.stream().map(e -> e.at().toString()).toArray(String[]::new),
                        entries.stream().map(e -> e.actorType().name()).toArray(String[]::new),
                        entries.stream().map(AuditEntry::actorId).toArray(String[]::new),
                        entries.stream().map(AuditEntry::action).toArray(String[]::new),
                        entries.stream().map(e -> e.outcome().name()).toArray(String[]::new),
                        entries.stream().map(AuditEntry::patientId).toArray(Long[]::new),
                        entries.stream().map(AuditEntry::resource).toArray(String[]::new),
                        entries.stream().map(e -> e.requestId() == null ? null : e.requestId().toString())
                                .toArray(String[]::new))
                .update();
    }
}
