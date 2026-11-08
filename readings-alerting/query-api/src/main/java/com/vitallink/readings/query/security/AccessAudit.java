package com.vitallink.readings.query.security;

import java.time.Clock;
import java.util.Collection;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.vitallink.readings.domain.AuditEntry;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.ports.AuditWriter;

/**
 * audit_log rows for coach-facing access (rule 9): ALLOWED after a PHI read succeeds and before the body is written,
 * so no PHI leaves without its row (a failed insert fails the request); DENIED for every refusal, because denials
 * are the misuse signal. Each row commits on its own: outside any transaction, on the primary pool.
 */
@Component
public class AccessAudit {

    private final AuditWriter audit;
    private final UuidV7 ids;
    private final Clock clock;

    public AccessAudit(AuditWriter audit, UuidV7 ids, Clock clock) {
        this.audit = audit;
        this.ids = ids;
        this.clock = clock;
    }

    public void allowed(Caller caller, String action, Long patientId, String resource, UUID requestId) {
        audit.record(entry(caller, action, AuditEntry.Outcome.ALLOWED, patientId, resource, requestId));
    }

    /** One ALLOWED row per patient shown (a queue page spans several patients). */
    public void allowedForEach(Caller caller, String action, Collection<Long> patientIds, String resource,
                               UUID requestId) {
        audit.recordAll(patientIds.stream()
                .map(p -> entry(caller, action, AuditEntry.Outcome.ALLOWED, p, resource, requestId))
                .toList());
    }

    public void denied(Caller caller, String action, Long patientId, String resource, UUID requestId) {
        audit.record(entry(caller, action, AuditEntry.Outcome.DENIED, patientId, resource, requestId));
    }

    private AuditEntry entry(Caller caller, String action, AuditEntry.Outcome outcome, Long patientId,
                             String resource, UUID requestId) {
        return new AuditEntry(ids.next(), clock.instant(), caller.auditActor(), caller.subject(), action, outcome,
                patientId, resource, requestId);
    }
}
