package com.vitallink.readings.alerts;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.AlertConflict;
import com.vitallink.readings.domain.AlertNotFoundException;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.ResolutionNoteRequiredException;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.domain.StateChange;
import com.vitallink.readings.ports.AlertService;
import com.vitallink.readings.ports.AlertStateRepository;
import com.vitallink.readings.ports.AuditWriter;
import com.vitallink.readings.ports.OutboxWriter;

/**
 * Pattern 4: one conditional UPDATE per coach action, then audit_log + alert.state_changed in the same transaction.
 * Zero rows re-reads the alert to say why: unknown (404), CRITICAL without a note (422) or changed meanwhile (409
 * with the current state).
 */
public class DefaultAlertService implements AlertService {

    private final AlertStateRepository alerts;
    private final OutboxWriter outbox;
    private final AuditWriter audit;
    private final TransactionTemplate tx;
    private final StateChangeRecords records;
    private final Clock clock;

    public DefaultAlertService(AlertStateRepository alerts, OutboxWriter outbox, AuditWriter audit,
                               TransactionTemplate tx, StateChangeRecords records, Clock clock) {
        this.alerts = alerts;
        this.outbox = outbox;
        this.audit = audit;
        this.tx = tx;
        this.records = records;
        this.clock = clock;
    }

    @Override
    public Alert acknowledge(long alertId, long coachId, int expectedVersion, UUID requestId) {
        Instant now = clock.instant();
        return tx.execute(status -> alerts.acknowledge(alertId, coachId, expectedVersion, now)
                .map(change -> commit(change, requestId))
                .orElseThrow(() -> new AlertConflict(current(alertId))));
    }

    @Override
    public Alert resolve(long alertId, long coachId, int expectedVersion, String note, UUID requestId) {
        String trimmedNote = note == null || note.isBlank() ? null : note.strip();
        Instant now = clock.instant();
        return tx.execute(status -> alerts.resolve(alertId, coachId, expectedVersion, trimmedNote, now)
                .map(change -> commit(change, requestId))
                .orElseThrow(() -> {
                    Alert current = current(alertId);
                    boolean wouldResolve = current.version() == expectedVersion
                            && current.status() != AlertStatus.RESOLVED;
                    if (wouldResolve && current.severity() == Severity.CRITICAL && trimmedNote == null) {
                        return new ResolutionNoteRequiredException(alertId);
                    }
                    return new AlertConflict(current);
                }));
    }

    private Alert commit(StateChange change, UUID requestId) {
        StateChangeRecords.Rows rows = records.rows(List.of(change), requestId);
        outbox.append(rows.events());
        audit.recordAll(rows.audits());
        return change.alert();
    }

    private Alert current(long alertId) {
        return alerts.find(alertId).orElseThrow(() -> new AlertNotFoundException(alertId));
    }
}
