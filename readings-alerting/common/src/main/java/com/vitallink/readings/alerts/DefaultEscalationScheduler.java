package com.vitallink.readings.alerts;

import java.time.Instant;
import java.util.List;

import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.domain.StateChange;
import com.vitallink.readings.ports.AlertStateRepository;
import com.vitallink.readings.ports.AuditWriter;
import com.vitallink.readings.ports.EscalationScheduler;
import com.vitallink.readings.ports.OutboxWriter;

/**
 * Pattern 5 / guard 7: claim, escalate, audit and announce in one transaction. Safe on any number of pods: rows
 * another instance holds are skipped, and escalate_at is cleared in the same UPDATE, so each alert escalates once.
 */
public class DefaultEscalationScheduler implements EscalationScheduler {

    private final AlertStateRepository alerts;
    private final OutboxWriter outbox;
    private final AuditWriter audit;
    private final TransactionTemplate tx;
    private final StateChangeRecords records;

    public DefaultEscalationScheduler(AlertStateRepository alerts, OutboxWriter outbox, AuditWriter audit,
                                      TransactionTemplate tx, StateChangeRecords records) {
        this.alerts = alerts;
        this.outbox = outbox;
        this.audit = audit;
        this.tx = tx;
        this.records = records;
    }

    @Override
    public int escalateDue(Instant now, int limit) {
        Integer escalated = tx.execute(status -> {
            List<StateChange> changes = alerts.escalateDue(now, limit);
            StateChangeRecords.Rows rows = records.rows(changes, null);
            outbox.append(rows.events());
            audit.recordAll(rows.audits());
            return changes.size();
        });
        return escalated == null ? 0 : escalated;
    }
}
