package com.vitallink.readings.alerts;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.domain.StateChange;
import com.vitallink.readings.ports.AlertStateRepository;
import com.vitallink.readings.ports.AuditWriter;
import com.vitallink.readings.ports.CareAssignmentRepository;
import com.vitallink.readings.ports.OutboxWriter;

/**
 * Panel change: the new PRIMARY coach takes over the patient's unresolved alerts. One transaction moves the
 * care_assignment row and the alerts, and writes one REASSIGNED event and audit row per moved alert. Replaying the
 * same change moves nothing and announces nothing. The version bump makes an older click on a moved alert a 409.
 */
public class ReassignmentService {

    private final CareAssignmentRepository assignments;
    private final AlertStateRepository alerts;
    private final OutboxWriter outbox;
    private final AuditWriter audit;
    private final TransactionTemplate tx;
    private final StateChangeRecords records;
    private final Clock clock;

    public ReassignmentService(CareAssignmentRepository assignments, AlertStateRepository alerts, OutboxWriter outbox,
                               AuditWriter audit, TransactionTemplate tx, StateChangeRecords records, Clock clock) {
        this.assignments = assignments;
        this.alerts = alerts;
        this.outbox = outbox;
        this.audit = audit;
        this.tx = tx;
        this.records = records;
        this.clock = clock;
    }

    /** Returns the number of alerts moved to newCoachId. */
    public int reassign(long patientId, long newCoachId, UUID requestId) {
        Integer moved = tx.execute(status -> {
            assignments.replacePrimary(patientId, newCoachId, clock.instant());
            List<StateChange> changes = alerts.reassignUnresolved(patientId, newCoachId);
            StateChangeRecords.Rows rows = records.rows(changes, requestId);
            outbox.append(rows.events());
            audit.recordAll(rows.audits());
            return changes.size();
        });
        return moved == null ? 0 : moved;
    }
}
