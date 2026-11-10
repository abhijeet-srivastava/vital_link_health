package com.vitallink.readings.query.security;

import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import com.vitallink.readings.domain.AlertNotFoundException;

/**
 * First access check, in the controller: scope, kind of caller and, for patient data, the coach's current
 * care_assignment (PRIMARY or BACKUP, open, started). Row-level security repeats the panel check in the database
 * (RlsScope). The panel is read on the primary, so a panel change takes effect on the next request even while a
 * replica lags. Every refusal writes a DENIED audit row before it throws.
 */
@Component
public class PanelAccessGuard {

    private final JdbcClient primary;
    private final AccessAudit audit;

    public PanelAccessGuard(JdbcClient primary, AccessAudit audit) {
        this.primary = primary;
        this.audit = audit;
    }

    /** A coach with {@code scope} reading or changing data of a patient in their panel. */
    public void requirePatient(Caller caller, String scope, long patientId, String action, String resource,
                               UUID requestId) {
        requireCoachScope(caller, scope, patientId, action, resource, requestId);
        if (!inPanel(caller.coachId(), patientId)) {
            deny(caller, action, patientId, resource, requestId, "Patient is not in the coach's panel");
        }
    }

    /** A coach reading their own queue; the queue of another coach is refused. */
    public void requireOwnQueue(Caller caller, long coachId, String action, String resource, UUID requestId) {
        requireCoachScope(caller, Scopes.ALERTS_READ, null, action, resource, requestId);
        if (caller.coachId() != coachId) {
            deny(caller, action, null, resource, requestId, "Coaches can read only their own queue");
        }
    }

    /**
     * A coach acting on an alert of a patient in their panel. The alert's patient comes from alert_patient_id(),
     * which can see rows the coach's policy hides, so a refusal is audited against the right patient.
     *
     * @throws AlertNotFoundException 404 when no such alert exists
     */
    public void requireAlert(Caller caller, String scope, long alertId, String action, UUID requestId) {
        String resource = "alert/" + alertId;
        requireCoachScope(caller, scope, null, action, resource, requestId);
        Long patientId = primary.sql("SELECT alert_patient_id(?)").param(alertId).query(Long.class).optional()
                .orElse(null);
        if (patientId == null) {
            throw new AlertNotFoundException(alertId);
        }
        if (!inPanel(caller.coachId(), patientId)) {
            deny(caller, action, patientId, resource, requestId, "Patient is not in the coach's panel");
        }
    }

    /** Any caller holding {@code scope} (clinician and service endpoints, which have no panel). */
    public void requireScope(Caller caller, String scope, Long patientId, String action, String resource,
                             UUID requestId) {
        if (!caller.has(scope)) {
            deny(caller, action, patientId, resource, requestId, "Missing scope " + scope);
        }
    }

    private void requireCoachScope(Caller caller, String scope, Long patientId, String action, String resource,
                                   UUID requestId) {
        requireScope(caller, scope, patientId, action, resource, requestId);
        if (!caller.isCoach()) {
            // Service-to-service reads are in the contract but not wired yet: they would need a pool without the
            // coach policy. Refused (and audited) rather than served without a panel.
            deny(caller, action, patientId, resource, requestId, "Only coaches can use this endpoint");
        }
    }

    private boolean inPanel(long coachId, long patientId) {
        return primary.sql("""
                        SELECT EXISTS (SELECT 1 FROM care_assignment
                                       WHERE coach_id = ? AND patient_id = ? AND valid_to IS NULL
                                         AND valid_from <= now())
                        """)
                .params(coachId, patientId)
                .query(Boolean.class)
                .single();
    }

    private void deny(Caller caller, String action, Long patientId, String resource, UUID requestId, String why) {
        audit.denied(caller, action, patientId, resource, requestId);
        throw new AccessDeniedProblem(why);
    }
}
