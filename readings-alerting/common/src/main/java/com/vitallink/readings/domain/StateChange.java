package com.vitallink.readings.domain;

/**
 * One alert state change, after the UPDATE: becomes one audit_log row and one alert.state_changed outbox row.
 *
 * @param alert           the alert as committed (its status is the target state)
 * @param from            status before the change; equals the target for REASSIGNED
 * @param previousCoachId the owner before a REASSIGNED change, otherwise null
 */
public record StateChange(Alert alert, AlertStatus from, Reason reason, AuditEntry.ActorType actorType,
                          String actorId, Long previousCoachId) {

    public enum Reason {
        ACKNOWLEDGED,
        RESOLVED,
        ESCALATED,
        REASSIGNED
    }

    /** audit_log.action per reason. */
    public String auditAction() {
        return switch (reason) {
            case ACKNOWLEDGED -> "ACK_ALERT";
            case RESOLVED -> "RESOLVE_ALERT";
            case ESCALATED -> "ESCALATE_ALERT";
            case REASSIGNED -> "REASSIGN_ALERT";
        };
    }
}
