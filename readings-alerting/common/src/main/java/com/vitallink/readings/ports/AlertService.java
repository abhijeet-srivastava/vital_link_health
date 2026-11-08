package com.vitallink.readings.ports;

import java.util.UUID;

import com.vitallink.readings.domain.Alert;

/**
 * Coach actions on an alert (guard 8). Each is one conditional UPDATE on version + legal source states, with the
 * audit_log row and the alert.state_changed outbox row in the same transaction. Raising alerts is the evaluator's
 * job (AlertEvaluator, BatchAlertEvaluator), not this port's.
 *
 * @see com.vitallink.readings.domain.AlertConflict 409: stale version or illegal source state, carries current state
 * @see com.vitallink.readings.domain.AlertNotFoundException 404
 */
public interface AlertService {

    /** OPEN or ESCALATED -> ACKNOWLEDGED; clears escalate_at, so an acknowledged alert can never escalate. */
    Alert acknowledge(long alertId, long coachId, int expectedVersion, UUID requestId);

    /**
     * Any unresolved -> RESOLVED; clears escalate_at.
     *
     * @throws com.vitallink.readings.domain.ResolutionNoteRequiredException 422: CRITICAL without a note
     */
    Alert resolve(long alertId, long coachId, int expectedVersion, String note, UUID requestId);
}
