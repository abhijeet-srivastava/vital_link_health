package com.vitallink.readings.ports;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.StateChange;

/**
 * Alert state transitions as single conditional UPDATEs (patterns 4 and 5). Each returns the committed changes; an
 * empty result means the guard did not match. Must run inside the caller's transaction.
 */
public interface AlertStateRepository {

    Optional<StateChange> acknowledge(long alertId, long coachId, int expectedVersion, Instant now);

    /** Zero rows also when the alert is CRITICAL and the note is blank; the caller tells the cases apart. */
    Optional<StateChange> resolve(long alertId, long coachId, int expectedVersion, String note, Instant now);

    /** SKIP LOCKED claim of alerts with escalate_at <= now; OPEN -> ESCALATED with escalate_at cleared. */
    List<StateChange> escalateDue(Instant now, int limit);

    /** Moves every unresolved alert of the patient not already owned by newCoachId. */
    List<StateChange> reassignUnresolved(long patientId, long newCoachId);

    Optional<Alert> find(long alertId);
}
