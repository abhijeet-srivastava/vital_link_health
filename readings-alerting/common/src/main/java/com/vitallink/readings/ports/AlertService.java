package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.AlertChange;
import com.vitallink.readings.domain.BatchOutcome;
import com.vitallink.readings.domain.BreachWithReading;

import java.util.List;
import java.util.Optional;

public interface AlertService {
    /** Set-based, one transaction for the whole batch; throws DuplicateKeyException to trigger the per-event fallback. */
    BatchOutcome raiseBatch(List<BreachWithReading> breaches);

    /** Per-event path; a duplicate occurrence means "already evaluated" and returns empty. */
    Optional<AlertChange> raise(BreachWithReading breach);

    Alert acknowledge(long alertId, long coachId, int expectedVersion);           // 409 on version mismatch

    Alert resolve(long alertId, long coachId, int expectedVersion, String note);  // note required for CRITICAL
}
