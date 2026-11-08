package com.vitallink.readings.ports;

import java.time.Instant;

public interface CareAssignmentRepository {

    /**
     * Makes coachId the patient's current PRIMARY from {@code at}: closes the open PRIMARY row of another coach and
     * inserts the new one. Returns false when coachId already is the current PRIMARY (replay). ux_primary_coach and
     * the no-overlap EXCLUDE constraint reject a concurrent conflicting change.
     */
    boolean replacePrimary(long patientId, long coachId, Instant at);
}
