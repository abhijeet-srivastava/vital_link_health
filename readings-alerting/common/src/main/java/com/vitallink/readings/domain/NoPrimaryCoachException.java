package com.vitallink.readings.domain;

/**
 * A breach for a patient with no current PRIMARY coach: alert.coach_id cannot be filled. Retried (the care-team
 * projection may be catching up), then dead-lettered; never dropped. The message carries the ID only.
 */
public class NoPrimaryCoachException extends RuntimeException {

    public NoPrimaryCoachException(long patientId) {
        super("No current PRIMARY coach for patient_id=" + patientId);
    }
}
