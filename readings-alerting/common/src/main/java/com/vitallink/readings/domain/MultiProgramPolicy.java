package com.vitallink.readings.domain;

/**
 * Patient in several programs without an override (open clinical decision, vitallink.alerting.multi-program-policy).
 * MOST_CONSERVATIVE evaluates every program default and keeps the most severe result; FIRST_ENROLLED uses the
 * program the patient joined first that has bands for the measure.
 */
public enum MultiProgramPolicy {
    MOST_CONSERVATIVE,
    FIRST_ENROLLED
}
