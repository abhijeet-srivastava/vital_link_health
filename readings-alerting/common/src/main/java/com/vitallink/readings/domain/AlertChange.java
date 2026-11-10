package com.vitallink.readings.domain;

import java.math.BigDecimal;

/** A change a coach must hear about: a new episode or a severity upgrade. Becomes one alerts.raised event. */
public record AlertChange(long alertId, long patientId, long coachId, Measure measure, BigDecimal observedValue,
                          Severity severity, Reason reason, long thresholdId, int occurrenceCnt, boolean late) {

    public enum Reason {
        RAISED,
        SEVERITY_UPGRADED
    }
}
