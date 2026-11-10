package com.vitallink.readings.domain;

import java.math.BigDecimal;

/** One measure of a reading outside its bands: the most severe band crossed (rule 8). */
public record Breach(Measure measure, BigDecimal observedValue, ThresholdBand band) {

    public Severity severity() {
        return band.severity();
    }
}
