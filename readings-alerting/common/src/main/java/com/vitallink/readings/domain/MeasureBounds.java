package com.vitallink.readings.domain;

import java.math.BigDecimal;

/** Device-error guard from the measure table; not a clinical threshold. Bounds are inclusive. */
public record MeasureBounds(Measure measure, String unit, BigDecimal minPlausible, BigDecimal maxPlausible) {

    public boolean contains(BigDecimal value) {
        return value.compareTo(minPlausible) >= 0 && value.compareTo(maxPlausible) <= 0;
    }
}
