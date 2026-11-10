package com.vitallink.readings.ports;

import com.vitallink.readings.domain.PlausibilityRules;

public interface MeasureCatalog {
    /** Plausibility bounds per measure, from the measure table (reference data; changes only by migration). */
    PlausibilityRules plausibilityRules();
}
