package com.vitallink.readings.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Bands in force per patient and measure, after overrides and the multi-program policy have been applied. */
public record EffectiveThresholds(Map<Long, Map<Measure, List<ThresholdBand>>> byPatient) {

    public EffectiveThresholds {
        byPatient = Map.copyOf(byPatient);
    }

    public List<ThresholdBand> bandsFor(long patientId, Measure measure) {
        return byPatient.getOrDefault(patientId, Map.of()).getOrDefault(measure, List.of());
    }

    /** The most severe band the value crosses for this patient, as a breach; empty when inside every band. */
    public Optional<Breach> breachOf(long patientId, Measure measure, BigDecimal value) {
        if (value == null) {
            return Optional.empty();
        }
        return AlertDecisions.mostSevere(bandsFor(patientId, measure), value)
                .map(band -> new Breach(measure, value, band));
    }
}
