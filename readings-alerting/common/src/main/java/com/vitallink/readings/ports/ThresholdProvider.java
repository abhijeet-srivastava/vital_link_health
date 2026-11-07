package com.vitallink.readings.ports;

import com.vitallink.readings.domain.EffectiveThresholds;

import java.util.List;

/** CachingThresholdProvider decorates DbThresholdProvider */
public interface ThresholdProvider {
    /** Patient overrides replace program defaults per measure; several programs -> most conservative. */
    EffectiveThresholds forPatients(List<Long> patientIds);
}
