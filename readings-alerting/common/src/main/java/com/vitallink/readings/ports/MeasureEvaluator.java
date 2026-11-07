package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Breach;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;

import java.util.List;

/** Strategy: one per reading type */
public interface MeasureEvaluator {
    ReadingType supports();

    /** Most severe band crossed per measure; empty when no band is crossed. */
    List<Breach> evaluate(Reading reading, EffectiveThresholds thresholds);
}
