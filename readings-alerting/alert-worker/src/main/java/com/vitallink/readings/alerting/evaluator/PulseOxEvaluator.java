package com.vitallink.readings.alerting.evaluator;

import static com.vitallink.readings.alerting.evaluator.BloodPressureEvaluator.decimal;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.vitallink.readings.domain.Breach;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.ports.MeasureEvaluator;

/**
 * Pulse oximeter row -> SPO2 from value_num, PULSE from pulse when present (reading_type_measure 3). PULSE uses the
 * same bands as a BP cuff's pulse (open clinical decision).
 */
public class PulseOxEvaluator implements MeasureEvaluator {

    @Override
    public ReadingType supports() {
        return ReadingType.PULSE_OX;
    }

    @Override
    public List<Breach> evaluate(Reading r, EffectiveThresholds thresholds) {
        return Stream.of(
                        thresholds.breachOf(r.patientId(), Measure.SPO2, r.valueNum()),
                        thresholds.breachOf(r.patientId(), Measure.PULSE, decimal(r.pulse())))
                .flatMap(Optional::stream)
                .toList();
    }
}
