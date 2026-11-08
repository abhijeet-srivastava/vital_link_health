package com.vitallink.readings.alerting.evaluator;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.vitallink.readings.domain.Breach;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.ports.MeasureEvaluator;

/** BP cuff row -> SYSTOLIC, DIASTOLIC and PULSE (reading_type_measure 1). */
public class BloodPressureEvaluator implements MeasureEvaluator {

    @Override
    public ReadingType supports() {
        return ReadingType.BLOOD_PRESSURE;
    }

    @Override
    public List<Breach> evaluate(Reading r, EffectiveThresholds thresholds) {
        return Stream.of(
                        thresholds.breachOf(r.patientId(), Measure.SYSTOLIC, decimal(r.systolic())),
                        thresholds.breachOf(r.patientId(), Measure.DIASTOLIC, decimal(r.diastolic())),
                        thresholds.breachOf(r.patientId(), Measure.PULSE, decimal(r.pulse())))
                .flatMap(Optional::stream)
                .toList();
    }

    static BigDecimal decimal(Integer value) {
        return value == null ? null : BigDecimal.valueOf(value);
    }
}
