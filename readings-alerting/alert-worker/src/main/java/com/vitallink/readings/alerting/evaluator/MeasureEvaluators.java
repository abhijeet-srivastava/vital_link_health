package com.vitallink.readings.alerting.evaluator;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vitallink.readings.domain.Breach;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.ports.MeasureEvaluator;

/** Strategy registry: exactly one MeasureEvaluator per ReadingType, checked at startup. */
public class MeasureEvaluators {

    private final Map<ReadingType, MeasureEvaluator> byType = new EnumMap<>(ReadingType.class);

    public MeasureEvaluators(List<MeasureEvaluator> evaluators) {
        for (MeasureEvaluator e : evaluators) {
            if (byType.putIfAbsent(e.supports(), e) != null) {
                throw new IllegalStateException("Two evaluators for reading type " + e.supports());
            }
        }
        Set<ReadingType> missing = EnumSet.allOf(ReadingType.class);
        missing.removeAll(byType.keySet());
        if (!missing.isEmpty()) {
            throw new IllegalStateException("No evaluator for reading types " + missing);
        }
    }

    public static MeasureEvaluators standard() {
        return new MeasureEvaluators(List.of(
                new BloodPressureEvaluator(),
                new PulseOxEvaluator(),
                new SingleValueEvaluator(ReadingType.GLUCOSE, Measure.GLUCOSE),
                new SingleValueEvaluator(ReadingType.WEIGHT, Measure.WEIGHT)));
    }

    /** Most severe band crossed per measure of the reading; empty when every value is inside its bands. */
    public List<Breach> evaluate(Reading reading, EffectiveThresholds thresholds) {
        return byType.get(reading.type()).evaluate(reading, thresholds);
    }
}
