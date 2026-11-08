package com.vitallink.readings.alerting.evaluator;

import java.util.List;

import com.vitallink.readings.domain.Breach;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.ports.MeasureEvaluator;

/** Reading types that carry one measure in value_num: glucose and weight. */
public class SingleValueEvaluator implements MeasureEvaluator {

    private final ReadingType type;
    private final Measure measure;

    public SingleValueEvaluator(ReadingType type, Measure measure) {
        this.type = type;
        this.measure = measure;
    }

    @Override
    public ReadingType supports() {
        return type;
    }

    @Override
    public List<Breach> evaluate(Reading r, EffectiveThresholds thresholds) {
        return thresholds.breachOf(r.patientId(), measure, r.valueNum()).stream().toList();
    }
}
