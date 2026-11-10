package com.vitallink.readings.alerting.evaluator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.domain.Breach;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.domain.ThresholdBand;
import com.vitallink.readings.domain.ThresholdBand.Operator;

class MeasureEvaluatorsTest {

    static final long PATIENT = 42;

    static final ThresholdBand SYS_CRITICAL = band(1, Measure.SYSTOLIC, Operator.GT, 180, Severity.CRITICAL);
    static final ThresholdBand DIA_HIGH = band(2, Measure.DIASTOLIC, Operator.GT, 100, Severity.HIGH);
    static final ThresholdBand PULSE_HIGH = band(3, Measure.PULSE, Operator.GT, 120, Severity.HIGH);
    static final ThresholdBand GLUCOSE_HIGH = band(4, Measure.GLUCOSE, Operator.GT, 250, Severity.HIGH);
    static final ThresholdBand SPO2_LOW = band(5, Measure.SPO2, Operator.LT, 92, Severity.HIGH);
    static final ThresholdBand SPO2_CRITICAL = band(6, Measure.SPO2, Operator.LT, 88, Severity.CRITICAL);
    static final ThresholdBand WEIGHT_HIGH = band(7, Measure.WEIGHT, Operator.GT, 150, Severity.MEDIUM);

    static final EffectiveThresholds THRESHOLDS = new EffectiveThresholds(Map.of(PATIENT, Map.of(
            Measure.SYSTOLIC, List.of(SYS_CRITICAL),
            Measure.DIASTOLIC, List.of(DIA_HIGH),
            Measure.PULSE, List.of(PULSE_HIGH),
            Measure.GLUCOSE, List.of(GLUCOSE_HIGH),
            Measure.SPO2, List.of(SPO2_LOW, SPO2_CRITICAL),
            Measure.WEIGHT, List.of(WEIGHT_HIGH))));

    final MeasureEvaluators evaluators = MeasureEvaluators.standard();

    @Test
    void blood_pressure_splits_into_systolic_diastolic_and_pulse() {
        List<Breach> breaches = evaluators.evaluate(bp(186, 105, 130), THRESHOLDS);

        assertThat(breaches).extracting(Breach::measure)
                .containsExactlyInAnyOrder(Measure.SYSTOLIC, Measure.DIASTOLIC, Measure.PULSE);
        assertThat(breaches).filteredOn(b -> b.measure() == Measure.SYSTOLIC).singleElement()
                .satisfies(b -> {
                    assertThat(b.band()).isEqualTo(SYS_CRITICAL);
                    assertThat(b.observedValue()).isEqualByComparingTo("186");
                });
    }

    @Test
    void blood_pressure_inside_every_band_gives_nothing() {
        assertThat(evaluators.evaluate(bp(120, 80, 70), THRESHOLDS)).isEmpty();
    }

    @Test
    void pulse_ox_gives_spo2_and_pulse_when_present() {
        List<Breach> both = evaluators.evaluate(reading(ReadingType.PULSE_OX, null, null, 130, "86"), THRESHOLDS);
        List<Breach> spo2Only = evaluators.evaluate(reading(ReadingType.PULSE_OX, null, null, null, "90"), THRESHOLDS);

        assertThat(both).extracting(Breach::band).containsExactlyInAnyOrder(SPO2_CRITICAL, PULSE_HIGH);
        assertThat(spo2Only).extracting(Breach::band).as("LT band, most severe crossed").containsExactly(SPO2_LOW);
    }

    @Test
    void glucose_and_weight_read_value_num() {
        assertThat(evaluators.evaluate(reading(ReadingType.GLUCOSE, null, null, null, "260"), THRESHOLDS))
                .extracting(Breach::band).containsExactly(GLUCOSE_HIGH);
        assertThat(evaluators.evaluate(reading(ReadingType.WEIGHT, null, null, null, "151.5"), THRESHOLDS))
                .extracting(Breach::band).containsExactly(WEIGHT_HIGH);
    }

    @Test
    void patient_without_thresholds_gives_nothing() {
        assertThat(evaluators.evaluate(bp(250, 140, 160), new EffectiveThresholds(Map.of()))).isEmpty();
    }

    @Test
    void every_reading_type_needs_exactly_one_strategy() {
        assertThatThrownBy(() -> new MeasureEvaluators(List.of(new BloodPressureEvaluator())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("GLUCOSE");
        assertThatThrownBy(() -> new MeasureEvaluators(List.of(new BloodPressureEvaluator(),
                new BloodPressureEvaluator())))
                .isInstanceOf(IllegalStateException.class);
    }

    static Reading bp(int systolic, int diastolic, int pulse) {
        return reading(ReadingType.BLOOD_PRESSURE, systolic, diastolic, pulse, null);
    }

    static Reading reading(ReadingType type, Integer systolic, Integer diastolic, Integer pulse, String value) {
        return new Reading(9, UUID.randomUUID(), Instant.parse("2026-11-08T06:00:00Z"), PATIENT, type, 1L,
                systolic, diastolic, pulse, value == null ? null : new BigDecimal(value), 0L);
    }

    static ThresholdBand band(long id, Measure measure, Operator op, int limit, Severity severity) {
        return new ThresholdBand(id, measure, op, BigDecimal.valueOf(limit), severity);
    }
}
