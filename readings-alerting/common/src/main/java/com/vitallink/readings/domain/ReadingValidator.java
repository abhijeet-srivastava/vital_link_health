package com.vitallink.readings.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Turns a device's reading into a canonical {@link Reading} or a rejection code. Pure: time and bounds are passed in.
 * Checks, in order: required fields, future timestamp, too old (when enabled), value shape, unit, plausibility.
 */
public final class ReadingValidator {

    /** Glucose mmol/L to mg/dL (molar mass of glucose 180.16 g/mol). */
    static final BigDecimal GLUCOSE_MMOL_TO_MG_DL = new BigDecimal("18.016");

    private enum Column { SYSTOLIC, DIASTOLIC, PULSE, VALUE_NUM }

    private record Field(String name, Measure measure, Column column, boolean required) {
    }

    /** canonicalUnit is assumed when the device sends none; it is the unit stored (measure.unit). */
    private record Spec(List<Field> fields, String canonicalUnit, Set<String> units) {
    }

    private static final Map<ReadingType, Spec> SPECS = Map.of(
            ReadingType.BLOOD_PRESSURE, new Spec(List.of(
                    new Field("systolic", Measure.SYSTOLIC, Column.SYSTOLIC, true),
                    new Field("diastolic", Measure.DIASTOLIC, Column.DIASTOLIC, true),
                    new Field("pulse", Measure.PULSE, Column.PULSE, false)), "mmHg", Set.of("mmHg")),
            ReadingType.GLUCOSE, new Spec(List.of(
                    new Field("glucose", Measure.GLUCOSE, Column.VALUE_NUM, true)), "mg/dL", Set.of("mg/dL", "mmol/L")),
            ReadingType.PULSE_OX, new Spec(List.of(
                    new Field("spo2", Measure.SPO2, Column.VALUE_NUM, true),
                    new Field("pulse", Measure.PULSE, Column.PULSE, false)), "%", Set.of("%")),
            ReadingType.WEIGHT, new Spec(List.of(
                    new Field("weight", Measure.WEIGHT, Column.VALUE_NUM, true)), "kg", Set.of("kg")));

    public sealed interface Outcome permits Valid, Rejected {
    }

    public record Valid(Reading reading) implements Outcome {
    }

    public record Rejected(UUID readingId, RejectionCode code) implements Outcome {
    }

    private final Duration maxFutureSkew;
    private final Duration rejectOlderThan;

    /** @param rejectOlderThan null disables the READING_TOO_OLD check (open clinical decision, flag). */
    public ReadingValidator(Duration maxFutureSkew, Duration rejectOlderThan) {
        this.maxFutureSkew = maxFutureSkew;
        this.rejectOlderThan = rejectOlderThan;
    }

    public Outcome validate(long deviceId, long patientId, ReadingInput in, PlausibilityRules rules, Instant now) {
        ReadingType type = parseType(in.type());
        Instant measuredAt = in.measuredAt() == null ? null : in.measuredAt().truncatedTo(ChronoUnit.MICROS);
        UUID readingId = in.readingId();
        if (readingId == null && type != null && measuredAt != null) {
            readingId = ReadingIds.derive(deviceId, in.deviceSeq(), measuredAt, type);
        }

        if (in.type() == null || measuredAt == null || in.values() == null) {
            return new Rejected(readingId, RejectionCode.MALFORMED_READING);
        }
        if (type == null) {
            return new Rejected(readingId, RejectionCode.UNSUPPORTED_TYPE);
        }
        if (measuredAt.isAfter(now.plus(maxFutureSkew))) {
            return new Rejected(readingId, RejectionCode.FUTURE_TIMESTAMP);
        }
        if (rejectOlderThan != null && measuredAt.isBefore(now.minus(rejectOlderThan))) {
            return new Rejected(readingId, RejectionCode.READING_TOO_OLD);
        }

        Spec spec = SPECS.get(type);
        String unit = in.unit() == null ? spec.canonicalUnit() : in.unit();
        if (!spec.units().contains(unit)) {
            return new Rejected(readingId, RejectionCode.UNSUPPORTED_UNIT);
        }
        Set<String> known = new HashSet<>();
        spec.fields().forEach(f -> known.add(f.name()));
        if (!known.containsAll(in.values().keySet())) {
            return new Rejected(readingId, RejectionCode.MALFORMED_READING);
        }

        Map<Column, BigDecimal> columns = new HashMap<>();
        for (Field f : spec.fields()) {
            BigDecimal raw = in.values().get(f.name());
            if (raw == null) {
                if (f.required()) {
                    return new Rejected(readingId, RejectionCode.MALFORMED_READING);
                }
                continue;
            }
            BigDecimal value = raw;
            if (f.measure() == Measure.GLUCOSE && "mmol/L".equals(unit)) {
                value = value.multiply(GLUCOSE_MMOL_TO_MG_DL);
            }
            if (f.column() == Column.VALUE_NUM) {
                value = value.setScale(2, RoundingMode.HALF_UP);
            } else if (value.stripTrailingZeros().scale() > 0) {
                return new Rejected(readingId, RejectionCode.MALFORMED_READING);   // smallint column
            }
            if (!rules.of(f.measure()).contains(value)) {
                return new Rejected(readingId, RejectionCode.IMPLAUSIBLE_VALUE);
            }
            columns.put(f.column(), value);
        }

        Integer systolic = intOrNull(columns.get(Column.SYSTOLIC));
        Integer diastolic = intOrNull(columns.get(Column.DIASTOLIC));
        Integer pulse = intOrNull(columns.get(Column.PULSE));
        BigDecimal valueNum = columns.get(Column.VALUE_NUM);
        return new Valid(new Reading(deviceId, readingId, measuredAt, patientId, type, in.deviceSeq(),
                systolic, diastolic, pulse, valueNum, PayloadHash.of(type, systolic, diastolic, pulse, valueNum)));
    }

    private static ReadingType parseType(String type) {
        if (type == null) {
            return null;
        }
        try {
            return ReadingType.valueOf(type);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static Integer intOrNull(BigDecimal v) {
        return v == null ? null : v.intValueExact();
    }
}
