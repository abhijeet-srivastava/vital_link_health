package com.vitallink.readings.domain;

import java.math.BigDecimal;

/** One threshold_rule row: value GT or LT limit means severity. Rules are immutable; thresholdId names the version. */
public record ThresholdBand(long thresholdId, Measure measure, Operator operator, BigDecimal limit, Severity severity) {

    public enum Operator {
        GT,
        LT
    }

    /** Strict comparison: a value equal to the limit is inside the band. */
    public boolean crossedBy(BigDecimal value) {
        int cmp = value.compareTo(limit);
        return operator == Operator.GT ? cmp > 0 : cmp < 0;
    }
}
