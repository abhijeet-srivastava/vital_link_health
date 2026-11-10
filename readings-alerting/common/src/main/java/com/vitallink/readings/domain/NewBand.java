package com.vitallink.readings.domain;

import java.math.BigDecimal;

/** A threshold band a clinician asks for; becomes a threshold_rule row (and so a ThresholdBand with an id). */
public record NewBand(ThresholdBand.Operator operator, BigDecimal limit, Severity severity) {
}
