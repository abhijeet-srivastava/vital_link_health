package com.vitallink.readings.domain;

import java.math.BigDecimal;
import java.time.Instant;

/** One reading_rollup row of a known patient: a measure's count, min, max and mean in one UTC hour. */
public record ReadingRollup(Measure measure, Instant hourStart, int count, BigDecimal min, BigDecimal max,
                            BigDecimal avg) {
}
