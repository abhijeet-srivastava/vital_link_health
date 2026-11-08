package com.vitallink.readings.domain;

import java.util.List;

/** The result of replacing a patient's bands for one measure: rule versions closed and inserted. */
public record ThresholdChange(long patientId, Measure measure, List<Long> closedIds, List<Long> insertedIds) {

    public ThresholdChange {
        closedIds = List.copyOf(closedIds);
        insertedIds = List.copyOf(insertedIds);
    }
}
