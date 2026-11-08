package com.vitallink.readings.domain;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public final class PlausibilityRules {

    private final Map<Measure, MeasureBounds> bounds = new EnumMap<>(Measure.class);

    public PlausibilityRules(List<MeasureBounds> rows) {
        rows.forEach(b -> bounds.put(b.measure(), b));
        for (Measure m : Measure.values()) {
            if (!bounds.containsKey(m)) {
                throw new IllegalStateException("measure table has no row for " + m);
            }
        }
    }

    public MeasureBounds of(Measure measure) {
        return bounds.get(measure);
    }
}
