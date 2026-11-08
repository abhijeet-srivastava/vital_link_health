package com.vitallink.readings.persistence;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.MeasureBounds;
import com.vitallink.readings.domain.PlausibilityRules;
import com.vitallink.readings.ports.MeasureCatalog;

/** Loads the measure table once, on first use; its rows change only through a migration (and a redeploy). */
public class JdbcMeasureCatalog implements MeasureCatalog {

    private final JdbcClient jdbc;
    private volatile PlausibilityRules rules;

    public JdbcMeasureCatalog(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public PlausibilityRules plausibilityRules() {
        PlausibilityRules loaded = rules;
        if (loaded == null) {
            loaded = new PlausibilityRules(jdbc.sql("SELECT measure_code, unit, min_plausible, max_plausible FROM measure")
                    .query((rs, n) -> new MeasureBounds(Measure.fromCode(rs.getInt("measure_code")), rs.getString("unit"),
                            rs.getBigDecimal("min_plausible"), rs.getBigDecimal("max_plausible")))
                    .list());
            rules = loaded;
        }
        return loaded;
    }
}
