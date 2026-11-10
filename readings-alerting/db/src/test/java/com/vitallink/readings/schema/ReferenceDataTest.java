package com.vitallink.readings.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.testsupport.IntegrationTestBase;

/** Reference rows every environment needs come from versioned migrations, never from the dev seed. */
class ReferenceDataTest extends IntegrationTestBase {

    @Test
    void measure_rows_come_from_a_versioned_migration_that_runs_before_the_schema_fixes() throws SQLException {
        // This class migrates db/migration only (no db/seed location).
        assertThat(strings("SELECT name FROM measure ORDER BY measure_code"))
                .containsExactly("SYSTOLIC", "DIASTOLIC", "PULSE", "GLUCOSE", "SPO2", "WEIGHT");
        assertThat(strings("""
                SELECT script FROM flyway_schema_history
                WHERE script IN ('V202611080902__insert_measures.sql', 'V202611081000__schema_fixes_after_stage2.sql')
                ORDER BY installed_rank
                """)).containsExactly("V202611080902__insert_measures.sql", "V202611081000__schema_fixes_after_stage2.sql");
    }

    @Test
    void reading_type_measure_maps_pulse_from_blood_pressure_and_pulse_ox() throws SQLException {
        assertThat(strings("SELECT reading_type::text FROM reading_type_measure WHERE measure_code = 3 ORDER BY 1"))
                .containsExactly("1", "3");
    }

    @Test
    void reading_type_measure_lists_every_measure_each_reading_type_carries() throws SQLException {
        assertThat(strings("""
                SELECT reading_type || ':' || string_agg(measure_code::text, ',' ORDER BY measure_code)
                FROM reading_type_measure GROUP BY reading_type ORDER BY reading_type
                """)).containsExactly("1:1,2,3", "2:4", "3:3,5", "4:6");
    }

    private static List<String> strings(String sql) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                values.add(rs.getString(1));
            }
        }
        return values;
    }
}
