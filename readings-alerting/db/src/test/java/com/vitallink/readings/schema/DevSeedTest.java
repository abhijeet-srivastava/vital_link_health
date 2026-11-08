package com.vitallink.readings.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.vitallink.readings.testsupport.IntegrationTestBase;

/** The dev seed (db/seed, a Flyway location used by compose and tests only). */
class DevSeedTest extends IntegrationTestBase {

    static final String SEED = "db/seed/R__dev_seed.sql";

    @BeforeAll
    static void seedAppliesThroughFlyway() {
        Flyway.configure()
                .dataSource(jdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration", "classpath:db/seed")
                .load()
                .migrate();
    }

    /** The base class truncates before each test; put the seed back directly. */
    @BeforeEach
    void reseed() throws Exception {
        exec(seedSql());
    }

    @Test
    void seed_has_programs_devices_bindings_and_coach_panels() throws SQLException {
        assertThat(count("SELECT count(*) FROM measure")).isEqualTo(6);
        assertThat(count("SELECT count(DISTINCT program_id) FROM threshold_rule WHERE valid_to IS NULL")).isEqualTo(2);
        assertThat(count("""
                SELECT count(*) FROM (
                  SELECT program_id FROM threshold_rule WHERE valid_to IS NULL AND measure_code IN (1, 2, 3, 4, 5)
                  GROUP BY program_id HAVING count(DISTINCT measure_code) = 5) p
                """)).as("both programs have bands for systolic, diastolic, pulse, glucose and SpO2").isEqualTo(2);
        assertThat(count("SELECT count(*) FROM threshold_rule WHERE severity = 'CRITICAL'")).isPositive();

        assertThat(count("SELECT count(*) FROM device")).isEqualTo(1000);
        assertThat(count("SELECT count(*) FROM device_binding WHERE valid_to IS NULL")).isEqualTo(1000);
        assertThat(count("SELECT count(DISTINCT patient_id) FROM device_binding WHERE valid_to IS NULL")).isEqualTo(1000);
        assertThat(count("SELECT count(*) FROM device_binding WHERE valid_to IS NOT NULL"))
                .as("one historical binding: device 1 was rebound").isEqualTo(1);

        assertThat(count("SELECT count(DISTINCT coach_id) FROM care_assignment")).isEqualTo(20);
        assertThat(count("""
                SELECT count(*) FROM (
                  SELECT patient_id FROM care_assignment WHERE valid_to IS NULL
                  GROUP BY patient_id
                  HAVING count(*) FILTER (WHERE role = 'PRIMARY') = 1 AND count(*) FILTER (WHERE role = 'BACKUP') = 1
                     AND count(DISTINCT coach_id) = 2) p
                """)).as("every patient has one primary and a different backup coach").isEqualTo(1000);

        assertThat(count("SELECT count(DISTINCT patient_id) FROM enrollment WHERE valid_to IS NULL")).isEqualTo(1000);
        assertThat(count("SELECT count(*) FROM enrollment WHERE patient_id = 1 AND valid_to IS NULL"))
                .as("patient 1 is the multi-program case").isEqualTo(2);
    }

    @Test
    void every_seeded_threshold_rule_is_labelled_not_clinical() throws SQLException {
        assertThat(count("SELECT count(*) FROM threshold_rule")).isPositive();
        assertThat(count("SELECT count(*) FROM threshold_rule WHERE created_by <> 'SEED-NOT-CLINICAL'")).isZero();
    }

    @Test
    void running_the_seed_again_changes_nothing() throws Exception {
        Map<String, Long> before = tableCounts();

        exec(seedSql());

        assertThat(tableCounts()).isEqualTo(before);
    }

    @Test
    void seed_contains_no_credentials_or_secrets() throws Exception {
        assertThat(count("SELECT count(*) FROM device_credential")).isZero();
        assertThat(seedSql()).doesNotContainIgnoringCase("device_credential");
    }

    private static Map<String, Long> tableCounts() throws SQLException {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (String table : new String[] {"threshold_rule", "device", "device_binding", "enrollment", "care_assignment"}) {
            counts.put(table, count("SELECT count(*) FROM " + table));
        }
        return counts;
    }

    private static String seedSql() throws IOException {
        try (InputStream in = DevSeedTest.class.getClassLoader().getResourceAsStream(SEED)) {
            assertThat(in).as(SEED + " on the classpath").isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static long count(String sql) throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
