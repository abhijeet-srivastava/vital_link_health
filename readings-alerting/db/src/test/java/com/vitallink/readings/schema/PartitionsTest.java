package com.vitallink.readings.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import com.vitallink.readings.testsupport.IntegrationTestBase;

/** pg_partman daily partitions (UTC days) for the three time-series tables, plus their DEFAULT partitions. */
class PartitionsTest extends IntegrationTestBase {

    static final DateTimeFormatter SUFFIX = DateTimeFormatter.BASIC_ISO_DATE;   // pg_partman: <table>_pYYYYMMDD

    @ParameterizedTest
    @ValueSource(strings = {"reading", "alert_occurrence", "audit_log"})
    void partitions_exist_from_two_days_back_to_seven_days_ahead_and_default_is_empty(String table) throws SQLException {
        List<String> partitions = partitionsOf(table);

        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        for (LocalDate day = today.minusDays(2); !day.isAfter(today.plusDays(7)); day = day.plusDays(1)) {
            assertThat(partitions).contains(table + "_p" + SUFFIX.format(day));
        }
        assertThat(partitions).contains(table + "_default");
        assertThat(count("SELECT count(*) FROM " + table + "_default")).isZero();
    }

    @Test
    void service_role_can_run_partition_maintenance_and_it_is_idempotent() throws SQLException {
        List<String> before = partitionsOf("reading");

        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("SET ROLE vitallink_service");
            assertThat(nonEmptyDefaults(st)).isEmpty();
            assertThat(nonEmptyDefaults(st)).isEmpty();
        }

        assertThat(partitionsOf("reading")).containsExactlyInAnyOrderElementsOf(before);
    }

    @Test
    void partition_maintenance_reports_rows_that_landed_in_a_default_partition() throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    INSERT INTO reading (device_id, reading_uuid, measured_at, patient_id, reading_type, value_num, payload_hash)
                    VALUES (1, '0190f1d2-0000-7000-8000-000000000009', now() + interval '60 days', 100, 2, 120, 42)
                    """);
            st.execute("SET ROLE vitallink_service");

            assertThat(nonEmptyDefaults(st)).containsExactly("public.reading_default");
        }
    }

    private static List<String> nonEmptyDefaults(Statement st) throws SQLException {
        List<String> names = new ArrayList<>();
        try (ResultSet rs = st.executeQuery("SELECT * FROM partman_maint.run()")) {
            while (rs.next()) {
                names.add(rs.getString(1));
            }
        }
        return names;
    }

    private static List<String> partitionsOf(String table) throws SQLException {
        List<String> names = new ArrayList<>();
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                     WHERE i.inhparent = 'public.%s'::regclass
                     """.formatted(table))) {
            while (rs.next()) {
                names.add(rs.getString(1));
            }
        }
        return names;
    }

    private static long count(String sql) throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
