package com.vitallink.readings.alerting.partitions;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.testsupport.IntegrationTestBase;

/** The job runs as the least-privilege login user, the same way compose runs alert-worker. */
class PartitionMaintenanceJobTest extends IntegrationTestBase {

    static PartitionMaintenanceJob job;

    @BeforeAll
    static void serviceLogin() throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE ROLE vitallink_app LOGIN PASSWORD 'test'");
            st.execute("GRANT vitallink_service TO vitallink_app");
        }
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(jdbcUrl());
        ds.setUser("vitallink_app");
        ds.setPassword("test");
        job = new PartitionMaintenanceJob(JdbcClient.create(ds));
    }

    @Test
    void maintenance_runs_as_the_service_role_and_finds_no_default_rows() {
        assertThat(job.run()).isEmpty();
        assertThat(job.run()).isEmpty();
    }

    @Test
    void maintenance_reports_default_partitions_that_hold_rows() throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    INSERT INTO reading (device_id, reading_uuid, measured_at, patient_id, reading_type, value_num, payload_hash)
                    VALUES (1, '0190f1d2-0000-7000-8000-000000000009', now() - interval '30 days', 100, 2, 120, 42)
                    """);
        }

        assertThat(job.run()).containsExactly("public.reading_default");
    }
}
