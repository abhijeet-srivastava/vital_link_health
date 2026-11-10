package com.vitallink.readings.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

class IntegrationTestBaseTest extends IntegrationTestBase {

    @Test
    void containers_start_and_postgres_has_partman_and_btree_gist() throws Exception {
        Set<String> available = new HashSet<>();
        try (Connection c = dataSource().getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT name FROM pg_available_extensions WHERE name IN ('pg_partman', 'btree_gist')")) {
            while (rs.next()) {
                available.add(rs.getString(1));
            }
        }
        assertThat(available).containsExactlyInAnyOrder("pg_partman", "btree_gist");

        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap()))) {
            assertThat(admin.describeCluster().nodes().get(30, TimeUnit.SECONDS)).hasSize(1);
        }

        assertThat(redisCli("PING")).isEqualTo("PONG");
    }
}
