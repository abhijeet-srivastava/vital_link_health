package com.vitallink.readings.query;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.vitallink.readings.testsupport.IntegrationTestBase;

/** With the prod profile the app refuses to start while dev-seed bands without clinical sign-off exist. */
class SeedThresholdGuardTest extends IntegrationTestBase {

    @Test
    void prod_profile_refuses_to_start_when_seed_threshold_rules_exist() throws SQLException {
        insertRule("SEED-NOT-CLINICAL");

        assertThatThrownBy(() -> start("prod").close())
                .rootCause()
                .hasMessageContaining("SEED-NOT-CLINICAL");
    }

    private static ConfigurableApplicationContext start(String profile) {
        return new SpringApplicationBuilder(QueryApiApplication.class)
                .web(WebApplicationType.NONE)
                .profiles(profile)
                .run( // command-line args: they win over the placeholders in application.yml
                        "--spring.datasource.url=" + jdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--spring.kafka.bootstrap-servers=" + kafkaBootstrap(),
                        "--spring.data.redis.host=" + redisHost(),
                        "--spring.data.redis.port=" + redisPort());
    }

    private static void insertRule(String createdBy) throws SQLException {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    INSERT INTO threshold_rule (program_id, measure_code, operator, limit_value, severity, created_by)
                    VALUES (1, 1, 'GT', 180, 'CRITICAL', '%s')
                    """.formatted(createdBy));
        }
    }
}
