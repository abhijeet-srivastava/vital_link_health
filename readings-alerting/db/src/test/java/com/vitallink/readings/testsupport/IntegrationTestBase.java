package com.vitallink.readings.testsupport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Real PostgreSQL (same image as compose, with pg_partman), Kafka (KRaft) and Redis for integration tests.
 *
 * <ul>
 *   <li>One container set per test class: static {@code @Container} fields are started before and stopped after
 *       each subclass.</li>
 *   <li>Flyway migrates once per class; every test starts from truncated tables and an empty Redis.</li>
 *   <li>Kafka topics cannot be truncated: tests use their own consumer group ids and assert on DB state.</li>
 * </ul>
 */
@Testcontainers
public abstract class IntegrationTestBase {

    static final String POSTGRES_IMAGE = new ImageFromDockerfile("vitallink/postgres-partman", false)
            .withFileFromPath(".", repoPath("docker/postgres"))
            .get();

    /** Tables kept across tests: migration history and reference data inserted by migrations. */
    protected static final Set<String> KEEP_TABLES = Set.of("flyway_schema_history", "measure", "reading_type_measure");

    /** Schemas owned by extensions; their tables hold configuration, not test data. */
    private static final Set<String> EXTENSION_SCHEMAS = Set.of("partman");

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName("vitallink");

    @Container
    protected static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");

    @Container
    protected static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    @BeforeAll
    static void migrate() {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @BeforeEach
    void resetState() throws Exception {
        truncateTables();
        redisCli("FLUSHALL");
    }

    protected static DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        return ds;
    }

    protected static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    protected static String kafkaBootstrap() {
        return KAFKA.getBootstrapServers();
    }

    protected static String redisHost() {
        return REDIS.getHost();
    }

    protected static int redisPort() {
        return REDIS.getMappedPort(6379);
    }

    protected static String redisCli(String... command) throws Exception {
        String[] argv = new String[command.length + 1];
        argv[0] = "redis-cli";
        System.arraycopy(command, 0, argv, 1, command.length);
        ExecResult result = REDIS.execInContainer(argv);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("redis-cli failed: " + result.getStderr());
        }
        return result.getStdout().trim();
    }

    private static void truncateTables() throws Exception {
        List<String> tables = new ArrayList<>();
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("""
                    SELECT format('%I.%I', n.nspname, c.relname), c.relname, n.nspname
                    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'
                    """)) {
                while (rs.next()) {
                    if (!KEEP_TABLES.contains(rs.getString(2)) && !EXTENSION_SCHEMAS.contains(rs.getString(3))) {
                        tables.add(rs.getString(1));
                    }
                }
            }
            if (!tables.isEmpty()) {
                st.execute("TRUNCATE " + String.join(", ", tables) + " RESTART IDENTITY CASCADE");
            }
        }
    }

    /** Surefire runs with the module as working directory; walk up to the repo root (the dir holding mvnw). */
    protected static Path repoPath(String relative) {
        for (Path dir = Path.of("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            if (Files.exists(dir.resolve("mvnw")) && Files.exists(dir.resolve(relative))) {
                return dir.resolve(relative);
            }
        }
        throw new IllegalStateException("Cannot find " + relative + " above " + Path.of("").toAbsolutePath());
    }
}
