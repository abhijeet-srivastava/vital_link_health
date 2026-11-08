package com.vitallink.readings.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.testsupport.IntegrationTestBase;

/**
 * Migrations must converge to references/schema.sql. The reference file is applied verbatim to a scratch database in
 * the same container and both catalogs are compared: tables, partitioning, storage options, columns, constraints and
 * indexes of the public schema. Partitions, extension schemas and Flyway's history table are left out.
 */
class SchemaConformanceTest extends IntegrationTestBase {

    static final String REFERENCE_SCHEMA = ".claude/skills/vitallink-service-implementation/references/schema.sql";

    /** Public-schema tables that are not partitions, minus Flyway's own table. */
    static final String TABLES = """
            SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND NOT c.relispartition
              AND c.relname <> 'flyway_schema_history'
            """;

    static final String CATALOG = """
            WITH t AS (%s)
            SELECT 'table ' || c.relname || ' kind=' || c.relkind::text
                   || ' part=' || coalesce(pg_get_partkeydef(c.oid), '-')
                   || ' opts=' || coalesce(array_to_string(c.reloptions, ','), '-')
            FROM pg_class c WHERE c.oid IN (SELECT oid FROM t)
            UNION ALL
            SELECT 'column ' || c.relname || '.' || a.attnum || ' ' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
                   || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END
                   || coalesce(' default ' || pg_get_expr(d.adbin, d.adrelid), '')
                   || CASE a.attgenerated WHEN 's' THEN ' stored' ELSE '' END
            FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE c.oid IN (SELECT oid FROM t) AND a.attnum > 0 AND NOT a.attisdropped
            UNION ALL
            SELECT 'constraint ' || c.relname || ' ' || k.conname || ' ' || pg_get_constraintdef(k.oid)
            FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid
            WHERE c.oid IN (SELECT oid FROM t)
            UNION ALL
            SELECT 'index ' || pg_get_indexdef(i.indexrelid)
            FROM pg_index i WHERE i.indrelid IN (SELECT oid FROM t)
            UNION ALL
            SELECT 'extension ' || extname FROM pg_extension WHERE extname NOT IN ('plpgsql', 'pg_partman')
            ORDER BY 1
            """.formatted(TABLES);

    @Test
    void schema_matches_reference_schema() throws Exception {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS reference_schema");
            st.execute("CREATE DATABASE reference_schema");
        }
        String referenceUrl = jdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/reference_schema$1");
        String reference = Files.readString(repoPath(REFERENCE_SCHEMA).toAbsolutePath());

        List<String> expected;
        try (Connection c = DriverManager.getConnection(referenceUrl, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement st = c.createStatement()) {
            st.execute(reference);
            expected = catalog(c);
        }
        List<String> actual;
        try (Connection c = dataSource().getConnection()) {
            actual = catalog(c);
        }

        assertThat(expected).as("reference catalog is not empty").hasSizeGreaterThan(100);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private static List<String> catalog(Connection c) throws SQLException {
        List<String> lines = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(CATALOG)) {
            while (rs.next()) {
                lines.add(rs.getString(1));
            }
        }
        return lines;
    }
}
