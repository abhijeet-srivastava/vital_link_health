package com.vitallink.readings.query;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

/**
 * Black-box query-api tests: real HTTP, real Postgres and Redis. The coach pools log in as {@code
 * vitallink_query_app} (a member of vitallink_query, not a superuser), so row-level security applies exactly as in
 * compose; the Testcontainers user is a superuser and would bypass it. Fixtures and assertions use that superuser
 * ({@link #owner}). Tokens are signed here with the same HS256 secret the app is configured with.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "vitallink.security.jwt.secret=" + QueryApiTestSupport.JWT_SECRET)
public abstract class QueryApiTestSupport extends SpringIntegrationTestBase {

    public static final String JWT_SECRET = "test-only-jwt-secret-0123456789abcdef";
    public static final String QUERY_LOGIN = "vitallink_query_app";
    public static final String QUERY_PASSWORD = "query-test-password";

    public static final String READINGS_READ = "readings:read";
    public static final String ALERTS_READ = "alerts:read";
    public static final String ALERTS_WRITE = "alerts:write";
    public static final String COACH_SCOPES = READINGS_READ + " " + ALERTS_READ + " " + ALERTS_WRITE;

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    protected static final ObjectMapper JSON = new ObjectMapper();

    protected final JdbcClient owner = JdbcClient.create(dataSource());

    @LocalServerPort
    protected int port;

    public record Response(int status, String contentType, JsonNode body, String etag) {
        public String code() {
            return body.path("code").asText(null);
        }
    }

    /** After the base class migrated: the coach login the app connects as. Roles are cluster-wide and survive. */
    @BeforeAll
    static void createQueryLogin() throws Exception {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    DO $$
                    BEGIN
                      IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '%1$s') THEN
                        CREATE ROLE %1$s LOGIN PASSWORD '%2$s';
                      END IF;
                    END $$;
                    GRANT vitallink_query TO %1$s;
                    """.formatted(QUERY_LOGIN, QUERY_PASSWORD));
        }
    }

    @DynamicPropertySource
    static void queryPools(DynamicPropertyRegistry registry) {
        for (String pool : List.of("primary", "replica")) {
            registry.add("vitallink.datasource." + pool + ".jdbc-url", POSTGRES::getJdbcUrl);
            registry.add("vitallink.datasource." + pool + ".username", () -> QUERY_LOGIN);
            registry.add("vitallink.datasource." + pool + ".password", () -> QUERY_PASSWORD);
        }
    }

    // ----- tokens -----

    public static String coachToken(long coachId) {
        return token(String.valueOf(coachId), "COACH", COACH_SCOPES);
    }

    public static String token(String subject, String actor, String scopes) {
        try {
            Instant now = Instant.now();
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .issuer("vitallink-dev")
                    .audience("query-api")
                    .subject(subject)
                    .claim("vl_actor", actor)
                    .claim("scope", scopes)
                    .issueTime(Date.from(now))
                    .expirationTime(Date.from(now.plus(Duration.ofMinutes(10))))
                    .build();
            SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
            jwt.sign(new MACSigner(JWT_SECRET.getBytes(StandardCharsets.UTF_8)));
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // ----- HTTP -----

    protected Response get(String token, String path) throws Exception {
        return send(token, "GET", path, null, Map.of());
    }

    protected Response send(String token, String method, String path, Object body, Map<String, String> headers)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        if (body != null) {
            request.header("Content-Type", "application/json");
        }
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        headers.forEach(request::header);
        HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        String text = response.body();
        JsonNode json = text == null || text.isBlank() ? JSON.createObjectNode() : JSON.readTree(text);
        return new Response(response.statusCode(), response.headers().firstValue("Content-Type").orElse(""), json,
                response.headers().firstValue("ETag").orElse(null));
    }

    // ----- fixtures (as the owner) -----

    protected static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MICROS);
    }

    protected void panel(long coachId, long patientId, String role) {
        owner.sql("INSERT INTO care_assignment (patient_id, coach_id, role, valid_from) VALUES (?, ?, ?, ?)")
                .params(patientId, coachId, role, utc(now().minus(Duration.ofDays(30))))
                .update();
    }

    protected void primary(long coachId, long patientId) {
        panel(coachId, patientId, "PRIMARY");
    }

    protected void endAssignment(long coachId, long patientId) {
        owner.sql("UPDATE care_assignment SET valid_to = ? WHERE coach_id = ? AND patient_id = ? AND valid_to IS NULL")
                .params(utc(now()), coachId, patientId)
                .update();
    }

    /** A blood-pressure reading stored directly (ingest is not under test here). */
    protected UUID bp(long patientId, Instant measuredAt, int systolic, int diastolic) {
        return bp(patientId, UUID.randomUUID(), measuredAt, systolic, diastolic);
    }

    protected UUID bp(long patientId, UUID readingUuid, Instant measuredAt, int systolic, int diastolic) {
        owner.sql("""
                        INSERT INTO reading (device_id, reading_uuid, measured_at, patient_id, reading_type, device_seq,
                                             systolic, diastolic, pulse, payload_hash)
                        VALUES (?, ?, ?, ?, 1, 1, ?, ?, 70, 42)
                        """)
                .params(patientId, readingUuid, utc(measuredAt), patientId, systolic, diastolic)
                .update();
        return readingUuid;
    }

    protected long alert(long patientId, long coachId, Measure measure, Severity severity, AlertStatus status,
                         Instant createdAt) {
        Instant at = createdAt;
        return owner.sql("""
                        INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, first_seen_at,
                                           last_seen_at, created_at, acknowledged_by, acknowledged_at, resolved_by,
                                           resolved_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        RETURNING alert_id
                        """)
                .params(patientId, measure.code(), coachId, severity.name(), status.name(), utc(at), utc(at), utc(at),
                        status == AlertStatus.ACKNOWLEDGED ? coachId : null,
                        status == AlertStatus.ACKNOWLEDGED ? utc(at) : null,
                        status == AlertStatus.RESOLVED ? coachId : null,
                        status == AlertStatus.RESOLVED ? utc(at) : null)
                .query(Long.class).single();
    }

    protected Map<String, Object> alertRow(long alertId) {
        return owner.sql("SELECT status, version, coach_id FROM alert WHERE alert_id = ?").param(alertId)
                .query().singleRow();
    }

    protected List<Map<String, Object>> audits(String action) {
        return owner.sql("""
                        SELECT actor_type, actor_id, action, outcome, patient_id, resource, request_id
                        FROM audit_log WHERE action = ? ORDER BY at, audit_id
                        """)
                .param(action).query().listOfRows();
    }

    protected long count(String sql, Object... params) {
        return owner.sql(sql).params(List.of(params)).query(Long.class).single();
    }

    protected static List<String> texts(JsonNode array, String field) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.path(field).asText()));
        return out;
    }

    protected static OffsetDateTime utc(Instant t) {
        return t == null ? null : t.atOffset(ZoneOffset.UTC);
    }
}
