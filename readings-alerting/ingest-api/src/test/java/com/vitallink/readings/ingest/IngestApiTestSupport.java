package com.vitallink.readings.ingest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.DeviceKeys;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

/**
 * Black-box ingest tests: real HTTP against a random port, real Postgres and Redis. Each test creates the devices,
 * bindings and keys it needs (no dev seed), so tests never depend on each other's credentials.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "vitallink.device-keys.pepper=" + IngestApiTestSupport.PEPPER)
abstract class IngestApiTestSupport extends SpringIntegrationTestBase {

    static final String PEPPER = "test-pepper";

    /** JDK client: HttpURLConnection cannot read a 401 answer to a streamed POST. */
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final SecureRandom RANDOM = new SecureRandom();

    @LocalServerPort
    int port;

    @Autowired
    JdbcClient jdbc;

    record Binding(long patientId, Instant from, Instant to) {
    }

    record Response(int status, String contentType, JsonNode body) {
        List<String> statuses() {
            List<String> statuses = new ArrayList<>();
            body.path("results").forEach(r -> statuses.add(r.path("status").asText()));
            return statuses;
        }

        Set<String> statusSet() {
            return new HashSet<>(statuses());
        }

        String code() {
            return body.path("code").asText(null);
        }
    }

    static Binding boundTo(long patientId, Instant from) {
        return new Binding(patientId, from, null);
    }

    static Binding boundTo(long patientId, Instant from, Instant to) {
        return new Binding(patientId, from, to);
    }

    static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    static Instant daysAgo(long days) {
        return now().minus(days, ChronoUnit.DAYS);
    }

    /** Device with the given binding history and one active key; returns the key token. */
    String givenDevice(long deviceId, Binding... bindings) {
        jdbc.sql("INSERT INTO device (device_id, serial_no, device_type) VALUES (?, ?, 'BP_CUFF')")
                .params(deviceId, "TEST-" + deviceId)
                .update();
        for (Binding b : bindings) {
            jdbc.sql("INSERT INTO device_binding (device_id, patient_id, valid_from, valid_to) VALUES (?, ?, ?, ?)")
                    .params(deviceId, b.patientId(), ts(b.from()), b.to() == null ? null : ts(b.to()))
                    .update();
        }
        DeviceKeys.ApiKey key = DeviceKeys.generate(RANDOM);
        jdbc.sql("INSERT INTO device_credential (key_id, device_id, secret_hash) VALUES (?, ?, ?)")
                .params(key.keyId(), deviceId, DeviceKeys.hash(key.secret(), PEPPER.getBytes(StandardCharsets.UTF_8)))
                .update();
        return key.token();
    }

    static Map<String, Object> bp(UUID readingId, Instant measuredAt, int systolic, int diastolic) {
        Map<String, Object> r = new LinkedHashMap<>();
        if (readingId != null) {
            r.put("reading_id", readingId.toString());
        }
        r.put("device_seq", 1);
        r.put("type", "BLOOD_PRESSURE");
        r.put("measured_at", measuredAt.toString());
        r.put("values", Map.of("systolic", systolic, "diastolic", diastolic, "pulse", 72));
        r.put("unit", "mmHg");
        return r;
    }

    static Map<String, Object> batch(long patientId, List<Map<String, Object>> readings) {
        return Map.of("patient_id", patientId, "readings", readings);
    }

    Response post(long deviceId, String deviceKey, Object body) throws Exception {
        return postRaw(deviceId, deviceKey, JSON.writeValueAsString(body));
    }

    Response postRaw(long deviceId, String deviceKey, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/v1/devices/" + deviceId + "/readings"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (deviceKey != null) {
            request.header("X-Device-Key", deviceKey);
        }
        HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        String text = response.body();
        JsonNode json = text == null || text.isBlank() ? JSON.createObjectNode() : JSON.readTree(text);
        return new Response(response.statusCode(), response.headers().firstValue("Content-Type").orElse(""), json);
    }

    long count(String sql, Object... params) {
        return jdbc.sql(sql).params(List.of(params)).query(Long.class).single();
    }

    static JsonNode parse(String json) throws Exception {
        return JSON.readTree(json);
    }

    static java.sql.Timestamp ts(Instant instant) {
        return java.sql.Timestamp.from(instant);
    }
}
