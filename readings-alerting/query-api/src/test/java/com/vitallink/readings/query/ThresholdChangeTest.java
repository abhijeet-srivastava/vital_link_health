package com.vitallink.readings.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/** PUT thresholds: close the old rule rows, insert new ones, emit thresholds.changed, evict caches. */
class ThresholdChangeTest extends QueryApiTestSupport {

    static final long PATIENT = 6101;
    static final String PATH = "/v1/patients/" + PATIENT + "/thresholds/SYSTOLIC";
    static final String CLINICIAN = token("clinician-7", "CLINICIAN", "thresholds:write");

    @Test
    void threshold_change_closes_old_rows_inserts_new_and_emits_thresholds_changed() throws Exception {
        long oldHigh = overrideRow("GT", 160, "HIGH");
        long oldCritical = overrideRow("GT", 180, "CRITICAL");
        long otherMeasure = owner.sql("""
                        INSERT INTO threshold_rule (patient_id, measure_code, operator, limit_value, severity, created_by)
                        VALUES (?, 2, 'GT', 110, 'HIGH', 'test') RETURNING threshold_id
                        """).param(PATIENT).query(Long.class).single();
        redisCli("SET", "thr:" + PATIENT + ":1", "[]");
        redisCli("SET", "thr:" + PATIENT + ":2", "[]");

        Response r = send(CLINICIAN, "PUT", PATH, bands(band("GT", 150, "HIGH"), band("GT", 175, "CRITICAL"),
                band("LT", 90, "CRITICAL")), Map.of());

        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body().path("changed").asBoolean()).isTrue();
        assertThat(r.body().path("threshold_ids")).hasSize(3);

        // Old rows closed, never deleted; new rows open; the other measure untouched.
        assertThat(validTo(oldHigh)).isNotNull();
        assertThat(validTo(oldCritical)).isNotNull();
        assertThat(validTo(otherMeasure)).isNull();
        assertThat(owner.sql("""
                        SELECT operator || ' ' || limit_value || ' ' || severity FROM threshold_rule
                        WHERE patient_id = ? AND measure_code = 1 AND valid_to IS NULL ORDER BY 1
                        """).param(PATIENT).query(String.class).list())
                .containsExactly("GT 150.00 HIGH", "GT 175.00 CRITICAL", "LT 90.00 CRITICAL");
        assertThat(owner.sql("SELECT DISTINCT created_by FROM threshold_rule WHERE valid_to IS NULL AND measure_code = 1")
                .query(String.class).list()).containsExactly("ADMIN:clinician-7");

        // One outbox row, keyed by patient, IDs only.
        List<Map<String, Object>> events = owner.sql("""
                        SELECT topic, event_type, aggregate_key, schema_version, payload::text AS payload
                        FROM outbox_event
                        """).query().listOfRows();
        assertThat(events).singleElement().satisfies(e -> {
            assertThat(e).containsEntry("topic", "thresholds.changed").containsEntry("event_type", "thresholds.changed")
                    .containsEntry("aggregate_key", String.valueOf(PATIENT));
            JsonNode payload = parse((String) e.get("payload"));
            assertThat(payload.path("patient_id").asLong()).isEqualTo(PATIENT);
            assertThat(payload.path("measure_code").asInt()).isEqualTo(1);
            assertThat(payload.path("schema_version").asInt()).isEqualTo(1);
            assertThat(payload.path("closed_threshold_ids")).hasSize(2);
            assertThat(payload.path("threshold_ids")).hasSize(3);
            assertThat(payload.toString()).doesNotContain("clinician-7");
        });

        // Redis entry for the changed measure is gone; the other measure's stays.
        assertThat(redisCli("EXISTS", "thr:" + PATIENT + ":1")).isEqualTo("0");
        assertThat(redisCli("EXISTS", "thr:" + PATIENT + ":2")).isEqualTo("1");
        assertThat(audits("UPDATE_THRESHOLD")).singleElement().satisfies(a -> assertThat(a)
                .containsEntry("outcome", "ALLOWED").containsEntry("actor_type", "ADMIN")
                .containsEntry("patient_id", PATIENT));
    }

    @Test
    void same_bands_replayed_is_a_noop_with_no_event() throws Exception {
        Object body = bands(band("GT", 150, "HIGH"), band("GT", 175, "CRITICAL"));
        assertThat(send(CLINICIAN, "PUT", PATH, body, Map.of()).status()).isEqualTo(200);

        Response replay = send(CLINICIAN, "PUT", PATH, bands(band("GT", 175.0, "CRITICAL"), band("GT", 150, "HIGH")),
                Map.of());

        assertThat(replay.status()).isEqualTo(200);
        assertThat(replay.body().path("changed").asBoolean()).isFalse();
        assertThat(count("SELECT count(*) FROM threshold_rule WHERE patient_id = ?", PATIENT)).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM outbox_event")).isEqualTo(1);
    }

    @Test
    void concurrent_puts_leave_exactly_one_open_set() throws Exception {
        for (int round = 0; round < 5; round++) {
            Object a = bands(band("GT", 150 + round, "HIGH"));
            Object b = bands(band("GT", 170 + round, "CRITICAL"), band("LT", 85, "CRITICAL"));
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            List<Response> responses = new ArrayList<>();
            try {
                List<Future<Response>> calls = new ArrayList<>();
                for (Object body : List.of(a, b)) {
                    calls.add(pool.submit(() -> {
                        start.await();
                        return send(CLINICIAN, "PUT", PATH, body, Map.of());
                    }));
                }
                start.countDown();
                for (Future<Response> f : calls) {
                    responses.add(f.get());
                }
            } finally {
                pool.shutdownNow();
            }

            assertThat(responses).extracting(Response::status).allMatch(s -> s == 200 || s == 409).contains(200);
            responses.stream().filter(r -> r.status() == 409)
                    .forEach(r -> assertThat(r.code()).isEqualTo("THRESHOLD_CONFLICT"));
            List<String> open = owner.sql("""
                            SELECT operator || ' ' || limit_value::int || ' ' || severity FROM threshold_rule
                            WHERE patient_id = ? AND measure_code = 1 AND valid_to IS NULL ORDER BY 1
                            """).param(PATIENT).query(String.class).list();
            assertThat(open).as("round %d: one request's bands, never a merge", round).isIn(
                    List.of("GT " + (150 + round) + " HIGH"),
                    List.of("GT " + (170 + round) + " CRITICAL", "LT 85 CRITICAL"));
            long winners = responses.stream().filter(r -> r.status() == 200 && r.body().path("changed").asBoolean())
                    .count();
            assertThat(count("SELECT count(*) FROM outbox_event")).isEqualTo(winners);
            owner.sql("DELETE FROM outbox_event").update();
        }
    }

    @Test
    void coach_token_without_thresholds_write_is_403_and_invalid_bands_400() throws Exception {
        Response coach = send(coachToken(11), "PUT", PATH, bands(band("GT", 150, "HIGH")), Map.of());
        Response implausible = send(CLINICIAN, "PUT", PATH, bands(band("GT", 9999, "HIGH")), Map.of());
        Response duplicate = send(CLINICIAN, "PUT", PATH, bands(band("GT", 150, "HIGH"), band("GT", 160, "HIGH")),
                Map.of());
        Response unknownMeasure = send(CLINICIAN, "PUT", "/v1/patients/" + PATIENT + "/thresholds/HEIGHT",
                bands(band("GT", 150, "HIGH")), Map.of());

        assertThat(coach.status()).isEqualTo(403);
        assertThat(audits("UPDATE_THRESHOLD")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "DENIED"));
        assertThat(implausible.code()).isEqualTo("INVALID_BANDS");
        assertThat(duplicate.code()).isEqualTo("INVALID_BANDS");
        assertThat(unknownMeasure.status()).isEqualTo(400);
        assertThat(count("SELECT count(*) FROM threshold_rule")).isZero();
        assertThat(count("SELECT count(*) FROM outbox_event")).isZero();
    }

    private long overrideRow(String operator, int limit, String severity) {
        return owner.sql("""
                        INSERT INTO threshold_rule (patient_id, measure_code, operator, limit_value, severity, created_by)
                        VALUES (?, 1, ?, ?, ?, 'test') RETURNING threshold_id
                        """).params(PATIENT, operator, limit, severity).query(Long.class).single();
    }

    private Object validTo(long thresholdId) {
        return owner.sql("SELECT valid_to FROM threshold_rule WHERE threshold_id = ?").param(thresholdId)
                .query((rs, n) -> rs.getObject(1)).list().get(0);
    }

    private static Map<String, Object> band(String operator, Number limit, String severity) {
        return Map.of("operator", operator, "limit", limit, "severity", severity);
    }

    @SafeVarargs
    private static Map<String, Object> bands(Map<String, Object>... bands) {
        return Map.of("bands", List.of(bands));
    }

    private static JsonNode parse(String json) {
        try {
            return JSON.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
