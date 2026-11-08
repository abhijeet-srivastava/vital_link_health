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

import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;

/** testing.md, Optimistic lock: two acknowledges with the same version, one 200 and one 409 (over HTTP). */
class AlertActionsHttpTest extends QueryApiTestSupport {

    static final long COACH = 51;
    static final long BACKUP = 52;
    static final long PATIENT = 5101;

    @Test
    void two_acknowledges_same_if_match_one_200_one_409_with_current_alert() throws Exception {
        primary(COACH, PATIENT);
        panel(BACKUP, PATIENT, "BACKUP");
        long alertId = alert(PATIENT, COACH, Measure.SYSTOLIC, Severity.CRITICAL, AlertStatus.OPEN, now());
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<Response>> calls = new ArrayList<>();
            for (long coach : new long[]{COACH, BACKUP}) {
                calls.add(pool.submit(() -> {
                    start.await();
                    return send(coachToken(coach), "POST", "/v1/alerts/" + alertId + "/acknowledge", null,
                            Map.of("If-Match", "\"0\""));
                }));
            }
            start.countDown();
            List<Response> responses = new ArrayList<>();
            for (Future<Response> f : calls) {
                responses.add(f.get());
            }

            assertThat(responses).extracting(Response::status).containsExactlyInAnyOrder(200, 409);
            Response ok = responses.stream().filter(r -> r.status() == 200).findFirst().orElseThrow();
            Response conflict = responses.stream().filter(r -> r.status() == 409).findFirst().orElseThrow();
            assertThat(ok.etag()).isEqualTo("\"1\"");
            assertThat(ok.body().path("status").asText()).isEqualTo("ACKNOWLEDGED");
            assertThat(conflict.code()).isEqualTo("ALERT_CONFLICT");
            assertThat(conflict.body().path("current").path("alert_id").asLong()).isEqualTo(alertId);
            assertThat(conflict.body().path("current").path("status").asText()).isEqualTo("ACKNOWLEDGED");
            assertThat(conflict.body().path("current").path("version").asInt()).isEqualTo(1);
            assertThat(conflict.etag()).isEqualTo("\"1\"");
        } finally {
            pool.shutdownNow();
        }
        assertThat(alertRow(alertId)).containsEntry("status", "ACKNOWLEDGED").containsEntry("version", 1);
        assertThat(audits("ACK_ALERT")).singleElement().satisfies(a -> assertThat(a).containsEntry("outcome", "ALLOWED"));
        assertThat(count("SELECT count(*) FROM outbox_event WHERE event_type = 'alert.state_changed'")).isEqualTo(1);
    }

    @Test
    void stale_if_match_is_409_with_current_alert() throws Exception {
        primary(COACH, PATIENT);
        long alertId = alert(PATIENT, COACH, Measure.SYSTOLIC, Severity.HIGH, AlertStatus.OPEN, now());
        String path = "/v1/alerts/" + alertId + "/acknowledge";
        assertThat(send(coachToken(COACH), "POST", path, null, Map.of("If-Match", "0")).status()).isEqualTo(200);

        Response stale = send(coachToken(COACH), "POST", "/v1/alerts/" + alertId + "/resolve",
                Map.of("note", "called patient"), Map.of("If-Match", "W/\"0\""));

        assertThat(stale.status()).isEqualTo(409);
        assertThat(stale.body().path("current").path("version").asInt()).isEqualTo(1);
        assertThat(stale.body().path("current").has("resolution_note")).isFalse();
    }

    @Test
    void missing_if_match_is_428() throws Exception {
        primary(COACH, PATIENT);
        long alertId = alert(PATIENT, COACH, Measure.SYSTOLIC, Severity.HIGH, AlertStatus.OPEN, now());

        Response r = send(coachToken(COACH), "POST", "/v1/alerts/" + alertId + "/acknowledge", null, Map.of());

        assertThat(r.status()).isEqualTo(428);
        assertThat(r.code()).isEqualTo("IF_MATCH_REQUIRED");
        assertThat(alertRow(alertId)).containsEntry("version", 0);
    }

    @Test
    void resolve_critical_without_note_is_422_and_with_note_200() throws Exception {
        primary(COACH, PATIENT);
        long alertId = alert(PATIENT, COACH, Measure.SYSTOLIC, Severity.CRITICAL, AlertStatus.OPEN, now());
        String path = "/v1/alerts/" + alertId + "/resolve";

        Response noNote = send(coachToken(COACH), "POST", path, Map.of(), Map.of("If-Match", "\"0\""));
        Response withNote = send(coachToken(COACH), "POST", path, Map.of("note", "BP rechecked, normal"),
                Map.of("If-Match", "\"0\""));

        assertThat(noNote.status()).isEqualTo(422);
        assertThat(noNote.code()).isEqualTo("RESOLUTION_NOTE_REQUIRED");
        assertThat(withNote.status()).isEqualTo(200);
        assertThat(withNote.etag()).isEqualTo("\"1\"");
        assertThat(withNote.body().path("status").asText()).isEqualTo("RESOLVED");
        assertThat(withNote.body().toString()).doesNotContain("rechecked");
    }

    @Test
    void unknown_alert_is_404_and_missing_scope_403() throws Exception {
        primary(COACH, PATIENT);
        long alertId = alert(PATIENT, COACH, Measure.SYSTOLIC, Severity.HIGH, AlertStatus.OPEN, now());

        Response unknown = send(coachToken(COACH), "POST", "/v1/alerts/999999/acknowledge", null,
                Map.of("If-Match", "\"0\""));
        Response readOnly = send(token(String.valueOf(COACH), "COACH", READINGS_READ + " " + ALERTS_READ), "POST",
                "/v1/alerts/" + alertId + "/acknowledge", null, Map.of("If-Match", "\"0\""));

        assertThat(unknown.status()).isEqualTo(404);
        assertThat(unknown.code()).isEqualTo("ALERT_NOT_FOUND");
        assertThat(readOnly.status()).isEqualTo(403);
        assertThat(audits("ACK_ALERT")).singleElement().satisfies(a -> assertThat(a).containsEntry("outcome", "DENIED"));
    }
}
