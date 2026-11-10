package com.vitallink.readings.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.ports.AlertQueueRepository;
import com.vitallink.readings.ports.ReadingQueryRepository;
import com.vitallink.readings.query.security.Caller;
import com.vitallink.readings.query.security.RlsScope;

/** testing.md, Access control: a coach cannot read a patient outside their panel; the denial is audited. */
class PanelAccessTest extends QueryApiTestSupport {

    static final long COACH_A = 11;
    static final long COACH_B = 12;
    static final long PATIENT_A = 1101;   // in A's panel
    static final long PATIENT_B = 1201;   // in B's panel only

    @Autowired
    RlsScope rls;

    @Autowired
    ReadingQueryRepository readings;

    @Autowired
    AlertQueueRepository queue;

    @Autowired
    @Qualifier("jdbcClient")
    JdbcClient coachPrimary;

    @Test
    void coach_cannot_read_patient_outside_panel_and_denial_is_audited() throws Exception {
        primary(COACH_A, PATIENT_A);
        primary(COACH_B, PATIENT_B);
        bp(PATIENT_B, now().minusSeconds(60), 185, 95);
        long alertB = alert(PATIENT_B, COACH_B, Measure.SYSTOLIC, Severity.CRITICAL, AlertStatus.OPEN, now());
        String token = coachToken(COACH_A);
        Instant to = now().plusSeconds(1);
        Instant from = to.minus(Duration.ofDays(1));

        Response latest = send(token, "GET", "/v1/patients/" + PATIENT_B + "/readings/latest?type=BLOOD_PRESSURE",
                null, Map.of("X-Request-Id", "0190f0c4-0000-7000-8000-000000000001"));
        Response range = get(token, "/v1/patients/" + PATIENT_B + "/readings?type=BLOOD_PRESSURE&from=" + from
                + "&to=" + to);
        Response ack = send(token, "POST", "/v1/alerts/" + alertB + "/acknowledge", null, Map.of("If-Match", "\"0\""));

        for (Response r : List.of(latest, range, ack)) {
            assertThat(r.status()).isEqualTo(403);
            assertThat(r.contentType()).startsWith("application/problem+json");
            assertThat(r.code()).isEqualTo("ACCESS_DENIED");
            assertThat(r.body().toString()).doesNotContain("185");
        }
        assertThat(latest.body().has("reading")).isFalse();

        List<Map<String, Object>> deniedLatest = audits("READ_LATEST_READING");
        assertThat(deniedLatest).singleElement().satisfies(a -> {
            assertThat(a.get("outcome")).isEqualTo("DENIED");
            assertThat(a.get("actor_type")).isEqualTo("COACH");
            assertThat(a.get("actor_id")).isEqualTo(String.valueOf(COACH_A));
            assertThat(a.get("patient_id")).isEqualTo(PATIENT_B);
            assertThat(a.get("request_id")).isEqualTo(UUID.fromString("0190f0c4-0000-7000-8000-000000000001"));
        });
        assertThat(audits("READ_READINGS")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "DENIED").containsEntry("patient_id", PATIENT_B));
        assertThat(audits("ACK_ALERT")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "DENIED").containsEntry("patient_id", PATIENT_B)
                        .containsEntry("resource", "alert/" + alertB));
        assertThat(alertRow(alertB)).containsEntry("status", "OPEN").containsEntry("version", 0);
        assertThat(count("SELECT count(*) FROM outbox_event")).isZero();
    }

    @Test
    void rls_blocks_cross_panel_read_even_if_controller_check_is_bypassed() {
        primary(COACH_A, PATIENT_A);
        primary(COACH_B, PATIENT_B);
        Instant t = now().minusSeconds(60);
        bp(PATIENT_A, t, 120, 80);
        bp(PATIENT_B, t, 185, 95);
        alert(PATIENT_A, COACH_A, Measure.SYSTOLIC, Severity.HIGH, AlertStatus.OPEN, t);
        long alertB = alert(PATIENT_B, COACH_B, Measure.SYSTOLIC, Severity.CRITICAL, AlertStatus.OPEN, t);
        Caller coachA = new Caller(String.valueOf(COACH_A), Caller.Kind.COACH, java.util.Set.of());
        Instant from = t.minusSeconds(3600);
        Instant to = t.plusSeconds(3600);

        // The repositories are called directly, as a controller that forgot its panel check would.
        assertThat(rls.read(coachA, () -> readings.findLatest(PATIENT_B, ReadingType.BLOOD_PRESSURE))).isEmpty();
        assertThat(rls.read(coachA, () -> readings.findRange(PATIENT_B, ReadingType.BLOOD_PRESSURE, from, to, null,
                100)).items()).isEmpty();
        // Coach A asking for coach B's queue sees nothing of patient B.
        assertThat(rls.read(coachA, () -> queue.queue(COACH_B, Optional.empty(), null, 50)).items()).isEmpty();
        int updated = rls.write(coachA, () -> coachPrimary.sql(
                        "UPDATE alert SET status = 'ACKNOWLEDGED', acknowledged_by = ?, acknowledged_at = now(), "
                                + "version = version + 1 WHERE alert_id = ?")
                .params(COACH_A, alertB).update());
        assertThat(updated).isZero();
        assertThat(alertRow(alertB)).containsEntry("status", "OPEN");

        // Same pool, own panel: visible. Without a coach context: nothing.
        assertThat(rls.read(coachA, () -> readings.findLatest(PATIENT_A, ReadingType.BLOOD_PRESSURE))).isPresent();
        assertThat(rls.read(coachA, () -> queue.queue(COACH_A, Optional.empty(), null, 50)).items()).hasSize(1);
        assertThat(coachPrimary.sql("SELECT count(*) FROM reading").query(Long.class).single()).isZero();
        assertThat(coachPrimary.sql("SELECT count(*) FROM alert").query(Long.class).single()).isZero();
    }

    @Test
    void allowed_read_writes_allowed_audit_row() throws Exception {
        primary(COACH_A, PATIENT_A);
        bp(PATIENT_A, now().minusSeconds(60), 120, 80);

        Response r = get(coachToken(COACH_A), "/v1/patients/" + PATIENT_A + "/readings/latest?type=BLOOD_PRESSURE");

        assertThat(r.status()).isEqualTo(200);
        assertThat(audits("READ_LATEST_READING")).singleElement().satisfies(a -> assertThat(a)
                .containsEntry("outcome", "ALLOWED").containsEntry("patient_id", PATIENT_A)
                .containsEntry("actor_id", String.valueOf(COACH_A))
                .containsEntry("resource", "readings/latest/BLOOD_PRESSURE"));
    }

    @Test
    void backup_coach_can_read() throws Exception {
        primary(COACH_B, PATIENT_A);
        panel(COACH_A, PATIENT_A, "BACKUP");
        bp(PATIENT_A, now().minusSeconds(60), 120, 80);

        assertThat(get(coachToken(COACH_A), "/v1/patients/" + PATIENT_A + "/readings/latest?type=BLOOD_PRESSURE")
                .status()).isEqualTo(200);
    }

    @Test
    void access_ends_when_care_assignment_is_closed() throws Exception {
        primary(COACH_A, PATIENT_A);
        bp(PATIENT_A, now().minusSeconds(60), 120, 80);
        String path = "/v1/patients/" + PATIENT_A + "/readings/latest?type=BLOOD_PRESSURE";
        assertThat(get(coachToken(COACH_A), path).status()).isEqualTo(200);

        endAssignment(COACH_A, PATIENT_A);

        assertThat(get(coachToken(COACH_A), path).status()).isEqualTo(403);
        assertThat(audits("READ_LATEST_READING")).extracting(a -> a.get("outcome"))
                .containsExactly("ALLOWED", "DENIED");
    }

    @Test
    void missing_scope_is_403_and_audited() throws Exception {
        primary(COACH_A, PATIENT_A);

        Response r = get(token(String.valueOf(COACH_A), "COACH", ALERTS_READ),
                "/v1/patients/" + PATIENT_A + "/readings/latest?type=BLOOD_PRESSURE");

        assertThat(r.status()).isEqualTo(403);
        assertThat(audits("READ_LATEST_READING")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "DENIED"));
    }

    @Test
    void non_coach_caller_cannot_read_patient_data() throws Exception {
        primary(COACH_A, PATIENT_A);

        Response r = get(token("thresholds-ui", "CLINICIAN", READINGS_READ),
                "/v1/patients/" + PATIENT_A + "/readings/latest?type=BLOOD_PRESSURE");

        assertThat(r.status()).isEqualTo(403);
        assertThat(audits("READ_LATEST_READING")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "DENIED").containsEntry("actor_type", "ADMIN"));
    }

    @Test
    void no_token_or_bad_token_is_401() throws Exception {
        String path = "/v1/patients/" + PATIENT_A + "/readings/latest?type=BLOOD_PRESSURE";

        Response none = get(null, path);
        String valid = coachToken(COACH_A);
        String[] parts = valid.split("\\.");
        String otherSubject = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                JSON.writeValueAsBytes(((com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(
                        java.util.Base64.getUrlDecoder().decode(parts[1]))).put("sub", "12")));
        Response forged = get(parts[0] + "." + otherSubject + "." + parts[2], path);
        Response unknownActor = get(token("11", "ROBOT", READINGS_READ), path);

        for (Response r : List.of(none, forged, unknownActor)) {
            assertThat(r.status()).isEqualTo(401);
            assertThat(r.code()).isEqualTo("UNAUTHENTICATED");
        }
    }
}
