package com.vitallink.readings.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;

/** Internal panel change: the new PRIMARY takes over the unresolved alerts and the old coach loses access. */
class CareAssignmentAdminTest extends QueryApiTestSupport {

    static final long OLD_COACH = 71;
    static final long NEW_COACH = 72;
    static final long PATIENT = 7101;
    static final String SERVICE = token("care-team-sync", "SERVICE", "assignments:write");

    @Test
    void reassignment_moves_alerts_and_new_coach_sees_them_old_coach_does_not() throws Exception {
        primary(OLD_COACH, PATIENT);
        long alertId = alert(PATIENT, OLD_COACH, Measure.SYSTOLIC, Severity.CRITICAL, AlertStatus.OPEN, now());

        Response r = send(SERVICE, "POST", "/internal/v1/care-assignments",
                Map.of("patient_id", PATIENT, "coach_id", NEW_COACH), Map.of());

        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body().path("moved_alerts").asInt()).isEqualTo(1);
        assertThat(alertRow(alertId)).containsEntry("coach_id", NEW_COACH);
        assertThat(texts(get(coachToken(NEW_COACH), "/v1/coaches/" + NEW_COACH + "/alerts").body().path("items"),
                "alert_id")).containsExactly(String.valueOf(alertId));
        assertThat(get(coachToken(OLD_COACH), "/v1/coaches/" + OLD_COACH + "/alerts").body().path("items")).isEmpty();
        assertThat(send(coachToken(OLD_COACH), "POST", "/v1/alerts/" + alertId + "/acknowledge", null,
                Map.of("If-Match", "\"1\"")).status()).isEqualTo(403);
        assertThat(audits("ASSIGN_PRIMARY_COACH")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("actor_type", "SERVICE").containsEntry("outcome", "ALLOWED"));
        assertThat(audits("REASSIGN_ALERT")).hasSize(1);
    }

    @Test
    void requires_assignments_write_scope() throws Exception {
        primary(OLD_COACH, PATIENT);

        Response coach = send(coachToken(OLD_COACH), "POST", "/internal/v1/care-assignments",
                Map.of("patient_id", PATIENT, "coach_id", OLD_COACH), Map.of());

        assertThat(coach.status()).isEqualTo(403);
        assertThat(audits("ASSIGN_PRIMARY_COACH")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "DENIED"));
        assertThat(count("SELECT count(*) FROM care_assignment")).isEqualTo(1);
    }
}
