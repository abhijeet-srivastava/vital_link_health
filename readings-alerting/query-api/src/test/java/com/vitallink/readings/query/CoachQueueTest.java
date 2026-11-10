package com.vitallink.readings.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;

/** api-contracts.md: the queue holds unresolved alerts only, ordered by severity_rank, then age. */
class CoachQueueTest extends QueryApiTestSupport {

    static final long COACH = 41;
    static final long OTHER_COACH = 42;

    @Test
    void queue_orders_by_severity_then_age() throws Exception {
        Instant t = now().minus(Duration.ofHours(3));
        // One patient per alert (one unresolved episode per patient and measure); created out of order on purpose.
        long highNew = givenAlert(4101, Severity.HIGH, AlertStatus.OPEN, t.plus(Duration.ofMinutes(50)));
        long criticalNew = givenAlert(4102, Severity.CRITICAL, AlertStatus.ESCALATED, t.plus(Duration.ofMinutes(40)));
        long mediumOld = givenAlert(4103, Severity.MEDIUM, AlertStatus.OPEN, t);
        long highOld = givenAlert(4104, Severity.HIGH, AlertStatus.ACKNOWLEDGED, t.plus(Duration.ofMinutes(10)));
        long criticalOld = givenAlert(4105, Severity.CRITICAL, AlertStatus.OPEN, t.plus(Duration.ofMinutes(5)));

        Response r = get(coachToken(COACH), "/v1/coaches/" + COACH + "/alerts");

        assertThat(r.status()).isEqualTo(200);
        assertThat(texts(r.body().path("items"), "alert_id")).containsExactly(
                String.valueOf(criticalOld), String.valueOf(criticalNew), String.valueOf(highOld),
                String.valueOf(highNew), String.valueOf(mediumOld));
        assertThat(r.body().path("next_cursor").isNull()).isTrue();
    }

    @Test
    void queue_shows_only_unresolved_alerts() throws Exception {
        Instant t = now().minus(Duration.ofHours(1));
        long open = givenAlert(4111, Severity.HIGH, AlertStatus.OPEN, t);
        givenAlert(4112, Severity.CRITICAL, AlertStatus.RESOLVED, t);
        long acked = givenAlert(4113, Severity.MEDIUM, AlertStatus.ACKNOWLEDGED, t);

        Response all = get(coachToken(COACH), "/v1/coaches/" + COACH + "/alerts");
        Response onlyOpen = get(coachToken(COACH), "/v1/coaches/" + COACH + "/alerts?status=OPEN");
        Response resolved = get(coachToken(COACH), "/v1/coaches/" + COACH + "/alerts?status=RESOLVED");

        assertThat(texts(all.body().path("items"), "alert_id"))
                .containsExactly(String.valueOf(open), String.valueOf(acked));
        assertThat(texts(all.body().path("items"), "status")).doesNotContain("RESOLVED");
        assertThat(texts(onlyOpen.body().path("items"), "alert_id")).containsExactly(String.valueOf(open));
        assertThat(resolved.status()).isEqualTo(400);
        assertThat(resolved.code()).isEqualTo("INVALID_STATUS");
    }

    @Test
    void queue_pages_with_cursor_across_equal_severity_and_created_at() throws Exception {
        // 120 HIGH alerts created at the same instant: only alert_id orders them, across three pages of 50.
        Instant t = now().minus(Duration.ofHours(1));
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            expected.add(String.valueOf(givenAlert(5000 + i, Severity.HIGH, AlertStatus.OPEN, t)));
        }

        List<String> seen = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            Response page = get(coachToken(COACH), "/v1/coaches/" + COACH + "/alerts"
                    + (cursor == null ? "" : "?cursor=" + URLEncoder.encode(cursor, StandardCharsets.UTF_8)));
            seen.addAll(texts(page.body().path("items"), "alert_id"));
            cursor = page.body().path("next_cursor").isNull() ? null : page.body().path("next_cursor").asText();
            pages++;
        } while (cursor != null && pages < 10);

        assertThat(pages).isEqualTo(3);
        assertThat(seen).containsExactlyElementsOf(expected);
    }

    @Test
    void coach_reading_another_coachs_queue_is_403_and_audited() throws Exception {
        givenAlert(4121, Severity.CRITICAL, AlertStatus.OPEN, now());

        Response r = get(coachToken(OTHER_COACH), "/v1/coaches/" + COACH + "/alerts");

        assertThat(r.status()).isEqualTo(403);
        assertThat(audits("READ_ALERT_QUEUE")).singleElement().satisfies(a -> assertThat(a)
                .containsEntry("outcome", "DENIED").containsEntry("actor_id", String.valueOf(OTHER_COACH))
                .containsEntry("resource", "coaches/" + COACH + "/alerts"));
    }

    @Test
    void queue_page_audits_each_patient_shown() throws Exception {
        Instant t = now().minus(Duration.ofHours(1));
        givenAlert(4131, Severity.CRITICAL, AlertStatus.OPEN, t);
        primary(COACH, 4132);
        alert(4132, COACH, Measure.SYSTOLIC, Severity.HIGH, AlertStatus.OPEN, t);
        alert(4132, COACH, Measure.DIASTOLIC, Severity.HIGH, AlertStatus.OPEN, t);

        assertThat(get(coachToken(COACH), "/v1/coaches/" + COACH + "/alerts").status()).isEqualTo(200);

        assertThat(audits("READ_ALERT_QUEUE")).extracting(a -> a.get("patient_id"))
                .containsExactlyInAnyOrder(4131L, 4132L);
        assertThat(audits("READ_ALERT_QUEUE")).allSatisfy(a -> assertThat(a).containsEntry("outcome", "ALLOWED"));
    }

    private long givenAlert(long patientId, Severity severity, AlertStatus status, Instant createdAt) {
        primary(COACH, patientId);
        return alert(patientId, COACH, Measure.SYSTOLIC, severity, status, createdAt);
    }
}
