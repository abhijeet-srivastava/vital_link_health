package com.vitallink.readings.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Range reads page with keyset cursors on (measured_at, reading_uuid), never OFFSET (rule 6). */
class ReadingRangeQueryTest extends QueryApiTestSupport {

    static final long COACH = 31;
    static final long PATIENT = 3101;

    Instant base;
    Instant from;
    Instant to;

    @BeforeEach
    void setUp() {
        primary(COACH, PATIENT);
        base = now().minus(Duration.ofHours(2));
        from = base.minus(Duration.ofDays(1));
        to = base.plus(Duration.ofHours(1));
    }

    record Row(Instant measuredAt, UUID id) {
    }

    @Test
    void range_query_pages_with_keyset_cursor_without_gaps_or_repeats() throws Exception {
        // 11 readings, several sharing a measured_at so ties fall across page boundaries (page size 3).
        List<Row> stored = new ArrayList<>();
        int[] minutes = {0, 0, 0, 5, 5, 10, 15, 15, 15, 15, 20};
        for (int m : minutes) {
            Instant at = base.minus(Duration.ofMinutes(m));
            stored.add(new Row(at, bp(PATIENT, at, 120 + m, 80)));
        }
        bp(PATIENT, from.minusSeconds(1), 150, 90);            // outside the range: never returned
        bp(PATIENT, to, 150, 90);                              // to is exclusive

        List<Row> paged = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            Response page = page(cursor, 3);
            assertThat(page.status()).isEqualTo(200);
            page.body().path("items").forEach(i -> paged.add(new Row(Instant.parse(i.path("measured_at").asText()),
                    UUID.fromString(i.path("reading_id").asText()))));
            cursor = page.body().path("next_cursor").isNull() ? null : page.body().path("next_cursor").asText();
            pages++;
        } while (cursor != null && pages < 10);

        List<Row> expected = stored.stream()
                .sorted(Comparator.comparing(Row::measuredAt).reversed()
                        // Postgres orders uuid by unsigned bytes, as the hex string does (UUID.compareTo is signed).
                        .thenComparing(r -> r.id().toString()))
                .toList();
        assertThat(pages).isEqualTo(4);
        assertThat(paged).containsExactlyElementsOf(expected);
        assertThat(audits("READ_READINGS")).hasSize(4)
                .allSatisfy(a -> assertThat(a).containsEntry("outcome", "ALLOWED").containsEntry("patient_id", PATIENT));
    }

    @Test
    void newer_reading_inserted_between_pages_does_not_shift_pages() throws Exception {
        // With OFFSET, a row arriving at the head between two requests pushes the last row of page 1 onto page 2.
        for (int m = 0; m < 6; m++) {
            bp(PATIENT, base.minus(Duration.ofMinutes(m)), 120, 80);
        }
        Response first = page(null, 3);
        List<String> firstIds = texts(first.body().path("items"), "reading_id");

        bp(PATIENT, base.plusSeconds(30), 125, 82);
        Response second = page(first.body().path("next_cursor").asText(), 3);
        List<String> secondIds = texts(second.body().path("items"), "reading_id");

        assertThat(secondIds).hasSize(3).doesNotContainAnyElementsOf(firstIds);
        assertThat(second.body().path("next_cursor").isNull()).isTrue();
    }

    @Test
    void invalid_ranges_limits_and_cursors_are_400() throws Exception {
        String token = coachToken(COACH);
        String p = "/v1/patients/" + PATIENT + "/readings?type=BLOOD_PRESSURE";

        assertThat(get(token, p + "&from=" + to.minus(Duration.ofDays(91)) + "&to=" + to).code())
                .isEqualTo("RANGE_TOO_WIDE");
        assertThat(get(token, p + "&from=" + to + "&to=" + from).code()).isEqualTo("INVALID_RANGE");
        assertThat(get(token, p + "&from=" + from).code()).isEqualTo("MISSING_PARAMETER");
        assertThat(get(token, p + "&from=" + from + "&to=" + to + "&limit=501").code()).isEqualTo("INVALID_LIMIT");
        assertThat(get(token, p + "&from=" + from + "&to=" + to + "&limit=0").code()).isEqualTo("INVALID_LIMIT");
        assertThat(get(token, p + "&from=" + from + "&to=" + to + "&cursor=bm90LWEtY3Vyc29y").code())
                .isEqualTo("INVALID_CURSOR");
        assertThat(get(token, p + "&from=" + from + "&to=" + to + "&cursor=%25%25").code())
                .isEqualTo("INVALID_CURSOR");
        assertThat(get(token, "/v1/patients/" + PATIENT + "/readings?type=NOPE&from=" + from + "&to=" + to).code())
                .isEqualTo("INVALID_PARAMETER");
        assertThat(get(token, p + "&from=" + from + "&to=" + to + "&limit=500").status()).isEqualTo(200);
    }

    private Response page(String cursor, int limit) throws Exception {
        String path = "/v1/patients/" + PATIENT + "/readings?type=BLOOD_PRESSURE&from=" + from + "&to=" + to
                + "&limit=" + limit
                + (cursor == null ? "" : "&cursor=" + URLEncoder.encode(cursor, StandardCharsets.UTF_8));
        return get(coachToken(COACH), path);
    }
}
