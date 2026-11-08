package com.vitallink.readings.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReadingIngestValidationTest extends IngestApiTestSupport {

    static final long DEVICE = 31;
    static final long PATIENT = 31;

    String key;

    @BeforeEach
    void device() {
        key = givenDevice(DEVICE, boundTo(PATIENT, daysAgo(200)));
    }

    @Test
    void batch_over_500_413() throws Exception {
        Response tooLarge = post(DEVICE, key, batch(PATIENT, readings(501)));

        assertThat(tooLarge.status()).isEqualTo(413);
        assertThat(tooLarge.contentType()).startsWith("application/problem+json");
        assertThat(tooLarge.code()).isEqualTo("BATCH_TOO_LARGE");
        assertThat(count("SELECT count(*) FROM reading")).isZero();

        Response atLimit = post(DEVICE, key, batch(PATIENT, readings(500)));
        assertThat(atLimit.status()).isEqualTo(202);
        assertThat(count("SELECT count(*) FROM reading")).isEqualTo(500);
    }

    @Test
    void reading_older_than_90_days_rejected_when_flag_on() throws Exception {
        Response response = post(DEVICE, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), daysAgo(91), 120, 80))));

        assertThat(response.status()).isEqualTo(422);
        assertThat(response.contentType()).startsWith("application/problem+json");
        assertThat(response.code()).isEqualTo("READING_TOO_OLD");
        assertThat(response.body().path("results").get(0).path("code").asText()).isEqualTo("READING_TOO_OLD");
        assertThat(count("SELECT count(*) FROM reading")).isZero();
        assertThat(count("SELECT count(*) FROM outbox_event")).isZero();
    }

    @Test
    void future_timestamp_item_rejected() throws Exception {
        Response tooFar = post(DEVICE, key, batch(PATIENT,
                List.of(bp(UUID.randomUUID(), now().plus(6, ChronoUnit.MINUTES), 120, 80))));
        Response withinSkew = post(DEVICE, key, batch(PATIENT,
                List.of(bp(UUID.randomUUID(), now().plus(4, ChronoUnit.MINUTES), 120, 80))));

        assertThat(tooFar.status()).isEqualTo(422);
        assertThat(tooFar.code()).isEqualTo("FUTURE_TIMESTAMP");
        assertThat(withinSkew.status()).isEqualTo(202);
        assertThat(withinSkew.statuses()).containsExactly("ACCEPTED");
        assertThat(count("SELECT count(*) FROM reading")).isEqualTo(1);
    }

    @Test
    void mixed_batch_returns_202_with_rejected_item() throws Exception {
        UUID good = UUID.randomUUID();
        UUID future = UUID.randomUUID();

        Response response = post(DEVICE, key, batch(PATIENT, List.of(
                bp(good, now().minusSeconds(60), 120, 80),
                bp(future, now().plus(1, ChronoUnit.HOURS), 120, 80))));

        assertThat(response.status()).isEqualTo(202);
        assertThat(response.statuses()).containsExactly("ACCEPTED", "REJECTED");
        assertThat(response.body().path("results").get(1).path("code").asText()).isEqualTo("FUTURE_TIMESTAMP");
        assertThat(response.body().path("results").get(1).path("reading_id").asText()).isEqualTo(future.toString());
        assertThat(count("SELECT count(*) FROM reading")).isEqualTo(1);
    }

    @Test
    void implausible_value_rejected_from_measure_bounds() throws Exception {
        // measure: systolic plausible 40..300
        Response response = post(DEVICE, key, batch(PATIENT, List.of(
                bp(UUID.randomUUID(), now().minusSeconds(60), 300, 80),
                bp(UUID.randomUUID(), now().minusSeconds(50), 301, 80))));

        assertThat(response.status()).isEqualTo(202);
        assertThat(response.statuses()).containsExactly("ACCEPTED", "REJECTED");
        assertThat(response.body().path("results").get(1).path("code").asText()).isEqualTo("IMPLAUSIBLE_VALUE");
    }

    @Test
    void malformed_body_422() throws Exception {
        Response notJson = postRaw(DEVICE, key, "{\"patient_id\": 31, \"readings\": [");
        Response noPatient = post(DEVICE, key, Map.of("readings", readings(1)));
        Response noReadings = post(DEVICE, key, Map.of("patient_id", PATIENT, "readings", List.of()));

        for (Response r : List.of(notJson, noPatient, noReadings)) {
            assertThat(r.status()).isEqualTo(422);
            assertThat(r.contentType()).startsWith("application/problem+json");
            assertThat(r.code()).isEqualTo("MALFORMED_REQUEST");
        }
    }

    private static List<Map<String, Object>> readings(int n) {
        Instant base = now().minusSeconds(3600);
        return IntStream.range(0, n).mapToObj(i -> bp(UUID.randomUUID(), base.plusSeconds(i), 120, 80)).toList();
    }
}
