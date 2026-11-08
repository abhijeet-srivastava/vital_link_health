package com.vitallink.readings.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import com.fasterxml.jackson.databind.JsonNode;

@ExtendWith(OutputCaptureExtension.class)
class ReadingIngestIdempotencyTest extends IngestApiTestSupport {

    static final long DEVICE = 11;
    static final long PATIENT = 8812;

    @Test
    void retried_batch_sent_concurrently_from_two_threads_is_stored_once_and_announced_once() throws Exception {
        String key = givenDevice(DEVICE, boundTo(PATIENT, daysAgo(30)));
        Instant now = now();
        Map<String, Object> body = batch(PATIENT, IntStream.range(0, 50)
                .mapToObj(i -> bp(UUID.randomUUID(), now.minusSeconds(60L * i), 120 + i % 30, 80))
                .toList());

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Response> first = pool.submit(() -> {
                start.await();
                return post(DEVICE, key, body);
            });
            Future<Response> second = pool.submit(() -> {
                start.await();
                return post(DEVICE, key, body);
            });
            start.countDown();
            Response a = first.get(60, TimeUnit.SECONDS);
            Response b = second.get(60, TimeUnit.SECONDS);

            assertThat(a.status()).isEqualTo(202);
            assertThat(b.status()).isEqualTo(202);
            assertThat(List.of(a.statusSet(), b.statusSet()))
                    .containsExactlyInAnyOrder(Set.of("ACCEPTED"), Set.of("DUPLICATE"));
        } finally {
            pool.shutdownNow();
        }

        assertThat(count("SELECT count(*) FROM reading WHERE device_id = ?", DEVICE)).isEqualTo(50);
        assertThat(count("SELECT count(DISTINCT reading_uuid) FROM reading WHERE device_id = ?", DEVICE)).isEqualTo(50);
        assertThat(count("SELECT count(*) FROM outbox_event")).isEqualTo(1);

        Map<String, Object> event = jdbc.sql("SELECT event_id, topic, event_type, aggregate_key, payload::text AS payload FROM outbox_event")
                .query().singleRow();
        assertThat(event.get("topic")).isEqualTo("readings.ingested");
        assertThat(event.get("event_type")).isEqualTo("readings.ingested");
        assertThat(event.get("aggregate_key")).isEqualTo(String.valueOf(PATIENT));
        assertThat(((UUID) event.get("event_id")).version()).isEqualTo(7);
        JsonNode payload = parse((String) event.get("payload"));
        assertThat(payload.path("readings")).hasSize(50);
        assertThat(payload.path("patient_id").asLong()).isEqualTo(PATIENT);
        assertThat(payload.path("event_id").asText()).isEqualTo(event.get("event_id").toString());
    }

    @Test
    void same_reading_id_with_different_values_is_conflict_and_original_row_unchanged() throws Exception {
        String key = givenDevice(DEVICE, boundTo(PATIENT, daysAgo(30)));
        UUID id = UUID.randomUUID();
        Instant at = now().minusSeconds(120);

        Response original = post(DEVICE, key, batch(PATIENT, List.of(bp(id, at, 150, 90))));
        long originalHash = count("SELECT payload_hash FROM reading WHERE reading_uuid = ?", id);
        Response changed = post(DEVICE, key, batch(PATIENT, List.of(bp(id, at, 151, 90))));

        assertThat(original.status()).isEqualTo(202);
        assertThat(original.statuses()).containsExactly("ACCEPTED");
        assertThat(changed.status()).isEqualTo(202);
        assertThat(changed.statuses()).containsExactly("CONFLICT");
        assertThat(changed.body().path("results").get(0).path("reading_id").asText()).isEqualTo(id.toString());

        assertThat(count("SELECT count(*) FROM reading WHERE reading_uuid = ?", id)).isEqualTo(1);
        assertThat(count("SELECT systolic FROM reading WHERE reading_uuid = ?", id)).isEqualTo(150);
        assertThat(count("SELECT payload_hash FROM reading WHERE reading_uuid = ?", id)).isEqualTo(originalHash);
        assertThat(count("SELECT count(*) FROM outbox_event")).isEqualTo(1);
    }

    @Test
    void same_reading_twice_in_one_batch_is_accepted_then_duplicate() throws Exception {
        String key = givenDevice(DEVICE, boundTo(PATIENT, daysAgo(30)));
        Map<String, Object> reading = bp(UUID.randomUUID(), now().minusSeconds(30), 140, 85);

        Response response = post(DEVICE, key, batch(PATIENT, List.of(reading, reading)));

        assertThat(response.status()).isEqualTo(202);
        assertThat(response.statuses()).containsExactly("ACCEPTED", "DUPLICATE");
        assertThat(count("SELECT count(*) FROM reading WHERE device_id = ?", DEVICE)).isEqualTo(1);
        assertThat(parse(jdbc.sql("SELECT payload::text FROM outbox_event").query(String.class).single())
                .path("readings")).hasSize(1);
    }

    @Test
    void reading_without_id_gets_stable_uuidv5_and_retry_is_duplicate() throws Exception {
        String key = givenDevice(DEVICE, boundTo(PATIENT, daysAgo(30)));
        Map<String, Object> body = batch(PATIENT, List.of(bp(null, now().minusSeconds(30), 130, 80)));

        Response first = post(DEVICE, key, body);
        Response retry = post(DEVICE, key, body);

        assertThat(first.statuses()).containsExactly("ACCEPTED");
        assertThat(retry.statuses()).containsExactly("DUPLICATE");
        String derived = first.body().path("results").get(0).path("reading_id").asText();
        assertThat(UUID.fromString(derived).version()).isEqualTo(5);
        assertThat(retry.body().path("results").get(0).path("reading_id").asText()).isEqualTo(derived);
        assertThat(count("SELECT count(*) FROM reading WHERE device_id = ?", DEVICE)).isEqualTo(1);
    }

    @Test
    void outbox_payload_and_logs_contain_no_phi_fields(CapturedOutput output) throws Exception {
        String key = givenDevice(DEVICE, boundTo(PATIENT, daysAgo(30)));
        UUID id = UUID.randomUUID();
        Instant at = now().minusSeconds(60);
        post(DEVICE, key, batch(PATIENT, List.of(bp(id, at, 187, 93))));
        post(DEVICE, key, batch(PATIENT, List.of(bp(id, at, 188, 93))));   // CONFLICT -> integrity log line

        JsonNode payload = parse(jdbc.sql("SELECT payload::text FROM outbox_event").query(String.class).single());
        assertThat(fieldNames(payload))
                .containsExactlyInAnyOrder("event_id", "type", "schema_version", "patient_id", "device_id", "readings");
        assertThat(payload.path("schema_version").asInt()).isEqualTo(1);
        assertThat(Set.of("reading_uuid", "reading_type", "measured_at", "device_seq",
                "systolic", "diastolic", "pulse", "value"))
                .containsAll(fieldNames(payload.path("readings").get(0)));

        String logs = output.getAll();
        assertThat(logs).contains("integrity").contains(id.toString());
        assertThat(logs).doesNotContain("systolic").doesNotContain("diastolic");
        // The values as standalone numbers (not inside a UUID, timestamp or port).
        assertThat(logs).doesNotContainPattern("(?<![\\w.:-])18[78](?!\\w)");
        assertThat(logs).doesNotContain(key.substring(key.lastIndexOf('_') + 1));
    }

    private static List<String> fieldNames(JsonNode node) {
        List<String> names = new ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }
}
