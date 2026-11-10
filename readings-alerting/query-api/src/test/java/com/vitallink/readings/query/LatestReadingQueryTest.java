package com.vitallink.readings.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.ports.LatestReadingCache;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/** testing.md, Latest reading: out-of-order readings never overwrite a newer cached value. */
class LatestReadingQueryTest extends QueryApiTestSupport {

    static final long COACH = 21;
    static final long PATIENT = 2101;
    static final String PATH = "/v1/patients/" + PATIENT + "/readings/latest?type=BLOOD_PRESSURE";
    static final String KEY = "latest:" + PATIENT + ":BLOOD_PRESSURE";

    @Autowired
    LatestReadingCache cache;

    @Autowired
    CircuitBreakerRegistry breakers;

    @BeforeEach
    void setUp() {
        breakers.getAllCircuitBreakers().forEach(cb -> cb.reset());
        primary(COACH, PATIENT);
    }

    @Test
    void out_of_order_readings_never_overwrite_newer_latest_cache_entry() throws Exception {
        Instant t = now().minusSeconds(600);
        UUID older = bp(PATIENT, t.minusSeconds(3600), 160, 95);
        UUID newer = bp(PATIENT, t, 130, 85);

        // Cache miss: the DB fallback returns the newest row and backfills the cache.
        Response first = get(coachToken(COACH), PATH);
        assertThat(first.status()).isEqualTo(200);
        assertThat(first.body().path("source").asText()).isEqualTo("db");
        assertThat(first.body().path("reading").path("reading_id").asText()).isEqualTo(newer.toString());
        assertThat(cachedReadingUuid()).isEqualTo(newer.toString());

        // A backfill computed from a lagging replica, or a late ingest of an older reading, arrives afterwards.
        cache.putIfNewer(PATIENT, ReadingType.BLOOD_PRESSURE, reading(older, t.minusSeconds(3600), 160, 95));
        cache.putIfNewer(PATIENT, ReadingType.BLOOD_PRESSURE, reading(UUID.randomUUID(), t.minusSeconds(1), 170, 99));
        assertThat(cachedReadingUuid()).isEqualTo(newer.toString());

        Response second = get(coachToken(COACH), PATH);
        assertThat(second.body().path("source").asText()).isEqualTo("cache");
        assertThat(second.body().path("reading").path("reading_id").asText()).isEqualTo(newer.toString());

        // A strictly newer reading still replaces it.
        UUID newest = UUID.randomUUID();
        cache.putIfNewer(PATIENT, ReadingType.BLOOD_PRESSURE, reading(newest, t.plusSeconds(1), 125, 80));
        assertThat(cachedReadingUuid()).isEqualTo(newest.toString());
    }

    @Test
    void latest_served_from_redis_in_ingest_format_without_db_row() throws Exception {
        // Written the way ingest-api writes it (snake_case JSON, measured_at as epoch micros); no DB row exists.
        Instant t = now().minusSeconds(30);
        UUID id = UUID.randomUUID();
        String json = """
                {"device_id":%d,"reading_uuid":"%s","measured_at":"%s","patient_id":%d,"type":"BLOOD_PRESSURE",\
                "device_seq":7,"systolic":141,"diastolic":91,"pulse":66,"value_num":null,"payload_hash":5}"""
                .formatted(PATIENT, id, t, PATIENT);
        redisCli("HSET", KEY, "m", String.valueOf(ChronoUnit.MICROS.between(Instant.EPOCH, t)), "v", json);

        Response r = get(coachToken(COACH), PATH);

        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body().path("source").asText()).isEqualTo("cache");
        assertThat(r.body().path("reading").path("reading_id").asText()).isEqualTo(id.toString());
        assertThat(r.body().path("reading").path("systolic").asInt()).isEqualTo(141);
        assertThat(r.body().path("reading").has("payload_hash")).isFalse();
    }

    @Test
    void no_reading_yet_is_404_and_still_audited() throws Exception {
        Response r = get(coachToken(COACH), PATH);

        assertThat(r.status()).isEqualTo(404);
        assertThat(r.code()).isEqualTo("NO_READING");
        assertThat(audits("READ_LATEST_READING")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "ALLOWED"));
    }

    private String cachedReadingUuid() throws Exception {
        return JSON.readTree(redisCli("HGET", KEY, "v")).path("reading_uuid").asText();
    }

    private static Reading reading(UUID id, Instant measuredAt, int systolic, int diastolic) {
        return new Reading(PATIENT, id, measuredAt, PATIENT, ReadingType.BLOOD_PRESSURE, 1L, systolic, diastolic, 70,
                null, 42L);
    }
}
