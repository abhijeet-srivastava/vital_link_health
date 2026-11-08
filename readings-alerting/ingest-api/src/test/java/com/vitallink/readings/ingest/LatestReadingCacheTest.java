package com.vitallink.readings.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

class LatestReadingCacheTest extends IngestApiTestSupport {

    static final long DEVICE = 51;
    static final long PATIENT = 51;
    static final String LATEST_BP = "latest:" + PATIENT + ":BLOOD_PRESSURE";

    @Autowired
    CircuitBreakerRegistry breakers;

    String key;

    @BeforeEach
    void setUp() {
        breakers.getAllCircuitBreakers().forEach(cb -> cb.reset());
        key = givenDevice(DEVICE, boundTo(PATIENT, daysAgo(30)));
    }

    @Test
    void out_of_order_readings_never_overwrite_newer_latest_cache_entry() throws Exception {
        Instant t = now().minusSeconds(600);
        UUID newer = UUID.randomUUID();
        UUID older = UUID.randomUUID();
        UUID oldest = UUID.randomUUID();
        UUID newest = UUID.randomUUID();

        post(DEVICE, key, batch(PATIENT, List.of(bp(newer, t, 130, 85))));
        post(DEVICE, key, batch(PATIENT, List.of(bp(older, t.minusSeconds(3600), 160, 95))));
        assertThat(cachedReadingUuid()).isEqualTo(newer.toString());

        // Within one batch the newest measured_at wins, whatever the order in the request.
        post(DEVICE, key, batch(PATIENT, List.of(
                bp(newest, t.plusSeconds(300), 128, 84),
                bp(oldest, t.minusSeconds(7200), 170, 99))));
        assertThat(cachedReadingUuid()).isEqualTo(newest.toString());
    }

    @Test
    void redis_down_ingest_still_returns_202() throws Exception {
        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            long started = System.nanoTime();
            Response response = post(DEVICE, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), now(), 120, 80))));
            Duration took = Duration.ofNanos(System.nanoTime() - started);

            assertThat(response.status()).isEqualTo(202);
            assertThat(response.statuses()).containsExactly("ACCEPTED");
            assertThat(took).isLessThan(Duration.ofSeconds(3));
            assertThat(count("SELECT count(*) FROM reading WHERE device_id = ?", DEVICE)).isEqualTo(1);
            assertThat(count("SELECT count(*) FROM outbox_event")).isEqualTo(1);
        } finally {
            REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
        }
    }

    private String cachedReadingUuid() throws Exception {
        return parse(redisCli("HGET", LATEST_BP, "v")).path("reading_uuid").asText();
    }
}
