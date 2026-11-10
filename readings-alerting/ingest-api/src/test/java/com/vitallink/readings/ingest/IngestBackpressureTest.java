package com.vitallink.readings.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import com.vitallink.readings.ingest.backpressure.OutboxBacklogMonitor;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/** Backpressure (idempotency-and-concurrency.md): 503 + jittered Retry-After when overloaded, 429 per device. */
@TestPropertySource(properties = {
        "vitallink.ingest.backpressure.max-in-flight=1",
        "vitallink.ingest.backpressure.max-outbox-age=PT60S",
        "vitallink.ingest.backpressure.max-outbox-rows=50",
        "vitallink.ingest.backpressure.outbox-sample-interval=PT0.1S",
        "vitallink.ingest.backpressure.retry-after-min=PT5S",
        "vitallink.ingest.backpressure.retry-after-max=PT30S",
        "vitallink.ingest.backpressure.device-burst=3",
        "vitallink.ingest.backpressure.device-requests-per-minute=1"})
class IngestBackpressureTest extends IngestApiTestSupport {

    static final long PATIENT = 61;

    @Autowired
    OutboxBacklogMonitor backlog;

    @Autowired
    CircuitBreakerRegistry breakers;

    @BeforeEach
    void resetState() {
        breakers.getAllCircuitBreakers().forEach(cb -> cb.reset());
        // Tables were just truncated; wait for the sampler to see it so no test inherits the last one's backlog.
        await().atMost(Duration.ofSeconds(5)).until(() -> !backlog.overloaded());
    }

    @Test
    void outbox_backlog_over_limit_returns_503_with_retry_after() throws Exception {
        String key = givenDevice(61, boundTo(PATIENT, daysAgo(30)));
        // An outbox row nobody published for 10 minutes: the relay or Kafka is down.
        jdbc.sql("""
                        INSERT INTO outbox_event (event_id, topic, event_type, aggregate_key, payload, created_at)
                        VALUES (?, 'readings.ingested', 'readings.ingested', '61', '{}'::jsonb, now() - interval '10 minutes')
                        """)
                .param(UUID.randomUUID())
                .update();
        await().atMost(Duration.ofSeconds(5)).until(backlog::overloaded);

        Response response = post(61, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), now(), 120, 80))));

        assertThat(response.status()).isEqualTo(503);
        assertThat(response.contentType()).startsWith("application/problem+json");
        assertThat(response.code()).isEqualTo("OVERLOADED");
        assertThat(response.body().path("reason").asText()).isEqualTo("OUTBOX_BACKLOG");
        assertThat(Integer.parseInt(response.retryAfter())).isBetween(5, 30);
        assertThat(count("SELECT count(*) FROM reading WHERE device_id = ?", 61L)).isZero();

        // Backlog drained: ingest accepts again.
        jdbc.sql("DELETE FROM outbox_event").update();
        await().atMost(Duration.ofSeconds(5)).until(() -> !backlog.overloaded());
        assertThat(post(61, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), now(), 120, 80)))).status())
                .isEqualTo(202);
    }

    @Test
    void outbox_row_count_over_limit_returns_503() throws Exception {
        String key = givenDevice(62, boundTo(PATIENT, daysAgo(30)));
        jdbc.sql("""
                        INSERT INTO outbox_event (event_id, topic, event_type, aggregate_key, payload)
                        SELECT gen_random_uuid(), 'readings.ingested', 'readings.ingested', '62', '{}'::jsonb
                        FROM generate_series(1, 51)
                        """)
                .update();
        await().atMost(Duration.ofSeconds(5)).until(backlog::overloaded);

        Response response = post(62, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), now(), 120, 80))));

        assertThat(response.status()).isEqualTo(503);
        assertThat(response.body().path("reason").asText()).isEqualTo("OUTBOX_BACKLOG");
    }

    @Test
    void in_flight_limit_reached_returns_503_with_retry_after() throws Exception {
        String key = givenDevice(63, boundTo(PATIENT, daysAgo(30)));
        CompletableFuture<Response> first;
        try (Connection blocker = dataSource().getConnection(); Statement st = blocker.createStatement()) {
            // Hold the only in-flight permit: the first request blocks on its outbox INSERT until we commit.
            blocker.setAutoCommit(false);
            st.execute("LOCK TABLE outbox_event IN EXCLUSIVE MODE");
            first = CompletableFuture.supplyAsync(() -> postUnchecked(63, key, UUID.randomUUID()));
            await().atMost(Duration.ofSeconds(10)).until(() -> count("""
                    SELECT count(*) FROM pg_stat_activity
                    WHERE wait_event_type = 'Lock' AND query ILIKE '%INSERT INTO outbox_event%'
                    """) == 1);

            Response second = post(63, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), now(), 120, 80))));

            assertThat(second.status()).isEqualTo(503);
            assertThat(second.code()).isEqualTo("OVERLOADED");
            assertThat(second.body().path("reason").asText()).isEqualTo("IN_FLIGHT");
            assertThat(Integer.parseInt(second.retryAfter())).isBetween(5, 30);
            blocker.commit();
        }
        assertThat(first.get(10, TimeUnit.SECONDS).status()).isEqualTo(202);
    }

    @Test
    void per_device_rate_over_limit_returns_429() throws Exception {
        String key = givenDevice(64, boundTo(PATIENT, daysAgo(30)));
        String otherKey = givenDevice(65, boundTo(66, daysAgo(30)));

        for (int i = 0; i < 3; i++) {
            assertThat(post(64, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), now(), 120, 80)))).status())
                    .isEqualTo(202);
        }
        Response limited = post(64, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), now(), 120, 80))));

        assertThat(limited.status()).isEqualTo(429);
        assertThat(limited.contentType()).startsWith("application/problem+json");
        assertThat(limited.code()).isEqualTo("RATE_LIMITED");
        // 1 request per minute: the next token is due within 60 s.
        assertThat(Integer.parseInt(limited.retryAfter())).isBetween(1, 60);
        assertThat(count("SELECT count(*) FROM reading WHERE device_id = ?", 64L)).isEqualTo(3);
        // The bucket is per device.
        assertThat(post(65, otherKey, batch(66, List.of(bp(UUID.randomUUID(), now(), 120, 80)))).status())
                .isEqualTo(202);
    }

    @Test
    void redis_down_rate_limiter_lets_requests_through() throws Exception {
        String key = givenDevice(67, boundTo(PATIENT, daysAgo(30)));
        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            long started = System.nanoTime();
            // More requests than the burst of 3: without Redis nothing is rate limited.
            for (int i = 0; i < 6; i++) {
                Instant t = now().minusSeconds(i);
                assertThat(post(67, key, batch(PATIENT, List.of(bp(UUID.randomUUID(), t, 120, 80)))).status())
                        .isEqualTo(202);
            }
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
        } finally {
            REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
        }
        assertThat(count("SELECT count(*) FROM reading WHERE device_id = ?", 67L)).isEqualTo(6);
    }

    private Response postUnchecked(long deviceId, String key, UUID readingId) {
        try {
            return post(deviceId, key, batch(PATIENT, List.of(bp(readingId, now(), 120, 80))));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
