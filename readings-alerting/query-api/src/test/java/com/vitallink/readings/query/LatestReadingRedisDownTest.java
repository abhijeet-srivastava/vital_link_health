package com.vitallink.readings.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/** Redis is an accelerator only: with Redis paused, latest falls back to the replica within its timeout. */
class LatestReadingRedisDownTest extends QueryApiTestSupport {

    static final long COACH = 22;
    static final long PATIENT = 2201;

    @Test
    void latest_falls_back_to_db_when_redis_is_down() throws Exception {
        primary(COACH, PATIENT);
        UUID id = bp(PATIENT, now().minusSeconds(60), 132, 84);

        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            long started = System.nanoTime();
            Response r = get(coachToken(COACH), "/v1/patients/" + PATIENT + "/readings/latest?type=BLOOD_PRESSURE");
            Duration took = Duration.ofNanos(System.nanoTime() - started);

            assertThat(r.status()).isEqualTo(200);
            assertThat(r.body().path("source").asText()).isEqualTo("db");
            assertThat(r.body().path("reading").path("reading_id").asText()).isEqualTo(id.toString());
            assertThat(took).isLessThan(Duration.ofSeconds(3));
            assertThat(audits("READ_LATEST_READING")).singleElement()
                    .satisfies(a -> assertThat(a).containsEntry("outcome", "ALLOWED"));
        } finally {
            REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
        }
    }
}
