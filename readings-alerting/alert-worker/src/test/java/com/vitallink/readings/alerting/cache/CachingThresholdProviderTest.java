package com.vitallink.readings.alerting.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.domain.ThresholdBand;
import com.vitallink.readings.domain.ThresholdBand.Operator;
import com.vitallink.readings.ports.ThresholdProvider;
import com.vitallink.readings.testsupport.IntegrationTestBase;

/** L1 Caffeine -> L2 Redis (MGET) -> DB, against a real Redis. The DB tier is a counting fake. */
class CachingThresholdProviderTest extends IntegrationTestBase {

    static final ThresholdBand SYS = new ThresholdBand(7, Measure.SYSTOLIC, Operator.GT, new BigDecimal("180.00"),
            Severity.CRITICAL);

    final CountingProvider db = new CountingProvider();
    final List<LettuceConnectionFactory> factories = new ArrayList<>();

    @AfterEach
    void closeConnections() {
        factories.forEach(LettuceConnectionFactory::destroy);
    }

    @Test
    void l1_hit_reaches_neither_redis_nor_db() throws Exception {
        CachingThresholdProvider provider = provider(redis(redisHost(), redisPort()));
        provider.forPatients(List.of(1L));
        redisCli("FLUSHALL");

        EffectiveThresholds again = provider.forPatients(List.of(1L));

        assertThat(again.bandsFor(1, Measure.SYSTOLIC)).containsExactly(SYS);
        assertThat(db.calls).hasSize(1);
        assertThat(redisCli("DBSIZE")).as("L1 hit did not refill Redis").isEqualTo("0");
    }

    @Test
    void l1_miss_reads_redis_then_loads_only_missing_patients_from_db_in_one_query() {
        StringRedisTemplate redis = redis(redisHost(), redisPort());
        provider(redis).forPatients(List.of(1L));        // warms Redis for patient 1 (another pod, say)
        db.calls.clear();

        EffectiveThresholds both = provider(redis).forPatients(List.of(1L, 2L));   // fresh L1

        assertThat(db.calls).containsExactly(List.of(2L));
        assertThat(both.bandsFor(1, Measure.SYSTOLIC)).containsExactly(SYS);
        assertThat(both.bandsFor(2, Measure.SYSTOLIC)).containsExactly(SYS);
        assertThat(redis.keys("thr:2:*")).as("one key per measure, empty measures included")
                .hasSize(Measure.values().length);
    }

    @Test
    void patient_without_bands_is_cached_too() {
        StringRedisTemplate redis = redis(redisHost(), redisPort());
        provider(redis).forPatients(List.of(99L));
        db.calls.clear();

        EffectiveThresholds none = provider(redis).forPatients(List.of(99L));

        assertThat(none.bandsFor(99, Measure.SYSTOLIC)).isEmpty();
        assertThat(db.calls).isEmpty();
    }

    @Test
    void redis_down_falls_back_to_db() {
        CachingThresholdProvider provider = provider(redis("127.0.0.1", 1));   // nothing listens on port 1

        EffectiveThresholds thresholds = provider.forPatients(List.of(1L));

        assertThat(thresholds.bandsFor(1, Measure.SYSTOLIC)).containsExactly(SYS);
        assertThat(db.calls).hasSize(1);
    }

    @Test
    void evict_drops_both_tiers() {
        StringRedisTemplate redis = redis(redisHost(), redisPort());
        CachingThresholdProvider provider = provider(redis);
        provider.forPatients(List.of(1L));

        provider.evict(1L);
        provider.forPatients(List.of(1L));

        assertThat(db.calls).hasSize(2);
    }

    CachingThresholdProvider provider(StringRedisTemplate redis) {
        return new CachingThresholdProvider(db, redis, Duration.ofMinutes(10), 1_000);
    }

    StringRedisTemplate redis(String host, int port) {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(host, port),
                LettuceClientConfiguration.builder().commandTimeout(Duration.ofMillis(250)).build());
        factory.afterPropertiesSet();
        factory.start();
        factories.add(factory);
        return new StringRedisTemplate(factory);
    }

    /** Patients 1 and 2 have one systolic band; everyone else has none. */
    static class CountingProvider implements ThresholdProvider {
        final List<List<Long>> calls = new ArrayList<>();

        @Override
        public EffectiveThresholds forPatients(List<Long> patientIds) {
            calls.add(patientIds.stream().sorted().toList());
            return new EffectiveThresholds(patientIds.stream().collect(Collectors.toMap(p -> p,
                    p -> p <= 2 ? Map.of(Measure.SYSTOLIC, List.of(SYS)) : Map.<Measure, List<ThresholdBand>>of())));
        }
    }
}
