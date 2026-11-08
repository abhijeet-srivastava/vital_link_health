package com.vitallink.readings.alerting.cache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.vitallink.readings.domain.EffectiveThresholds;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.ThresholdBand;
import com.vitallink.readings.ports.ThresholdProvider;

/**
 * Decorator: Caffeine L1 per patient -> Redis L2 {@code thr:{patientId}:{measureCode}} (one MGET) -> the DB provider
 * for whatever is still missing, in one query. TTLs are jittered so a 6 AM warm-up does not expire all at once.
 * Redis is an accelerator only: any Redis error falls through to the DB. Staleness is bounded by the TTL until the
 * thresholds.changed consumer (stage 5b) calls {@link #evict}.
 */
public class CachingThresholdProvider implements ThresholdProvider {

    private static final Logger log = LoggerFactory.getLogger(CachingThresholdProvider.class);
    private static final TypeReference<List<ThresholdBand>> BANDS = new TypeReference<>() {
    };
    private static final List<Measure> MEASURES = List.of(Measure.values());

    private final ThresholdProvider delegate;
    private final StringRedisTemplate redis;
    private final Duration ttl;
    private final ObjectMapper json = new ObjectMapper();
    private final Cache<Long, Map<Measure, List<ThresholdBand>>> local;

    public CachingThresholdProvider(ThresholdProvider delegate, StringRedisTemplate redis, Duration ttl, long maxSize) {
        this.delegate = delegate;
        this.redis = redis;
        this.ttl = ttl;
        this.local = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfter(Expiry.<Long, Map<Measure, List<ThresholdBand>>>creating((k, v) -> jittered()))
                .build();
    }

    static String key(long patientId, Measure measure) {
        return "thr:" + patientId + ":" + measure.code();
    }

    @Override
    public EffectiveThresholds forPatients(List<Long> patientIds) {
        // getAll is single-flight per key: concurrent misses for one patient share one load.
        return new EffectiveThresholds(local.getAll(Set.copyOf(patientIds), this::load));
    }

    public void evict(long patientId) {
        local.invalidate(patientId);
        try {
            redis.delete(MEASURES.stream().map(m -> key(patientId, m)).toList());
        } catch (RuntimeException e) {
            log.warn("Threshold cache eviction in Redis failed patient_id={} error={}", patientId,
                    e.getClass().getSimpleName());
        }
    }

    private Map<Long, Map<Measure, List<ThresholdBand>>> load(Set<? extends Long> missing) {
        Map<Long, Map<Measure, List<ThresholdBand>>> found = fromRedis(missing);
        List<Long> fromDb = missing.stream().filter(p -> !found.containsKey(p)).map(Long::longValue).toList();
        if (!fromDb.isEmpty()) {
            Map<Long, Map<Measure, List<ThresholdBand>>> loaded = delegate.forPatients(fromDb).byPatient();
            for (Long p : fromDb) {
                found.put(p, loaded.getOrDefault(p, Map.of()));
            }
            toRedis(fromDb, found);
        }
        return found;
    }

    /** A patient counts as a hit only when every measure key is present (empty lists are cached too). */
    private Map<Long, Map<Measure, List<ThresholdBand>>> fromRedis(Set<? extends Long> patients) {
        Map<Long, Map<Measure, List<ThresholdBand>>> found = new HashMap<>();
        List<Long> order = List.copyOf(patients);
        List<String> keys = new ArrayList<>();
        order.forEach(p -> MEASURES.forEach(m -> keys.add(key(p, m))));
        try {
            List<String> values = redis.opsForValue().multiGet(keys);
            if (values == null) {
                return found;
            }
            for (int i = 0; i < order.size(); i++) {
                Map<Measure, List<ThresholdBand>> bands = new EnumMap<>(Measure.class);
                boolean complete = true;
                for (int j = 0; j < MEASURES.size(); j++) {
                    String v = values.get(i * MEASURES.size() + j);
                    if (v == null) {
                        complete = false;
                        break;
                    }
                    List<ThresholdBand> list = json.readValue(v, BANDS);
                    if (!list.isEmpty()) {
                        bands.put(MEASURES.get(j), list);
                    }
                }
                if (complete) {
                    found.put(order.get(i), bands);
                }
            }
        } catch (Exception e) {
            // Class name only; fall through to the DB for everything not yet found.
            log.warn("Threshold cache read from Redis failed patients={} error={}", order.size(),
                    e.getClass().getSimpleName());
        }
        return found;
    }

    private void toRedis(List<Long> patients, Map<Long, Map<Measure, List<ThresholdBand>>> bands) {
        try {
            Map<String, String> values = new HashMap<>();
            for (Long p : patients) {
                for (Measure m : MEASURES) {
                    values.put(key(p, m), json.writeValueAsString(bands.get(p).getOrDefault(m, List.of())));
                }
            }
            redis.executePipelined((RedisCallback<Object>) connection -> {
                StringRedisConnection c = (StringRedisConnection) connection;
                values.forEach((k, v) -> c.setEx(k, jittered().toSeconds(), v));
                return null;
            });
        } catch (Exception e) {
            log.warn("Threshold cache write to Redis failed patients={} error={}", patients.size(),
                    e.getClass().getSimpleName());
        }
    }

    /** ttl plus up to 10% random. */
    private Duration jittered() {
        return ttl.plusMillis(ThreadLocalRandom.current().nextLong(Math.max(1, ttl.toMillis() / 10)));
    }
}
