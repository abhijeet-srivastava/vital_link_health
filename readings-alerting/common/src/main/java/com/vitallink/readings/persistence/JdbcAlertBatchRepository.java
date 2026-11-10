package com.vitallink.readings.persistence;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.ActiveEpisode;
import com.vitallink.readings.domain.AlertOccurrence;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.BreachWithReading;
import com.vitallink.readings.domain.EpisodeGroupPlan;
import com.vitallink.readings.domain.EpisodeKey;
import com.vitallink.readings.domain.EpisodeRow;
import com.vitallink.readings.domain.EpisodeUpdate;
import com.vitallink.readings.domain.LockedEpisode;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.NoPrimaryCoachException;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.ports.AlertBatchRepository;

/**
 * Pattern 3 in SQL: each step is one statement over {@code unnest} arrays, whatever the batch size. The database
 * still decides: occurrence PK (guard 4) and ux_alert_active_episode (guard 3); nothing here uses ON CONFLICT.
 */
public class JdbcAlertBatchRepository implements AlertBatchRepository {

    private final JdbcClient jdbc;

    public JdbcAlertBatchRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<BreachWithReading> withoutExistingOccurrences(List<BreachWithReading> breaches) {
        if (breaches.isEmpty()) {
            return breaches;
        }
        // The explicit measured_at range lets the planner prune alert_occurrence partitions.
        Instant min = breaches.stream().map(BreachWithReading::measuredAt).min(Instant::compareTo).orElseThrow();
        Instant max = breaches.stream().map(BreachWithReading::measuredAt).max(Instant::compareTo).orElseThrow();
        Set<Integer> seen = new HashSet<>(jdbc.sql("""
                        SELECT k.ord FROM unnest(?::bigint[], ?::uuid[], ?::smallint[], ?::timestamptz[])
                                          WITH ORDINALITY AS k (device_id, reading_uuid, measure_code, measured_at, ord)
                        WHERE EXISTS (SELECT 1 FROM alert_occurrence o
                                      WHERE o.device_id = k.device_id AND o.reading_uuid = k.reading_uuid
                                        AND o.measure_code = k.measure_code AND o.measured_at = k.measured_at
                                        AND o.measured_at BETWEEN ? AND ?)
                        """)
                .params(longs(breaches, BreachWithReading::deviceId),
                        strings(breaches, b -> b.readingUuid().toString()),
                        measures(breaches, BreachWithReading::measure),
                        strings(breaches, b -> b.measuredAt().toString()),
                        JdbcReadingRepository.utc(min), JdbcReadingRepository.utc(max))
                .query((rs, n) -> rs.getInt(1) - 1)
                .list());
        return IntStream.range(0, breaches.size()).filter(i -> !seen.contains(i)).mapToObj(breaches::get).toList();
    }

    @Override
    public Map<EpisodeKey, LockedEpisode> lockActiveEpisodes(Collection<EpisodeKey> keys) {
        if (keys.isEmpty()) {
            return Map.of();
        }
        List<EpisodeKey> list = List.copyOf(keys);
        // ORDER BY alert_id: one lock order for every consumer, so a rebalance overlap cannot deadlock.
        List<LockedEpisode> locked = jdbc.sql("""
                        SELECT a.alert_id, a.patient_id, a.measure_code, a.status, a.severity, a.is_late, a.coach_id,
                               a.occurrence_cnt
                        FROM alert a
                        JOIN unnest(?::bigint[], ?::smallint[]) AS k (patient_id, measure_code)
                          ON a.patient_id = k.patient_id AND a.measure_code = k.measure_code
                        WHERE a.status <> 'RESOLVED'
                        ORDER BY a.alert_id
                        FOR UPDATE OF a
                        """)
                .params(longs(list, EpisodeKey::patientId), measures(list, EpisodeKey::measure))
                .query((rs, n) -> new LockedEpisode(
                        new EpisodeKey(rs.getLong("patient_id"), Measure.fromCode(rs.getInt("measure_code"))),
                        new ActiveEpisode(rs.getLong("alert_id"), AlertStatus.valueOf(rs.getString("status")),
                                Severity.valueOf(rs.getString("severity")), rs.getBoolean("is_late")),
                        rs.getLong("coach_id"), rs.getInt("occurrence_cnt")))
                .list();
        Map<EpisodeKey, LockedEpisode> byKey = new HashMap<>();
        locked.forEach(e -> byKey.put(e.key(), e));
        return byKey;
    }

    @Override
    public Map<EpisodeKey, EpisodeRow> insertEpisodes(List<EpisodeGroupPlan> plans) {
        if (plans.isEmpty()) {
            return Map.of();
        }
        // version = count - 1: the folds the per-event path would have applied after opening the episode.
        Map<EpisodeKey, EpisodeRow> created = new HashMap<>();
        jdbc.sql("""
                        INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, is_late, escalate_at,
                                           first_seen_at, last_seen_at, occurrence_cnt, version)
                        SELECT u.patient_id, u.measure_code, ca.coach_id, u.severity, u.status, u.is_late, u.escalate_at,
                               u.first_seen_at, u.last_seen_at, u.occurrence_cnt, u.occurrence_cnt - 1
                        FROM unnest(?::bigint[], ?::smallint[], ?::text[], ?::text[], ?::boolean[], ?::timestamptz[],
                                    ?::timestamptz[], ?::timestamptz[], ?::int[])
                             AS u (patient_id, measure_code, severity, status, is_late, escalate_at, first_seen_at,
                                   last_seen_at, occurrence_cnt)
                        JOIN care_assignment ca
                          ON ca.patient_id = u.patient_id AND ca.role = 'PRIMARY' AND ca.valid_to IS NULL
                        RETURNING alert_id, patient_id, measure_code, coach_id, occurrence_cnt
                        """)
                .params(longs(plans, p -> p.key().patientId()),
                        measures(plans, p -> p.key().measure()),
                        strings(plans, p -> p.severity().name()),
                        strings(plans, p -> p.status().name()),
                        plans.stream().map(EpisodeGroupPlan::late).toArray(Boolean[]::new),
                        strings(plans, p -> p.escalateAt() == null ? null : p.escalateAt().toString()),
                        strings(plans, p -> p.firstSeen().toString()),
                        strings(plans, p -> p.lastSeen().toString()),
                        plans.stream().map(EpisodeGroupPlan::count).toArray(Integer[]::new))
                .query((rs, n) -> Map.entry(
                        new EpisodeKey(rs.getLong("patient_id"), Measure.fromCode(rs.getInt("measure_code"))),
                        new EpisodeRow(rs.getLong("alert_id"), rs.getLong("coach_id"), rs.getInt("occurrence_cnt"))))
                .list()
                .forEach(e -> created.put(e.getKey(), e.getValue()));
        // No current PRIMARY coach means no row: no alert nobody owns (D25). Rolls the batch back.
        plans.stream().map(EpisodeGroupPlan::key).filter(k -> !created.containsKey(k)).findFirst()
                .ifPresent(k -> {
                    throw new NoPrimaryCoachException(k.patientId());
                });
        return created;
    }

    @Override
    public void updateEpisodes(List<EpisodeUpdate> updates) {
        List<EpisodeUpdate> repeats = updates.stream().filter(u -> !u.plan().upgraded()).toList();
        List<EpisodeUpdate> upgrades = updates.stream().filter(u -> u.plan().upgraded()).toList();
        if (!repeats.isEmpty()) {
            // Unindexed columns only, so repeat breaches stay HOT updates. version moves once per breach, as the
            // per-event path does.
            expectRows(repeats, jdbc.sql("""
                            UPDATE alert a
                            SET occurrence_cnt = a.occurrence_cnt + u.n,
                                first_seen_at = LEAST(a.first_seen_at, u.first_seen_at),
                                last_seen_at = GREATEST(a.last_seen_at, u.last_seen_at),
                                is_late = u.is_late, version = a.version + u.n
                            FROM unnest(?::bigint[], ?::int[], ?::timestamptz[], ?::timestamptz[], ?::boolean[])
                                 AS u (alert_id, n, first_seen_at, last_seen_at, is_late)
                            WHERE a.alert_id = u.alert_id
                            """)
                    .params(longs(repeats, EpisodeUpdate::alertId),
                            repeats.stream().map(u -> u.plan().count()).toArray(Integer[]::new),
                            strings(repeats, u -> u.plan().firstSeen().toString()),
                            strings(repeats, u -> u.plan().lastSeen().toString()),
                            repeats.stream().map(u -> u.plan().late()).toArray(Boolean[]::new))
                    .update());
        }
        if (!upgrades.isEmpty()) {
            expectRows(upgrades, jdbc.sql("""
                            UPDATE alert a
                            SET occurrence_cnt = a.occurrence_cnt + u.n,
                                first_seen_at = LEAST(a.first_seen_at, u.first_seen_at),
                                last_seen_at = GREATEST(a.last_seen_at, u.last_seen_at),
                                is_late = u.is_late, severity = u.severity, status = u.status,
                                escalate_at = u.escalate_at, version = a.version + u.n
                            FROM unnest(?::bigint[], ?::int[], ?::timestamptz[], ?::timestamptz[], ?::boolean[],
                                        ?::text[], ?::text[], ?::timestamptz[])
                                 AS u (alert_id, n, first_seen_at, last_seen_at, is_late, severity, status, escalate_at)
                            WHERE a.alert_id = u.alert_id
                            """)
                    .params(longs(upgrades, EpisodeUpdate::alertId),
                            upgrades.stream().map(u -> u.plan().count()).toArray(Integer[]::new),
                            strings(upgrades, u -> u.plan().firstSeen().toString()),
                            strings(upgrades, u -> u.plan().lastSeen().toString()),
                            upgrades.stream().map(u -> u.plan().late()).toArray(Boolean[]::new),
                            strings(upgrades, u -> u.plan().severity().name()),
                            strings(upgrades, u -> u.plan().status().name()),
                            strings(upgrades, u -> u.plan().escalateAt() == null ? null
                                    : u.plan().escalateAt().toString()))
                    .update());
        }
    }

    @Override
    public void insertOccurrences(List<AlertOccurrence> occurrences) {
        if (occurrences.isEmpty()) {
            return;
        }
        jdbc.sql("""
                        INSERT INTO alert_occurrence (device_id, reading_uuid, measure_code, measured_at, alert_id,
                                                      observed_value, threshold_id, severity)
                        SELECT * FROM unnest(?::bigint[], ?::uuid[], ?::smallint[], ?::timestamptz[], ?::bigint[],
                                             ?::numeric[], ?::bigint[], ?::text[])
                        """)
                .params(longs(occurrences, o -> o.breach().deviceId()),
                        strings(occurrences, o -> o.breach().readingUuid().toString()),
                        measures(occurrences, o -> o.breach().measure()),
                        strings(occurrences, o -> o.breach().measuredAt().toString()),
                        longs(occurrences, AlertOccurrence::alertId),
                        strings(occurrences, o -> o.breach().breach().observedValue().toPlainString()),
                        longs(occurrences, o -> o.breach().breach().band().thresholdId()),
                        strings(occurrences, o -> o.breach().breach().severity().name()))
                .update();
    }

    /** Every target row is locked by this transaction, so a short count is a bug, not a race. */
    private static void expectRows(List<EpisodeUpdate> updates, int rows) {
        if (rows != updates.size()) {
            throw new IllegalStateException("Episode update matched " + rows + " of " + updates.size() + " rows");
        }
    }

    private static <T> Long[] longs(List<T> rows, Function<T, Long> f) {
        return rows.stream().map(f).toArray(Long[]::new);
    }

    private static <T> String[] strings(List<T> rows, Function<T, String> f) {
        return rows.stream().map(f).toArray(String[]::new);
    }

    private static <T> Integer[] measures(List<T> rows, Function<T, Measure> f) {
        return rows.stream().map(r -> f.apply(r).code()).toArray(Integer[]::new);
    }
}
