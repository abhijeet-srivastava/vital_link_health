package com.vitallink.readings.alerting.evaluator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.alerting.cache.CachingThresholdProvider;
import com.vitallink.readings.domain.AlertDecisions;
import com.vitallink.readings.domain.AlertOccurrence;
import com.vitallink.readings.domain.BreachWithReading;
import com.vitallink.readings.domain.EpisodeGroupPlan;
import com.vitallink.readings.domain.EpisodeKey;
import com.vitallink.readings.domain.EpisodeRow;
import com.vitallink.readings.domain.EpisodeUpdate;
import com.vitallink.readings.domain.LockedEpisode;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.persistence.JdbcAlertBatchRepository;
import com.vitallink.readings.persistence.JdbcAuditWriter;
import com.vitallink.readings.persistence.JdbcOutboxWriter;
import com.vitallink.readings.ports.AlertBatchRepository;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The batch path called directly, so each test controls exactly which events share a transaction. The clock is
 * fixed, so escalate_at and is_late from the batch path and from the per-event replay compare exactly.
 */
@SpringBootTest
@ActiveProfiles("evaluator")
class BatchAlertEvaluatorTest extends EvaluatorTestSupport {

    static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    static final long PROGRAM = 1;
    static final long COACH = 41;

    @TestConfiguration
    static class FixedClock {
        @Bean
        @Primary
        Clock fixedClock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }
    }

    @Autowired
    CachingThresholdProvider thresholds;

    @Autowired
    AlertDecisions decisions;

    @Autowired
    TransactionTemplate tx;

    @Autowired
    AlertEvaluator perEvent;

    @Autowired
    AlertRecords records;

    @Autowired
    MeterRegistry meters;

    RecordingRepository repository;
    BatchAlertEvaluator batch;

    @BeforeEach
    void wire() {
        givenSystolicBands(PROGRAM);
        repository = new RecordingRepository(new JdbcAlertBatchRepository(jdbc));
        batch = new BatchAlertEvaluator(thresholds, MeasureEvaluators.standard(), decisions, repository,
                new JdbcOutboxWriter(jdbc), new JdbcAuditWriter(jdbc), tx, records, perEvent, meters);
    }

    @Test
    void batch_with_already_evaluated_reading_rolls_back_and_replay_matches_clean_run() {
        // Clean run: E1 was evaluated earlier and committed; the batch pre-check skips it.
        Scenario clean = new Scenario(9001, 9002);
        perEvent.evaluate(clean.e1.eventId(), clean.e1.patientId(), clean.e1.readings());
        double fallbacksBefore = fallbacks("unique_violation");
        batch.evaluateWithFallback(clean.events());
        assertThat(fallbacks("unique_violation")).as("nothing to roll back in the clean run").isEqualTo(fallbacksBefore);

        // Rebalance overlap: another consumer commits E1 after this batch's pre-check, before its writes.
        Scenario overlap = new Scenario(9011, 9012);
        repository.afterPrecheck = () -> CompletableFuture.runAsync(() -> perEvent.evaluate(overlap.e1.eventId(),
                overlap.e1.patientId(), overlap.e1.readings())).join();

        List<BatchAlertEvaluator.Event> failed = batch.evaluateWithFallback(overlap.events());

        assertThat(failed).isEmpty();
        assertThat(fallbacks("unique_violation")).as("batch rolled back once").isEqualTo(fallbacksBefore + 1);
        assertThat(overlap.snapshot()).as("per-event replay leaves the clean-run state, nothing from the rolled-back "
                + "batch").isEqualTo(clean.snapshot());
    }

    @Test
    void same_event_twice_in_one_batch_rolls_back_and_replays_once() {
        Scenario clean = new Scenario(9021, 9022);
        batch.evaluateWithFallback(clean.events());
        double fallbacksBefore = fallbacks("unique_violation");

        // The relay re-published E1 and both copies landed in the same poll.
        Scenario republished = new Scenario(9031, 9032);
        List<BatchAlertEvaluator.Event> events = new ArrayList<>(republished.events());
        events.add(republished.e1);

        batch.evaluateWithFallback(events);

        assertThat(fallbacks("unique_violation")).isEqualTo(fallbacksBefore + 1);
        assertThat(republished.snapshot()).isEqualTo(clean.snapshot());
    }

    @Test
    void several_breaches_same_patient_measure_in_one_batch_become_one_row_update_with_right_count() {
        givenPatient(9041, COACH, PROGRAM);
        perEvent.evaluate(IDS.next(), 9041, List.of(bp(9041, 170, NOW.minusSeconds(60))));
        long alertId = alerts(9041).get(0).alertId();

        batch.evaluateWithFallback(List.of(
                batchEvent(9041, bp(9041, 171, NOW.minusSeconds(30))),
                batchEvent(9041, bp(9041, 172, NOW.minusSeconds(20))),
                batchEvent(9041, bp(9041, 173, NOW.minusSeconds(10)))));

        assertThat(repository.updates).as("one UPDATE statement").singleElement().satisfies(rows ->
                assertThat(rows).as("one row for the episode").singleElement().satisfies(u -> {
                    assertThat(u.alertId()).isEqualTo(alertId);
                    assertThat(u.plan().count()).isEqualTo(3);
                }));
        assertThat(repository.inserts).isEmpty();
        assertThat(alerts(9041)).singleElement().satisfies(a -> {
            assertThat(a.occurrenceCnt()).isEqualTo(4);
            assertThat(a.version()).as("one version step per breach, as the per-event path").isEqualTo(3);
            assertThat(a.severity()).isEqualTo("HIGH");
        });
        assertThat(occurrences(alertId)).isEqualTo(4);
        assertThat(alertEvents()).as("repeat breaches announce nothing").hasSize(1);
        assertThat(auditActions(9041)).containsExactly("RAISE_ALERT");
    }

    @Test
    void several_breaches_for_a_new_episode_in_one_batch_become_one_insert_with_right_count() {
        givenPatient(9051, COACH, PROGRAM);

        batch.evaluateWithFallback(List.of(
                batchEvent(9051, bp(9051, 171, NOW.minusSeconds(30))),
                batchEvent(9051, bp(9051, 172, NOW.minusSeconds(20))),
                batchEvent(9051, bp(9051, 173, NOW.minusSeconds(10)))));

        assertThat(repository.inserts).singleElement().satisfies(rows ->
                assertThat(rows).singleElement().satisfies(p -> assertThat(p.count()).isEqualTo(3)));
        assertThat(repository.updates).isEmpty();
        assertThat(alerts(9051)).singleElement().satisfies(a -> {
            assertThat(a.occurrenceCnt()).isEqualTo(3);
            assertThat(a.version()).isEqualTo(2);
        });
        assertThat(alertEvents()).singleElement().satisfies(e -> assertThat(e.payload())
                .containsEntry("reason", "RAISED").containsEntry("occurrence_cnt", 1));
    }

    // ----- helpers -----

    /**
     * Two patients with the same coach and bands. E1 HIGH and E2 CRITICAL for the first (open, then upgrade), E3 HIGH
     * for the second. Two scenarios with the same readings and different patient ids compare by role.
     */
    class Scenario {
        final long first;
        final long second;
        final BatchAlertEvaluator.Event e1;
        final BatchAlertEvaluator.Event e2;
        final BatchAlertEvaluator.Event e3;

        Scenario(long first, long second) {
            this.first = first;
            this.second = second;
            givenPatient(first, COACH, PROGRAM);
            givenPatient(second, COACH, PROGRAM);
            e1 = batchEvent(first, bp(first, 170, NOW.minusSeconds(60)));
            e2 = batchEvent(first, bp(first, 186, NOW.minusSeconds(30)));
            e3 = batchEvent(second, bp(second, 175, NOW.minusSeconds(10)));
        }

        List<BatchAlertEvaluator.Event> events() {
            return List.of(e1, e2, e3);
        }

        /** Rows of both patients with ids replaced by roles (A, B) and event ids by E1..E3. */
        List<String> snapshot() {
            Map<Long, String> roles = Map.of(first, "A", second, "B");
            Map<UUID, String> labels = Map.of(e1.eventId(), "E1", e2.eventId(), "E2", e3.eventId(), "E3");
            List<String> rows = new ArrayList<>();
            roles.forEach((patient, role) -> {
                rows.addAll(jdbc.sql("""
                                SELECT measure_code, coach_id, severity, status, occurrence_cnt, is_late, escalate_at,
                                       first_seen_at, last_seen_at, version
                                FROM alert WHERE patient_id = ?
                                """)
                        .param(patient)
                        .query((rs, n) -> "alert " + role + " " + columns(rs, 10)).list());
                rows.addAll(jdbc.sql("""
                                SELECT o.measure_code, o.measured_at, o.severity, o.threshold_id, o.observed_value
                                FROM alert_occurrence o JOIN alert a USING (alert_id) WHERE a.patient_id = ?
                                """)
                        .param(patient)
                        .query((rs, n) -> "occurrence " + role + " " + columns(rs, 5)).list());
                rows.addAll(jdbc.sql("""
                                SELECT topic, payload - 'event_id' - 'alert_id' - 'patient_id'
                                FROM outbox_event WHERE event_type = 'alerts.raised' AND aggregate_key = ?
                                """)
                        .param(String.valueOf(patient))
                        .query((rs, n) -> "event " + role + " " + rs.getString(1) + " "
                                + new TreeMap<>(readJson(rs.getString(2)))).list());
                rows.addAll(jdbc.sql("SELECT action, request_id FROM audit_log WHERE patient_id = ?")
                        .param(patient)
                        .query((rs, n) -> "audit " + role + " " + rs.getString(1) + " "
                                + labels.get(rs.getObject(2, UUID.class))).list());
            });
            return rows.stream().sorted().toList();
        }
    }

    static BatchAlertEvaluator.Event batchEvent(long patientId, Reading... readings) {
        return new BatchAlertEvaluator.Event(IDS.next(), patientId, List.of(readings));
    }

    static String columns(java.sql.ResultSet rs, int count) throws java.sql.SQLException {
        StringBuilder out = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            out.append(i == 1 ? "" : " ").append(rs.getString(i));
        }
        return out.toString();
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> readJson(String value) {
        try {
            return json.readValue(value, Map.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    double fallbacks(String reason) {
        var counter = meters.find("vitallink.evaluator.batch.fallbacks").tag("reason", reason).counter();
        return counter == null ? 0 : counter.count();
    }

    /** Records the batch statements and can run a hook between the pre-check and the writes. */
    static class RecordingRepository implements AlertBatchRepository {

        private final AlertBatchRepository delegate;
        final List<List<EpisodeGroupPlan>> inserts = new ArrayList<>();
        final List<List<EpisodeUpdate>> updates = new ArrayList<>();
        volatile Runnable afterPrecheck;

        RecordingRepository(AlertBatchRepository delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<BreachWithReading> withoutExistingOccurrences(List<BreachWithReading> breaches) {
            List<BreachWithReading> fresh = delegate.withoutExistingOccurrences(breaches);
            Runnable hook = afterPrecheck;
            afterPrecheck = null;   // once: the per-event replay must not trigger it again
            if (hook != null) {
                hook.run();
            }
            return fresh;
        }

        @Override
        public Map<EpisodeKey, LockedEpisode> lockActiveEpisodes(Collection<EpisodeKey> keys) {
            return delegate.lockActiveEpisodes(keys);
        }

        @Override
        public Map<EpisodeKey, EpisodeRow> insertEpisodes(List<EpisodeGroupPlan> plans) {
            inserts.add(List.copyOf(plans));
            return delegate.insertEpisodes(plans);
        }

        @Override
        public void updateEpisodes(List<EpisodeUpdate> updates) {
            this.updates.add(List.copyOf(updates));
            delegate.updateEpisodes(updates);
        }

        @Override
        public void insertOccurrences(List<AlertOccurrence> occurrences) {
            delegate.insertOccurrences(occurrences);
        }
    }
}
