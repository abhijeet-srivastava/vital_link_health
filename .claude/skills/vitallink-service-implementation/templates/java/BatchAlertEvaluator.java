// Template: batched, idempotent Kafka consumer for readings.ingested.
// Shape to copy for any consumer: batch path in one DB transaction, per-event fallback on unique
// violations, Kafka offsets acknowledged only after the DB commit, DLQ after bounded retries.

package com.vitallink.readings.alerting;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
class ReadingsIngestedListener {

    private final BatchAlertEvaluator evaluator;

    ReadingsIngestedListener(BatchAlertEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    // Container config: batch listener, max.poll.records = 500, fetch.max.wait.ms = 200,
    // AckMode.MANUAL, cooperative-sticky assignor, DefaultErrorHandler with backoff ->
    // DeadLetterPublishingRecoverer to "readings.ingested.dlq".
    @KafkaListener(topics = "readings.ingested", groupId = "alert-evaluator", batch = "true")
    void onBatch(List<ConsumerRecord<String, ReadingsIngested>> records, Acknowledgment ack) {
        List<ReadingsIngested> events = records.stream().map(ConsumerRecord::value).toList();
        try {
            evaluator.evaluateBatch(events);                 // one transaction for the whole batch
        } catch (DuplicateKeyException alreadySeenOrRace) {
            // Only during a rebalance or a replay: the batch rolled back entirely.
            // Replay per event; each one is idempotent on its own.
            events.forEach(evaluator::evaluateOne);
        }
        ack.acknowledge();                                   // offsets only after DB commit(s)
    }
}

@Component
class BatchAlertEvaluator {

    private final ThresholdProvider thresholds;
    private final MeasureEvaluators evaluators;              // Strategy registry by ReadingType
    private final AlertBatchRepository alerts;               // explicit SQL (JdbcClient / jOOQ)
    private final OutboxWriter outbox;
    private final AlertDecisions decisions;                  // pure domain logic, unit-tested
    private final TransactionTemplate tx;                    // explicit boundaries: one tx per call

    BatchAlertEvaluator(ThresholdProvider thresholds, MeasureEvaluators evaluators,
                        AlertBatchRepository alerts, OutboxWriter outbox, AlertDecisions decisions,
                        TransactionTemplate tx) {
        this.thresholds = thresholds;
        this.evaluators = evaluators;
        this.alerts = alerts;
        this.outbox = outbox;
        this.decisions = decisions;
        this.tx = tx;
    }

    /** One transaction for the whole batch. A unique violation rolls it back and propagates. */
    public void evaluateBatch(List<ReadingsIngested> events) {
        tx.executeWithoutResult(status -> evaluateInCurrentTx(events));
    }

    /**
     * Per-event fallback, each in its own transaction. Never catch DuplicateKeyException inside a
     * transaction and keep going: PostgreSQL has already aborted it, so the catch sits outside.
     */
    public void evaluateOne(ReadingsIngested event) {
        try {
            tx.executeWithoutResult(status -> evaluateInCurrentTx(List.of(event)));
        } catch (DuplicateKeyException alreadyEvaluated) {
            // Occurrences for this event were committed earlier: it was processed before. Nothing to do.
        }
    }

    private void evaluateInCurrentTx(List<ReadingsIngested> events) {
        // 1. Evaluate in memory: thresholds loaded once for all patients in the batch.
        var effective = thresholds.forPatients(PatientIds.of(events));
        List<BreachWithReading> breaches = evaluators.evaluateAll(events, effective);
        if (breaches.isEmpty()) return;

        // 2. Guard 4: drop breaches already evaluated (one query).
        breaches = alerts.withoutExistingOccurrences(breaches);
        if (breaches.isEmpty()) return;

        // 3. Guard 3: lock every unresolved episode the batch touches (one SELECT ... FOR UPDATE).
        var episodes = alerts.lockActiveEpisodes(EpisodeKeys.of(breaches));

        // 4. Decide per (patient, measure): new episode, fold (count += n), or severity upgrade.
        //    Upgrade to CRITICAL on an ACKNOWLEDGED alert -> OPEN + escalate_at (configurable policy).
        var plan = decisions.plan(breaches, episodes);

        // 5. Write set-based. Unique violations propagate as DuplicateKeyException -> rollback -> fallback.
        var created = alerts.insertEpisodes(plan.newEpisodes());
        alerts.updateEpisodes(plan.updates());               // only unindexed columns unless upgraded (HOT)
        alerts.insertOccurrences(plan.occurrences(created)); // PK (device_id, reading_uuid, measure_code, measured_at)

        // 6. Announce only new episodes and upgrades; CRITICAL goes to the critical lane.
        outbox.append(plan.events(created));                 // topic alerts.raised or alerts.raised.critical
    }
}
