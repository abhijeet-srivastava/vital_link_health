package com.vitallink.readings.alerting.evaluator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.messaging.KafkaTopics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * readings.ingested -> {@link BatchAlertEvaluator}, one poll (up to 500 records or 200 ms) at a time. Offsets are
 * acknowledged only after the batch, or every per-event replay, has committed (rule 3). Malformed records go to the
 * DLQ at once; an event that fails on the per-event path too is moved to readings.ingested.retry before the ack, so
 * the partition keeps flowing and a CRITICAL reading behind it is not held (D29). If that move fails, nothing is
 * acknowledged and the container redelivers the whole poll; every step is idempotent.
 */
public class ReadingsIngestedBatchListener {

    public static final String GROUP_ID = "alert-evaluator";

    private final BatchAlertEvaluator evaluator;
    private final ObjectMapper json;
    private final KafkaTemplate<String, String> producer;
    private final EvaluatorDeadLetters deadLetters;
    private final Duration sendTimeout;
    private final Counter failures;

    public ReadingsIngestedBatchListener(BatchAlertEvaluator evaluator, ObjectMapper json,
                                         KafkaTemplate<String, String> producer, EvaluatorDeadLetters deadLetters,
                                         Duration sendTimeout, MeterRegistry meters) {
        this.evaluator = evaluator;
        this.json = json;
        this.producer = producer;
        this.deadLetters = deadLetters;
        this.sendTimeout = sendTimeout;
        this.failures = ReadingsIngestedListener.failures(meters);
    }

    @KafkaListener(id = GROUP_ID, groupId = GROUP_ID, topics = KafkaTopics.READINGS_INGESTED,
            containerFactory = "evaluatorBatchListenerFactory", batch = "true")
    public void onBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) throws Exception {
        Map<BatchAlertEvaluator.Event, ConsumerRecord<String, String>> recordOf = new IdentityHashMap<>();
        List<BatchAlertEvaluator.Event> events = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            try {
                BatchAlertEvaluator.Event event = ReadingsIngestedListener.event(json, record.value());
                recordOf.put(event, record);
                events.add(event);
            } catch (Exception malformed) {
                // Parsing is deterministic: retrying cannot help.
                failures.increment();
                deadLetters.accept(record, malformed);
            }
        }
        for (BatchAlertEvaluator.Event failed : evaluator.evaluateWithFallback(events)) {
            failures.increment();
            toRetryTopic(recordOf.get(failed));
        }
        ack.acknowledge();
    }

    private void toRetryTopic(ConsumerRecord<String, String> record)
            throws InterruptedException, ExecutionException, TimeoutException {
        ProducerRecord<String, String> retry = new ProducerRecord<>(KafkaTopics.retry(KafkaTopics.READINGS_INGESTED),
                null, record.key(), record.value(), record.headers());
        producer.send(retry).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
