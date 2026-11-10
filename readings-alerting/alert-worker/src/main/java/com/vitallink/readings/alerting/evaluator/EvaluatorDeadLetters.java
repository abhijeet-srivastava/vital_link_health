package com.vitallink.readings.alerting.evaluator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Moves a readings.ingested record to readings.ingested.dlq (same key, original headers plus the exception headers)
 * and waits for the broker ack. Counts it and logs coordinates only: payloads hold reading values.
 */
public class EvaluatorDeadLetters implements ConsumerRecordRecoverer {

    private static final Logger log = LoggerFactory.getLogger(EvaluatorDeadLetters.class);

    private final DeadLetterPublishingRecoverer publisher;
    private final Counter deadLettered;

    public EvaluatorDeadLetters(DeadLetterPublishingRecoverer publisher, MeterRegistry meters) {
        this.publisher = publisher;
        this.deadLettered = Counter.builder("vitallink.evaluator.dlq")
                .description("readings.ingested records that reached the DLQ").register(meters);
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception cause) {
        publisher.accept(record, cause);
        deadLettered.increment();
        log.warn("Dead-lettered topic={} partition={} offset={} error={}", record.topic(), record.partition(),
                record.offset(), rootCause(cause).getClass().getName());
    }

    private static Throwable rootCause(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t;
    }
}
