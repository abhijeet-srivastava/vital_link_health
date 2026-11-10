package com.vitallink.readings.alerting.notify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Moves a notifier record to {@code <topic>.dlq}, counts it per topic and logs coordinates only. */
public class NotifierDeadLetters implements ConsumerRecordRecoverer {

    private static final Logger log = LoggerFactory.getLogger(NotifierDeadLetters.class);

    private final DeadLetterPublishingRecoverer publisher;
    private final MeterRegistry meters;

    public NotifierDeadLetters(DeadLetterPublishingRecoverer publisher, MeterRegistry meters) {
        this.publisher = publisher;
        this.meters = meters;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception cause) {
        publisher.accept(record, cause);
        Counter.builder("vitallink.notifier.dlq").description("notifier records that reached the DLQ")
                .tag("topic", record.topic()).register(meters).increment();
        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        log.warn("Dead-lettered topic={} partition={} offset={} error={}", record.topic(), record.partition(),
                record.offset(), root.getClass().getName());
    }
}
