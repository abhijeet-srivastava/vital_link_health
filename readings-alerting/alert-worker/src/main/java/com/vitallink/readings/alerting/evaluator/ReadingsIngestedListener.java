package com.vitallink.readings.alerting.evaluator;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.messaging.ReadingsIngestedV1;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * readings.ingested.retry -> {@link AlertEvaluator}, one record at a time. Records arrive here only after failing on
 * the per-event path of {@link ReadingsIngestedBatchListener}. The container's error handler retries each with a
 * fixed backoff and then moves it to readings.ingested.dlq (EvaluatorKafkaConfig). The backoff holds only this retry
 * partition, never the main topic (D29). The offset is acknowledged only after the evaluation committed (rule 3).
 */
public class ReadingsIngestedListener {

    public static final String GROUP_ID = ReadingsIngestedBatchListener.GROUP_ID + ".retry";

    private final AlertEvaluator evaluator;
    private final ObjectMapper json;
    private final Counter failures;

    public ReadingsIngestedListener(AlertEvaluator evaluator, ObjectMapper json, MeterRegistry meters) {
        this.evaluator = evaluator;
        this.json = json;
        this.failures = failures(meters);
    }

    @KafkaListener(id = GROUP_ID, groupId = GROUP_ID, topics = KafkaTopics.READINGS_INGESTED + ".retry",
            containerFactory = "evaluatorRetryListenerFactory")
    public void onEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws JsonProcessingException {
        try {
            BatchAlertEvaluator.Event event = event(json, record.value());
            evaluator.evaluate(event.eventId(), event.patientId(), event.readings());
        } catch (JsonProcessingException | RuntimeException e) {
            failures.increment();
            throw e;
        }
        ack.acknowledge();
    }

    /** Shared by both listeners: every failed delivery of a readings.ingested event, each retry included. */
    static Counter failures(MeterRegistry meters) {
        return Counter.builder("vitallink.evaluator.failures")
                .description("readings.ingested deliveries that failed (each retry counts)")
                .register(meters);
    }

    /** Record value -> event; throws on anything that is not a readings.ingested v1 payload. */
    static BatchAlertEvaluator.Event event(ObjectMapper json, String value) throws JsonProcessingException {
        if (value == null) {
            throw new IllegalArgumentException("readings.ingested record without a value");
        }
        ReadingsIngestedV1 event = json.readValue(value, ReadingsIngestedV1.class);
        if (event.schemaVersion() != ReadingsIngestedV1.SCHEMA_VERSION || event.readings() == null
                || event.eventId() == null) {
            throw new IllegalArgumentException("Unsupported readings.ingested schema_version=" + event.schemaVersion());
        }
        return new BatchAlertEvaluator.Event(event.eventId(), event.patientId(), readings(event));
    }

    /** Payload items -> domain readings. payload_hash is an ingest concern and not carried in the event. */
    static List<Reading> readings(ReadingsIngestedV1 event) {
        return event.readings().stream()
                .map(i -> new Reading(event.deviceId(), i.readingUuid(), Instant.parse(i.measuredAt()),
                        event.patientId(), ReadingType.valueOf(i.readingType()), i.deviceSeq(), i.systolic(),
                        i.diastolic(), i.pulse(), i.value(), 0L))
                .toList();
    }
}
