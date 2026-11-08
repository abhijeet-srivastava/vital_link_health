package com.vitallink.readings.messaging;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Topics and partition counts from references/architecture.md. Counts are fixed up front: adding partitions later
 * remaps keys and breaks per-patient ordering. Each topic has a {@code .dlq} with the same count; topics whose
 * consumer retries without blocking its partition also have a {@code .retry} with the same count.
 */
public final class KafkaTopics {

    public static final String READINGS_INGESTED = ReadingsIngestedV1.TOPIC;
    public static final String ALERTS_RAISED = "alerts.raised";
    public static final String ALERTS_RAISED_CRITICAL = "alerts.raised.critical";
    public static final String ALERT_STATE_CHANGED = "alert.state_changed";
    public static final String THRESHOLDS_CHANGED = "thresholds.changed";
    public static final String DEVICE_BINDING_CHANGED = "device.binding_changed";

    public record TopicSpec(String name, int partitions) {
    }

    private static final List<TopicSpec> MAIN = List.of(
            new TopicSpec(READINGS_INGESTED, 128),
            new TopicSpec(ALERTS_RAISED, 32),
            new TopicSpec(ALERTS_RAISED_CRITICAL, 16),
            new TopicSpec(ALERT_STATE_CHANGED, 32),
            new TopicSpec(THRESHOLDS_CHANGED, 8),
            new TopicSpec(DEVICE_BINDING_CHANGED, 8));

    /** Consumers that retry through {@code <topic>.retry} (non-blocking): the evaluator. */
    private static final Set<String> WITH_RETRY_TOPIC = Set.of(READINGS_INGESTED);

    private KafkaTopics() {
    }

    public static String dlq(String topic) {
        return topic + ".dlq";
    }

    public static String retry(String topic) {
        return topic + ".retry";
    }

    /** Every topic, its DLQ and, where the consumer retries without blocking, its retry topic. */
    public static List<TopicSpec> all() {
        return MAIN.stream()
                .flatMap(t -> Stream.concat(
                        Stream.of(t, new TopicSpec(dlq(t.name()), t.partitions())),
                        WITH_RETRY_TOPIC.contains(t.name())
                                ? Stream.of(new TopicSpec(retry(t.name()), t.partitions()))
                                : Stream.empty()))
                .toList();
    }
}
