package com.vitallink.readings.alerting.cache;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import com.vitallink.readings.messaging.KafkaTopics;

/**
 * thresholds.changed -> evict the patient's effective bands from L1 (this pod) and Redis. Every pod must hear every
 * change, so each pod joins with its own group id and starts at the latest offset: a pod that starts later has an
 * empty L1 anyway. Never fails a record: a bad one is skipped and the cache TTL (10 min) bounds the staleness.
 * The key is the patient_id (aggregate_key of the threshold API's outbox row).
 */
public class ThresholdsChangedListener {

    public static final String ID = "thresholds-eviction";

    private static final Logger log = LoggerFactory.getLogger(ThresholdsChangedListener.class);

    private final CachingThresholdProvider cache;

    public ThresholdsChangedListener(CachingThresholdProvider cache) {
        this.cache = cache;
    }

    @KafkaListener(id = ID, groupId = "alert-evaluator-thresholds-${random.uuid}",
            topics = KafkaTopics.THRESHOLDS_CHANGED, containerFactory = "thresholdsListenerFactory")
    public void onChange(ConsumerRecord<String, String> record) {
        Long patientId = patientId(record.key());
        if (patientId == null) {
            log.warn("thresholds.changed without a patient_id key topic={} partition={} offset={}", record.topic(),
                    record.partition(), record.offset());
            return;
        }
        cache.evict(patientId);
    }

    private static Long patientId(String key) {
        if (key == null) {
            return null;
        }
        try {
            return Long.parseLong(key);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
