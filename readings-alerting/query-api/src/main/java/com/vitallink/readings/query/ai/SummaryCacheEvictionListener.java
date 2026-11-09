package com.vitallink.readings.query.ai;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.vitallink.readings.cache.CacheKeys;
import com.vitallink.readings.messaging.KafkaTopics;

/**
 * A raised alert (new episode or severity upgrade, either lane) makes the patient's cached summary stale: delete
 * summary:{patientId}. The cache is shared Redis, so one group for all query-api pods is enough. The key is the
 * patient_id (aggregate_key of the alerting outbox). Never fails a record: a bad key or a Redis error is logged and
 * skipped, and the 15 min TTL bounds the staleness.
 */
@Component
public class SummaryCacheEvictionListener {

    public static final String ID = "summary-eviction";

    private static final Logger log = LoggerFactory.getLogger(SummaryCacheEvictionListener.class);

    private final StringRedisTemplate redis;

    public SummaryCacheEvictionListener(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @KafkaListener(id = ID, groupId = "query-summary-eviction",
            topics = {KafkaTopics.ALERTS_RAISED, KafkaTopics.ALERTS_RAISED_CRITICAL})
    public void onRaised(ConsumerRecord<String, String> record) {
        long patientId;
        try {
            patientId = Long.parseLong(record.key());
        } catch (NumberFormatException e) {
            log.warn("alerts.raised without a patient_id key topic={} partition={} offset={}", record.topic(),
                    record.partition(), record.offset());
            return;
        }
        try {
            redis.delete(CacheKeys.summary(patientId));
        } catch (RuntimeException e) {
            log.warn("Summary eviction failed patient_id={} error={}", patientId, e.getClass().getSimpleName());
        }
    }
}
