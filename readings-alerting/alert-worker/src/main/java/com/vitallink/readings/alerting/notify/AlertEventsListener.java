package com.vitallink.readings.alerting.notify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.ports.NotificationService;

/**
 * Group {@code notifier}: alerts.raised only (non-critical new episodes and upgrades). One record per transaction; the
 * offset is acknowledged only after the notification rows committed (rule 3). Failures retry with backoff, then go to
 * {@code <topic>.dlq} (NotifierKafkaConfig). alert.state_changed is consumed by the critical group
 * ({@link StateChangedEventsListener}): escalations must not wait behind this topic's backlog.
 */
public class AlertEventsListener {

    public static final String GROUP_ID = "notifier";

    private final NotificationService notifications;
    private final ObjectMapper json;

    public AlertEventsListener(NotificationService notifications, ObjectMapper json) {
        this.notifications = notifications;
        this.json = json;
    }

    @KafkaListener(id = GROUP_ID, groupId = GROUP_ID, topics = KafkaTopics.ALERTS_RAISED,
            containerFactory = "notifierListenerFactory")
    public void onEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws JsonProcessingException {
        AlertEvents.parse(json, record.topic(), record.value()).ifPresent(notifications::record);
        ack.acknowledge();
    }
}
