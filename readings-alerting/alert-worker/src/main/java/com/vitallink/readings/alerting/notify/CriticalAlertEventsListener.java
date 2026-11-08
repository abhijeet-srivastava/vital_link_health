package com.vitallink.readings.alerting.notify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.ports.NotificationService;

/**
 * Group {@code notifier-critical} (D13), container {@code notifier-critical}: alerts.raised.critical, so a backlog on
 * alerts.raised never delays a CRITICAL. alert.state_changed is in the same group but its own container
 * ({@link StateChangedEventsListener}), so neither topic's backlog holds the other's consumers. Same recording path
 * and offset rule as {@link AlertEventsListener}.
 */
public class CriticalAlertEventsListener {

    public static final String GROUP_ID = "notifier-critical";

    private final NotificationService notifications;
    private final ObjectMapper json;

    public CriticalAlertEventsListener(NotificationService notifications, ObjectMapper json) {
        this.notifications = notifications;
        this.json = json;
    }

    @KafkaListener(id = GROUP_ID, groupId = GROUP_ID, topics = KafkaTopics.ALERTS_RAISED_CRITICAL,
            containerFactory = "notifierCriticalListenerFactory")
    public void onEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws JsonProcessingException {
        AlertEvents.parse(json, record.topic(), record.value()).ifPresent(notifications::record);
        ack.acknowledge();
    }
}
