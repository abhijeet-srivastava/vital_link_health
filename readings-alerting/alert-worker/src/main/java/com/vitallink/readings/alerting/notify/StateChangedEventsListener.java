package com.vitallink.readings.alerting.notify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.messaging.KafkaTopics;
import com.vitallink.readings.ports.NotificationService;

/**
 * alert.state_changed in group {@code notifier-critical} (D32): escalations are always CRITICAL and page two coaches,
 * so they share the critical lane's latency budget and pods instead of the alerts.raised consumers. A container of
 * its own (id {@code notifier-critical-state}, same group id) gives this topic its own consumers: a burst of
 * reassignments after a panel change cannot hold up alerts.raised.critical, and the reverse. ACKNOWLEDGED and RESOLVED
 * records are parsed and acknowledged without writing anything.
 */
public class StateChangedEventsListener {

    public static final String LISTENER_ID = CriticalAlertEventsListener.GROUP_ID + "-state";

    private final NotificationService notifications;
    private final ObjectMapper json;

    public StateChangedEventsListener(NotificationService notifications, ObjectMapper json) {
        this.notifications = notifications;
        this.json = json;
    }

    @KafkaListener(id = LISTENER_ID, groupId = CriticalAlertEventsListener.GROUP_ID,
            topics = KafkaTopics.ALERT_STATE_CHANGED, containerFactory = "notifierCriticalListenerFactory")
    public void onEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws JsonProcessingException {
        AlertEvents.parse(json, record.topic(), record.value()).ifPresent(notifications::record);
        ack.acknowledge();
    }
}
