package com.vitallink.readings.ports;

import com.vitallink.readings.domain.AlertEvent;

public interface NotificationService {
    /** INSERT ... ON CONFLICT (trigger_event_id, coach_id, channel) DO NOTHING per recipient and channel. */
    void record(AlertEvent event);
}
