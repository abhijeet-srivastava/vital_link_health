package com.vitallink.readings.ports;

import com.vitallink.readings.domain.OutboxEvent;

import java.util.List;

public interface OutboxWriter {
    /** Must be called inside the caller's transaction. event_id = UUIDv7. */
    void append(List<OutboxEvent> events);
}
