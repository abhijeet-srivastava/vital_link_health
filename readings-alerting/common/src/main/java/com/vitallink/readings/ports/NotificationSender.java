package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Channel;

public interface NotificationSender {
    /** Claims due PENDING rows for one channel with SKIP LOCKED; provider idempotency key = notification_id. */
    int sendDue(Channel channel, int limit);
}
