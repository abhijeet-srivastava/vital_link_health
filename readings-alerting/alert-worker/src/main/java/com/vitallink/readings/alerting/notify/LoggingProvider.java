package com.vitallink.readings.alerting.notify;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vitallink.readings.domain.PendingNotification;
import com.vitallink.readings.ports.DeliveryProvider;

/**
 * Fake provider for local runs and tests: logs ids only and deduplicates on the idempotency key (notification_id) as
 * a real push or SMS provider does, so a repeated call delivers nothing new. Keeps every key in memory: a fake only.
 */
abstract class LoggingProvider implements DeliveryProvider {

    private static final Logger log = LoggerFactory.getLogger(LoggingProvider.class);

    private final String name;
    private final Map<Long, Integer> calls = new ConcurrentHashMap<>();

    LoggingProvider(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void deliver(PendingNotification n) {
        int seen = calls.merge(n.notificationId(), 1, Integer::sum);
        if (seen == 1) {
            log.info("Delivered provider={} notification_id={} alert_id={} coach_id={} reason={} severity={}", name,
                    n.notificationId(), n.alertId(), n.coachId(), n.reason(), n.severity());
        } else {
            log.info("Duplicate ignored provider={} notification_id={}", name, n.notificationId());
        }
    }

    /** Calls per idempotency key so far. */
    public Map<Long, Integer> calls() {
        return Map.copyOf(calls);
    }
}
