package com.vitallink.readings.alerting.notify;

/**
 * Dummy SMS provider ("SMS_NOTIFIER"): logs and deduplicates on notification_id instead of sending (the assignment
 * allows logging or persisting). Delivers SMS rows of escalations and the SMS fallback of a PUSH whose circuit is
 * open.
 */
public class DummySmsNotifier extends LoggingProvider {

    public DummySmsNotifier() {
        super("SMS_NOTIFIER");
    }
}
