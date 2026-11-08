package com.vitallink.readings.alerting.notify;

/** Logging stand-in for the push provider (the assignment allows logging or persisting instead of sending). */
public class LoggingPushProvider extends LoggingProvider {

    public LoggingPushProvider() {
        super("push");
    }
}
