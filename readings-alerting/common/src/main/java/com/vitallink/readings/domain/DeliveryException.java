package com.vitallink.readings.domain;

/** A provider rejected or failed a delivery; the row is retried with backoff. The message carries ids only. */
public class DeliveryException extends RuntimeException {

    public DeliveryException(String message) {
        super(message);
    }

    public DeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
