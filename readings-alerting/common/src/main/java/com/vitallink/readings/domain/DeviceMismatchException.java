package com.vitallink.readings.domain;

/** 403: the key belongs to another device, or the device was not bound to the patient at a reading's measured_at. */
public class DeviceMismatchException extends RuntimeException {

    public enum Reason { KEY_FOR_OTHER_DEVICE, DEVICE_NOT_BOUND }

    private final Reason reason;

    public DeviceMismatchException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
