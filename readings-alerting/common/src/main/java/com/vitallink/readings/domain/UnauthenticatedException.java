package com.vitallink.readings.domain;

/** 401. The message never contains the key secret. */
public class UnauthenticatedException extends RuntimeException {
    public UnauthenticatedException(String message) {
        super(message);
    }
}
