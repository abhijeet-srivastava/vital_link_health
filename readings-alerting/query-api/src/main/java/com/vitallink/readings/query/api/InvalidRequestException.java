package com.vitallink.readings.query.api;

/** 400 with a machine-readable code. The message names the parameter, never its value. */
public class InvalidRequestException extends RuntimeException {

    private final String code;

    public InvalidRequestException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
