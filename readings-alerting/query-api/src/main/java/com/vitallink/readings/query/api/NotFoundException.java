package com.vitallink.readings.query.api;

/** 404 with a machine-readable code (e.g. no reading of the type yet). */
public class NotFoundException extends RuntimeException {

    private final String code;

    public NotFoundException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
