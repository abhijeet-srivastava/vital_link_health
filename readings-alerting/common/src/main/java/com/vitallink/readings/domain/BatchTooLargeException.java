package com.vitallink.readings.domain;

/** 413. */
public class BatchTooLargeException extends RuntimeException {
    public BatchTooLargeException(int size, int max) {
        super("Batch has " + size + " readings; the maximum is " + max);
    }
}
