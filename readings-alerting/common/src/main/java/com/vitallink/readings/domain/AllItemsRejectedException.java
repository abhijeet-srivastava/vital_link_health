package com.vitallink.readings.domain;

import java.util.Objects;

/**
 * 422: every reading in the batch was rejected, so nothing is stored and there is no "rest" to accept with a 202.
 * {@link #code()} is the shared rejection code, or null when the items were rejected for different reasons.
 */
public class AllItemsRejectedException extends RuntimeException {

    private final IngestResult result;

    public AllItemsRejectedException(IngestResult result) {
        super("All " + result.results().size() + " readings were rejected");
        this.result = result;
    }

    public IngestResult result() {
        return result;
    }

    public RejectionCode code() {
        RejectionCode first = result.results().get(0).code();
        return result.results().stream().allMatch(r -> Objects.equals(r.code(), first)) ? first : null;
    }
}
