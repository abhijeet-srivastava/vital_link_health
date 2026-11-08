package com.vitallink.readings.domain;

import java.util.UUID;

/** readingId is null only for a rejected reading that had neither an id nor enough fields to derive one. */
public record ItemResult(UUID readingId, ItemStatus status, RejectionCode code) {

    public static ItemResult of(UUID readingId, ItemStatus status) {
        return new ItemResult(readingId, status, null);
    }

    public static ItemResult rejected(UUID readingId, RejectionCode code) {
        return new ItemResult(readingId, ItemStatus.REJECTED, code);
    }
}
