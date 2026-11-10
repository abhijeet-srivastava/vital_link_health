package com.vitallink.readings.query.api;

import java.util.UUID;

import org.springframework.stereotype.Component;

import com.vitallink.readings.messaging.UuidV7;

/** audit_log.request_id: the caller's X-Request-Id when it is a UUID, otherwise a fresh UUIDv7. */
@Component
public class RequestIds {

    public static final String HEADER = "X-Request-Id";

    private final UuidV7 ids;

    public RequestIds(UuidV7 ids) {
        this.ids = ids;
    }

    public UUID resolve(String header) {
        if (header != null && !header.isBlank()) {
            try {
                return UUID.fromString(header.strip());
            } catch (IllegalArgumentException e) {
                // fall through: a malformed id is replaced, not echoed
            }
        }
        return ids.next();
    }
}
