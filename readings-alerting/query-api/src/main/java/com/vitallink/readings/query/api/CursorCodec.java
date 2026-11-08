package com.vitallink.readings.query.api;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.UUID;

import com.vitallink.readings.domain.Cursor;
import com.vitallink.readings.domain.QueueCursor;

/**
 * Opaque keyset cursors (api-contracts.md, Pagination): base64url of a versioned, prefixed position. Readings:
 * {@code r1:<measured_at epoch micros>:<reading_uuid>}; queue: {@code q1:<severity_rank>:<created_at micros>:<alert_id>}.
 * Microseconds keep the exact timestamptz value, so the keyset comparison neither skips nor repeats a row. A cursor
 * holds positions only (no PHI); row-level security still applies to whatever it points at.
 */
public final class CursorCodec {

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private CursorCodec() {
    }

    public static String encode(Cursor c) {
        return encode("r1:" + micros(c.measuredAt()) + ":" + c.readingUuid());
    }

    public static String encode(QueueCursor c) {
        return encode("q1:" + c.severityRank() + ":" + micros(c.createdAt()) + ":" + c.alertId());
    }

    /** Null or blank means the first page. */
    public static Cursor decodeReading(String token) {
        String[] parts = decode(token, "r1", 3);
        if (parts == null) {
            return null;
        }
        try {
            return new Cursor(instant(Long.parseLong(parts[1])), UUID.fromString(parts[2]));
        } catch (IllegalArgumentException e) {
            throw invalid();
        }
    }

    public static QueueCursor decodeQueue(String token) {
        String[] parts = decode(token, "q1", 4);
        if (parts == null) {
            return null;
        }
        try {
            int rank = Integer.parseInt(parts[1]);
            if (rank < 1 || rank > 3) {
                throw invalid();
            }
            return new QueueCursor(rank, instant(Long.parseLong(parts[2])), Long.parseLong(parts[3]));
        } catch (IllegalArgumentException e) {
            throw invalid();
        }
    }

    private static String encode(String plain) {
        return ENCODER.encodeToString(plain.getBytes(StandardCharsets.UTF_8));
    }

    private static String[] decode(String token, String version, int parts) {
        if (token == null || token.isBlank()) {
            return null;
        }
        String plain;
        try {
            plain = new String(DECODER.decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw invalid();
        }
        String[] split = plain.split(":", -1);
        if (split.length != parts || !version.equals(split[0])) {
            throw invalid();
        }
        return split;
    }

    private static long micros(Instant t) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, t);
    }

    private static Instant instant(long micros) {
        return Instant.EPOCH.plus(micros, ChronoUnit.MICROS);
    }

    private static InvalidRequestException invalid() {
        return new InvalidRequestException("INVALID_CURSOR", "cursor is not a cursor issued by this endpoint");
    }
}
