package com.vitallink.readings.domain;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.UUID;

/**
 * Server-derived reading ids (D4): when a device sends no reading_id, the id is UUIDv5 of
 * {@code device_id/device_seq/measured_at/reading_type}, so a retry of the same measurement derives the same id.
 */
public final class ReadingIds {

    /** Fixed namespace for VitalLink reading ids. Changing it changes every derived id: never change it. */
    static final UUID NAMESPACE = UUID.fromString("5b0e4f8e-3c1a-4d2b-9f6e-0a7c1d2e3f40");

    private ReadingIds() {
    }

    public static UUID derive(long deviceId, Long deviceSeq, Instant measuredAt, ReadingType type) {
        String name = deviceId + "/" + (deviceSeq == null ? "" : deviceSeq) + "/" + measuredAt + "/" + type.name();
        return v5(NAMESPACE, name);
    }

    static UUID v5(UUID namespace, String name) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 unavailable", e);
        }
        sha1.update(ByteBuffer.allocate(16)
                .putLong(namespace.getMostSignificantBits())
                .putLong(namespace.getLeastSignificantBits())
                .array());
        byte[] hash = sha1.digest(name.getBytes(StandardCharsets.UTF_8));
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);   // version 5
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);   // IETF variant
        ByteBuffer bb = ByteBuffer.wrap(hash, 0, 16);
        return new UUID(bb.getLong(), bb.getLong());
    }
}
