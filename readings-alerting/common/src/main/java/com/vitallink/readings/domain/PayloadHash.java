package com.vitallink.readings.domain;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * reading.payload_hash: first 64 bits of SHA-256 over the canonical values. Same reading id with a different hash
 * is a CONFLICT. Inputs must already be canonical (valueNum at scale 2), or equal values would hash differently.
 */
public final class PayloadHash {

    private PayloadHash() {
    }

    public static long of(ReadingType type, Integer systolic, Integer diastolic, Integer pulse, BigDecimal valueNum) {
        String canonical = type.code() + "|" + str(systolic) + "|" + str(diastolic) + "|" + str(pulse) + "|"
                + (valueNum == null ? "" : valueNum.toPlainString());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest, 0, 8).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String str(Integer v) {
        return v == null ? "" : v.toString();
    }
}
