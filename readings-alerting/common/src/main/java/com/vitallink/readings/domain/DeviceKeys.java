package com.vitallink.readings.domain;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Device API keys {@code vl_<key_id>_<secret>} (security-phi.md). {@code key_id} is the public lookup key in
 * {@code device_credential}; only HMAC-SHA256(secret, pepper) is stored, never the secret.
 */
public final class DeviceKeys {

    /** key_id: 16 lowercase hex chars, never an underscore, so the secret (base64url) may contain one. */
    private static final Pattern TOKEN = Pattern.compile("vl_([0-9a-f]{16})_([A-Za-z0-9_-]+)");
    private static final int KEY_ID_BYTES = 8;
    private static final int SECRET_BYTES = 32;

    private DeviceKeys() {
    }

    public record ApiKey(String keyId, String secret) {
        public String token() {
            return "vl_" + keyId + "_" + secret;
        }

        @Override
        public String toString() {
            return "ApiKey[keyId=" + keyId + "]";
        }
    }

    public static ApiKey generate(SecureRandom random) {
        byte[] id = new byte[KEY_ID_BYTES];
        byte[] secret = new byte[SECRET_BYTES];
        random.nextBytes(id);
        random.nextBytes(secret);
        return new ApiKey(HexFormat.of().formatHex(id), Base64.getUrlEncoder().withoutPadding().encodeToString(secret));
    }

    public static Optional<ApiKey> parse(String token) {
        Matcher m = TOKEN.matcher(token);
        return m.matches() ? Optional.of(new ApiKey(m.group(1), m.group(2))) : Optional.empty();
    }

    public static byte[] hash(String secret, byte[] pepper) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(pepper, "HmacSHA256"));
            return mac.doFinal(secret.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    /** Constant-time comparison of the secret's HMAC with the stored hash. */
    public static boolean matches(String secret, byte[] pepper, byte[] storedHash) {
        return MessageDigest.isEqual(hash(secret, pepper), storedHash);
    }
}
