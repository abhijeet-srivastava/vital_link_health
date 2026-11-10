package com.vitallink.readings.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DeviceKeysTest {

    static final byte[] PEPPER = "test-pepper".getBytes(StandardCharsets.UTF_8);

    @Test
    void generated_key_round_trips_through_its_token() {
        DeviceKeys.ApiKey key = DeviceKeys.generate(new SecureRandom());

        assertThat(key.token()).startsWith("vl_" + key.keyId() + "_");
        assertThat(DeviceKeys.parse(key.token())).contains(key);
    }

    @Test
    void generated_keys_are_unique() {
        SecureRandom random = new SecureRandom();

        assertThat(DeviceKeys.generate(random)).isNotEqualTo(DeviceKeys.generate(random));
    }

    @Test
    void secret_may_contain_underscores_because_key_id_never_does() {
        assertThat(DeviceKeys.parse("vl_0123456789abcdef_ab_cd-ef"))
                .contains(new DeviceKeys.ApiKey("0123456789abcdef", "ab_cd-ef"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "vl_", "vl__secret", "vl_0123456789abcdef_", "xx_0123456789abcdef_secret",
            "vl_0123456789ABCDEF_secret", "vl_0123_secret"})
    void malformed_tokens_are_rejected(String token) {
        assertThat(DeviceKeys.parse(token)).isEmpty();
    }

    @Test
    void hash_is_hmac_sha256_keyed_by_the_pepper() {
        // RFC 4231 test case 2: key "Jefe", data "what do ya want for nothing?"
        byte[] hash = DeviceKeys.hash("what do ya want for nothing?", "Jefe".getBytes(StandardCharsets.UTF_8));

        assertThat(HexFormat.of().formatHex(hash))
                .isEqualTo("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    }

    @Test
    void matches_only_the_right_secret_and_pepper() {
        DeviceKeys.ApiKey key = DeviceKeys.generate(new SecureRandom());
        byte[] stored = DeviceKeys.hash(key.secret(), PEPPER);

        assertThat(DeviceKeys.matches(key.secret(), PEPPER, stored)).isTrue();
        assertThat(DeviceKeys.matches(key.secret() + "x", PEPPER, stored)).isFalse();
        assertThat(DeviceKeys.matches(key.secret(), "other".getBytes(StandardCharsets.UTF_8), stored)).isFalse();
    }

    @Test
    void to_string_never_reveals_the_secret() {
        DeviceKeys.ApiKey key = DeviceKeys.generate(new SecureRandom());

        assertThat(key.toString()).contains(key.keyId()).doesNotContain(key.secret());
    }
}
