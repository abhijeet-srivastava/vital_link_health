package com.vitallink.readings.ingest.seed;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.domain.DeviceKeys;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

@SpringBootTest(properties = "vitallink.device-keys.pepper=test-pepper")
@ActiveProfiles("seed")
@ExtendWith(OutputCaptureExtension.class)
class DevCredentialSeederTest extends SpringIntegrationTestBase {

    static final byte[] PEPPER = "test-pepper".getBytes(StandardCharsets.UTF_8);
    static final Pattern LOGGED_KEY = Pattern.compile("vl_[0-9a-f]{16}_[A-Za-z0-9_-]{43}");

    @Autowired
    DevCredentialSeeder seeder;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void devSeed() throws Exception {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("db/seed/R__dev_seed.sql")) {
            jdbc.sql(new String(in.readAllBytes(), StandardCharsets.UTF_8)).update();
        }
    }

    @Test
    void every_device_gets_one_active_credential_and_five_working_demo_keys_are_logged(CapturedOutput output) {
        seeder.run(null);

        assertThat(count("SELECT count(*) FROM device_credential WHERE status = 'ACTIVE'")).isEqualTo(1000);
        assertThat(count("SELECT count(DISTINCT device_id) FROM device_credential WHERE status = 'ACTIVE'")).isEqualTo(1000);

        Set<String> keys = loggedKeys(output.getAll());
        assertThat(keys).hasSize(5);
        for (String token : keys) {
            DeviceKeys.ApiKey key = DeviceKeys.parse(token).orElseThrow();
            byte[] stored = jdbc.sql("SELECT secret_hash FROM device_credential WHERE key_id = ? AND status = 'ACTIVE'")
                    .param(key.keyId()).query(byte[].class).single();
            long deviceId = jdbc.sql("SELECT device_id FROM device_credential WHERE key_id = ?")
                    .param(key.keyId()).query(Long.class).single();

            assertThat(DeviceKeys.matches(key.secret(), PEPPER, stored)).isTrue();
            assertThat(deviceId).isIn(DevCredentialSeeder.DEMO_DEVICES);
        }
    }

    @Test
    void rerun_rotates_only_the_demo_keys(CapturedOutput output) {
        seeder.run(null);
        Set<String> first = loggedKeys(output.getAll());

        seeder.run(null);
        Set<String> all = loggedKeys(output.getAll());

        assertThat(all).hasSize(10).containsAll(first);
        assertThat(count("SELECT count(*) FROM device_credential WHERE status = 'ACTIVE'")).isEqualTo(1000);
        assertThat(count("""
                SELECT count(*) FROM device_credential WHERE status = 'REVOKED' AND device_id = ANY(?::bigint[])
                """, (Object) DevCredentialSeeder.DEMO_DEVICES.toArray(Long[]::new))).isEqualTo(5);
        assertThat(count("SELECT count(*) FROM device_credential")).isEqualTo(1005);
    }

    @Test
    void secrets_are_never_stored(CapturedOutput output) {
        seeder.run(null);

        for (String token : loggedKeys(output.getAll())) {
            String secret = DeviceKeys.parse(token).orElseThrow().secret();
            assertThat(count("SELECT count(*) FROM device_credential WHERE encode(secret_hash, 'escape') LIKE '%' || ? || '%'",
                    secret)).isZero();
        }
    }

    private static Set<String> loggedKeys(String log) {
        Set<String> keys = new LinkedHashSet<>();
        Matcher m = LOGGED_KEY.matcher(log);
        while (m.find()) {
            keys.add(m.group());
        }
        return keys;
    }

    private long count(String sql, Object... params) {
        return jdbc.sql(sql).params(List.of(params)).query(Long.class).single();
    }
}
