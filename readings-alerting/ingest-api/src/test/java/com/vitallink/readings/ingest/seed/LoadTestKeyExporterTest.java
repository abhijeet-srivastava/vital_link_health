package com.vitallink.readings.ingest.seed;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.domain.DeviceKeys;
import com.vitallink.readings.testsupport.SpringIntegrationTestBase;

@SpringBootTest(properties = {
        "vitallink.device-keys.pepper=test-pepper",
        "vitallink.seed.load-keys-file=" + LoadTestKeyExporterTest.KEYS_FILE})
@ActiveProfiles("seed")
@ExtendWith(OutputCaptureExtension.class)
class LoadTestKeyExporterTest extends SpringIntegrationTestBase {

    static final String KEYS_FILE = "target/load-keys-test/keys.csv";
    static final byte[] PEPPER = "test-pepper".getBytes(StandardCharsets.UTF_8);

    @Autowired
    LoadTestKeyExporter exporter;

    @Autowired
    ApplicationContext context;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void devSeed() throws Exception {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("db/seed/R__dev_seed.sql")) {
            jdbc.sql(new String(in.readAllBytes(), StandardCharsets.UTF_8)).update();
        }
        new DevCredentialSeeder(jdbc, "test-pepper").run(null);
    }

    @Test
    void demo_keys_are_not_rotated_when_exporting_load_keys() {
        assertThat(context.getBeansOfType(DevCredentialSeeder.class)).isEmpty();
    }

    @Test
    void every_non_demo_device_gets_a_working_key_in_the_file_and_demo_keys_are_untouched(CapturedOutput output)
            throws Exception {
        List<String> demoKeyIdsBefore = activeKeyIds(DevCredentialSeeder.DEMO_DEVICES);

        exporter.run(null);

        List<String> lines = Files.readAllLines(Path.of(KEYS_FILE));
        assertThat(lines.get(0)).isEqualTo("device_id,key");
        List<String> rows = lines.subList(1, lines.size());
        assertThat(rows).hasSize(1000 - DevCredentialSeeder.DEMO_DEVICES.size());
        for (String row : rows) {
            String[] cols = row.split(",");
            long deviceId = Long.parseLong(cols[0]);
            DeviceKeys.ApiKey key = DeviceKeys.parse(cols[1]).orElseThrow();
            byte[] stored = jdbc.sql("""
                            SELECT secret_hash FROM device_credential
                            WHERE key_id = ? AND device_id = ? AND status = 'ACTIVE'
                            """)
                    .params(key.keyId(), deviceId).query(byte[].class).single();

            assertThat(deviceId).isNotIn(DevCredentialSeeder.DEMO_DEVICES);
            assertThat(DeviceKeys.matches(key.secret(), PEPPER, stored)).isTrue();
            assertThat(output.getAll()).doesNotContain(key.secret());
        }
        assertThat(activeKeyIds(DevCredentialSeeder.DEMO_DEVICES)).isEqualTo(demoKeyIdsBefore);
        assertThat(jdbc.sql("SELECT count(*) FROM device_credential WHERE status = 'ACTIVE'")
                .query(Long.class).single()).isEqualTo(1000);
    }

    @Test
    void keys_file_is_readable_by_its_owner_only() throws Exception {
        exporter.run(null);

        assertThat(Files.getPosixFilePermissions(Path.of(KEYS_FILE)))
                .isEqualTo(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    }

    private List<String> activeKeyIds(List<Long> devices) {
        return jdbc.sql("""
                        SELECT key_id FROM device_credential
                        WHERE status = 'ACTIVE' AND device_id = ANY(?::bigint[]) ORDER BY device_id
                        """)
                .param(devices.toArray(Long[]::new)).query(String.class).list();
    }
}
