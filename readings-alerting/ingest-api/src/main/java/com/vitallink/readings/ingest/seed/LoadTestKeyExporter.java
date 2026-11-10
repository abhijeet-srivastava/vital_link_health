package com.vitallink.readings.ingest.seed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vitallink.readings.domain.DeviceKeys;

/**
 * Dev only ({@code seed} profile with {@code vitallink.seed.load-keys-file} set): rotates the key of every seeded
 * device except {@link DevCredentialSeeder#DEMO_DEVICES} and writes {@code device_id,key} to that file (owner-only)
 * for the k6 load test. Keys go to the file, never to the log; the demo keys keep working.
 */
@Component
@Profile("seed")
@ConditionalOnProperty("vitallink.seed.load-keys-file")
public class LoadTestKeyExporter implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadTestKeyExporter.class);

    private final JdbcClient jdbc;
    private final byte[] pepper;
    private final Path keysFile;
    private final SecureRandom random = new SecureRandom();

    public LoadTestKeyExporter(JdbcClient jdbc, @Value("${vitallink.device-keys.pepper}") String pepper,
                               @Value("${vitallink.seed.load-keys-file}") String keysFile) {
        if (pepper.isBlank()) {
            throw new IllegalStateException("vitallink.device-keys.pepper (VITALLINK_DEVICE_KEY_PEPPER) is not set");
        }
        this.jdbc = jdbc;
        this.pepper = pepper.getBytes(StandardCharsets.UTF_8);
        this.keysFile = Path.of(keysFile);
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        Long[] demo = DevCredentialSeeder.DEMO_DEVICES.toArray(Long[]::new);
        List<Long> devices = jdbc.sql("SELECT device_id FROM device WHERE device_id <> ALL(?) ORDER BY device_id")
                .param(demo)
                .query(Long.class)
                .list();
        jdbc.sql("UPDATE device_credential SET status = 'REVOKED' WHERE status = 'ACTIVE' AND device_id <> ALL(?)")
                .param(demo)
                .update();

        List<String> keyIds = new ArrayList<>(devices.size());
        List<String> hashes = new ArrayList<>(devices.size());
        StringBuilder csv = new StringBuilder("device_id,key\n");
        for (Long deviceId : devices) {
            DeviceKeys.ApiKey key = DeviceKeys.generate(random);
            keyIds.add(key.keyId());
            hashes.add(HexFormat.of().formatHex(DeviceKeys.hash(key.secret(), pepper)));
            csv.append(deviceId).append(',').append(key.token()).append('\n');
        }
        jdbc.sql("""
                        INSERT INTO device_credential (key_id, device_id, secret_hash)
                        SELECT k, d, decode(h, 'hex')
                        FROM unnest(?::text[], ?::bigint[], ?::text[]) AS t (k, d, h)
                        """)
                .params(keyIds.toArray(String[]::new), devices.toArray(Long[]::new), hashes.toArray(String[]::new))
                .update();
        write(csv.toString());
        log.info("Load test: {} device keys rotated and written to the keys file", devices.size());
    }

    private void write(String csv) {
        try {
            if (keysFile.getParent() != null) {
                Files.createDirectories(keysFile.getParent());
            }
            Files.deleteIfExists(keysFile);
            Files.createFile(keysFile, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.writeString(keysFile, csv, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write the load-test keys file", e);
        }
    }
}
