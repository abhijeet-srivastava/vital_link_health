package com.vitallink.readings.ingest.seed;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vitallink.readings.domain.DeviceKeys;

/**
 * Dev only ({@code seed} profile): gives every seeded device an API key. Secrets are random and exist only in memory;
 * the database gets HMAC(secret, pepper). The keys of {@link #DEMO_DEVICES} are rotated on every run and logged so a
 * developer always has working keys; no other profile ever logs a key.
 */
@Component
@Profile("seed")
@ConditionalOnExpression("'${vitallink.seed.load-keys-file:}'.isEmpty()")   // load-key runs leave demo keys alone
public class DevCredentialSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DevCredentialSeeder.class);

    /** Two BP cuffs, two glucometers, one pulse oximeter from R__dev_seed.sql. */
    public static final List<Long> DEMO_DEVICES = List.of(1L, 2L, 601L, 602L, 851L);

    private final JdbcClient jdbc;
    private final byte[] pepper;
    private final SecureRandom random = new SecureRandom();

    public DevCredentialSeeder(JdbcClient jdbc, @Value("${vitallink.device-keys.pepper}") String pepper) {
        if (pepper.isBlank()) {
            throw new IllegalStateException("vitallink.device-keys.pepper (VITALLINK_DEVICE_KEY_PEPPER) is not set");
        }
        this.jdbc = jdbc;
        this.pepper = pepper.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        Long[] demo = DEMO_DEVICES.toArray(Long[]::new);
        jdbc.sql("UPDATE device_credential SET status = 'REVOKED' WHERE status = 'ACTIVE' AND device_id = ANY(?)")
                .param(demo)
                .update();

        List<Long> devices = jdbc.sql("""
                        SELECT d.device_id FROM device d
                        WHERE NOT EXISTS (SELECT 1 FROM device_credential c
                                          WHERE c.device_id = d.device_id AND c.status = 'ACTIVE')
                        ORDER BY d.device_id
                        """)
                .query(Long.class)
                .list();
        if (devices.isEmpty()) {
            log.warn("No devices without an active key; is the dev seed (db/seed) applied?");
            return;
        }

        List<String> keyIds = new ArrayList<>(devices.size());
        List<String> hashes = new ArrayList<>(devices.size());
        List<DeviceKeys.ApiKey> demoKeys = new ArrayList<>();
        for (Long deviceId : devices) {
            DeviceKeys.ApiKey key = DeviceKeys.generate(random);
            keyIds.add(key.keyId());
            hashes.add(HexFormat.of().formatHex(DeviceKeys.hash(key.secret(), pepper)));
            if (DEMO_DEVICES.contains(deviceId)) {
                demoKeys.add(key);
            }
        }
        int inserted = jdbc.sql("""
                        INSERT INTO device_credential (key_id, device_id, secret_hash)
                        SELECT k, d, decode(h, 'hex')
                        FROM unnest(?::text[], ?::bigint[], ?::text[]) AS t (k, d, h)
                        """)
                .params(keyIds.toArray(String[]::new), devices.toArray(Long[]::new), hashes.toArray(String[]::new))
                .update();

        log.info("Dev seed: {} device keys created, {} demo keys rotated", inserted, demoKeys.size());
        for (int i = 0; i < devices.size(); i++) {
            if (DEMO_DEVICES.contains(devices.get(i))) {
                DeviceKeys.ApiKey key = demoKeys.remove(0);
                log.info("Dev device key device_id={} key={}", devices.get(i), key.token());
            }
        }
    }
}
