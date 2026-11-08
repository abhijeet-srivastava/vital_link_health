package com.vitallink.readings.persistence;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.DeviceBinding;
import com.vitallink.readings.domain.DeviceCredential;
import com.vitallink.readings.ports.DeviceDirectory;

public class JdbcDeviceDirectory implements DeviceDirectory {

    private final JdbcClient jdbc;

    public JdbcDeviceDirectory(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<DeviceCredential> findActiveCredential(String keyId) {
        return jdbc.sql("""
                        SELECT c.key_id, c.device_id, c.secret_hash, c.expires_at
                        FROM device_credential c JOIN device d ON d.device_id = c.device_id
                        WHERE c.key_id = ? AND c.status = 'ACTIVE' AND d.status = 'ACTIVE'
                        """)
                .param(keyId)
                .query((rs, n) -> new DeviceCredential(rs.getString("key_id"), rs.getLong("device_id"),
                        rs.getBytes("secret_hash"), instant(rs.getObject("expires_at", OffsetDateTime.class))))
                .optional();
    }

    @Override
    public List<DeviceBinding> findBindings(long deviceId, Instant from, Instant to) {
        // PK (device_id, valid_from) range scan.
        return jdbc.sql("""
                        SELECT patient_id, valid_from, valid_to
                        FROM device_binding
                        WHERE device_id = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
                        ORDER BY valid_from
                        """)
                .params(deviceId, JdbcReadingRepository.utc(to), JdbcReadingRepository.utc(from))
                .query((rs, n) -> new DeviceBinding(rs.getLong("patient_id"),
                        instant(rs.getObject("valid_from", OffsetDateTime.class)),
                        instant(rs.getObject("valid_to", OffsetDateTime.class))))
                .list();
    }

    private static Instant instant(OffsetDateTime t) {
        return t == null ? null : t.toInstant();
    }
}
