package com.vitallink.readings.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.ExistingHash;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingKey;
import com.vitallink.readings.ports.ReadingRepository;

/** Ingest write path (guard 1): one set-based insert per batch; the database decides what is new. */
public class JdbcReadingRepository implements ReadingRepository {

    /** Rows are sent sorted by key, so two overlapping batches lock rows in the same order and cannot deadlock. */
    private static final Comparator<Reading> KEY_ORDER =
            Comparator.comparing(Reading::readingUuid).thenComparing(Reading::measuredAt);

    private final JdbcClient jdbc;

    public JdbcReadingRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<ReadingKey> insertIgnoringDuplicates(List<Reading> batch) {
        List<Reading> rows = batch.stream().sorted(KEY_ORDER).toList();
        return jdbc.sql("""
                        INSERT INTO reading (device_id, reading_uuid, measured_at, patient_id, reading_type, device_seq,
                                             systolic, diastolic, pulse, value_num, payload_hash)
                        SELECT * FROM unnest(?::bigint[], ?::uuid[], ?::timestamptz[], ?::bigint[], ?::smallint[],
                                             ?::bigint[], ?::smallint[], ?::smallint[], ?::smallint[], ?::numeric[],
                                             ?::bigint[])
                        ON CONFLICT (device_id, reading_uuid, measured_at) DO NOTHING
                        RETURNING reading_uuid, measured_at
                        """)
                .params(
                        rows.stream().map(Reading::deviceId).toArray(Long[]::new),
                        rows.stream().map(r -> r.readingUuid().toString()).toArray(String[]::new),
                        rows.stream().map(r -> r.measuredAt().toString()).toArray(String[]::new),
                        rows.stream().map(Reading::patientId).toArray(Long[]::new),
                        rows.stream().map(r -> r.type().code()).toArray(Integer[]::new),
                        rows.stream().map(Reading::deviceSeq).toArray(Long[]::new),
                        rows.stream().map(Reading::systolic).toArray(Integer[]::new),
                        rows.stream().map(Reading::diastolic).toArray(Integer[]::new),
                        rows.stream().map(Reading::pulse).toArray(Integer[]::new),
                        rows.stream().map(r -> plain(r.valueNum())).toArray(String[]::new),
                        rows.stream().map(Reading::payloadHash).toArray(Long[]::new))
                .query((rs, n) -> new ReadingKey(rs.getObject("reading_uuid", UUID.class),
                        rs.getObject("measured_at", OffsetDateTime.class).toInstant()))
                .list();
    }

    @Override
    public List<ExistingHash> findPayloadHashes(long deviceId, List<ReadingKey> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }
        Instant min = keys.stream().map(ReadingKey::measuredAt).min(Comparator.naturalOrder()).orElseThrow();
        Instant max = keys.stream().map(ReadingKey::measuredAt).max(Comparator.naturalOrder()).orElseThrow();
        return jdbc.sql("""
                        SELECT r.reading_uuid, r.measured_at, r.payload_hash
                        FROM reading r
                        JOIN unnest(?::uuid[], ?::timestamptz[]) AS k (reading_uuid, measured_at)
                          ON r.reading_uuid = k.reading_uuid AND r.measured_at = k.measured_at
                        WHERE r.device_id = ? AND r.measured_at BETWEEN ? AND ?
                        """)
                .params(
                        keys.stream().map(k -> k.readingUuid().toString()).toArray(String[]::new),
                        keys.stream().map(k -> k.measuredAt().toString()).toArray(String[]::new),
                        deviceId, utc(min), utc(max))
                .query((rs, n) -> new ExistingHash(
                        new ReadingKey(rs.getObject("reading_uuid", UUID.class),
                                rs.getObject("measured_at", OffsetDateTime.class).toInstant()),
                        rs.getLong("payload_hash")))
                .list();
    }

    private static String plain(BigDecimal v) {
        return v == null ? null : v.toPlainString();
    }

    static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }
}
