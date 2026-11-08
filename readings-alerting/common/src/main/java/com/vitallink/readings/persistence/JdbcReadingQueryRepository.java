package com.vitallink.readings.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.Cursor;
import com.vitallink.readings.domain.Page;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.ports.ReadingQueryRepository;

/**
 * Reading reads in ix_reading_patient_time order (patient_id, reading_type, measured_at DESC, reading_uuid): the
 * index serves both the range filter and the keyset predicate, so a page costs limit + 1 index entries wherever it
 * starts. No OFFSET (rule 6). Bounded by from/to, which also prunes partitions.
 */
public class JdbcReadingQueryRepository implements ReadingQueryRepository {

    private static final String COLUMNS = """
            device_id, reading_uuid, measured_at, patient_id, reading_type, device_seq, systolic, diastolic, pulse,
            value_num, payload_hash""";

    private final JdbcClient jdbc;

    public JdbcReadingQueryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Page<Reading> findRange(long patientId, ReadingType type, Instant from, Instant to, Cursor cursor,
                                   int limit) {
        List<Reading> rows;
        if (cursor == null) {
            rows = jdbc.sql("""
                            SELECT %s FROM reading
                            WHERE patient_id = ? AND reading_type = ? AND measured_at >= ? AND measured_at < ?
                            ORDER BY measured_at DESC, reading_uuid
                            LIMIT ?
                            """.formatted(COLUMNS))
                    .params(patientId, type.code(), utc(from), utc(to), limit + 1)
                    .query((rs, n) -> reading(rs))
                    .list();
        } else {
            // Next page: strictly after the cursor in (measured_at DESC, reading_uuid ASC) order.
            rows = jdbc.sql("""
                            SELECT %s FROM reading
                            WHERE patient_id = ? AND reading_type = ? AND measured_at >= ? AND measured_at < ?
                              AND (measured_at < ? OR (measured_at = ? AND reading_uuid > ?))
                            ORDER BY measured_at DESC, reading_uuid
                            LIMIT ?
                            """.formatted(COLUMNS))
                    .params(patientId, type.code(), utc(from), utc(to), utc(cursor.measuredAt()),
                            utc(cursor.measuredAt()), cursor.readingUuid(), limit + 1)
                    .query((rs, n) -> reading(rs))
                    .list();
        }
        return Page.of(rows, limit);
    }

    @Override
    public Optional<Reading> findLatest(long patientId, ReadingType type) {
        return jdbc.sql("""
                        SELECT %s FROM reading
                        WHERE patient_id = ? AND reading_type = ?
                        ORDER BY measured_at DESC, reading_uuid
                        LIMIT 1
                        """.formatted(COLUMNS))
                .params(patientId, type.code())
                .query((rs, n) -> reading(rs))
                .optional();
    }

    private static Reading reading(ResultSet rs) throws SQLException {
        return new Reading(rs.getLong("device_id"), rs.getObject("reading_uuid", UUID.class),
                rs.getObject("measured_at", OffsetDateTime.class).toInstant(), rs.getLong("patient_id"),
                ReadingType.fromCode(rs.getInt("reading_type")), rs.getObject("device_seq", Long.class),
                rs.getObject("systolic", Integer.class), rs.getObject("diastolic", Integer.class),
                rs.getObject("pulse", Integer.class), rs.getBigDecimal("value_num"), rs.getLong("payload_hash"));
    }

    private static OffsetDateTime utc(Instant t) {
        return JdbcReadingRepository.utc(t);
    }
}
