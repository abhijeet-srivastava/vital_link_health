package com.vitallink.readings.persistence;

import static com.vitallink.readings.persistence.JdbcReadingRepository.utc;

import java.time.Instant;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.ports.CareAssignmentRepository;

/** PRIMARY handover as touching ranges: the old row ends where the new one starts (ex_primary_coach_no_overlap). */
public class JdbcCareAssignmentRepository implements CareAssignmentRepository {

    private final JdbcClient jdbc;

    public JdbcCareAssignmentRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean replacePrimary(long patientId, long coachId, Instant at) {
        jdbc.sql("""
                        UPDATE care_assignment SET valid_to = ?
                        WHERE patient_id = ? AND role = 'PRIMARY' AND valid_to IS NULL AND coach_id <> ?
                        """)
                .params(utc(at), patientId, coachId)
                .update();
        // No-op when coachId is already the open PRIMARY; ux_primary_coach rejects a concurrent second PRIMARY.
        return jdbc.sql("""
                        INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
                        SELECT ?, ?, 'PRIMARY', ?
                        WHERE NOT EXISTS (SELECT 1 FROM care_assignment
                                          WHERE patient_id = ? AND role = 'PRIMARY' AND valid_to IS NULL)
                        """)
                .params(patientId, coachId, utc(at), patientId)
                .update() == 1;
    }
}
