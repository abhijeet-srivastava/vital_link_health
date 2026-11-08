package com.vitallink.readings.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class ReadingIngestAccessTest extends IngestApiTestSupport {

    static final long PATIENT_A = 1000;
    static final long PATIENT_B = 1;

    @Test
    void bad_key_401() throws Exception {
        String key = givenDevice(21, boundTo(PATIENT_A, daysAgo(30)));
        Map<String, Object> body = batch(PATIENT_A, List.of(bp(UUID.randomUUID(), now(), 120, 80)));
        String keyId = key.substring(3, 19);

        List<String> badKeys = List.of(
                "vl_" + keyId + "_wrongsecretwrongsecretwrongsecretwrongsec",   // known key_id, wrong secret
                "vl_0123456789abcdef_" + key.substring(20),                    // unknown key_id
                "not-a-device-key");
        for (String bad : badKeys) {
            Response response = post(21, bad, body);
            assertThat(response.status()).as(bad).isEqualTo(401);
            assertThat(response.contentType()).startsWith("application/problem+json");
            assertThat(response.code()).isEqualTo("UNAUTHENTICATED");
        }
        Response missing = post(21, null, body);
        assertThat(missing.status()).isEqualTo(401);
        assertThat(missing.contentType()).startsWith("application/problem+json");

        assertThat(count("SELECT count(*) FROM reading")).isZero();
    }

    @Test
    void key_of_another_device_is_403() throws Exception {
        String keyOf21 = givenDevice(21, boundTo(PATIENT_A, daysAgo(30)));
        givenDevice(22, boundTo(PATIENT_A, daysAgo(30)));

        Response response = post(22, keyOf21, batch(PATIENT_A, List.of(bp(UUID.randomUUID(), now(), 120, 80))));

        assertThat(response.status()).isEqualTo(403);
        assertThat(response.contentType()).startsWith("application/problem+json");
        assertThat(count("SELECT count(*) FROM reading")).isZero();
    }

    @Test
    void device_bound_to_patient_a_posting_for_patient_b_gets_403() throws Exception {
        String key = givenDevice(21, boundTo(PATIENT_A, daysAgo(30)));

        Response response = post(21, key, batch(PATIENT_B, List.of(bp(UUID.randomUUID(), now().minusSeconds(10), 120, 80))));

        assertThat(response.status()).isEqualTo(403);
        assertThat(response.contentType()).startsWith("application/problem+json");
        assertThat(response.code()).isEqualTo("DEVICE_NOT_BOUND");
        assertThat(count("SELECT count(*) FROM reading")).isZero();
        assertThat(count("SELECT count(*) FROM outbox_event")).isZero();
        assertThat(count("""
                SELECT count(*) FROM audit_log
                WHERE actor_type = 'DEVICE' AND actor_id = '21' AND outcome = 'DENIED' AND patient_id = ?
                """, PATIENT_B)).isEqualTo(1);
    }

    @Test
    void reading_measured_before_rebinding_is_stored_for_old_patient() throws Exception {
        Instant rebound = daysAgo(10);
        String key = givenDevice(21, boundTo(PATIENT_A, daysAgo(60), rebound), boundTo(PATIENT_B, rebound));
        UUID buffered = UUID.randomUUID();
        Instant beforeRebind = rebound.minusSeconds(3600);

        Response forOld = post(21, key, batch(PATIENT_A, List.of(bp(buffered, beforeRebind, 125, 82))));
        Response forNew = post(21, key, batch(PATIENT_B, List.of(bp(UUID.randomUUID(), beforeRebind, 125, 82))));
        Response currentForNew = post(21, key, batch(PATIENT_B, List.of(bp(UUID.randomUUID(), now(), 125, 82))));

        assertThat(forOld.status()).isEqualTo(202);
        assertThat(forOld.statuses()).containsExactly("ACCEPTED");
        assertThat(count("SELECT patient_id FROM reading WHERE reading_uuid = ?", buffered)).isEqualTo(PATIENT_A);
        assertThat(forNew.status()).isEqualTo(403);
        assertThat(currentForNew.status()).isEqualTo(202);
        assertThat(count("SELECT count(*) FROM reading WHERE patient_id = ?", PATIENT_B)).isEqualTo(1);
    }

    @Test
    void accepted_ingest_writes_one_allowed_audit_row() throws Exception {
        String key = givenDevice(21, boundTo(PATIENT_A, daysAgo(30)));

        post(21, key, batch(PATIENT_A, List.of(bp(UUID.randomUUID(), now(), 120, 80), bp(UUID.randomUUID(), now(), 121, 80))));

        assertThat(count("""
                SELECT count(*) FROM audit_log
                WHERE actor_type = 'DEVICE' AND actor_id = '21' AND action = 'INGEST_READINGS'
                  AND outcome = 'ALLOWED' AND patient_id = ?
                """, PATIENT_A)).isEqualTo(1);
    }
}
