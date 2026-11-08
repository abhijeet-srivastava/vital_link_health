package com.vitallink.readings.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

/** Open clinical decision: rejecting readings older than 90 days is a flag; blank turns it off. */
@TestPropertySource(properties = "vitallink.ingest.reject-older-than=")
class ReadingIngestOldReadingsFlagOffTest extends IngestApiTestSupport {

    @Test
    void reading_older_than_90_days_accepted_when_flag_off() throws Exception {
        String key = givenDevice(41, boundTo(41, daysAgo(200)));
        UUID id = UUID.randomUUID();

        Response response = post(41, key, batch(41, List.of(bp(id, daysAgo(91), 120, 80))));

        assertThat(response.status()).isEqualTo(202);
        assertThat(response.statuses()).containsExactly("ACCEPTED");
        assertThat(count("SELECT count(*) FROM reading_default WHERE reading_uuid = ?", id)).isEqualTo(1);
    }
}
