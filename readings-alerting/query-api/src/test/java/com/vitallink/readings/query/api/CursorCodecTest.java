package com.vitallink.readings.query.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.domain.Cursor;
import com.vitallink.readings.domain.QueueCursor;

class CursorCodecTest {

    @Test
    void reading_and_queue_cursors_round_trip_at_microsecond_precision() {
        Cursor reading = new Cursor(Instant.parse("2026-11-07T06:42:10.123456Z"), UUID.randomUUID());
        QueueCursor queue = new QueueCursor(2, Instant.parse("2026-11-07T06:42:10.000001Z"), 42L);

        assertThat(CursorCodec.decodeReading(CursorCodec.encode(reading))).isEqualTo(reading);
        assertThat(CursorCodec.decodeQueue(CursorCodec.encode(queue))).isEqualTo(queue);
        assertThat(CursorCodec.decodeReading(null)).isNull();
        assertThat(CursorCodec.decodeQueue(" ")).isNull();
    }

    @Test
    void foreign_tampered_or_garbage_cursors_are_rejected() {
        String readingCursor = CursorCodec.encode(new Cursor(Instant.now(), UUID.randomUUID()));
        String queueCursor = CursorCodec.encode(new QueueCursor(1, Instant.now(), 1));

        for (String bad : new String[]{"%%%", b64("r1:abc:not-a-uuid"), b64("r2:1:" + UUID.randomUUID()),
                b64("q1:9:1:1"), queueCursor}) {
            assertThatThrownBy(() -> CursorCodec.decodeReading(bad)).isInstanceOf(InvalidRequestException.class)
                    .hasMessageNotContaining(bad);
        }
        assertThatThrownBy(() -> CursorCodec.decodeQueue(readingCursor)).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> CursorCodec.decodeQueue(b64("q1:9:1:1"))).isInstanceOf(InvalidRequestException.class);
    }

    private static String b64(String s) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }
}
