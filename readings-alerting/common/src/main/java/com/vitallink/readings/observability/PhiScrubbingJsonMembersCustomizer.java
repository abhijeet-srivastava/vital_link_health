package com.vitallink.readings.observability;

import java.util.Locale;
import java.util.Set;

import org.springframework.boot.json.JsonWriter;
import org.springframework.boot.logging.structured.StructuredLoggingJsonMembersCustomizer;

/**
 * Drops PHI-tagged fields (MDC keys, key-value pairs) from structured JSON logs.
 * Stub: a fixed deny-list; the tested scrubber arrives in stage 9.
 */
public class PhiScrubbingJsonMembersCustomizer implements StructuredLoggingJsonMembersCustomizer<Object> {

    static final Set<String> PHI_FIELDS = Set.of(
            "name", "first_name", "last_name", "email", "phone", "address", "dob", "date_of_birth", "note", "notes");

    @Override
    public void customize(JsonWriter.Members<Object> members) {
        members.applyingPathFilter(path -> PHI_FIELDS.contains(path.name().toLowerCase(Locale.ROOT)));
    }
}
