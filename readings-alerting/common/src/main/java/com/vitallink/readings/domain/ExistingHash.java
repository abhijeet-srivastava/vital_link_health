package com.vitallink.readings.domain;

/** payload_hash of a stored reading, used to tell DUPLICATE from CONFLICT. */
public record ExistingHash(ReadingKey key, long payloadHash) {
}
