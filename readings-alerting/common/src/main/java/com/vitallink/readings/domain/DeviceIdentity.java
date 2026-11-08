package com.vitallink.readings.domain;

/** An authenticated device. keyId is the public half of the key; the secret never leaves the authenticator. */
public record DeviceIdentity(long deviceId, String keyId) {
}
