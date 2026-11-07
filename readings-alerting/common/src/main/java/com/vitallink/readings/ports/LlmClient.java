package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Prompt;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface LlmClient {
    CompletableFuture<String> complete(Prompt prompt, Duration timeout);
}
