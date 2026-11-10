package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Prompt;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface LlmClient {
    /**
     * Completes a de-identified prompt. The returned future must finish within {@code timeout} or be cancellable:
     * cancelling it must abort the underlying provider request (as {@code HttpClient.sendAsync} futures do), because
     * callers cancel it when their timeout fires.
     */
    CompletableFuture<String> complete(Prompt prompt, Duration timeout);
}
