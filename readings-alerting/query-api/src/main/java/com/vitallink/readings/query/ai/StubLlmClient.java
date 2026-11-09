package com.vitallink.readings.query.ai;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.vitallink.readings.domain.Prompt;
import com.vitallink.readings.ports.LlmClient;

/**
 * Deterministic stand-in for an LLM provider; no real provider is wired. The answer depends only on the prompt's data
 * lines. {@code VITALLINK_LLM_STUB_MODE} makes it misbehave like a provider would: {@code timeout} never answers,
 * {@code error} fails every call. It keeps the last prompt and a call count so tests can check what was sent.
 */
public class StubLlmClient implements LlmClient {

    public enum Mode {
        OK,
        TIMEOUT,
        ERROR
    }

    private static final Pattern LINE = Pattern.compile("^(\\w+) (\\S+) \\| h-\\d+ \\| n=(\\d+) min=(\\S+) max=(\\S+) avg=\\S+$");

    private final Mode mode;
    private final AtomicInteger calls = new AtomicInteger();
    private volatile Prompt lastPrompt;
    private volatile CompletableFuture<String> lastCall;

    public StubLlmClient(Mode mode) {
        this.mode = mode;
    }

    @Override
    public CompletableFuture<String> complete(Prompt prompt, Duration timeout) {
        calls.incrementAndGet();
        lastPrompt = prompt;
        CompletableFuture<String> call = switch (mode) {
            case OK -> CompletableFuture.completedFuture(answer(prompt));
            case TIMEOUT -> new CompletableFuture<>();   // a provider that hangs until the caller cancels it
            case ERROR -> CompletableFuture.failedFuture(new IllegalStateException("stub LLM in error mode"));
        };
        lastCall = call;
        return call;
    }

    public int calls() {
        return calls.get();
    }

    public Prompt lastPrompt() {
        return lastPrompt;
    }

    /** The future returned by the last call; in timeout mode the caller must cancel it. */
    public CompletableFuture<String> lastCall() {
        return lastCall;
    }

    private static String answer(Prompt prompt) {
        record Range(String unit, int count, BigDecimal min, BigDecimal max) {
            Range merge(Range o) {
                return new Range(unit, count + o.count, min.min(o.min), max.max(o.max));
            }
        }
        Map<String, Range> byMeasure = new LinkedHashMap<>();
        prompt.data().lines().forEach(line -> {
            Matcher m = LINE.matcher(line);
            if (m.matches()) {
                byMeasure.merge(m.group(1), new Range(m.group(2), Integer.parseInt(m.group(3)),
                        new BigDecimal(m.group(4)), new BigDecimal(m.group(5))), Range::merge);
            }
        });
        if (byMeasure.isEmpty()) {
            return "No measurements to summarise.";
        }
        return byMeasure.entrySet().stream()
                .map(e -> "%s stayed between %s and %s %s across %d readings.".formatted(
                        e.getKey().charAt(0) + e.getKey().substring(1).toLowerCase(),
                        e.getValue().min().stripTrailingZeros().toPlainString(),
                        e.getValue().max().stripTrailingZeros().toPlainString(), e.getValue().unit(),
                        e.getValue().count()))
                .collect(Collectors.joining(" "));
    }
}
