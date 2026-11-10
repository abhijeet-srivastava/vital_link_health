package com.vitallink.readings.query.ai;

import java.time.Instant;
import java.util.UUID;

import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.vitallink.readings.domain.Summary;
import com.vitallink.readings.ports.ReadingSummaryService;
import com.vitallink.readings.query.api.RequestIds;
import com.vitallink.readings.query.security.AccessAudit;
import com.vitallink.readings.query.security.Caller;
import com.vitallink.readings.query.security.PanelAccessGuard;
import com.vitallink.readings.query.security.Scopes;

/**
 * AI summary of a patient's recent readings, same order as the other PHI reads: panel check (403 + DENIED audit),
 * summary, ALLOWED audit, response. Always 200 for an allowed caller: an unavailable LLM yields {@code source: rules}.
 * The response is labelled advisory; it is never used for severity, escalation or notification.
 */
@RestController
public class SummaryController {

    static final String READ_SUMMARY = "READ_SUMMARY";
    static final String RESOURCE = "summary";
    static final String LLM_LABEL =
            "AI-generated summary. Advisory only, not a clinical assessment; alert severity comes from threshold rules.";
    static final String RULES_LABEL =
            "Rule-based summary (AI unavailable). Advisory only, not a clinical assessment.";

    private final PanelAccessGuard guard;
    private final AccessAudit audit;
    private final ReadingSummaryService summaries;
    private final RequestIds requestIds;
    private final SummaryConfig.SummaryProperties props;

    public SummaryController(PanelAccessGuard guard, AccessAudit audit, ReadingSummaryService summaries,
                             RequestIds requestIds, SummaryConfig.SummaryProperties props) {
        this.guard = guard;
        this.audit = audit;
        this.summaries = summaries;
        this.requestIds = requestIds;
        this.props = props;
    }

    public record SummaryResponse(long patientId, String source, boolean advisory, String label, String text,
                                  long windowHours, Instant generatedAt) {

        static SummaryResponse of(long patientId, Summary s) {
            boolean llm = s.source() == Summary.Source.LLM;
            return new SummaryResponse(patientId, llm ? "llm" : "rules", true, llm ? LLM_LABEL : RULES_LABEL,
                    s.text(), s.window().toHours(), s.generatedAt());
        }
    }

    @GetMapping("/v1/patients/{patientId}/summary")
    public SummaryResponse summary(@PathVariable long patientId, @AuthenticationPrincipal Jwt jwt,
                                   @RequestHeader(value = RequestIds.HEADER, required = false) String requestHeader) {
        Caller caller = Caller.from(jwt);
        UUID requestId = requestIds.resolve(requestHeader);
        guard.requirePatient(caller, Scopes.READINGS_READ, patientId, READ_SUMMARY, RESOURCE, requestId);

        Summary summary = summaries.summarize(patientId, props.window());
        audit.allowed(caller, READ_SUMMARY, patientId, RESOURCE, requestId);
        return SummaryResponse.of(patientId, summary);
    }
}
