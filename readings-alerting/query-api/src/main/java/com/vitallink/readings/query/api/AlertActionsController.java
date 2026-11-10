package com.vitallink.readings.query.api;

import java.util.UUID;

import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.ports.AlertService;
import com.vitallink.readings.query.security.Caller;
import com.vitallink.readings.query.security.PanelAccessGuard;
import com.vitallink.readings.query.security.RlsScope;
import com.vitallink.readings.query.security.Scopes;

/**
 * Coach actions. {@code If-Match} carries the alert version (428 without it); the change is the existing
 * conditional UPDATE (DefaultAlertService), run inside the coach's row-level-security transaction. 200 returns the
 * alert with its new version as ETag; 409 returns the current alert (ProblemDetailsAdvice).
 */
@RestController
public class AlertActionsController {

    static final String ACK = "ACK_ALERT";
    static final String RESOLVE = "RESOLVE_ALERT";

    private final PanelAccessGuard guard;
    private final RlsScope rls;
    private final AlertService alerts;
    private final RequestIds requestIds;

    public AlertActionsController(PanelAccessGuard guard, RlsScope rls, AlertService alerts, RequestIds requestIds) {
        this.guard = guard;
        this.rls = rls;
        this.alerts = alerts;
        this.requestIds = requestIds;
    }

    public record ResolveRequest(String note) {
    }

    @PostMapping("/v1/alerts/{alertId}/acknowledge")
    public ResponseEntity<Alert> acknowledge(@PathVariable long alertId,
                                             @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch,
                                             @AuthenticationPrincipal Jwt jwt,
                                             @RequestHeader(value = RequestIds.HEADER, required = false)
                                             String requestHeader) {
        Caller caller = Caller.from(jwt);
        UUID requestId = requestIds.resolve(requestHeader);
        guard.requireAlert(caller, Scopes.ALERTS_WRITE, alertId, ACK, requestId);
        int version = version(ifMatch);
        Alert alert = rls.write(caller, () -> alerts.acknowledge(alertId, caller.coachId(), version, requestId));
        return withETag(alert);
    }

    @PostMapping("/v1/alerts/{alertId}/resolve")
    public ResponseEntity<Alert> resolve(@PathVariable long alertId,
                                         @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch,
                                         @RequestBody(required = false) ResolveRequest body,
                                         @AuthenticationPrincipal Jwt jwt,
                                         @RequestHeader(value = RequestIds.HEADER, required = false)
                                         String requestHeader) {
        Caller caller = Caller.from(jwt);
        UUID requestId = requestIds.resolve(requestHeader);
        guard.requireAlert(caller, Scopes.ALERTS_WRITE, alertId, RESOLVE, requestId);
        int version = version(ifMatch);
        String note = body == null ? null : body.note();
        Alert alert = rls.write(caller, () -> alerts.resolve(alertId, caller.coachId(), version, note, requestId));
        return withETag(alert);
    }

    static ResponseEntity<Alert> withETag(Alert alert) {
        return ResponseEntity.ok().eTag(etag(alert)).body(alert);
    }

    static String etag(Alert alert) {
        return "\"" + alert.version() + "\"";
    }

    /** Accepts {@code "3"}, {@code W/"3"} or {@code 3}. */
    static int version(String ifMatch) {
        if (ifMatch == null || ifMatch.isBlank()) {
            throw new PreconditionRequiredException("If-Match with the alert version is required");
        }
        String v = ifMatch.strip();
        if (v.startsWith("W/")) {
            v = v.substring(2);
        }
        if (v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
            v = v.substring(1, v.length() - 1);
        }
        try {
            int version = Integer.parseInt(v);
            if (version < 0) {
                throw new NumberFormatException();
            }
            return version;
        } catch (NumberFormatException e) {
            throw new InvalidRequestException("INVALID_IF_MATCH", "If-Match must be the alert version");
        }
    }
}
