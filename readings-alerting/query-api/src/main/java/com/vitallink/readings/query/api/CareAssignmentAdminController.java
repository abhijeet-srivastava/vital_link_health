package com.vitallink.readings.query.api;

import java.util.UUID;

import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.vitallink.readings.alerts.ReassignmentService;
import com.vitallink.readings.query.security.AccessAudit;
import com.vitallink.readings.query.security.Caller;
import com.vitallink.readings.query.security.PanelAccessGuard;
import com.vitallink.readings.query.security.Scopes;

/**
 * Internal: applies a panel change from the care-team system (the projection feed is out of scope, PLAN.md). Makes
 * coach_id the patient's PRIMARY and moves the unresolved alerts (ReassignmentService). Runs on the admin pool
 * (vitallink_service), because moving alerts between coaches is not one coach's action and the coach policy must
 * not have a bypass.
 */
@RestController
public class CareAssignmentAdminController {

    static final String ASSIGN = "ASSIGN_PRIMARY_COACH";

    private final PanelAccessGuard guard;
    private final AccessAudit audit;
    private final ReassignmentService reassignment;
    private final RequestIds requestIds;

    public CareAssignmentAdminController(PanelAccessGuard guard, AccessAudit audit, ReassignmentService reassignment,
                                         RequestIds requestIds) {
        this.guard = guard;
        this.audit = audit;
        this.reassignment = reassignment;
        this.requestIds = requestIds;
    }

    public record AssignmentRequest(Long patientId, Long coachId) {
    }

    public record AssignmentResponse(long patientId, long coachId, int movedAlerts) {
    }

    @PostMapping("/internal/v1/care-assignments")
    public AssignmentResponse assign(@RequestBody AssignmentRequest body, @AuthenticationPrincipal Jwt jwt,
                                     @RequestHeader(value = RequestIds.HEADER, required = false)
                                     String requestHeader) {
        Caller caller = Caller.from(jwt);
        UUID requestId = requestIds.resolve(requestHeader);
        Long patientId = body == null ? null : body.patientId();
        guard.requireScope(caller, Scopes.ASSIGNMENTS_WRITE, patientId, ASSIGN, "care-assignments", requestId);
        if (patientId == null || body.coachId() == null || patientId <= 0 || body.coachId() <= 0) {
            throw new InvalidRequestException("INVALID_ASSIGNMENT", "patient_id and coach_id are required");
        }
        int moved = reassignment.reassign(patientId, body.coachId(), requestId);
        audit.allowed(caller, ASSIGN, patientId, "care-assignments/coach/" + body.coachId(), requestId);
        return new AssignmentResponse(patientId, body.coachId(), moved);
    }
}
