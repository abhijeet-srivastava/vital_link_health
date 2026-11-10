package com.vitallink.readings.query.api;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Page;
import com.vitallink.readings.domain.QueueCursor;
import com.vitallink.readings.ports.AlertQueueRepository;
import com.vitallink.readings.query.config.QueryProperties;
import com.vitallink.readings.query.security.AccessAudit;
import com.vitallink.readings.query.security.Caller;
import com.vitallink.readings.query.security.PanelAccessGuard;
import com.vitallink.readings.query.security.RlsScope;

/** The coach's work queue: unresolved alerts, most severe first, then oldest (api-contracts.md). */
@RestController
public class CoachQueueController {

    static final String READ_QUEUE = "READ_ALERT_QUEUE";

    private final PanelAccessGuard guard;
    private final AccessAudit audit;
    private final RlsScope rls;
    private final AlertQueueRepository queue;
    private final RequestIds requestIds;
    private final QueryProperties props;

    public CoachQueueController(PanelAccessGuard guard, AccessAudit audit, RlsScope rls, AlertQueueRepository queue,
                                RequestIds requestIds, QueryProperties props) {
        this.guard = guard;
        this.audit = audit;
        this.rls = rls;
        this.queue = queue;
        this.requestIds = requestIds;
        this.props = props;
    }

    public record QueueResponse(long coachId, List<Alert> items, String nextCursor) {
    }

    @GetMapping("/v1/coaches/{coachId}/alerts")
    public QueueResponse queue(@PathVariable long coachId, @RequestParam(required = false) AlertStatus status,
                               @RequestParam(required = false) String cursor, @AuthenticationPrincipal Jwt jwt,
                               @RequestHeader(value = RequestIds.HEADER, required = false) String requestHeader) {
        Caller caller = Caller.from(jwt);
        UUID requestId = requestIds.resolve(requestHeader);
        String resource = "coaches/" + coachId + "/alerts";
        guard.requireOwnQueue(caller, coachId, READ_QUEUE, resource, requestId);
        if (status == AlertStatus.RESOLVED) {
            throw new InvalidRequestException("INVALID_STATUS", "The queue holds unresolved alerts only");
        }
        QueueCursor after = CursorCodec.decodeQueue(cursor);

        Page<Alert> page = rls.read(caller,
                () -> queue.queue(coachId, Optional.ofNullable(status), after, props.queuePageSize()));
        audit.allowedForEach(caller, READ_QUEUE,
                new LinkedHashSet<>(page.items().stream().map(Alert::patientId).toList()), resource, requestId);
        String next = page.hasMore() ? CursorCodec.encode(QueueCursor.after(page.last())) : null;
        return new QueueResponse(coachId, page.items(), next);
    }
}
