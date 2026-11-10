package com.vitallink.readings.query.api;

import java.time.Duration;
import java.time.Instant;
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

import com.vitallink.readings.domain.Cursor;
import com.vitallink.readings.domain.Page;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;
import com.vitallink.readings.ports.LatestReadingCache;
import com.vitallink.readings.ports.ReadingQueryRepository;
import com.vitallink.readings.query.config.QueryProperties;
import com.vitallink.readings.query.security.AccessAudit;
import com.vitallink.readings.query.security.Caller;
import com.vitallink.readings.query.security.PanelAccessGuard;
import com.vitallink.readings.query.security.RlsScope;
import com.vitallink.readings.query.security.Scopes;

/**
 * Coach reads of a patient's readings. Order per request: panel check (403 + DENIED audit), read (Redis or the
 * replica under row-level security), ALLOWED audit, response.
 */
@RestController
public class ReadingsQueryController {

    static final String READ_LATEST = "READ_LATEST_READING";
    static final String READ_RANGE = "READ_READINGS";

    private final PanelAccessGuard guard;
    private final AccessAudit audit;
    private final RlsScope rls;
    private final ReadingQueryRepository readings;
    private final LatestReadingCache latest;
    private final RequestIds requestIds;
    private final QueryProperties props;

    public ReadingsQueryController(PanelAccessGuard guard, AccessAudit audit, RlsScope rls,
                                   ReadingQueryRepository readings, LatestReadingCache latest, RequestIds requestIds,
                                   QueryProperties props) {
        this.guard = guard;
        this.audit = audit;
        this.rls = rls;
        this.readings = readings;
        this.latest = latest;
        this.requestIds = requestIds;
        this.props = props;
    }

    public record LatestResponse(long patientId, ReadingType type, String source, ReadingView reading) {
    }

    public record RangeResponse(long patientId, ReadingType type, List<ReadingView> items, String nextCursor) {
    }

    /**
     * Redis first; on a miss (or Redis down: the cache returns empty within its timeout or an open breaker) the
     * replica, then a backfill through the same compare-and-set ingest uses, so a backfill can never replace a newer
     * reading ingest cached meanwhile. The Redis path has no row-level security behind it: the panel check above is
     * its only gate.
     */
    @GetMapping("/v1/patients/{patientId}/readings/latest")
    public LatestResponse latest(@PathVariable long patientId, @RequestParam ReadingType type,
                                 @AuthenticationPrincipal Jwt jwt,
                                 @RequestHeader(value = RequestIds.HEADER, required = false) String requestHeader) {
        Caller caller = Caller.from(jwt);
        UUID requestId = requestIds.resolve(requestHeader);
        String resource = "readings/latest/" + type;
        guard.requirePatient(caller, Scopes.READINGS_READ, patientId, READ_LATEST, resource, requestId);

        String source = "cache";
        Optional<Reading> reading = latest.get(patientId, type);
        if (reading.isEmpty()) {
            source = "db";
            reading = rls.read(caller, () -> readings.findLatest(patientId, type));
            reading.ifPresent(r -> latest.putIfNewer(patientId, type, r));
        }
        audit.allowed(caller, READ_LATEST, patientId, resource, requestId);
        Reading r = reading.orElseThrow(() -> new NotFoundException("NO_READING", "No reading of this type yet"));
        return new LatestResponse(patientId, type, source, ReadingView.of(r));
    }

    @GetMapping("/v1/patients/{patientId}/readings")
    public RangeResponse range(@PathVariable long patientId, @RequestParam ReadingType type,
                               @RequestParam Instant from, @RequestParam Instant to,
                               @RequestParam(required = false) String cursor,
                               @RequestParam(required = false) Integer limit,
                               @AuthenticationPrincipal Jwt jwt,
                               @RequestHeader(value = RequestIds.HEADER, required = false) String requestHeader) {
        Caller caller = Caller.from(jwt);
        UUID requestId = requestIds.resolve(requestHeader);
        String resource = "readings/" + type;
        guard.requirePatient(caller, Scopes.READINGS_READ, patientId, READ_RANGE, resource, requestId);

        if (!to.isAfter(from)) {
            throw new InvalidRequestException("INVALID_RANGE", "to must be after from");
        }
        if (Duration.between(from, to).compareTo(props.maxRange()) > 0) {
            throw new InvalidRequestException("RANGE_TOO_WIDE", "from..to spans more than " + props.maxRange());
        }
        int pageSize = limit == null ? props.defaultPageSize() : limit;
        if (pageSize < 1 || pageSize > props.maxPageSize()) {
            throw new InvalidRequestException("INVALID_LIMIT", "limit must be 1.." + props.maxPageSize());
        }
        Cursor after = CursorCodec.decodeReading(cursor);

        Page<Reading> page = rls.read(caller, () -> readings.findRange(patientId, type, from, to, after, pageSize));
        audit.allowed(caller, READ_RANGE, patientId, resource, requestId);
        String next = page.hasMore() ? CursorCodec.encode(Cursor.after(page.last())) : null;
        return new RangeResponse(patientId, type, page.items().stream().map(ReadingView::of).toList(), next);
    }
}
