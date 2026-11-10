package com.vitallink.readings.query.api;

import java.net.URI;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.vitallink.readings.domain.AlertConflict;
import com.vitallink.readings.domain.AlertNotFoundException;
import com.vitallink.readings.domain.ResolutionNoteRequiredException;
import com.vitallink.readings.query.security.AccessDeniedProblem;

/**
 * RFC 7807 problem+json with a stable {@code type} URI and a machine-readable {@code code}, same shape as ingest-api.
 * Details never echo request bodies or parameter values. 409 carries the current alert so the client can redisplay
 * it without another round trip (api-contracts.md).
 */
@RestControllerAdvice
public class ProblemDetailsAdvice extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ProblemDetailsAdvice.class);

    @ExceptionHandler(AccessDeniedProblem.class)
    ResponseEntity<Object> denied(AccessDeniedProblem e) {
        return problem(HttpStatus.FORBIDDEN, "ACCESS_DENIED", e.getMessage());
    }

    @ExceptionHandler(InvalidRequestException.class)
    ResponseEntity<Object> invalid(InvalidRequestException e) {
        return problem(HttpStatus.BAD_REQUEST, e.code(), e.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    ResponseEntity<Object> notFound(NotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, e.code(), e.getMessage());
    }

    @ExceptionHandler(AlertNotFoundException.class)
    ResponseEntity<Object> alertNotFound(AlertNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "ALERT_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(AlertConflict.class)
    ResponseEntity<Object> conflict(AlertConflict e) {
        ResponseEntity<Object> response = problem(HttpStatus.CONFLICT, "ALERT_CONFLICT",
                "The alert changed; the current alert is attached");
        ((ProblemDetail) response.getBody()).setProperty("current", e.current());
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .headers(response.getHeaders())
                .eTag(AlertActionsController.etag(e.current()))
                .body(response.getBody());
    }

    @ExceptionHandler(ResolutionNoteRequiredException.class)
    ResponseEntity<Object> noteRequired(ResolutionNoteRequiredException e) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "RESOLUTION_NOTE_REQUIRED", e.getMessage());
    }

    @ExceptionHandler(PreconditionRequiredException.class)
    ResponseEntity<Object> preconditionRequired(PreconditionRequiredException e) {
        return problem(HttpStatus.PRECONDITION_REQUIRED, "IF_MATCH_REQUIRED", e.getMessage());
    }

    @ExceptionHandler(ThresholdController.ThresholdConflictException.class)
    ResponseEntity<Object> thresholdConflict(ThresholdController.ThresholdConflictException e) {
        return problem(HttpStatus.CONFLICT, "THRESHOLD_CONFLICT", e.getMessage());
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<Object> typeMismatch(MethodArgumentTypeMismatchException e) {
        return problem(HttpStatus.BAD_REQUEST, "INVALID_PARAMETER", "Invalid value for " + e.getName());
    }

    @Override
    protected ResponseEntity<Object> handleMissingServletRequestParameter(MissingServletRequestParameterException e,
                                                                          HttpHeaders headers, HttpStatusCode status,
                                                                          WebRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "MISSING_PARAMETER", "Missing parameter " + e.getParameterName());
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException e,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST", "Request body is not valid");
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Object> unexpected(Exception e) {
        // No message or stack trace text: driver messages can contain row values ("Failing row contains ...").
        log.error("Unhandled query error type={} at={}", e.getClass().getName(),
                e.getStackTrace().length > 0 ? e.getStackTrace()[0] : "?");
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Unexpected error");
    }

    static ResponseEntity<Object> problem(HttpStatus status, String code, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create("/problems/" + code.toLowerCase(Locale.ROOT).replace('_', '-')));
        problem.setTitle(status.getReasonPhrase());
        problem.setProperty("code", code);
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(problem);
    }
}
