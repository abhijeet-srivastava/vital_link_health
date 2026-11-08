package com.vitallink.readings.ingest.api;

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
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.vitallink.readings.domain.AllItemsRejectedException;
import com.vitallink.readings.domain.BatchTooLargeException;
import com.vitallink.readings.domain.DeviceMismatchException;
import com.vitallink.readings.domain.MalformedBatchException;
import com.vitallink.readings.domain.UnauthenticatedException;

/**
 * RFC 7807 problem+json for every error, with a stable {@code type} URI and a machine-readable {@code code}.
 * Extends ResponseEntityExceptionHandler so framework errors (404, 405, 415 ...) are problem+json too. Details never
 * echo the request body: it holds PHI.
 */
@RestControllerAdvice
public class ProblemDetailsAdvice extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ProblemDetailsAdvice.class);

    @ExceptionHandler(UnauthenticatedException.class)
    ResponseEntity<Object> unauthenticated(UnauthenticatedException e) {
        return problem(HttpStatus.UNAUTHORIZED, "UNAUTHENTICATED", e.getMessage());
    }

    @ExceptionHandler(DeviceMismatchException.class)
    ResponseEntity<Object> deviceMismatch(DeviceMismatchException e) {
        return problem(HttpStatus.FORBIDDEN, e.reason().name(), e.getMessage());
    }

    @ExceptionHandler(BatchTooLargeException.class)
    ResponseEntity<Object> batchTooLarge(BatchTooLargeException e) {
        return problem(HttpStatus.PAYLOAD_TOO_LARGE, "BATCH_TOO_LARGE", e.getMessage());
    }

    @ExceptionHandler(MalformedBatchException.class)
    ResponseEntity<Object> malformed(MalformedBatchException e) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "MALFORMED_REQUEST", e.getMessage());
    }

    @ExceptionHandler(AllItemsRejectedException.class)
    ResponseEntity<Object> allRejected(AllItemsRejectedException e) {
        String code = e.code() == null ? "READINGS_REJECTED" : e.code().name();
        ResponseEntity<Object> response = problem(HttpStatus.UNPROCESSABLE_ENTITY, code, e.getMessage());
        ((ProblemDetail) response.getBody()).setProperty("results", IngestResponse.of(e.result()).results());
        return response;
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException e,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "MALFORMED_REQUEST", "Request body is not a valid readings batch");
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Object> unexpected(Exception e) {
        // No message or stack trace text: driver messages can contain row values ("Failing row contains ...").
        log.error("Unhandled ingest error type={} at={}", e.getClass().getName(),
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
