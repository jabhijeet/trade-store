package com.tradestore.adapter.rest;

import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.MaturityDateInPastException;
import com.tradestore.domain.TradeRejectedException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Maps exceptions to RFC 9457 problem responses. Malformed requests are handled by the base class as {@code 400}.
 */
@Slf4j
@RestControllerAdvice
public class RestExceptionHandler extends ResponseEntityExceptionHandler {

    static final String RETRY_AFTER_SECONDS = "5";

    /** {@code 409 Conflict}. */
    @ExceptionHandler(LowerVersionTradeException.class)
    public ProblemDetail handleLowerVersion(LowerVersionTradeException exception) {
        return rejection(HttpStatus.CONFLICT, "Lower trade version", exception);
    }

    /** {@code 422 Unprocessable Entity}. */
    @ExceptionHandler(MaturityDateInPastException.class)
    public ProblemDetail handleMaturityDateInPast(MaturityDateInPastException exception) {
        return rejection(HttpStatus.UNPROCESSABLE_CONTENT, "Maturity date in the past", exception);
    }

    /** {@code 404 Not Found}. */
    @ExceptionHandler(TradeNotFoundException.class)
    public ProblemDetail handleTradeNotFound(TradeNotFoundException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, exception.getMessage());
        problem.setTitle("Trade not found");
        return problem;
    }

    /** {@code 429 Too Many Requests}: the client should wait {@code Retry-After} seconds. */
    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ProblemDetail> handleRateLimitExceeded(RateLimitExceededException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS,
                "Too many requests. Retry after %d seconds.".formatted(exception.getRetryAfterSeconds()));
        problem.setTitle("Rate limit exceeded");
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(exception.getRetryAfterSeconds()))
                .body(problem);
    }

    /** {@code 503 Service Unavailable}: the database is temporarily unavailable; the caller should retry. */
    @ExceptionHandler(TransientDataAccessException.class)
    public ResponseEntity<ProblemDetail> handleTransientDataAccess(TransientDataAccessException exception) {
        log.warn("Transient database failure, returning 503: {}", exception.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "The trade store is temporarily unavailable. Retry later.");
        problem.setTitle("Service unavailable");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(problem);
    }

    private static ProblemDetail rejection(HttpStatus status, String title, TradeRejectedException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, exception.getMessage());
        problem.setTitle(title);
        problem.setProperty("tradeId", exception.getTradeId());
        problem.setProperty("version", exception.getVersion());
        return problem;
    }
}
