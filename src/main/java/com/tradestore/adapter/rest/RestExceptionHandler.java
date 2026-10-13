package com.tradestore.adapter.rest;

import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.MaturityDateInPastException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Maps exceptions to RFC 9457 problem responses. Malformed requests are handled by the base class as {@code 400}.
 */
@RestControllerAdvice
public class RestExceptionHandler extends ResponseEntityExceptionHandler {

    /** {@code 409 Conflict}. */
    @ExceptionHandler(LowerVersionTradeException.class)
    public ProblemDetail handleLowerVersion(LowerVersionTradeException exception) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@code 422 Unprocessable Entity}. */
    @ExceptionHandler(MaturityDateInPastException.class)
    public ProblemDetail handleMaturityDateInPast(MaturityDateInPastException exception) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@code 404 Not Found}. */
    @ExceptionHandler(TradeNotFoundException.class)
    public ProblemDetail handleTradeNotFound(TradeNotFoundException exception) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@code 503 Service Unavailable}: the database is temporarily unavailable; the caller should retry. */
    @ExceptionHandler(TransientDataAccessException.class)
    public ProblemDetail handleTransientDataAccess(TransientDataAccessException exception) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
