package com.tradestore.exception;

import java.time.Duration;
import lombok.Getter;

/**
 * The client has used up its request budget; answered with {@code 429 Too Many Requests} and {@code Retry-After}.
 */
@Getter
public class RateLimitExceededException extends RuntimeException {

    /** Whole seconds until the client may retry, at least {@code 1}. */
    private final long retryAfterSeconds;

    public RateLimitExceededException(String clientId, Duration retryAfter) {
        super("Rate limit exceeded for client " + clientId);
        long seconds = retryAfter.toSeconds() + (retryAfter.toNanosPart() > 0 ? 1 : 0);
        this.retryAfterSeconds = Math.max(1, seconds);
    }
}
