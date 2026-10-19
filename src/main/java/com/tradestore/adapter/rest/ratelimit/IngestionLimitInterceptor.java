package com.tradestore.adapter.rest.ratelimit;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.exception.RateLimitExceededException;
import com.tradestore.observability.TradeMetrics;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.security.Principal;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Guards {@code POST /api/v1/trades}, which has no Kafka buffer in front of the database. Runs after authentication
 * and before the request body is read, so rejected requests cost almost nothing:
 * <ol>
 *   <li>the client's rate limit is checked ({@code 429} with {@code Retry-After} when exceeded);</li>
 *   <li>the body must declare its length ({@code 411} otherwise) and stay within the configured maximum
 *       ({@code 413} otherwise), so an oversized body is never parsed.</li>
 * </ol>
 * Clients are identified by the authenticated principal, which is the JWT subject.
 */
@Component
@RequiredArgsConstructor
public class IngestionLimitInterceptor implements HandlerInterceptor {

    private final ClientRateLimiter rateLimiter;
    private final TradeStoreProperties properties;
    private final TradeMetrics metrics;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!HttpMethod.POST.matches(request.getMethod())) {
            return true;
        }
        Principal principal = request.getUserPrincipal();
        if (principal == null) {
            throw new IllegalStateException("Ingestion requests must be authenticated before rate limiting");
        }
        String clientId = principal.getName();
        rateLimiter.tryAcquire(clientId).ifPresent(retryAfter -> {
            metrics.rateLimited();
            throw new RateLimitExceededException(clientId, retryAfter);
        });

        long contentLength = request.getContentLengthLong();
        if (contentLength < 0) {
            throw new ResponseStatusException(HttpStatus.LENGTH_REQUIRED, "Content-Length header is required");
        }
        long maxBytes = properties.rest().maxRequestSize().toBytes();
        if (contentLength > maxBytes) {
            throw new ResponseStatusException(HttpStatus.CONTENT_TOO_LARGE,
                    "Request body exceeds %d bytes".formatted(maxBytes));
        }
        return true;
    }
}
