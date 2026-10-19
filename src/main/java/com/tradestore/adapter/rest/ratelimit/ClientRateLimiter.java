package com.tradestore.adapter.rest.ratelimit;

import com.tradestore.config.TradeStoreProperties;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.ConsumptionProbe;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * One Bucket4j token bucket per client, so a noisy producer cannot starve the others. Each bucket holds
 * {@code requestsPerSecond} tokens and refills them greedily over one second, which allows a burst of that size and
 * the same sustained rate.
 * <p>
 * Buckets are held in memory, so the limit applies per instance. Clients are authenticated producers, a small and
 * known set, so the map does not need eviction.
 */
@Component
public class ClientRateLimiter {

    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();
    private final long requestsPerSecond;

    public ClientRateLimiter(TradeStoreProperties properties) {
        this.requestsPerSecond = properties.rest().requestsPerSecond();
    }

    /**
     * Takes one token from the client's bucket.
     *
     * @return empty if the request is admitted; otherwise how long the client should wait before retrying
     */
    public Optional<Duration> tryAcquire(String clientId) {
        ConsumptionProbe probe = buckets.computeIfAbsent(clientId, id -> newBucket())
                .tryConsumeAndReturnRemaining(1);
        return probe.isConsumed()
                ? Optional.empty()
                : Optional.of(Duration.ofNanos(probe.getNanosToWaitForRefill()));
    }

    private Bucket newBucket() {
        return Bucket.builder()
                .addLimit(limit -> limit.capacity(requestsPerSecond)
                        .refillGreedy(requestsPerSecond, Duration.ofSeconds(1)))
                .build();
    }
}
