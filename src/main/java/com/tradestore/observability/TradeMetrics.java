package com.tradestore.observability;

import com.tradestore.enums.EventSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Application counters, published through Actuator ({@code /actuator/metrics/<name>}).
 * <ul>
 *   <li>{@value #TRADES_PROCESSED} – every processed message, tagged by {@code source} and {@code outcome}
 *       ({@code CREATED}, {@code UPDATED}, {@code REPLACED}, {@code REJECTED_LOWER_VERSION},
 *       {@code REJECTED_MATURITY_IN_PAST} or {@code DUPLICATE}); the sum per source is the number received;</li>
 *   <li>{@value #TRADES_EXPIRED} – trades marked expired by the expiry scheduler;</li>
 *   <li>{@value #RATE_LIMITED} – REST ingestion requests rejected with {@code 429};</li>
 *   <li>{@value #PROCESSED_MESSAGES_PURGED} – old idempotency records deleted by the purge job.</li>
 * </ul>
 * The outbox backlog is a gauge, see {@link OutboxBacklogMetrics}.
 */
@Component
@RequiredArgsConstructor
public class TradeMetrics {

    public static final String TRADES_PROCESSED = "tradestore.trades.processed";
    public static final String TRADES_EXPIRED = "tradestore.trades.expired";
    public static final String RATE_LIMITED = "tradestore.rest.rate.limited";
    public static final String PROCESSED_MESSAGES_PURGED = "tradestore.processed.messages.purged";

    /** Outcome tag for a redelivered message that was answered from its recorded outcome. */
    public static final String DUPLICATE_OUTCOME = "DUPLICATE";

    private final MeterRegistry registry;

    public void tradeProcessed(EventSource source, String outcome) {
        Counter.builder(TRADES_PROCESSED)
                .description("Processed trade messages by source and outcome")
                .tag("source", source.name())
                .tag("outcome", outcome)
                .register(registry)
                .increment();
    }

    public void tradesExpired(int count) {
        registry.counter(TRADES_EXPIRED).increment(count);
    }

    public void rateLimited() {
        registry.counter(RATE_LIMITED).increment();
    }

    public void processedMessagesPurged(int count) {
        registry.counter(PROCESSED_MESSAGES_PURGED).increment(count);
    }
}
