package com.tradestore.persistence.mongo;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.TradeEventType;
import java.time.Instant;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Immutable trade history entry. The id is the outbox event id, so redelivery by the relay is an idempotent upsert.
 */
@Document(collection = "trade_events")
public record TradeEventDocument(
        @Id String id,
        @Indexed String tradeId,
        int version,
        TradeEventType type,
        EventSource source,
        String reason,
        @Indexed Instant occurredAt,
        Map<String, Object> payload) {
}
