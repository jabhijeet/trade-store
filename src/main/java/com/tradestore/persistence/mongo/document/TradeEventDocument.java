package com.tradestore.persistence.mongo.document;

import com.tradestore.enums.EventSource;
import com.tradestore.enums.TradeEventType;
import java.time.Instant;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Immutable trade history entry. The id is the outbox event id, so redelivery by the relay is an idempotent upsert.
 * The collection and its indexes are created by the Liquibase MongoDB changelog.
 */
@Document(collection = "trade_events")
public record TradeEventDocument(
        @Id String id,
        String tradeId,
        int version,
        TradeEventType type,
        EventSource source,
        String reason,
        Instant occurredAt,
        Map<String, Object> payload) {
}
