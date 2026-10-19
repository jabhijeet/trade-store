package com.tradestore.domain;

import com.tradestore.enums.EventSource;
import com.tradestore.enums.TradeEventType;
import java.time.Instant;
import java.util.Map;

/**
 * One entry in the history of a trade.
 */
public record TradeHistoryEvent(
        String id,
        String tradeId,
        int version,
        TradeEventType type,
        EventSource source,
        String reason,
        Instant occurredAt,
        Map<String, Object> payload) {
}
