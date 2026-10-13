package com.tradestore.adapter.rest;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.TradeEventType;
import com.tradestore.domain.TradeHistoryEvent;
import java.time.Instant;
import java.util.Map;

public record TradeEventResponse(
        String id,
        String tradeId,
        int version,
        TradeEventType type,
        EventSource source,
        String reason,
        Instant occurredAt,
        Map<String, Object> payload) {

    public static TradeEventResponse from(TradeHistoryEvent event) {
        return new TradeEventResponse(event.id(), event.tradeId(), event.version(), event.type(), event.source(),
                event.reason(), event.occurredAt(), event.payload());
    }
}
