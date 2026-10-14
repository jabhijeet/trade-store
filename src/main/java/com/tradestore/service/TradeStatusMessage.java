package com.tradestore.service;

import com.tradestore.domain.TradeEventType;
import java.time.Instant;

/**
 * JSON contract of a message on the {@code trade-status} topic.
 */
public record TradeStatusMessage(String eventId, String tradeId, int version, TradeEventType type,
                                 Instant occurredAt) {
}
