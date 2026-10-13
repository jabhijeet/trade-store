package com.tradestore.service;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.TradeEventType;
import com.tradestore.persistence.sql.OutboxEventRepository;
import java.time.Clock;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Appends trade events to the outbox. Must be called inside the transaction that makes the state change, so the
 * change and its event commit together.
 */
@Component
@RequiredArgsConstructor
public class OutboxWriter {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    /**
     * @param payload the trade the event describes, serialized to JSON
     * @param reason  rejection reason, or {@code null} for other event types
     */
    public void append(String tradeId, int version, TradeEventType type, EventSource source, String reason,
                       Object payload) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
