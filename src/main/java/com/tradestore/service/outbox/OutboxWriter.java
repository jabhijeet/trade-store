package com.tradestore.service.outbox;

import com.tradestore.enums.EventSource;
import com.tradestore.enums.TradeEventType;
import com.tradestore.persistence.sql.entity.OutboxEventEntity;
import com.tradestore.persistence.sql.repository.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
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
        var event = new OutboxEventEntity(UUID.randomUUID(), tradeId, version, type, source, reason,
                jsonMapper.writeValueAsString(payload), Instant.now(clock), null, null);
        outboxEventRepository.save(event);
    }
}
