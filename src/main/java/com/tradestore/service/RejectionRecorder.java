package com.tradestore.service;

import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.TradeCommand;
import com.tradestore.domain.TradeEventType;
import com.tradestore.domain.TradeRejectedException;
import com.tradestore.persistence.sql.ProcessedMessageEntity;
import com.tradestore.persistence.sql.ProcessedMessageRepository;
import java.time.Clock;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Records a rejected trade in its own transaction, so the rejection survives the rollback of the processing
 * transaction.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RejectionRecorder {

    private final ProcessedMessageRepository processedMessageRepository;
    private final OutboxWriter outboxWriter;
    private final Clock clock;

    /**
     * Stores the message id with the rejection outcome and appends a {@code REJECTED} event with the reason.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(TradeCommand command, TradeRejectedException rejection) {
        IncomingTrade trade = command.trade();
        processedMessageRepository.save(new ProcessedMessageEntity(
                command.messageId(), trade.tradeId(), rejection.outcome(), Instant.now(clock)));
        outboxWriter.append(trade.tradeId(), trade.version(), TradeEventType.REJECTED, command.source(),
                rejection.getMessage(), trade);
        log.warn("Trade rejected: tradeId={}, version={}, source={}, messageId={}, reason={}",
                trade.tradeId(), trade.version(), command.source(), command.messageId(), rejection.getMessage());
    }
}
