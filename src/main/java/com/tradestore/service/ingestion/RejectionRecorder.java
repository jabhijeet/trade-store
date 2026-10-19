package com.tradestore.service.ingestion;

import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.TradeCommand;
import com.tradestore.enums.TradeEventType;
import com.tradestore.exception.TradeRejectedException;
import com.tradestore.persistence.sql.entity.ProcessedMessageEntity;
import com.tradestore.persistence.sql.repository.ProcessedMessageRepository;
import com.tradestore.service.outbox.OutboxWriter;
import java.time.Clock;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Records a rejected trade in the processing transaction. Rejections are thrown before anything else is written,
 * and {@link TradeProcessingService#process} commits instead of rolling back on a {@link TradeRejectedException},
 * so the record is stored without a second transaction or a second pool connection.
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
    @Transactional(propagation = Propagation.MANDATORY)
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
