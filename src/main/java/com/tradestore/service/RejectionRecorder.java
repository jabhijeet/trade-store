package com.tradestore.service;

import com.tradestore.domain.TradeCommand;
import com.tradestore.domain.TradeRejectedException;
import com.tradestore.persistence.sql.ProcessedMessageRepository;
import java.time.Clock;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Records a rejected trade in its own transaction, so the rejection survives the rollback of the processing
 * transaction.
 */
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
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
