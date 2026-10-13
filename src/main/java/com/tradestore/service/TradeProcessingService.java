package com.tradestore.service;

import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.MaturityDateInPastException;
import com.tradestore.domain.SaveOutcome;
import com.tradestore.domain.TradeCommand;
import com.tradestore.persistence.sql.ProcessedMessageRepository;
import com.tradestore.persistence.sql.TradeJpaRepository;
import com.tradestore.validation.TradeValidationRule;
import java.time.Clock;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The only writer of trade state. Used by both ingestion paths (Kafka and REST).
 */
@Service
@RequiredArgsConstructor
public class TradeProcessingService {

    private final TradeJpaRepository tradeRepository;
    private final ProcessedMessageRepository processedMessageRepository;
    private final List<TradeValidationRule> rules;
    private final OutboxWriter outboxWriter;
    private final RejectionRecorder rejectionRecorder;
    private final Clock clock;

    /**
     * Processes one trade in a single transaction:
     * <ol>
     *   <li>a message id that was already processed returns its recorded outcome (or rethrows its rejection);</li>
     *   <li>the stored trade is loaded with a row lock;</li>
     *   <li>every validation rule runs; a failing rule's rejection is recorded and rethrown;</li>
     *   <li>the trade is inserted or updated, and its event is appended to the outbox;</li>
     *   <li>the message id is recorded with the outcome.</li>
     * </ol>
     *
     * @throws LowerVersionTradeException  if the version is lower than the stored version
     * @throws MaturityDateInPastException if the maturity date is earlier than today
     */
    @Transactional
    public SaveOutcome process(TradeCommand command) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
