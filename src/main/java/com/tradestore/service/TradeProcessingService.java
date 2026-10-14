package com.tradestore.service;

import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.MaturityDateInPastException;
import com.tradestore.domain.MessageOutcome;
import com.tradestore.domain.SaveOutcome;
import com.tradestore.domain.Trade;
import com.tradestore.domain.TradeCommand;
import com.tradestore.domain.TradeEventType;
import com.tradestore.domain.TradeRejectedException;
import com.tradestore.persistence.sql.ProcessedMessageEntity;
import com.tradestore.persistence.sql.ProcessedMessageRepository;
import com.tradestore.persistence.sql.TradeEntity;
import com.tradestore.persistence.sql.TradeJpaRepository;
import com.tradestore.validation.TradeValidationRule;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The only writer of trade state. Used by both ingestion paths (Kafka and REST).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TradeProcessingService {

    /** PostgreSQL {@code unique_violation}. */
    private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";

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
     *   <li>every validation rule runs; a failing rule's rejection is recorded and rethrown, and the transaction
     *       commits the rejection record (nothing else has been written at that point);</li>
     *   <li>the trade is inserted or updated, and its event is appended to the outbox;</li>
     *   <li>the message id is recorded with the outcome.</li>
     * </ol>
     *
     * @throws LowerVersionTradeException  if the version is lower than the stored version
     * @throws MaturityDateInPastException if the maturity date is earlier than today
     */
    @Transactional(noRollbackFor = TradeRejectedException.class)
    public SaveOutcome process(TradeCommand command) {
        Optional<ProcessedMessageEntity> processed = processedMessageRepository.findById(command.messageId());
        if (processed.isPresent()) {
            log.info("Duplicate message ignored: messageId={}, outcome={}",
                    command.messageId(), processed.get().getOutcome());
            return replayOutcome(command, processed.get().getOutcome());
        }

        IncomingTrade incoming = command.trade();
        LocalDate today = LocalDate.now(clock);
        Optional<TradeEntity> current = tradeRepository.findByIdForUpdate(incoming.tradeId());
        validate(command, current.map(TradeEntity::toDomain), today);

        SaveOutcome outcome = current.map(stored -> stored.getVersion() == incoming.version()
                        ? SaveOutcome.REPLACED
                        : SaveOutcome.UPDATED)
                .orElse(SaveOutcome.CREATED);
        TradeEntity trade = apply(incoming, current.orElseGet(TradeEntity::new), today);
        save(trade, current.isEmpty());

        outboxWriter.append(incoming.tradeId(), incoming.version(), eventType(outcome), command.source(), null,
                trade.toDomain());
        processedMessageRepository.save(new ProcessedMessageEntity(
                command.messageId(), incoming.tradeId(), messageOutcome(outcome), Instant.now(clock)));
        log.info("Trade stored: tradeId={}, version={}, outcome={}, source={}, messageId={}",
                incoming.tradeId(), incoming.version(), outcome, command.source(), command.messageId());
        return outcome;
    }

    /**
     * Writes the trade immediately, so a concurrent insert of the same new trade id surfaces here. There is no row to
     * lock for a new trade id, so two first versions arriving at once (for example one per ingestion path) can both
     * pass validation; the second insert then fails on the primary key. That is reported as a transient
     * {@link ConcurrencyFailureException}: the Kafka path retries the message, the REST path answers {@code 503}, and
     * on retry the version check runs against the stored row. Other integrity violations (for example a value too
     * long for its column) are permanent and rethrown unchanged.
     */
    private void save(TradeEntity trade, boolean isNewTrade) {
        try {
            tradeRepository.saveAndFlush(trade);
        } catch (DataIntegrityViolationException exception) {
            if (isNewTrade && isUniqueViolation(exception)) {
                throw new ConcurrencyFailureException(
                        "Trade %s was inserted concurrently; retry".formatted(trade.getTradeId()), exception);
            }
            throw exception;
        }
    }

    private static boolean isUniqueViolation(Throwable exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql && UNIQUE_VIOLATION_SQL_STATE.equals(sql.getSQLState())) {
                return true;
            }
        }
        return false;
    }

    private void validate(TradeCommand command, Optional<Trade> current, LocalDate today) {
        try {
            rules.forEach(rule -> rule.validate(command.trade(), current, today));
        } catch (TradeRejectedException rejection) {
            rejectionRecorder.record(command, rejection);
            throw rejection;
        }
    }

    /**
     * Copies the incoming trade onto the entity. Each accepted version is received today, and a newly accepted
     * version is never expired because its maturity date has just passed the maturity rule.
     */
    private TradeEntity apply(IncomingTrade incoming, TradeEntity entity, LocalDate today) {
        entity.setTradeId(incoming.tradeId());
        entity.setVersion(incoming.version());
        entity.setCounterPartyId(incoming.counterPartyId());
        entity.setBookId(incoming.bookId());
        entity.setMaturityDate(incoming.maturityDate());
        entity.setCreatedDate(today);
        entity.setExpired(false);
        entity.setUpdatedAt(Instant.now(clock));
        return entity;
    }

    /**
     * Answers a redelivered message from its recorded outcome, without writing anything. A rejection is rethrown
     * so callers get the same result as the first time.
     */
    private SaveOutcome replayOutcome(TradeCommand command, MessageOutcome recorded) {
        IncomingTrade incoming = command.trade();
        return switch (recorded) {
            case CREATED -> SaveOutcome.CREATED;
            case UPDATED -> SaveOutcome.UPDATED;
            case REPLACED -> SaveOutcome.REPLACED;
            case REJECTED_LOWER_VERSION -> throw new LowerVersionTradeException(incoming.tradeId(),
                    incoming.version(), tradeRepository.findById(incoming.tradeId())
                            .map(TradeEntity::getVersion)
                            .orElse(incoming.version()));
            case REJECTED_MATURITY_IN_PAST -> throw new MaturityDateInPastException(incoming.tradeId(),
                    incoming.version(), incoming.maturityDate(), LocalDate.now(clock));
        };
    }

    private static TradeEventType eventType(SaveOutcome outcome) {
        return switch (outcome) {
            case CREATED -> TradeEventType.CREATED;
            case UPDATED -> TradeEventType.UPDATED;
            case REPLACED -> TradeEventType.REPLACED;
        };
    }

    private static MessageOutcome messageOutcome(SaveOutcome outcome) {
        return switch (outcome) {
            case CREATED -> MessageOutcome.CREATED;
            case UPDATED -> MessageOutcome.UPDATED;
            case REPLACED -> MessageOutcome.REPLACED;
        };
    }
}
