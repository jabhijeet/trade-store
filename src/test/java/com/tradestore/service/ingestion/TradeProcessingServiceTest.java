package com.tradestore.service.ingestion;

import static com.tradestore.TradeFixtures.FIXED_CLOCK;
import static com.tradestore.TradeFixtures.TODAY;
import static com.tradestore.TradeFixtures.incoming;
import static com.tradestore.TradeFixtures.storedEntity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tradestore.enums.EventSource;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.enums.MessageOutcome;
import com.tradestore.enums.SaveOutcome;
import com.tradestore.domain.TradeCommand;
import com.tradestore.enums.TradeEventType;
import com.tradestore.exception.LowerVersionTradeException;
import com.tradestore.exception.MaturityDateInPastException;
import com.tradestore.observability.TradeMetrics;
import com.tradestore.persistence.sql.entity.OutboxEventEntity;
import com.tradestore.persistence.sql.repository.OutboxEventRepository;
import com.tradestore.persistence.sql.entity.ProcessedMessageEntity;
import com.tradestore.persistence.sql.repository.ProcessedMessageRepository;
import com.tradestore.persistence.sql.entity.TradeEntity;
import com.tradestore.persistence.sql.repository.TradeJpaRepository;
import com.tradestore.service.outbox.OutboxWriter;
import com.tradestore.validation.MaturityDateRule;
import com.tradestore.validation.VersionRule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class TradeProcessingServiceTest {

    private static final String MESSAGE_ID = "kafka:trades:0:42";

    @Mock
    private TradeJpaRepository tradeRepository;
    @Mock
    private ProcessedMessageRepository processedMessageRepository;
    @Mock
    private OutboxEventRepository outboxEventRepository;

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private TradeProcessingService service;

    @BeforeEach
    void setUp() {
        var outboxWriter = new OutboxWriter(outboxEventRepository, JsonMapper.builder().build(), FIXED_CLOCK);
        var rejectionRecorder = new RejectionRecorder(processedMessageRepository, outboxWriter, FIXED_CLOCK);
        service = new TradeProcessingService(tradeRepository, processedMessageRepository,
                List.of(new MaturityDateRule(), new VersionRule()), outboxWriter, rejectionRecorder,
                new TradeMetrics(meterRegistry), FIXED_CLOCK);
    }

    @Test
    void process_WhenNewTradeIsInsertedConcurrently_ThrowsTransientConcurrencyFailure() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T1")).thenReturn(Optional.empty());
        when(tradeRepository.saveAndFlush(any(TradeEntity.class))).thenThrow(integrityViolation("23505"));

        assertThatThrownBy(() -> service.process(command(incoming("T1", 1))))
                .isInstanceOf(ConcurrencyFailureException.class)
                .hasMessageContaining("T1");
        verify(outboxEventRepository, never()).save(any());
        verify(processedMessageRepository, never()).save(any());
    }

    @Test
    void process_WhenNewTradeHasValueTooLongForColumn_RethrowsOriginalException() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T1")).thenReturn(Optional.empty());
        when(tradeRepository.saveAndFlush(any(TradeEntity.class))).thenThrow(integrityViolation("22001"));

        assertThatThrownBy(() -> service.process(command(incoming("T1", 1))))
                .isExactlyInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void process_WhenUpdatingExistingTradeFailsIntegrityCheck_RethrowsOriginalException() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T2")).thenReturn(Optional.of(storedEntity("T2", 1, false)));
        when(tradeRepository.saveAndFlush(any(TradeEntity.class))).thenThrow(integrityViolation("23505"));

        assertThatThrownBy(() -> service.process(command(incoming("T2", 2))))
                .isExactlyInstanceOf(DataIntegrityViolationException.class);
    }

    private static DataIntegrityViolationException integrityViolation(String sqlState) {
        return new DataIntegrityViolationException("could not execute statement",
                new SQLException("constraint or data error", sqlState));
    }

    @Test
    void process_WhenTradeIsNew_StoresTradeAndReturnsCreated() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T1")).thenReturn(Optional.empty());

        SaveOutcome outcome = service.process(command(incoming("T1", 1)));

        assertThat(outcome).isEqualTo(SaveOutcome.CREATED);
        TradeEntity saved = capturedTrade();
        assertThat(saved.getTradeId()).isEqualTo("T1");
        assertThat(saved.getVersion()).isEqualTo(1);
        assertThat(saved.getCreatedDate()).isEqualTo(TODAY);
        assertThat(saved.isExpired()).isFalse();
        assertThat(capturedOutboxEvent().getType()).isEqualTo(TradeEventType.CREATED);
        assertThat(capturedProcessedMessage().getOutcome()).isEqualTo(MessageOutcome.CREATED);
        assertThat(processedCount("CREATED")).isEqualTo(1);
    }

    @Test
    void process_WhenVersionHigherThanStored_UpdatesTradeAndReturnsUpdated() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T2")).thenReturn(Optional.of(storedEntity("T2", 1, false)));

        SaveOutcome outcome = service.process(command(incoming("T2", 2)));

        assertThat(outcome).isEqualTo(SaveOutcome.UPDATED);
        TradeEntity saved = capturedTrade();
        assertThat(saved.getVersion()).isEqualTo(2);
        assertThat(saved.getCreatedDate()).isEqualTo(TODAY);
        assertThat(capturedOutboxEvent().getType()).isEqualTo(TradeEventType.UPDATED);
        assertThat(capturedProcessedMessage().getOutcome()).isEqualTo(MessageOutcome.UPDATED);
    }

    @Test
    void process_WhenVersionSameAsStored_ReplacesTradeAndReturnsReplaced() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T2")).thenReturn(Optional.of(storedEntity("T2", 2, false)));
        var replacement = new IncomingTrade("T2", 2, "CP-2", "B2", TODAY.plusMonths(6));

        SaveOutcome outcome = service.process(command(replacement));

        assertThat(outcome).isEqualTo(SaveOutcome.REPLACED);
        TradeEntity saved = capturedTrade();
        assertThat(saved.getCounterPartyId()).isEqualTo("CP-2");
        assertThat(saved.getBookId()).isEqualTo("B2");
        assertThat(saved.getMaturityDate()).isEqualTo(TODAY.plusMonths(6));
        assertThat(capturedOutboxEvent().getType()).isEqualTo(TradeEventType.REPLACED);
    }

    @Test
    void process_WhenVersionLowerThanStored_ThrowsAndRecordsRejection() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T2")).thenReturn(Optional.of(storedEntity("T2", 2, false)));

        assertThatThrownBy(() -> service.process(command(incoming("T2", 1))))
                .isInstanceOf(LowerVersionTradeException.class);

        verify(tradeRepository, never()).saveAndFlush(any());
        assertThat(capturedProcessedMessage().getOutcome()).isEqualTo(MessageOutcome.REJECTED_LOWER_VERSION);
        OutboxEventEntity event = capturedOutboxEvent();
        assertThat(event.getType()).isEqualTo(TradeEventType.REJECTED);
        assertThat(event.getReason()).contains("lower than stored version 2");
        assertThat(processedCount("REJECTED_LOWER_VERSION")).isEqualTo(1);
    }

    @Test
    void process_WhenMaturityDateBeforeToday_ThrowsAndRecordsRejection() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T3")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.process(command(incoming("T3", 1, TODAY.minusDays(1)))))
                .isInstanceOf(MaturityDateInPastException.class);

        verify(tradeRepository, never()).saveAndFlush(any());
        assertThat(capturedProcessedMessage().getOutcome()).isEqualTo(MessageOutcome.REJECTED_MATURITY_IN_PAST);
        assertThat(capturedOutboxEvent().getType()).isEqualTo(TradeEventType.REJECTED);
    }

    @Test
    void process_WhenMaturityDateIsToday_StoresTrade() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T1")).thenReturn(Optional.empty());

        SaveOutcome outcome = service.process(command(incoming("T1", 1, TODAY)));

        assertThat(outcome).isEqualTo(SaveOutcome.CREATED);
    }

    @Test
    void process_WhenStoredTradeIsExpired_NewVersionResetsExpiredFlag() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.empty());
        when(tradeRepository.findByIdForUpdate("T3")).thenReturn(Optional.of(storedEntity("T3", 3, true)));

        service.process(command(incoming("T3", 4)));

        assertThat(capturedTrade().isExpired()).isFalse();
    }

    @Test
    void process_WhenMessageAlreadyProcessed_ReturnsRecordedOutcomeWithoutWriting() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.of(
                new ProcessedMessageEntity(MESSAGE_ID, "T1", MessageOutcome.CREATED, Instant.now(FIXED_CLOCK))));

        SaveOutcome outcome = service.process(command(incoming("T1", 1)));

        assertThat(outcome).isEqualTo(SaveOutcome.CREATED);
        verify(tradeRepository, never()).saveAndFlush(any());
        verify(outboxEventRepository, never()).save(any());
        verify(processedMessageRepository, never()).save(any());
        assertThat(processedCount(TradeMetrics.DUPLICATE_OUTCOME)).isEqualTo(1);
        assertThat(processedCount("CREATED")).isZero();
    }

    @Test
    void process_WhenRejectedMessageIsRedelivered_RethrowsRejectionWithoutRecordingAgain() {
        when(processedMessageRepository.findById(MESSAGE_ID)).thenReturn(Optional.of(new ProcessedMessageEntity(
                MESSAGE_ID, "T2", MessageOutcome.REJECTED_LOWER_VERSION, Instant.now(FIXED_CLOCK))));
        when(tradeRepository.findById("T2")).thenReturn(Optional.of(storedEntity("T2", 2, false)));

        assertThatThrownBy(() -> service.process(command(incoming("T2", 1))))
                .isInstanceOf(LowerVersionTradeException.class);

        verify(outboxEventRepository, never()).save(any());
        verify(processedMessageRepository, never()).save(any());
    }

    private double processedCount(String outcome) {
        var counter = meterRegistry.find(TradeMetrics.TRADES_PROCESSED)
                .tags("source", "KAFKA", "outcome", outcome)
                .counter();
        return counter == null ? 0 : counter.count();
    }

    private static TradeCommand command(IncomingTrade trade) {
        return new TradeCommand(MESSAGE_ID, EventSource.KAFKA, trade);
    }

    private TradeEntity capturedTrade() {
        var captor = ArgumentCaptor.forClass(TradeEntity.class);
        verify(tradeRepository).saveAndFlush(captor.capture());
        return captor.getValue();
    }

    private OutboxEventEntity capturedOutboxEvent() {
        var captor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEventEntity event = captor.getValue();
        assertThat(event.getId()).isNotNull();
        assertThat(event.getSource()).isEqualTo(EventSource.KAFKA);
        assertThat(event.getPayload()).isNotBlank();
        assertThat(event.getCreatedAt()).isEqualTo(Instant.now(FIXED_CLOCK));
        return event;
    }

    private ProcessedMessageEntity capturedProcessedMessage() {
        var captor = ArgumentCaptor.forClass(ProcessedMessageEntity.class);
        verify(processedMessageRepository).save(captor.capture());
        ProcessedMessageEntity processed = captor.getValue();
        assertThat(processed.getMessageId()).isEqualTo(MESSAGE_ID);
        return processed;
    }
}
