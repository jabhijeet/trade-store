package com.tradestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.MaturityDateInPastException;
import com.tradestore.domain.SaveOutcome;
import com.tradestore.domain.Trade;
import com.tradestore.domain.TradeCommand;
import com.tradestore.domain.TradeEventType;
import com.tradestore.domain.TradeHistoryEvent;
import com.tradestore.persistence.mongo.TradeEventMongoRepository;
import com.tradestore.persistence.mongo.TradeViewMongoRepository;
import com.tradestore.persistence.sql.OutboxEventRepository;
import com.tradestore.persistence.sql.ProcessedMessageRepository;
import com.tradestore.persistence.sql.TradeJpaRepository;
import com.tradestore.service.ExpiryScheduler;
import com.tradestore.service.OutboxRelay;
import com.tradestore.service.TradeProcessingService;
import com.tradestore.service.TradeQueryService;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.TestPropertySource;

/**
 * The sample table from the assignment, end to end against real PostgreSQL and MongoDB:
 * <pre>
 * Trade Id  Version  Counter-Party Id  Book-Id  Maturity Date  Created Date   Expired
 * T1        1        CP-1              B1       20/05/2020     &lt;today date&gt;   N
 * T2        2        CP-2              B1       20/05/2021     &lt;today date&gt;   N
 * T2        1        CP-1              B1       20/05/2021     14/03/2015     N
 * T3        3        CP-3              B2       20/05/2014     &lt;today date&gt;   Y
 * </pre>
 * Every maturity date in the table is in the past today, so the store would reject these trades if they arrived now
 * (see {@link #sampleTradeArrivingToday_IsRejectedBecauseItsMaturityDateHasPassed}). The table describes trades that
 * were received while their maturity dates were still in the future, which is reproduced here by moving the business
 * clock: "today" in the table is 10/01/2020, when T3 is already expired and T1 and T2 are not yet.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "trade-store.scheduling.enabled=false")
class SampleDataScenarioIT {

    private static final LocalDate T3_RECEIVED = LocalDate.of(2014, 1, 10);
    private static final LocalDate T2_V1_RECEIVED = LocalDate.of(2015, 3, 14);
    private static final LocalDate TABLE_TODAY = LocalDate.of(2020, 1, 10);

    private static final IncomingTrade T1_V1 = trade("T1", 1, "CP-1", "B1", LocalDate.of(2020, 5, 20));
    private static final IncomingTrade T2_V1 = trade("T2", 1, "CP-1", "B1", LocalDate.of(2021, 5, 20));
    private static final IncomingTrade T2_V2 = trade("T2", 2, "CP-2", "B1", LocalDate.of(2021, 5, 20));
    private static final IncomingTrade T3_V3 = trade("T3", 3, "CP-3", "B2", LocalDate.of(2014, 5, 20));

    @Autowired
    private MutableClock clock;
    @Autowired
    private TradeProcessingService tradeProcessingService;
    @Autowired
    private ExpiryScheduler expiryScheduler;
    @Autowired
    private OutboxRelay outboxRelay;
    @Autowired
    private TradeQueryService tradeQueryService;
    @Autowired
    private TradeJpaRepository tradeRepository;
    @Autowired
    private ProcessedMessageRepository processedMessageRepository;
    @Autowired
    private OutboxEventRepository outboxEventRepository;
    @Autowired
    private TradeEventMongoRepository tradeEventRepository;
    @Autowired
    private TradeViewMongoRepository tradeViewRepository;

    @BeforeEach
    void setUp() {
        tradeRepository.deleteAll();
        processedMessageRepository.deleteAll();
        outboxEventRepository.deleteAll();
        tradeEventRepository.deleteAll();
        tradeViewRepository.deleteAll();
    }

    @AfterEach
    void resetClock() {
        clock.reset();
    }

    @Test
    void sampleTable_IsReproducedWhenEachTradeArrivesBeforeItsMaturityDate() {
        loadSampleTable();

        assertThat(currentTrade("T1")).isEqualTo(new Trade("T1", 1, "CP-1", "B1",
                LocalDate.of(2020, 5, 20), TABLE_TODAY, false));
        assertThat(currentTrade("T2")).isEqualTo(new Trade("T2", 2, "CP-2", "B1",
                LocalDate.of(2021, 5, 20), TABLE_TODAY, false));
        assertThat(currentTrade("T3")).isEqualTo(new Trade("T3", 3, "CP-3", "B2",
                LocalDate.of(2014, 5, 20), T3_RECEIVED, true));
    }

    @Test
    void sampleTable_KeepsEarlierVersionOfT2InItsHistory() {
        loadSampleTable();

        var history = tradeQueryService.findHistory("T2", PageRequest.of(0, 10)).getContent();

        assertThat(history).extracting(TradeHistoryEvent::type, TradeHistoryEvent::version)
                .containsExactly(
                        tuple(TradeEventType.CREATED, 1),
                        tuple(TradeEventType.UPDATED, 2));
        assertThat(history.getFirst().payload())
                .containsEntry("counterPartyId", "CP-1")
                .containsEntry("createdDate", "2015-03-14");
    }

    @Test
    void sampleTable_IsAvailableByBookFromTheMongoProjection() {
        loadSampleTable();

        var bookB1 = tradeQueryService.findTradesByBook("B1", PageRequest.of(0, 10, Sort.by("tradeId")));
        var bookB2 = tradeQueryService.findTradesByBook("B2", PageRequest.of(0, 10, Sort.by("tradeId")));

        assertThat(bookB1.getContent()).containsExactly(currentTrade("T1"), currentTrade("T2"));
        assertThat(bookB2.getContent()).containsExactly(currentTrade("T3"));
    }

    @Test
    void lowerVersion_IsRejectedWithExceptionAndStoredTradeIsUnchanged() {
        loadSampleTable();
        clock.setToday(TABLE_TODAY.plusDays(1));

        assertThatThrownBy(() -> process(T2_V1))
                .isInstanceOf(LowerVersionTradeException.class)
                .hasMessage("Trade T2 version 1 is lower than stored version 2");

        assertThat(currentTrade("T2").version()).isEqualTo(2);
        deliverAllEvents();
        assertThat(tradeQueryService.findHistory("T2", PageRequest.of(0, 10)).getContent())
                .extracting(TradeHistoryEvent::type)
                .containsExactly(TradeEventType.CREATED, TradeEventType.UPDATED, TradeEventType.REJECTED);
    }

    @Test
    void sameVersion_ReplacesStoredTrade() {
        loadSampleTable();
        clock.setToday(TABLE_TODAY);

        SaveOutcome outcome = process(trade("T2", 2, "CP-9", "B1", LocalDate.of(2021, 5, 20)));

        assertThat(outcome).isEqualTo(SaveOutcome.REPLACED);
        assertThat(currentTrade("T2").counterPartyId()).isEqualTo("CP-9");
        assertThat(currentTrade("T2").version()).isEqualTo(2);
    }

    @Test
    void sampleTradeArrivingToday_IsRejectedBecauseItsMaturityDateHasPassed() {
        assertThatThrownBy(() -> process(T1_V1)).isInstanceOf(MaturityDateInPastException.class);

        assertThat(tradeRepository.findById("T1")).isEmpty();
    }

    @Test
    void storedTrade_IsMarkedExpiredOnlyAfterItsMaturityDate() {
        clock.setToday(TABLE_TODAY);
        process(T1_V1);

        clock.setToday(LocalDate.of(2020, 5, 20));
        assertThat(expiryScheduler.expireMaturedTrades()).isZero();
        assertThat(currentTrade("T1").expired()).isFalse();

        clock.setToday(LocalDate.of(2020, 5, 21));
        assertThat(expiryScheduler.expireMaturedTrades()).isEqualTo(1);
        assertThat(currentTrade("T1").expired()).isTrue();
    }

    /**
     * Receives each sample trade on a date when its maturity date was still in the future, runs the expiry
     * scheduler on the table's "today" and delivers every event to MongoDB.
     */
    private void loadSampleTable() {
        clock.setToday(T3_RECEIVED);
        assertThat(process(T3_V3)).isEqualTo(SaveOutcome.CREATED);

        clock.setToday(T2_V1_RECEIVED);
        assertThat(process(T2_V1)).isEqualTo(SaveOutcome.CREATED);

        clock.setToday(TABLE_TODAY);
        assertThat(process(T1_V1)).isEqualTo(SaveOutcome.CREATED);
        assertThat(process(T2_V2)).isEqualTo(SaveOutcome.UPDATED);
        assertThat(expiryScheduler.expireMaturedTrades()).isEqualTo(1);

        deliverAllEvents();
    }

    private void deliverAllEvents() {
        while (outboxEventRepository.countPendingMongoDelivery() > 0) {
            outboxRelay.deliverPendingEvents();
        }
    }

    private SaveOutcome process(IncomingTrade trade) {
        return tradeProcessingService.process(
                new TradeCommand("rest:" + UUID.randomUUID(), EventSource.REST, trade));
    }

    private Trade currentTrade(String tradeId) {
        return tradeQueryService.findTrade(tradeId).orElseThrow();
    }

    private static IncomingTrade trade(String tradeId, int version, String counterPartyId, String bookId,
                                       LocalDate maturityDate) {
        return new IncomingTrade(tradeId, version, counterPartyId, bookId, maturityDate);
    }
}
