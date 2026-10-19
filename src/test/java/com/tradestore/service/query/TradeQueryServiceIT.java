package com.tradestore.service.query;

import static com.tradestore.TradeFixtures.TODAY;
import static org.assertj.core.api.Assertions.assertThat;

import com.tradestore.TestcontainersConfiguration;
import com.tradestore.enums.EventSource;
import com.tradestore.domain.Trade;
import com.tradestore.enums.TradeEventType;
import com.tradestore.domain.TradeFilter;
import com.tradestore.domain.TradeHistoryEvent;
import com.tradestore.persistence.mongo.document.TradeEventDocument;
import com.tradestore.persistence.mongo.repository.TradeEventMongoRepository;
import com.tradestore.persistence.sql.entity.TradeEntity;
import com.tradestore.persistence.sql.repository.TradeJpaRepository;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "trade-store.scheduling.enabled=false")
class TradeQueryServiceIT {

    @Autowired
    private TradeQueryService tradeQueryService;
    @Autowired
    private TradeJpaRepository tradeRepository;
    @Autowired
    private TradeEventMongoRepository tradeEventRepository;

    @BeforeEach
    void setUp() {
        tradeRepository.deleteAll();
        tradeEventRepository.deleteAll();
        tradeRepository.save(trade("T1", "CP-1", "B1", false));
        tradeRepository.save(trade("T2", "CP-2", "B1", false));
        tradeRepository.save(trade("T3", "CP-3", "B2", true));
    }

    @Test
    void findTrade_ReturnsCurrentTradeOrEmpty() {
        assertThat(tradeQueryService.findTrade("T1")).get().extracting(Trade::counterPartyId).isEqualTo("CP-1");
        assertThat(tradeQueryService.findTrade("T9")).isEmpty();
    }

    @Test
    void findTrades_WithoutFilter_ReturnsAllTradesPaged() {
        Page<Trade> page = tradeQueryService.findTrades(new TradeFilter(null, null, null),
                PageRequest.of(0, 2, Sort.by("tradeId")));

        assertThat(page.getTotalElements()).isEqualTo(3);
        assertThat(page.getContent()).extracting(Trade::tradeId).containsExactly("T1", "T2");
    }

    @Test
    void findTrades_WithFilters_ReturnsOnlyMatchingTrades() {
        Page<Trade> page = tradeQueryService.findTrades(new TradeFilter("B1", null, false),
                PageRequest.of(0, 10, Sort.by("tradeId")));

        assertThat(page.getContent()).extracting(Trade::tradeId).containsExactly("T1", "T2");
        assertThat(tradeQueryService.findTrades(new TradeFilter(null, "CP-3", true), PageRequest.of(0, 10))
                .getContent()).extracting(Trade::tradeId).containsExactly("T3");
    }

    @Test
    void findHistory_ReturnsEventsOfTradeInOccurrenceOrder() {
        tradeEventRepository.save(event("e2", "T2", 2, TradeEventType.UPDATED, "2026-10-13T10:00:00Z"));
        tradeEventRepository.save(event("e1", "T2", 1, TradeEventType.CREATED, "2026-10-12T10:00:00Z"));
        tradeEventRepository.save(event("e3", "T1", 1, TradeEventType.CREATED, "2026-10-12T10:00:00Z"));

        Page<TradeHistoryEvent> history = tradeQueryService.findHistory("T2", PageRequest.of(0, 10));

        assertThat(history.getContent()).extracting(TradeHistoryEvent::id).containsExactly("e1", "e2");
    }

    private static TradeEntity trade(String tradeId, String counterPartyId, String bookId, boolean expired) {
        return new TradeEntity(tradeId, 1, counterPartyId, bookId, TODAY.plusYears(1), TODAY, expired,
                Instant.parse("2026-10-13T09:00:00Z"));
    }

    private static TradeEventDocument event(String id, String tradeId, int version, TradeEventType type,
                                            String occurredAt) {
        return new TradeEventDocument(id, tradeId, version, type, EventSource.KAFKA, null,
                Instant.parse(occurredAt), Map.of("tradeId", tradeId));
    }
}
