package com.tradestore.service;

import static com.tradestore.TradeFixtures.FIXED_CLOCK;
import static com.tradestore.TradeFixtures.TODAY;
import static com.tradestore.TradeFixtures.properties;
import static com.tradestore.TradeFixtures.stored;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.TradeEventType;
import com.tradestore.persistence.sql.OutboxEventEntity;
import com.tradestore.persistence.sql.OutboxEventRepository;
import com.tradestore.persistence.sql.TradeExpiryRepository;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class TradeExpiryServiceTest {

    @Mock
    private TradeExpiryRepository tradeExpiryRepository;
    @Mock
    private OutboxEventRepository outboxEventRepository;

    private TradeExpiryService service;

    @BeforeEach
    void setUp() {
        var outboxWriter = new OutboxWriter(outboxEventRepository, JsonMapper.builder().build(), FIXED_CLOCK);
        service = new TradeExpiryService(tradeExpiryRepository, outboxWriter, properties());
    }

    @Test
    void expireNextBatch_WhenTradesMatured_AppendsExpiredEventPerTradeAndReturnsCount() {
        when(tradeExpiryRepository.expireMaturedTrades(TODAY, 500))
                .thenReturn(List.of(stored("T1", 1), stored("T3", 3)));

        int expired = service.expireNextBatch(TODAY);

        assertThat(expired).isEqualTo(2);
        var captor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(outboxEventRepository, times(2)).save(captor.capture());
        assertThat(captor.getAllValues())
                .extracting(OutboxEventEntity::getTradeId, OutboxEventEntity::getVersion,
                        OutboxEventEntity::getType, OutboxEventEntity::getSource)
                .containsExactly(
                        tuple("T1", 1, TradeEventType.EXPIRED, EventSource.SCHEDULER),
                        tuple("T3", 3, TradeEventType.EXPIRED, EventSource.SCHEDULER));
    }

    @Test
    void expireNextBatch_WhenNothingMatured_ReturnsZeroAndWritesNoEvents() {
        when(tradeExpiryRepository.expireMaturedTrades(TODAY, 500)).thenReturn(List.of());

        int expired = service.expireNextBatch(TODAY);

        assertThat(expired).isZero();
        verify(outboxEventRepository, never()).save(any());
    }
}
