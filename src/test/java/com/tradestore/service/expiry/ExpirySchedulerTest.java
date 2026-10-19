package com.tradestore.service.expiry;

import static com.tradestore.TradeFixtures.FIXED_CLOCK;
import static com.tradestore.TradeFixtures.TODAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tradestore.observability.TradeMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExpirySchedulerTest {

    @Mock
    private TradeExpiryService tradeExpiryService;

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @Test
    void expireMaturedTrades_ProcessesBatchesUntilNoneRemainAndReturnsTotal() {
        when(tradeExpiryService.expireNextBatch(TODAY)).thenReturn(500, 120, 0);
        var scheduler = new ExpiryScheduler(tradeExpiryService, new TradeMetrics(meterRegistry), FIXED_CLOCK);

        int total = scheduler.expireMaturedTrades();

        assertThat(total).isEqualTo(620);
        verify(tradeExpiryService, times(3)).expireNextBatch(TODAY);
        assertThat(meterRegistry.counter(TradeMetrics.TRADES_EXPIRED).count()).isEqualTo(620);
    }

    @Test
    void expireMaturedTrades_WhenNothingMatured_ReturnsZero() {
        when(tradeExpiryService.expireNextBatch(TODAY)).thenReturn(0);
        var scheduler = new ExpiryScheduler(tradeExpiryService, new TradeMetrics(meterRegistry), FIXED_CLOCK);

        assertThat(scheduler.expireMaturedTrades()).isZero();
    }
}
