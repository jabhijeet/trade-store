package com.tradestore.service.housekeeping;

import static com.tradestore.TradeFixtures.FIXED_CLOCK;
import static com.tradestore.TradeFixtures.properties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tradestore.observability.TradeMetrics;
import com.tradestore.persistence.sql.repository.ProcessedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessedMessagePurgeSchedulerTest {

    private static final Instant CUTOFF = Instant.now(FIXED_CLOCK).minus(Duration.ofDays(14));

    @Mock
    private ProcessedMessageRepository processedMessageRepository;

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @Test
    void purgeExpiredRecords_DeletesBatchesOlderThanRetentionUntilNoneRemain() {
        when(processedMessageRepository.deleteProcessedBefore(CUTOFF, 1000)).thenReturn(1000, 250, 0);

        int total = scheduler().purgeExpiredRecords();

        assertThat(total).isEqualTo(1250);
        verify(processedMessageRepository, times(3)).deleteProcessedBefore(CUTOFF, 1000);
        assertThat(meterRegistry.counter(TradeMetrics.PROCESSED_MESSAGES_PURGED).count()).isEqualTo(1250);
    }

    @Test
    void purgeExpiredRecords_WhenNothingIsOld_ReturnsZero() {
        when(processedMessageRepository.deleteProcessedBefore(CUTOFF, 1000)).thenReturn(0);

        assertThat(scheduler().purgeExpiredRecords()).isZero();
    }

    private ProcessedMessagePurgeScheduler scheduler() {
        return new ProcessedMessagePurgeScheduler(processedMessageRepository, properties(),
                new TradeMetrics(meterRegistry), FIXED_CLOCK);
    }
}
