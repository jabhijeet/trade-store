package com.tradestore.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.tradestore.persistence.sql.OutboxEventRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutboxBacklogMetricsTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @BeforeEach
    void bind() {
        new OutboxBacklogMetrics(outboxEventRepository).bindTo(meterRegistry);
    }

    @Test
    void pendingGauge_ReportsUndeliveredEventsPerDestination() {
        when(outboxEventRepository.countPendingMongoDelivery()).thenReturn(3L);
        when(outboxEventRepository.countPendingKafkaDelivery()).thenReturn(7L);

        assertThat(pending("mongodb")).isEqualTo(3);
        assertThat(pending("kafka")).isEqualTo(7);
    }

    @Test
    void pendingGauge_IsReadOnEachScrape() {
        when(outboxEventRepository.countPendingKafkaDelivery()).thenReturn(5L, 0L);

        assertThat(pending("kafka")).isEqualTo(5);
        assertThat(pending("kafka")).isZero();
    }

    private double pending(String destination) {
        return meterRegistry.get(OutboxBacklogMetrics.OUTBOX_PENDING).tag("destination", destination).gauge().value();
    }
}
