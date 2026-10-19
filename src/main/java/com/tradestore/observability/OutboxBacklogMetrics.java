package com.tradestore.observability;

import com.tradestore.persistence.sql.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Gauge {@value #OUTBOX_PENDING}: outbox events not yet delivered, tagged by {@code destination}
 * ({@code mongodb}, {@code kafka}). A growing value means a destination is down or the relay is falling behind.
 * Each reading runs a count query backed by the destination's partial index, so it stays cheap while the backlog
 * is small.
 */
@Component
@RequiredArgsConstructor
public class OutboxBacklogMetrics implements MeterBinder {

    public static final String OUTBOX_PENDING = "tradestore.outbox.pending";

    private final OutboxEventRepository outboxEventRepository;

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder(OUTBOX_PENDING, outboxEventRepository, OutboxEventRepository::countPendingMongoDelivery)
                .description("Outbox events not yet delivered")
                .tag("destination", "mongodb")
                .register(registry);
        Gauge.builder(OUTBOX_PENDING, outboxEventRepository, OutboxEventRepository::countPendingKafkaDelivery)
                .description("Outbox events not yet delivered")
                .tag("destination", "kafka")
                .register(registry);
    }
}
