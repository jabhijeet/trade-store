package com.tradestore.service;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.persistence.mongo.TradeEventDocument;
import com.tradestore.persistence.mongo.TradeEventMongoRepository;
import com.tradestore.persistence.mongo.TradeViewProjector;
import com.tradestore.persistence.sql.OutboxEventEntity;
import com.tradestore.persistence.sql.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Delivers outbox events to MongoDB (all events, into the history and the {@code trade_views} projection) and to
 * the {@code trade-status} topic ({@code EXPIRED} events).
 * <p>
 * Each destination is read, delivered and marked independently, so an outage of one never holds up the other.
 * Within a poll, the first failure for a destination stops further deliveries to it until the next poll: that bounds
 * a poll to one timeout per destination (keeping it well inside the ShedLock lease) and preserves per-trade order on
 * the {@code trade-status} topic.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    static final long KAFKA_SEND_TIMEOUT_SECONDS = 10;
    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final OutboxEventRepository outboxEventRepository;
    private final TradeEventMongoRepository tradeEventRepository;
    private final TradeViewProjector tradeViewProjector;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final TradeStoreProperties properties;
    private final Clock clock;

    @Scheduled(fixedDelayString = "${trade-store.outbox.poll-interval-ms}")
    @SchedulerLock(name = "outbox-relay")
    public void run() {
        int delivered = deliverPendingEvents();
        log.debug("Outbox poll completed: deliveries={}", delivered);
    }

    /**
     * Delivers one batch of pending events to each destination.
     *
     * @return the number of successful deliveries in this poll, counting each destination separately
     */
    public int deliverPendingEvents() {
        Limit batch = Limit.of(properties.outbox().batchSize());
        int delivered = deliverUntilFirstFailure(outboxEventRepository.findPendingMongoDelivery(batch),
                this::deliverToMongo);
        delivered += deliverUntilFirstFailure(outboxEventRepository.findPendingKafkaDelivery(batch),
                this::deliverToKafka);
        return delivered;
    }

    private static int deliverUntilFirstFailure(List<OutboxEventEntity> events,
                                                Predicate<OutboxEventEntity> delivery) {
        int delivered = 0;
        for (OutboxEventEntity event : events) {
            if (!delivery.test(event)) {
                break;
            }
            delivered++;
        }
        return delivered;
    }

    private boolean deliverToMongo(OutboxEventEntity event) {
        try {
            tradeEventRepository.save(toDocument(event));
            tradeViewProjector.apply(event.getType(), event.getPayload());
        } catch (DataAccessException exception) {
            log.warn("MongoDB delivery failed, will retry next poll: eventId={}, tradeId={}, error={}",
                    event.getId(), event.getTradeId(), exception.getMessage());
            return false;
        }
        outboxEventRepository.markMongoDelivered(event.getId(), Instant.now(clock));
        return true;
    }

    private boolean deliverToKafka(OutboxEventEntity event) {
        try {
            String message = jsonMapper.writeValueAsString(new TradeStatusMessage(event.getId().toString(),
                    event.getTradeId(), event.getVersion(), event.getType(), event.getCreatedAt()));
            kafkaTemplate.send(properties.topics().tradeStatus(), event.getTradeId(), message)
                    .get(KAFKA_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.warn("Kafka delivery interrupted, will retry next poll: eventId={}", event.getId());
            return false;
        } catch (ExecutionException | TimeoutException | KafkaException exception) {
            log.warn("Kafka delivery failed, will retry next poll: eventId={}, tradeId={}, error={}",
                    event.getId(), event.getTradeId(), exception.getMessage());
            return false;
        }
        outboxEventRepository.markKafkaDelivered(event.getId(), Instant.now(clock));
        return true;
    }

    private TradeEventDocument toDocument(OutboxEventEntity event) {
        return new TradeEventDocument(event.getId().toString(), event.getTradeId(), event.getVersion(),
                event.getType(), event.getSource(), event.getReason(), event.getCreatedAt(),
                jsonMapper.readValue(event.getPayload(), PAYLOAD_TYPE));
    }
}
