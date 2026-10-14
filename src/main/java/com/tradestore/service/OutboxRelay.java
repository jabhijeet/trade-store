package com.tradestore.service;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.persistence.mongo.TradeEventDocument;
import com.tradestore.persistence.mongo.TradeEventMongoRepository;
import com.tradestore.persistence.sql.OutboxEventEntity;
import com.tradestore.persistence.sql.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * Delivers outbox events to MongoDB (all events) and to the {@code trade-status} topic ({@code EXPIRED} events).
 * Each destination is tracked separately; a failed delivery is retried on the next poll.
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
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final TradeStoreProperties properties;
    private final Clock clock;

    @Scheduled(fixedDelayString = "${trade-store.outbox.poll-interval-ms}")
    @SchedulerLock(name = "outbox-relay")
    public void run() {
        int read = deliverPendingEvents();
        log.debug("Outbox poll completed: eventsRead={}", read);
    }

    /**
     * Delivers one batch of pending events.
     *
     * @return the number of events read in this poll
     */
    public int deliverPendingEvents() {
        List<OutboxEventEntity> events =
                outboxEventRepository.findUndelivered(Limit.of(properties.outbox().batchSize()));
        for (OutboxEventEntity event : events) {
            deliverToMongo(event);
            deliverToKafka(event);
            outboxEventRepository.save(event);
        }
        return events.size();
    }

    private void deliverToMongo(OutboxEventEntity event) {
        if (event.getMongoDeliveredAt() != null) {
            return;
        }
        try {
            tradeEventRepository.save(toDocument(event));
            event.setMongoDeliveredAt(Instant.now(clock));
        } catch (DataAccessException exception) {
            log.warn("MongoDB delivery failed, will retry: eventId={}, tradeId={}, error={}",
                    event.getId(), event.getTradeId(), exception.getMessage());
        }
    }

    private void deliverToKafka(OutboxEventEntity event) {
        if (!event.requiresKafkaDelivery() || event.getKafkaDeliveredAt() != null) {
            return;
        }
        try {
            String message = jsonMapper.writeValueAsString(new TradeStatusMessage(event.getId().toString(),
                    event.getTradeId(), event.getVersion(), event.getType(), event.getCreatedAt()));
            kafkaTemplate.send(properties.topics().tradeStatus(), event.getTradeId(), message)
                    .get(KAFKA_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            event.setKafkaDeliveredAt(Instant.now(clock));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.warn("Kafka delivery interrupted, will retry: eventId={}", event.getId());
        } catch (ExecutionException | TimeoutException | KafkaException exception) {
            log.warn("Kafka delivery failed, will retry: eventId={}, tradeId={}, error={}",
                    event.getId(), event.getTradeId(), exception.getMessage());
        }
    }

    private TradeEventDocument toDocument(OutboxEventEntity event) {
        return new TradeEventDocument(event.getId().toString(), event.getTradeId(), event.getVersion(),
                event.getType(), event.getSource(), event.getReason(), event.getCreatedAt(),
                jsonMapper.readValue(event.getPayload(), PAYLOAD_TYPE));
    }
}
