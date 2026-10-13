package com.tradestore.service;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.persistence.mongo.TradeEventMongoRepository;
import com.tradestore.persistence.sql.OutboxEventRepository;
import java.time.Clock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Delivers outbox events to MongoDB (all events) and to the {@code trade-status} topic ({@code EXPIRED} events).
 * Each destination is tracked separately; a failed delivery is retried on the next poll.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

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
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
