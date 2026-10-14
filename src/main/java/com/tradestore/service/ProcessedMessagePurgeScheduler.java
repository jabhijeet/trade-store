package com.tradestore.service;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.observability.TradeMetrics;
import com.tradestore.persistence.sql.ProcessedMessageRepository;
import java.time.Clock;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Daily job that deletes idempotency records older than the retention period, so {@code processed_message} does not
 * grow without bound. The retention must exceed the Kafka retention period: a message that can still be redelivered
 * must still be recognised as a duplicate. ShedLock ensures a single active instance.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProcessedMessagePurgeScheduler {

    private final ProcessedMessageRepository processedMessageRepository;
    private final TradeStoreProperties properties;
    private final TradeMetrics metrics;
    private final Clock clock;

    @Scheduled(cron = "${trade-store.purge.cron}", zone = "UTC")
    @SchedulerLock(name = "purge-processed-messages")
    public void run() {
        int purged = purgeExpiredRecords();
        log.info("Processed message purge completed: purgedRecords={}", purged);
    }

    /**
     * Deletes old records batch by batch, one transaction per batch, until none are left.
     *
     * @return the total number of records deleted
     */
    public int purgeExpiredRecords() {
        TradeStoreProperties.Purge purge = properties.purge();
        Instant cutoff = Instant.now(clock).minus(purge.processedMessageRetention());
        int total = 0;
        int deleted;
        do {
            deleted = processedMessageRepository.deleteProcessedBefore(cutoff, purge.batchSize());
            total += deleted;
            metrics.processedMessagesPurged(deleted);
        } while (deleted > 0);
        return total;
    }
}
