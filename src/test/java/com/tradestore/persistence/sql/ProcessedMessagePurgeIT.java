package com.tradestore.persistence.sql;

import static com.tradestore.TradeFixtures.FIXED_CLOCK;
import static org.assertj.core.api.Assertions.assertThat;

import com.tradestore.TestcontainersConfiguration;
import com.tradestore.domain.MessageOutcome;
import com.tradestore.service.ProcessedMessagePurgeScheduler;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "trade-store.scheduling.enabled=false")
class ProcessedMessagePurgeIT {

    private static final Instant NOW = Instant.now(FIXED_CLOCK);

    @Autowired
    private ProcessedMessageRepository processedMessageRepository;
    @Autowired
    private ProcessedMessagePurgeScheduler purgeScheduler;

    @BeforeEach
    void setUp() {
        processedMessageRepository.deleteAll();
    }

    @Test
    void deleteProcessedBefore_DeletesAtMostOneBatchOfOldRows() {
        save("old-1", NOW.minus(Duration.ofDays(20)));
        save("old-2", NOW.minus(Duration.ofDays(19)));
        save("old-3", NOW.minus(Duration.ofDays(18)));

        int deleted = processedMessageRepository.deleteProcessedBefore(NOW.minus(Duration.ofDays(14)), 2);

        assertThat(deleted).isEqualTo(2);
        assertThat(processedMessageRepository.count()).isEqualTo(1);
    }

    @Test
    void purgeExpiredRecords_DeletesOnlyRecordsOlderThanRetention() {
        save("kafka:trades:0:1", NOW.minus(Duration.ofDays(15)));
        save("kafka:trades:0:2", NOW.minus(Duration.ofDays(14)).plusSeconds(1));
        save("rest:recent", NOW.minus(Duration.ofHours(1)));

        int purged = purgeScheduler.purgeExpiredRecords();

        assertThat(purged).isEqualTo(1);
        assertThat(processedMessageRepository.findAll()).extracting(ProcessedMessageEntity::getMessageId)
                .containsExactlyInAnyOrder("kafka:trades:0:2", "rest:recent");
    }

    private void save(String messageId, Instant processedAt) {
        processedMessageRepository.save(
                new ProcessedMessageEntity(messageId, "T1", MessageOutcome.CREATED, processedAt));
    }
}
