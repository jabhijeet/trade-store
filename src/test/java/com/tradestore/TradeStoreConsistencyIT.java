package com.tradestore;

import static com.tradestore.TradeFixtures.TODAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.DockerClient;
import com.tradestore.domain.EventSource;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.TradeCommand;
import com.tradestore.domain.TradeEventType;
import com.tradestore.persistence.mongo.TradeEventDocument;
import com.tradestore.persistence.mongo.TradeEventMongoRepository;
import com.tradestore.service.TradeExpiryService;
import com.tradestore.service.TradeProcessingService;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Verifies that PostgreSQL (current trade state) and MongoDB (trade history) stay consistent when one of the
 * services fails part-way through, and that retries do not duplicate history. Each test pauses a real container
 * to simulate an outage, then resumes it and waits for both stores to converge.
 */
@SpringBootTest
@Import({TestcontainersConfiguration.class, TradeStoreConsistencyIT.FastFailingClients.class})
@TestPropertySource(properties = {
        "trade-store.outbox.poll-interval-ms=200",
        "spring.datasource.hikari.data-source-properties.socketTimeout=3",
        "spring.kafka.producer.properties.max.block.ms=2000"
})
class TradeStoreConsistencyIT {

    private static final Duration CONVERGENCE_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration OUTAGE_OBSERVATION = Duration.ofSeconds(3);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private TradeProcessingService tradeProcessingService;
    @Autowired
    private TradeExpiryService tradeExpiryService;
    @Autowired
    private TradeEventMongoRepository tradeEventRepository;
    @Autowired
    private JdbcClient jdbcClient;
    @Autowired
    private PostgreSQLContainer postgres;
    @Autowired
    private MongoDBContainer mongo;
    @Autowired
    private KafkaContainer kafka;

    @BeforeEach
    void cleanUp() {
        jdbcClient.sql("DELETE FROM outbox_event").update();
        jdbcClient.sql("DELETE FROM processed_message").update();
        jdbcClient.sql("DELETE FROM trades").update();
        tradeEventRepository.deleteAll();
    }

    @Test
    void mongoOutage_TradeIsCommittedAndHistoryCatchesUpAfterRecovery() {
        pause(mongo);
        try {
            publish("C1", 1);

            await().atMost(CONVERGENCE_TIMEOUT).until(() -> tradeVersion("C1") == 1);
            await().during(OUTAGE_OBSERVATION).atMost(OUTAGE_OBSERVATION.plusSeconds(5))
                    .until(() -> pendingMongoDeliveries("C1") == 1);
        } finally {
            unpause(mongo);
        }

        await().atMost(CONVERGENCE_TIMEOUT).until(() -> pendingMongoDeliveries("C1") == 0);
        assertThat(history("C1")).extracting(TradeEventDocument::type).containsExactly(TradeEventType.CREATED);
        assertThat(history("C1").getFirst().id()).isEqualTo(outboxEventIds("C1").getFirst());
    }

    @Test
    void postgresOutage_MessageIsRetriedAndStoredExactlyOnce() {
        pause(postgres);
        try {
            publish("C2", 1);
            await().pollDelay(OUTAGE_OBSERVATION).until(() -> true);
            assertThat(history("C2")).isEmpty();
        } finally {
            unpause(postgres);
        }

        await().atMost(CONVERGENCE_TIMEOUT).until(() -> tradeVersion("C2") == 1);
        await().atMost(CONVERGENCE_TIMEOUT).until(() -> history("C2").size() == 1);
        assertThat(count("processed_message", "C2")).isEqualTo(1);
        assertThat(count("outbox_event", "C2")).isEqualTo(1);
        assertThat(history("C2")).extracting(TradeEventDocument::type).containsExactly(TradeEventType.CREATED);
    }

    @Test
    void relayCrashAfterMongoWrite_RedeliveryDoesNotDuplicateHistory() {
        tradeProcessingService.process(command("rest:" + UUID.randomUUID(), "C3", 1));
        await().atMost(CONVERGENCE_TIMEOUT).until(() -> pendingMongoDeliveries("C3") == 0);

        // Simulate a crash between the MongoDB write and marking the event delivered in PostgreSQL.
        jdbcClient.sql("UPDATE outbox_event SET mongo_delivered_at = NULL WHERE trade_id = 'C3'").update();
        await().atMost(CONVERGENCE_TIMEOUT).until(() -> pendingMongoDeliveries("C3") == 0);

        assertThat(history("C3")).hasSize(1);
    }

    @Test
    void redeliveredMessage_TradeAndHistoryAreWrittenOnce() {
        TradeCommand command = command("kafka:trades:0:" + System.nanoTime(), "C4", 1);

        tradeProcessingService.process(command);
        tradeProcessingService.process(command);

        await().atMost(CONVERGENCE_TIMEOUT).until(() -> pendingMongoDeliveries("C4") == 0);
        assertThat(count("processed_message", "C4")).isEqualTo(1);
        assertThat(count("outbox_event", "C4")).isEqualTo(1);
        assertThat(history("C4")).hasSize(1);
    }

    @Test
    void failedPostgresTransaction_LeavesNoTradeAndNoHistory() {
        String oversizedTradeId = "C5" + "X".repeat(70);

        assertThatThrownBy(() -> tradeProcessingService.process(command("rest:" + UUID.randomUUID(),
                oversizedTradeId, 1))).isInstanceOf(DataAccessException.class);

        assertThat(count("trades", oversizedTradeId)).isZero();
        assertThat(count("outbox_event", oversizedTradeId)).isZero();
        assertThat(count("processed_message", oversizedTradeId)).isZero();
        await().during(OUTAGE_OBSERVATION).atMost(OUTAGE_OBSERVATION.plusSeconds(5))
                .until(() -> history(oversizedTradeId).isEmpty());
    }

    @Test
    void kafkaOutage_ExpiryHistoryReachesMongoAndStatusIsPublishedAfterRecovery() {
        jdbcClient.sql("""
                INSERT INTO trades (trade_id, version, counter_party_id, book_id, maturity_date, created_date,
                                    expired, updated_at)
                VALUES ('C6', 1, 'CP-1', 'B1', :maturity, :created, FALSE, now())""")
                .param("maturity", TODAY.minusDays(1))
                .param("created", TODAY.minusYears(1))
                .update();

        pause(kafka);
        try {
            assertThat(tradeExpiryService.expireNextBatch(TODAY)).isEqualTo(1);

            await().atMost(CONVERGENCE_TIMEOUT).until(() -> pendingMongoDeliveries("C6") == 0);
            assertThat(history("C6")).extracting(TradeEventDocument::type).containsExactly(TradeEventType.EXPIRED);
            assertThat(pendingKafkaDeliveries("C6")).isEqualTo(1);
        } finally {
            unpause(kafka);
        }

        await().atMost(CONVERGENCE_TIMEOUT).until(() -> pendingKafkaDeliveries("C6") == 0);
        assertThat(history("C6")).hasSize(1);
    }

    private void publish(String tradeId, int version) {
        String message = """
                {"schemaVersion":1,"tradeId":"%s","version":%d,"counterPartyId":"CP-1","bookId":"B1",\
                "maturityDate":"%s"}""".formatted(tradeId, version, TODAY.plusYears(1));
        kafkaTemplate.send("trades", tradeId, message).join();
    }

    private static TradeCommand command(String messageId, String tradeId, int version) {
        return new TradeCommand(messageId, EventSource.REST,
                new IncomingTrade(tradeId, version, "CP-1", "B1", TODAY.plusYears(1)));
    }

    private int tradeVersion(String tradeId) {
        return jdbcClient.sql("SELECT version FROM trades WHERE trade_id = :id")
                .param("id", tradeId)
                .query(Integer.class)
                .optional()
                .orElse(0);
    }

    private long pendingMongoDeliveries(String tradeId) {
        return jdbcClient.sql("SELECT count(*) FROM outbox_event WHERE trade_id = :id AND mongo_delivered_at IS NULL")
                .param("id", tradeId)
                .query(Long.class)
                .single();
    }

    private long pendingKafkaDeliveries(String tradeId) {
        return jdbcClient.sql("""
                        SELECT count(*) FROM outbox_event
                        WHERE trade_id = :id AND type = 'EXPIRED' AND kafka_delivered_at IS NULL""")
                .param("id", tradeId)
                .query(Long.class)
                .single();
    }

    private List<String> outboxEventIds(String tradeId) {
        return jdbcClient.sql("SELECT id::text FROM outbox_event WHERE trade_id = :id ORDER BY created_at")
                .param("id", tradeId)
                .query(String.class)
                .list();
    }

    private long count(String table, String tradeId) {
        return jdbcClient.sql("SELECT count(*) FROM " + table + " WHERE trade_id = :id")
                .param("id", tradeId)
                .query(Long.class)
                .single();
    }

    private List<TradeEventDocument> history(String tradeId) {
        return tradeEventRepository.findAll().stream()
                .filter(event -> event.tradeId().equals(tradeId))
                .toList();
    }

    private static void pause(GenericContainer<?> container) {
        docker().pauseContainerCmd(container.getContainerId()).exec();
    }

    private static void unpause(GenericContainer<?> container) {
        docker().unpauseContainerCmd(container.getContainerId()).exec();
    }

    private static DockerClient docker() {
        return DockerClientFactory.instance().client();
    }

    /**
     * Short MongoDB timeouts, so an outage surfaces as a failed delivery within seconds instead of the driver's
     * 30-second default.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class FastFailingClients {

        @Bean
        MongoClientSettingsBuilderCustomizer fastFailingMongo() {
            return builder -> builder
                    .applyToClusterSettings(cluster -> cluster.serverSelectionTimeout(2, TimeUnit.SECONDS))
                    .applyToSocketSettings(socket -> socket
                            .connectTimeout(2, TimeUnit.SECONDS)
                            .readTimeout(2, TimeUnit.SECONDS));
        }
    }
}
