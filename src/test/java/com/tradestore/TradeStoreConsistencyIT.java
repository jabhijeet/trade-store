package com.tradestore;

import static com.tradestore.TradeFixtures.TODAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.DockerClient;
import com.tradestore.enums.EventSource;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.enums.SaveOutcome;
import com.tradestore.domain.TradeCommand;
import com.tradestore.enums.TradeEventType;
import com.tradestore.persistence.mongo.document.TradeEventDocument;
import com.tradestore.persistence.mongo.repository.TradeEventMongoRepository;
import com.tradestore.service.expiry.TradeExpiryService;
import com.tradestore.service.ingestion.TradeProcessingService;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
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
        // Small batches, so a backlog larger than one batch is cheap to create
        "trade-store.outbox.batch-size=2",
        "trade-store.expiry.batch-size=2",
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
    private PlatformTransactionManager transactionManager;
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

    /**
     * Two first versions of the same new trade id: the second insert blocks on the first's uncommitted row, then fails
     * on the primary key once the first commits. It must surface as a transient failure (so callers retry), and a
     * retry must apply the normal version check against the stored row.
     */
    @Test
    void concurrentInsertOfNewTradeId_LoserGetsTransientFailureAndRetrySucceeds() throws Exception {
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        TradeCommand loser = command("rest:" + UUID.randomUUID(), "C9", 1);
        CompletableFuture<Throwable> loserResult = new CompletableFuture<>();

        transaction.executeWithoutResult(status -> {
            jdbcClient.sql("""
                    INSERT INTO trades (trade_id, version, counter_party_id, book_id, maturity_date, created_date,
                                        expired, updated_at)
                    VALUES ('C9', 1, 'CP-1', 'B1', :maturity, :created, FALSE, now())""")
                    .param("maturity", TODAY.plusYears(1))
                    .param("created", TODAY)
                    .update();
            Thread.ofVirtual().start(() -> {
                try {
                    tradeProcessingService.process(loser);
                    loserResult.complete(null);
                } catch (Throwable failure) {
                    loserResult.complete(failure);
                }
            });
            // Commit only once the loser's insert is waiting on this transaction's uncommitted row
            await().atMost(CONVERGENCE_TIMEOUT).until(() -> jdbcClient.sql("""
                            SELECT count(*) FROM pg_stat_activity
                            WHERE wait_event_type = 'Lock' AND query ILIKE 'insert into trades%'""")
                    .query(Long.class).single() == 1);
        });

        assertThat(loserResult.get(CONVERGENCE_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                .isInstanceOf(ConcurrencyFailureException.class);
        assertThat(count("processed_message", "C9")).isZero();

        assertThat(tradeProcessingService.process(loser)).isEqualTo(SaveOutcome.REPLACED);
        assertThat(tradeVersion("C9")).isEqualTo(1);
    }

    /**
     * Regression: more {@code EXPIRED} events waiting for Kafka than fit in one relay batch must not stop newer events
     * from reaching MongoDB. The batch size is 2 in this test class, and three expiry events wait for Kafka.
     */
    @Test
    void kafkaOutageWithBacklogLargerThanBatch_NewerEventsStillReachMongo() {
        for (String tradeId : List.of("C7A", "C7B", "C7C")) {
            jdbcClient.sql("""
                    INSERT INTO trades (trade_id, version, counter_party_id, book_id, maturity_date, created_date,
                                        expired, updated_at)
                    VALUES (:id, 1, 'CP-1', 'B1', :maturity, :created, FALSE, now())""")
                    .param("id", tradeId)
                    .param("maturity", TODAY.minusDays(1))
                    .param("created", TODAY.minusYears(1))
                    .update();
        }

        pause(kafka);
        try {
            assertThat(tradeExpiryService.expireNextBatch(TODAY)).isEqualTo(2);
            assertThat(tradeExpiryService.expireNextBatch(TODAY)).isEqualTo(1);
            tradeProcessingService.process(command("rest:" + UUID.randomUUID(), "C8", 1));

            await().atMost(CONVERGENCE_TIMEOUT).untilAsserted(() -> {
                assertThat(history("C8")).extracting(TradeEventDocument::type)
                        .containsExactly(TradeEventType.CREATED);
                assertThat(pendingMongoDeliveries("C7C")).isZero();
            });
            assertThat(pendingKafkaDeliveries("C7A") + pendingKafkaDeliveries("C7B") + pendingKafkaDeliveries("C7C"))
                    .isEqualTo(3);
        } finally {
            unpause(kafka);
        }

        await().atMost(CONVERGENCE_TIMEOUT).until(() ->
                pendingKafkaDeliveries("C7A") + pendingKafkaDeliveries("C7B") + pendingKafkaDeliveries("C7C") == 0);
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
