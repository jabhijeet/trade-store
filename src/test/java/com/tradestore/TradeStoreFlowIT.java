package com.tradestore;

import static com.tradestore.TradeFixtures.TODAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.tradestore.enums.TradeEventType;
import com.tradestore.persistence.mongo.document.TradeEventDocument;
import com.tradestore.persistence.mongo.repository.TradeEventMongoRepository;
import com.tradestore.persistence.sql.entity.TradeEntity;
import com.tradestore.persistence.sql.repository.TradeJpaRepository;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;

/**
 * End-to-end flow through real Kafka, PostgreSQL and MongoDB, based on the assignment's sample data.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "trade-store.outbox.poll-interval-ms=200")
class TradeStoreFlowIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private TradeJpaRepository tradeRepository;
    @Autowired
    private TradeEventMongoRepository tradeEventRepository;
    @Autowired
    private JdbcClient jdbcClient;

    /**
     * Waits for the relay to deliver the previous test's events before clearing both stores. Otherwise an event
     * still pending in the outbox (for example a {@code REPLACED} event for a trade id reused by the next test) would
     * reach MongoDB after the cleanup and appear in the next test's history.
     */
    @BeforeEach
    void cleanUp() {
        await().atMost(TIMEOUT).until(() -> jdbcClient
                .sql("SELECT count(*) FROM outbox_event WHERE mongo_delivered_at IS NULL")
                .query(Long.class).single() == 0);
        jdbcClient.sql("DELETE FROM outbox_event").update();
        jdbcClient.sql("DELETE FROM processed_message").update();
        tradeRepository.deleteAll();
        tradeEventRepository.deleteAll();
    }

    @Test
    void newTradeFromKafka_IsStoredAndRecordedInHistory() {
        publish("T1", 1, "CP-1", "B1", TODAY.plusYears(1));

        TradeEntity stored = awaitTrade("T1", 1);
        assertThat(stored.getCreatedDate()).isEqualTo(TODAY);
        assertThat(stored.isExpired()).isFalse();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(historyTypes("T1"))
                .containsExactly(TradeEventType.CREATED));
    }

    @Test
    void lowerVersionAfterHigherVersion_IsRejectedAndStoredVersionKept() {
        publish("T2", 2, "CP-2", "B1", TODAY.plusYears(1));
        awaitTrade("T2", 2);

        publish("T2", 1, "CP-1", "B1", TODAY.plusYears(1));

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(historyTypes("T2"))
                .containsExactlyInAnyOrder(TradeEventType.CREATED, TradeEventType.REJECTED));
        assertThat(tradeRepository.findById("T2")).get()
                .extracting(TradeEntity::getVersion, TradeEntity::getCounterPartyId)
                .containsExactly(2, "CP-2");
    }

    @Test
    void sameVersion_ReplacesStoredRecord() {
        publish("T2", 2, "CP-2", "B1", TODAY.plusYears(1));
        awaitTrade("T2", 2);

        publish("T2", 2, "CP-3", "B2", TODAY.plusYears(2));

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(tradeRepository.findById("T2")).get()
                .extracting(TradeEntity::getCounterPartyId, TradeEntity::getBookId)
                .containsExactly("CP-3", "B2"));
    }

    @Test
    void maturityDateInPast_IsRejectedAndNotStored() {
        publish("T3", 3, "CP-3", "B2", TODAY.minusDays(1));

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(historyTypes("T3"))
                .containsExactly(TradeEventType.REJECTED));
        assertThat(tradeRepository.findById("T3")).isEmpty();
    }

    private void publish(String tradeId, int version, String counterPartyId, String bookId, LocalDate maturity) {
        String message = """
                {"schemaVersion":1,"tradeId":"%s","version":%d,"counterPartyId":"%s","bookId":"%s",\
                "maturityDate":"%s"}""".formatted(tradeId, version, counterPartyId, bookId, maturity);
        kafkaTemplate.send("trades", tradeId, message).join();
    }

    private TradeEntity awaitTrade(String tradeId, int version) {
        return await().atMost(TIMEOUT).until(
                () -> tradeRepository.findById(tradeId).filter(t -> t.getVersion() == version).orElse(null),
                trade -> trade != null);
    }

    private List<TradeEventType> historyTypes(String tradeId) {
        return tradeEventRepository.findAll().stream()
                .filter(event -> event.tradeId().equals(tradeId))
                .map(TradeEventDocument::type)
                .toList();
    }
}
