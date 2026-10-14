package com.tradestore.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.tradestore.TestcontainersConfiguration;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

/**
 * Verifies that Liquibase applied both changelogs at startup.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "trade-store.scheduling.enabled=false")
class SchemaMigrationIT {

    @Autowired
    private JdbcClient jdbcClient;
    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    void postgresChangelog_CreatesAllTables() {
        List<String> tables = jdbcClient.sql("""
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public' ORDER BY table_name""")
                .query(String.class)
                .list();

        assertThat(tables).contains("trades", "processed_message", "outbox_event", "shedlock",
                "databasechangelog");
    }

    @Test
    void mongoChangelog_CreatesTradeEventsCollectionWithHistoryIndex() {
        assertThat(mongoTemplate.collectionExists("trade_events")).isTrue();

        List<String> indexNames = mongoTemplate.getCollection("trade_events").listIndexes()
                .map(index -> index.getString("name"))
                .into(new ArrayList<>());
        assertThat(indexNames).contains("idx_trade_events_trade_id_occurred_at");

        assertThat(mongoTemplate.getCollection("DATABASECHANGELOG").countDocuments(
                new Document("id", "001-create-trade-events"))).isEqualTo(1);
    }

    @Test
    void mongoChangelog_CreatesTradeViewsCollectionWithBookIndex() {
        assertThat(mongoTemplate.collectionExists("trade_views")).isTrue();

        List<String> indexNames = mongoTemplate.getCollection("trade_views").listIndexes()
                .map(index -> index.getString("name"))
                .into(new ArrayList<>());
        assertThat(indexNames).contains("idx_trade_views_book_id");
    }
}
