package com.tradestore.config.database;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.MongoClientSettings;
import com.tradestore.TestcontainersConfiguration;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

/**
 * Verifies that both connection pools are built from the application configuration.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "trade-store.scheduling.enabled=false")
class DatabasePoolIT {

    @Autowired
    private DataSource dataSource;
    @Autowired
    private MongoClientSettings mongoClientSettings;
    @Autowired
    private ObjectProvider<MongoClientSettingsBuilderCustomizer> mongoCustomizers;

    @Test
    void postgresPool_IsHikariWithConfiguredSettings() {
        assertThat(dataSource).isInstanceOf(HikariDataSource.class);
        HikariDataSource hikari = (HikariDataSource) dataSource;

        assertThat(hikari.getPoolName()).isEqualTo("trade-store-postgres");
        assertThat(hikari.getMaximumPoolSize()).isEqualTo(10);
        assertThat(hikari.getMinimumIdle()).isEqualTo(2);
        assertThat(hikari.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(5).toMillis());
        assertThat(hikari.getLeakDetectionThreshold()).isEqualTo(Duration.ofSeconds(60).toMillis());
    }

    @Test
    void mongoPool_UsesConfiguredDriverPoolSettings() {
        var builder = MongoClientSettings.builder(mongoClientSettings);
        mongoCustomizers.orderedStream().forEach(customizer -> customizer.customize(builder));

        assertThat(builder.build().getConnectionPoolSettings().getMaxSize()).isEqualTo(20);
        assertThat(builder.build().getConnectionPoolSettings().getMinSize()).isEqualTo(2);
    }
}
