package com.tradestore.config.database;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class DatabaseConfigTest {

    private final MongoPoolProperties pool = new MongoPoolProperties(25, 3, Duration.ofSeconds(4),
            Duration.ofMinutes(6), Duration.ofMinutes(40));

    @Test
    void mongoPoolCustomizer_AppliesConfiguredPoolSettings() {
        var builder = MongoClientSettings.builder();

        new DatabaseConfig().mongoPoolCustomizer(pool).customize(builder);

        ConnectionPoolSettings settings = builder.build().getConnectionPoolSettings();
        assertThat(settings.getMaxSize()).isEqualTo(25);
        assertThat(settings.getMinSize()).isEqualTo(3);
        assertThat(settings.getMaxWaitTime(TimeUnit.MILLISECONDS)).isEqualTo(4_000);
        assertThat(settings.getMaxConnectionIdleTime(TimeUnit.MINUTES)).isEqualTo(6);
        assertThat(settings.getMaxConnectionLifeTime(TimeUnit.MINUTES)).isEqualTo(40);
    }

    @Test
    void mongoPoolCustomizer_OverridesPoolOptionsFromConnectionString() {
        var builder = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost:27017/tradestore?maxPoolSize=500"));

        new DatabaseConfig().mongoPoolCustomizer(pool).customize(builder);

        assertThat(builder.build().getConnectionPoolSettings().getMaxSize()).isEqualTo(25);
    }
}
