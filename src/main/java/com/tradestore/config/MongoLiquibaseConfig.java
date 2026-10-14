package com.tradestore.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.mongodb.autoconfigure.MongoConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;

/**
 * Wires the MongoDB changelog run. The connection comes from {@link MongoConnectionDetails}, so the same
 * configuration works with {@code spring.mongodb.uri} and with Testcontainers service connections. The database
 * name comes from the {@link MongoDatabaseFactory}, so migrations target the database the repositories use.
 * Disabled with {@code trade-store.mongodb.liquibase.enabled=false}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBooleanProperty(name = "trade-store.mongodb.liquibase.enabled", matchIfMissing = true)
public class MongoLiquibaseConfig {

    @Bean
    public MongoLiquibaseMigration mongoLiquibaseMigration(MongoConnectionDetails connectionDetails,
                                                           MongoDatabaseFactory mongoDatabaseFactory,
                                                           MongoLiquibaseProperties properties) {
        return new MongoLiquibaseMigration(connectionDetails.getConnectionString().getConnectionString(),
                mongoDatabaseFactory.getMongoDatabase().getName(), properties.changeLog());
    }
}
