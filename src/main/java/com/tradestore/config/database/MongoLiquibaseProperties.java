package com.tradestore.config.database;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings for applying the MongoDB changelog, bound from {@code trade-store.mongodb.liquibase}.
 *
 * @param enabled   whether to apply the changelog at startup
 * @param changeLog classpath location of the MongoDB master changelog
 */
@ConfigurationProperties(prefix = "trade-store.mongodb.liquibase")
public record MongoLiquibaseProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("db/changelog/mongodb/db.changelog-master.yaml") String changeLog) {
}
