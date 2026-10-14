package com.tradestore.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Connection pool settings of the MongoDB driver, bound from {@code trade-store.mongodb.pool}. MongoDB does not use
 * JDBC, so its connections are pooled by the driver rather than HikariCP.
 *
 * @param maxSize                 maximum connections per server
 * @param minSize                 connections kept open per server, even when idle
 * @param maxWaitTime             how long a caller waits for a free connection before failing
 * @param maxConnectionIdleTime   idle time after which a connection is closed
 * @param maxConnectionLifeTime   age after which a connection is closed and replaced
 */
@Validated
@ConfigurationProperties(prefix = "trade-store.mongodb.pool")
public record MongoPoolProperties(
        @DefaultValue("20") @Min(1) int maxSize,
        @DefaultValue("2") @Min(0) int minSize,
        @DefaultValue("5s") @NotNull Duration maxWaitTime,
        @DefaultValue("5m") @NotNull Duration maxConnectionIdleTime,
        @DefaultValue("30m") @NotNull Duration maxConnectionLifeTime) {
}
