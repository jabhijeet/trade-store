package com.tradestore.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Application settings bound from the {@code trade-store} prefix.
 *
 * @param topics Kafka topic names
 * @param expiry expiry scheduler settings
 * @param outbox outbox relay settings
 */
@Validated
@ConfigurationProperties(prefix = "trade-store")
public record TradeStoreProperties(
        @Valid @NotNull Topics topics,
        @Valid @NotNull Expiry expiry,
        @Valid @NotNull Outbox outbox) {

    public record Topics(@NotBlank String trades, @NotBlank String tradeStatus) {
    }

    public record Expiry(@NotBlank String cron, @Min(1) int batchSize) {
    }

    public record Outbox(@Min(1) long pollIntervalMs, @Min(1) int batchSize) {
    }
}
