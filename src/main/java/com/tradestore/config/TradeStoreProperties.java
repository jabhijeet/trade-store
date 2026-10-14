package com.tradestore.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

/**
 * Application settings bound from the {@code trade-store} prefix.
 *
 * @param topics Kafka topic names
 * @param expiry expiry scheduler settings
 * @param outbox outbox relay settings
 * @param rest   limits for the REST ingestion endpoint
 * @param purge  purge job for old {@code processed_message} rows
 */
@Validated
@ConfigurationProperties(prefix = "trade-store")
public record TradeStoreProperties(
        @Valid @NotNull Topics topics,
        @Valid @NotNull Expiry expiry,
        @Valid @NotNull Outbox outbox,
        @Valid @NotNull Rest rest,
        @Valid @NotNull Purge purge) {

    public record Topics(@NotBlank String trades, @NotBlank String tradeStatus) {
    }

    public record Expiry(@NotBlank String cron, @Min(1) int batchSize) {
    }

    public record Outbox(@Min(1) long pollIntervalMs, @Min(1) int batchSize) {
    }

    /**
     * @param requestsPerSecond sustained ingestion requests per second allowed for each client on each instance;
     *                          also the burst size
     * @param maxRequestSize    largest accepted ingestion request body
     */
    public record Rest(@Min(1) long requestsPerSecond, @NotNull DataSize maxRequestSize) {
    }

    /**
     * @param cron                       when the purge job runs (UTC)
     * @param processedMessageRetention  how long processed message ids are kept; must exceed the Kafka retention
     *                                   period and the longest time a REST client may retry with the same key
     * @param batchSize                  rows deleted per transaction
     */
    public record Purge(@NotBlank String cron, @NotNull Duration processedMessageRetention, @Min(1) int batchSize) {
    }
}
