package com.tradestore;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.Trade;
import com.tradestore.persistence.sql.entity.TradeEntity;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.springframework.util.unit.DataSize;

/**
 * Shared test data. All tests run against a fixed business date, {@link #TODAY}.
 */
public final class TradeFixtures {

    public static final LocalDate TODAY = LocalDate.of(2026, 10, 13);
    public static final Clock FIXED_CLOCK =
            Clock.fixed(TODAY.atTime(10, 0).toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
    public static final LocalDate FUTURE_MATURITY = TODAY.plusYears(1);

    private TradeFixtures() {
    }

    public static TradeStoreProperties properties() {
        return new TradeStoreProperties(
                new TradeStoreProperties.Topics("trades", "trade-status"),
                new TradeStoreProperties.Expiry("0 5 0 * * *", 500),
                new TradeStoreProperties.Outbox(1000, 200),
                new TradeStoreProperties.Rest(100, DataSize.ofKilobytes(16)),
                new TradeStoreProperties.Purge("0 30 0 * * *", Duration.ofDays(14), 1000));
    }

    public static IncomingTrade incoming(String tradeId, int version) {
        return incoming(tradeId, version, FUTURE_MATURITY);
    }

    public static IncomingTrade incoming(String tradeId, int version, LocalDate maturityDate) {
        return new IncomingTrade(tradeId, version, "CP-1", "B1", maturityDate);
    }

    public static Trade stored(String tradeId, int version) {
        return new Trade(tradeId, version, "CP-1", "B1", FUTURE_MATURITY, TODAY.minusDays(10), false);
    }

    public static TradeEntity storedEntity(String tradeId, int version, boolean expired) {
        return new TradeEntity(tradeId, version, "CP-1", "B1", FUTURE_MATURITY, TODAY.minusDays(10), expired,
                Instant.parse("2026-10-03T09:00:00Z"));
    }
}
