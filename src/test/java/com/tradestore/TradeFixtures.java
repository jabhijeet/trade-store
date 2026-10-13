package com.tradestore;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.Trade;
import com.tradestore.persistence.sql.TradeEntity;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * Shared test data. All tests run against a fixed business date, {@link #TODAY}.
 */
public final class TradeFixtures {

    public static final ZoneId ZONE = ZoneId.of("Europe/London");
    public static final LocalDate TODAY = LocalDate.of(2026, 10, 13);
    public static final Clock FIXED_CLOCK = Clock.fixed(TODAY.atTime(10, 0).atZone(ZONE).toInstant(), ZONE);
    public static final LocalDate FUTURE_MATURITY = TODAY.plusYears(1);

    private TradeFixtures() {
    }

    public static TradeStoreProperties properties() {
        return new TradeStoreProperties(ZONE,
                new TradeStoreProperties.Topics("trades", "trade-status"),
                new TradeStoreProperties.Expiry("0 5 0 * * *", 500),
                new TradeStoreProperties.Outbox(1000, 200));
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
