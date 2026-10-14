package com.tradestore;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * UTC test clock that a test can move to another business date. Starts at {@link TradeFixtures#FIXED_CLOCK}; a
 * test that moves it must {@link #reset()} it afterwards, because the Spring context (and so this clock) is shared.
 */
public final class MutableClock extends Clock {

    private volatile Instant instant = TradeFixtures.FIXED_CLOCK.instant();

    /** Moves the clock to 10:00 UTC on {@code date}. */
    public void setToday(LocalDate date) {
        instant = date.atTime(10, 0).toInstant(ZoneOffset.UTC);
    }

    public void reset() {
        instant = TradeFixtures.FIXED_CLOCK.instant();
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException("The trade store runs in UTC");
    }

    @Override
    public Instant instant() {
        return instant;
    }
}
