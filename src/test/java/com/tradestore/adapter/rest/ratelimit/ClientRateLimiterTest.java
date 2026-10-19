package com.tradestore.adapter.rest.ratelimit;

import static com.tradestore.TradeFixtures.properties;
import static org.assertj.core.api.Assertions.assertThat;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.exception.RateLimitExceededException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ClientRateLimiterTest {

    @Test
    void tryAcquire_AdmitsBurstUpToRateThenAsksClientToWait() {
        var limiter = limiter(3);

        assertThat(limiter.tryAcquire("client-a")).isEmpty();
        assertThat(limiter.tryAcquire("client-a")).isEmpty();
        assertThat(limiter.tryAcquire("client-a")).isEmpty();
        assertThat(limiter.tryAcquire("client-a")).hasValueSatisfying(wait ->
                assertThat(wait).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(1)));
    }

    @Test
    void tryAcquire_KeepsSeparateBudgetPerClient() {
        var limiter = limiter(1);

        assertThat(limiter.tryAcquire("client-a")).isEmpty();
        assertThat(limiter.tryAcquire("client-a")).isPresent();
        assertThat(limiter.tryAcquire("client-b")).isEmpty();
    }

    @Test
    void rateLimitExceededException_RoundsRetryAfterUpToWholeSecondsAndAtLeastOne() {
        assertThat(new RateLimitExceededException("c", Duration.ofMillis(1)).getRetryAfterSeconds()).isEqualTo(1);
        assertThat(new RateLimitExceededException("c", Duration.ofMillis(1500)).getRetryAfterSeconds()).isEqualTo(2);
        assertThat(new RateLimitExceededException("c", Duration.ofSeconds(3)).getRetryAfterSeconds()).isEqualTo(3);
        assertThat(new RateLimitExceededException("c", Duration.ZERO).getRetryAfterSeconds()).isEqualTo(1);
    }

    private static ClientRateLimiter limiter(long requestsPerSecond) {
        TradeStoreProperties defaults = properties();
        return new ClientRateLimiter(new TradeStoreProperties(defaults.topics(), defaults.expiry(), defaults.outbox(),
                new TradeStoreProperties.Rest(requestsPerSecond, defaults.rest().maxRequestSize()),
                defaults.purge()));
    }
}
