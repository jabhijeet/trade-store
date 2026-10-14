package com.tradestore.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the clock that defines "today" for all business rules, so tests can replace it with a fixed clock.
 * The store runs entirely in UTC: business dates, timestamps and the expiry schedule.
 */
@Configuration
public class ClockConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
