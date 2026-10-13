package com.tradestore.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the clock that defines "today" for all business rules, so tests can replace it with a fixed clock.
 */
@Configuration
public class ClockConfig {

    @Bean
    public Clock clock(TradeStoreProperties properties) {
        return Clock.system(properties.businessZone());
    }
}
