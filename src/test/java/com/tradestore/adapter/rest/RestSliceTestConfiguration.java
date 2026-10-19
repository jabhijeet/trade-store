package com.tradestore.adapter.rest;

import com.tradestore.adapter.rest.ratelimit.ClientRateLimiter;
import com.tradestore.config.SecurityConfig;
import com.tradestore.config.TradeStoreProperties;
import com.tradestore.config.WebConfig;
import com.tradestore.observability.TradeMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * What a {@code @WebMvcTest} slice needs besides the controller: the real security rules, the ingestion limits
 * (the interceptor and {@link WebConfig} are picked up by the slice itself) and the application properties. Tests
 * authenticate with {@code SecurityMockMvcRequestPostProcessors.jwt()}, so no token issuer is contacted.
 */
@TestConfiguration(proxyBeanMethods = false)
@EnableConfigurationProperties(TradeStoreProperties.class)
@Import({SecurityConfig.class, ClientRateLimiter.class, TradeMetrics.class})
public class RestSliceTestConfiguration {

    @Bean
    MeterRegistry meterRegistry() {
        return new SimpleMeterRegistry();
    }
}
