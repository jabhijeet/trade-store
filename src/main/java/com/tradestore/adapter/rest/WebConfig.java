package com.tradestore.adapter.rest;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Applies the ingestion limits to the ingestion endpoint only; read endpoints are not rate limited.
 */
@Configuration
@RequiredArgsConstructor
public class WebConfig implements WebMvcConfigurer {

    private final IngestionLimitInterceptor ingestionLimitInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(ingestionLimitInterceptor).addPathPatterns("/api/v1/trades");
    }
}
