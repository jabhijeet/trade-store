package com.tradestore.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Describes the REST API for the OpenAPI document ({@code /v3/api-docs}) and Swagger UI ({@code /swagger-ui.html}).
 * Every API endpoint needs a JWT bearer token, so the document declares one bearer scheme and applies it globally:
 * Swagger UI's <em>Authorize</em> button then sends the pasted token with each request.
 */
@Configuration
public class OpenApiConfig {

    static final String BEARER_SCHEME = "bearer-jwt";

    @Bean
    public OpenAPI tradeStoreOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Trade Store API")
                        .version("v1")
                        .description("""
                                Trade ingestion (backup to Kafka) and trade queries. Requests need a JWT bearer \
                                token from the configured issuer: `trades:write` to submit trades, `trades:read` \
                                to read them."""))
                .components(new Components().addSecuritySchemes(BEARER_SCHEME, new SecurityScheme()
                        .type(SecurityScheme.Type.HTTP)
                        .scheme("bearer")
                        .bearerFormat("JWT")))
                .addSecurityItem(new SecurityRequirement().addList(BEARER_SCHEME));
    }
}
