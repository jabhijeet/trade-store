package com.tradestore.config.database;

import java.util.concurrent.TimeUnit;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * Connection pools for the two databases. Each database has its own, independently sized pool:
 * <ul>
 *   <li><b>PostgreSQL</b> – HikariCP, configured with {@code spring.datasource.hikari.*} in
 *       {@code application.yml}. Spring Boot builds the pool from those properties and from the connection
 *       details, so Testcontainers service connections keep working.</li>
 *   <li><b>MongoDB</b> – the MongoDB driver's own pool (MongoDB does not use JDBC, so HikariCP does not apply),
 *       configured with {@code trade-store.mongodb.pool.*} and applied by {@link #mongoPoolCustomizer}.</li>
 * </ul>
 * Both pools publish Micrometer metrics: {@code hikaricp.connections.*} and {@code mongodb.driver.pool.*}.
 */
@Configuration(proxyBeanMethods = false)
public class DatabaseConfig {

    /**
     * Applies the MongoDB pool settings. Runs after Spring Boot's standard customizer, so these properties take
     * precedence over pool options in the connection string.
     */
    @Bean
    @Order(Ordered.LOWEST_PRECEDENCE)
    public MongoClientSettingsBuilderCustomizer mongoPoolCustomizer(MongoPoolProperties pool) {
        return builder -> builder.applyToConnectionPoolSettings(settings -> settings
                .maxSize(pool.maxSize())
                .minSize(pool.minSize())
                .maxWaitTime(pool.maxWaitTime().toMillis(), TimeUnit.MILLISECONDS)
                .maxConnectionIdleTime(pool.maxConnectionIdleTime().toMillis(), TimeUnit.MILLISECONDS)
                .maxConnectionLifeTime(pool.maxConnectionLifeTime().toMillis(), TimeUnit.MILLISECONDS));
    }
}
