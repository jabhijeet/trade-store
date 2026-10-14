package com.tradestore;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real PostgreSQL, MongoDB and Kafka for integration tests (requires Docker), plus a fixed business clock.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer("postgres:18");
    }

    @Bean
    @ServiceConnection
    MongoDBContainer mongoContainer() {
        return new MongoDBContainer("mongo:8.0");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return new KafkaContainer("apache/kafka:4.1.0");
    }

    /** Fixed at {@link TradeFixtures#TODAY} unless a test moves it. */
    @Bean
    @Primary
    MutableClock testClock() {
        return new MutableClock();
    }
}
