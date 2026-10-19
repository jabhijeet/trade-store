package com.tradestore.config;

import com.tradestore.exception.InvalidTradeMessageException;
import com.tradestore.exception.TradeRejectedException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Kafka error handling and topic declarations.
 * <ul>
 *   <li>Invalid messages and business rejections are not retried.</li>
 *   <li>Other failures (for example the database being unavailable) are retried with exponential backoff and,
 *       once retries are exhausted, sent to the dead-letter topic {@code <topic>-dlt}.</li>
 * </ul>
 */
@Configuration
public class KafkaConfig {

    static final String DEAD_LETTER_SUFFIX = "-dlt";

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<?, ?> kafkaOperations) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, exception) -> new TopicPartition(record.topic() + DEAD_LETTER_SUFFIX, record.partition()));
        var backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(60_000L);
        backOff.setMaxElapsedTime(10 * 60_000L);
        var errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(InvalidTradeMessageException.class, TradeRejectedException.class);
        return errorHandler;
    }

    @Bean
    public NewTopic tradesTopic(TradeStoreProperties properties) {
        return TopicBuilder.name(properties.topics().trades()).partitions(6).build();
    }

    @Bean
    public NewTopic tradesDeadLetterTopic(TradeStoreProperties properties) {
        return TopicBuilder.name(properties.topics().trades() + DEAD_LETTER_SUFFIX).partitions(6).build();
    }

    @Bean
    public NewTopic tradeStatusTopic(TradeStoreProperties properties) {
        return TopicBuilder.name(properties.topics().tradeStatus()).partitions(6).build();
    }
}
