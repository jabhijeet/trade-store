package com.tradestore.adapter.kafka;

import com.tradestore.service.TradeProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Primary ingestion adapter. Translates a Kafka record into a trade command; all rules live in
 * {@link TradeProcessingService}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TradeConsumer {

    private final TradeProcessingService tradeProcessingService;
    private final JsonMapper jsonMapper;

    /**
     * Processes one record. Business rejections are logged and completed normally, so the offset is committed
     * and the partition is not blocked. Invalid messages (malformed JSON, missing fields, unsupported schema
     * version, or a record key that differs from the trade id) throw {@link InvalidTradeMessageException}, which
     * the error handler sends to the dead-letter topic without retrying.
     */
    @KafkaListener(topics = "${trade-store.topics.trades}")
    public void onTrade(ConsumerRecord<String, String> record) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Message id used for idempotency: {@code kafka:<topic>:<partition>:<offset>}.
     */
    static String messageId(ConsumerRecord<?, ?> record) {
        return "kafka:%s:%d:%d".formatted(record.topic(), record.partition(), record.offset());
    }
}
