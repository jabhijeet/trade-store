package com.tradestore.adapter.kafka;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.TradeCommand;
import com.tradestore.domain.TradeRejectedException;
import com.tradestore.service.TradeProcessingService;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Primary ingestion adapter. Translates a Kafka record into a trade command; all rules live in
 * {@link TradeProcessingService}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TradeConsumer {

    /** Matches the {@code VARCHAR(64)} identifier columns and the REST request limits. */
    static final int MAX_IDENTIFIER_LENGTH = 64;

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
        String messageId = messageId(record);
        IncomingTrade trade = toIncomingTrade(parse(record.value()));
        if (!trade.tradeId().equals(record.key())) {
            throw new InvalidTradeMessageException("Record key %s does not match trade id %s"
                    .formatted(record.key(), trade.tradeId()));
        }

        try {
            tradeProcessingService.process(new TradeCommand(messageId, EventSource.KAFKA, trade));
        } catch (TradeRejectedException rejection) {
            log.info("Kafka trade rejected, offset will be committed: messageId={}, reason={}",
                    messageId, rejection.getMessage());
        }
    }

    /**
     * Message id used for idempotency: {@code kafka:<topic>:<partition>:<offset>}.
     */
    static String messageId(ConsumerRecord<?, ?> record) {
        return "kafka:%s:%d:%d".formatted(record.topic(), record.partition(), record.offset());
    }

    private TradeMessage parse(String value) {
        if (value == null) {
            throw new InvalidTradeMessageException("Trade message has no value");
        }
        try {
            return jsonMapper.readValue(value, TradeMessage.class);
        } catch (JacksonException exception) {
            throw new InvalidTradeMessageException("Trade message is not valid JSON", exception);
        }
    }

    private static IncomingTrade toIncomingTrade(TradeMessage message) {
        if (message.schemaVersion() == null || message.schemaVersion() != TradeMessage.SUPPORTED_SCHEMA_VERSION) {
            throw new InvalidTradeMessageException("Unsupported schemaVersion: " + message.schemaVersion());
        }

        List<String> invalidFields = new ArrayList<>();
        if (isInvalidIdentifier(message.tradeId())) {
            invalidFields.add("tradeId");
        }
        if (message.version() == null || message.version() < 1) {
            invalidFields.add("version");
        }
        if (isInvalidIdentifier(message.counterPartyId())) {
            invalidFields.add("counterPartyId");
        }
        if (isInvalidIdentifier(message.bookId())) {
            invalidFields.add("bookId");
        }
        if (message.maturityDate() == null) {
            invalidFields.add("maturityDate");
        }
        if (!invalidFields.isEmpty()) {
            throw new InvalidTradeMessageException("Trade message has missing or invalid fields: " + invalidFields);
        }

        return new IncomingTrade(message.tradeId(), message.version(), message.counterPartyId(), message.bookId(),
                message.maturityDate());
    }

    /**
     * Identifiers must fit their database columns; an oversized value would otherwise fail on every retry.
     */
    private static boolean isInvalidIdentifier(String value) {
        return value == null || value.isBlank() || value.length() > MAX_IDENTIFIER_LENGTH;
    }
}
