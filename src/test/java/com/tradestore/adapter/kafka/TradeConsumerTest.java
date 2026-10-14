package com.tradestore.adapter.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.SaveOutcome;
import com.tradestore.domain.TradeCommand;
import com.tradestore.service.TradeProcessingService;
import java.time.LocalDate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.QueryTimeoutException;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class TradeConsumerTest {

    private static final String VALID_MESSAGE = """
            {"schemaVersion":1,"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1",
             "maturityDate":"2027-05-20"}""";

    @Mock
    private TradeProcessingService tradeProcessingService;

    private TradeConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new TradeConsumer(tradeProcessingService, JsonMapper.builder().build());
    }

    @Test
    void onTrade_WhenMessageValid_ProcessesCommandWithKafkaMessageId() {
        when(tradeProcessingService.process(any())).thenReturn(SaveOutcome.CREATED);

        consumer.onTrade(record(VALID_MESSAGE));

        var captor = ArgumentCaptor.forClass(TradeCommand.class);
        verify(tradeProcessingService).process(captor.capture());
        TradeCommand command = captor.getValue();
        assertThat(command.messageId()).isEqualTo("kafka:trades:3:42");
        assertThat(command.source()).isEqualTo(EventSource.KAFKA);
        assertThat(command.trade())
                .isEqualTo(new IncomingTrade("T1", 1, "CP-1", "B1", LocalDate.of(2027, 5, 20)));
    }

    @Test
    void onTrade_WhenTradeRejected_CompletesNormallySoOffsetIsCommitted() {
        when(tradeProcessingService.process(any())).thenThrow(new LowerVersionTradeException("T1", 1, 2));

        assertThatCode(() -> consumer.onTrade(record(VALID_MESSAGE))).doesNotThrowAnyException();
    }

    @Test
    void onTrade_WhenTransientFailure_PropagatesForRetry() {
        when(tradeProcessingService.process(any())).thenThrow(new QueryTimeoutException("lock timeout"));

        assertThatThrownBy(() -> consumer.onTrade(record(VALID_MESSAGE)))
                .isInstanceOf(QueryTimeoutException.class);
    }

    @Test
    void onTrade_WhenJsonMalformed_ThrowsInvalidTradeMessageException() {
        assertThatThrownBy(() -> consumer.onTrade(record("{not json")))
                .isInstanceOf(InvalidTradeMessageException.class);
        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void onTrade_WhenSchemaVersionUnsupported_ThrowsInvalidTradeMessageException() {
        String message = VALID_MESSAGE.replace("\"schemaVersion\":1", "\"schemaVersion\":99");

        assertThatThrownBy(() -> consumer.onTrade(record(message)))
                .isInstanceOf(InvalidTradeMessageException.class)
                .hasMessageContaining("99");
        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void onTrade_WhenRequiredFieldMissing_ThrowsInvalidTradeMessageException() {
        String message = VALID_MESSAGE.replace("\"tradeId\":\"T1\",", "");

        assertThatThrownBy(() -> consumer.onTrade(record(message)))
                .isInstanceOf(InvalidTradeMessageException.class)
                .hasMessageContaining("tradeId");
        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void onTrade_WhenIdentifierLongerThanColumn_ThrowsInvalidTradeMessageException() {
        String longId = "T".repeat(65);
        String message = VALID_MESSAGE.replace("\"tradeId\":\"T1\"", "\"tradeId\":\"" + longId + "\"");

        assertThatThrownBy(() -> consumer.onTrade(new ConsumerRecord<>("trades", 3, 42L, longId, message)))
                .isInstanceOf(InvalidTradeMessageException.class)
                .hasMessageContaining("tradeId");
        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void onTrade_WhenRecordKeyDiffersFromTradeId_ThrowsInvalidTradeMessageException() {
        var record = new ConsumerRecord<>("trades", 3, 42L, "T9", VALID_MESSAGE);

        assertThatThrownBy(() -> consumer.onTrade(record))
                .isInstanceOf(InvalidTradeMessageException.class);
        verify(tradeProcessingService, never()).process(any());
    }

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("trades", 3, 42L, "T1", value);
    }
}
