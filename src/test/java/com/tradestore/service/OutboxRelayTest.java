package com.tradestore.service;

import static com.tradestore.TradeFixtures.FIXED_CLOCK;
import static com.tradestore.TradeFixtures.properties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.TradeEventType;
import com.tradestore.persistence.mongo.TradeEventDocument;
import com.tradestore.persistence.mongo.TradeEventMongoRepository;
import com.tradestore.persistence.sql.OutboxEventEntity;
import com.tradestore.persistence.sql.OutboxEventRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    private static final String PAYLOAD = """
            {"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2027-10-13"}""";

    @Mock
    private OutboxEventRepository outboxEventRepository;
    @Mock
    private TradeEventMongoRepository tradeEventRepository;
    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxRelay relay;

    @BeforeEach
    void setUp() {
        relay = new OutboxRelay(outboxEventRepository, tradeEventRepository, kafkaTemplate,
                JsonMapper.builder().build(), properties(), FIXED_CLOCK);
    }

    @Test
    void deliverPendingEvents_WhenCreatedEvent_UpsertsToMongoOnlyAndMarksMongoDelivered() {
        OutboxEventEntity event = event(TradeEventType.CREATED);
        when(outboxEventRepository.findUndelivered(Limit.of(200))).thenReturn(List.of(event));

        int read = relay.deliverPendingEvents();

        assertThat(read).isEqualTo(1);
        var captor = ArgumentCaptor.forClass(TradeEventDocument.class);
        verify(tradeEventRepository).save(captor.capture());
        TradeEventDocument document = captor.getValue();
        assertThat(document.id()).isEqualTo(event.getId().toString());
        assertThat(document.tradeId()).isEqualTo("T1");
        assertThat(document.type()).isEqualTo(TradeEventType.CREATED);
        assertThat(document.payload()).containsEntry("bookId", "B1");
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        assertThat(event.getMongoDeliveredAt()).isEqualTo(Instant.now(FIXED_CLOCK));
        verify(outboxEventRepository).save(event);
    }

    @Test
    void deliverPendingEvents_WhenExpiredEvent_DeliversToMongoAndTradeStatusTopic() {
        OutboxEventEntity event = event(TradeEventType.EXPIRED);
        when(outboxEventRepository.findUndelivered(Limit.of(200))).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("trade-status"), eq("T1"), contains("EXPIRED")))
                .thenReturn(CompletableFuture.completedFuture(null));

        relay.deliverPendingEvents();

        verify(tradeEventRepository).save(any(TradeEventDocument.class));
        assertThat(event.getMongoDeliveredAt()).isEqualTo(Instant.now(FIXED_CLOCK));
        assertThat(event.getKafkaDeliveredAt()).isEqualTo(Instant.now(FIXED_CLOCK));
    }

    @Test
    void deliverPendingEvents_WhenMongoFails_StillPublishesToKafkaAndLeavesMongoPending() {
        OutboxEventEntity event = event(TradeEventType.EXPIRED);
        when(outboxEventRepository.findUndelivered(Limit.of(200))).thenReturn(List.of(event));
        when(tradeEventRepository.save(any(TradeEventDocument.class)))
                .thenThrow(new DataAccessResourceFailureException("MongoDB unavailable"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        relay.deliverPendingEvents();

        assertThat(event.getMongoDeliveredAt()).isNull();
        assertThat(event.getKafkaDeliveredAt()).isEqualTo(Instant.now(FIXED_CLOCK));
    }

    @Test
    void deliverPendingEvents_WhenKafkaFails_MarksMongoDeliveredAndLeavesKafkaPending() {
        OutboxEventEntity event = event(TradeEventType.EXPIRED);
        when(outboxEventRepository.findUndelivered(Limit.of(200))).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("Kafka unavailable")));

        relay.deliverPendingEvents();

        assertThat(event.getMongoDeliveredAt()).isEqualTo(Instant.now(FIXED_CLOCK));
        assertThat(event.getKafkaDeliveredAt()).isNull();
    }

    @Test
    void deliverPendingEvents_WhenEventAlreadyDeliveredToMongo_DoesNotUpsertAgain() {
        OutboxEventEntity event = event(TradeEventType.EXPIRED);
        event.setMongoDeliveredAt(Instant.parse("2026-10-13T08:00:00Z"));
        when(outboxEventRepository.findUndelivered(Limit.of(200))).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        relay.deliverPendingEvents();

        verify(tradeEventRepository, never()).save(any(TradeEventDocument.class));
        assertThat(event.getKafkaDeliveredAt()).isEqualTo(Instant.now(FIXED_CLOCK));
    }

    @Test
    void deliverPendingEvents_WhenNothingPending_ReturnsZero() {
        when(outboxEventRepository.findUndelivered(Limit.of(200))).thenReturn(List.of());

        assertThat(relay.deliverPendingEvents()).isZero();
    }

    private static OutboxEventEntity event(TradeEventType type) {
        return new OutboxEventEntity(UUID.randomUUID(), "T1", 1, type, EventSource.KAFKA, null, PAYLOAD,
                Instant.parse("2026-10-13T07:00:00Z"), null, null);
    }
}
