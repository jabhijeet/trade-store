package com.tradestore.persistence.sql;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.TradeEventType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * A trade event committed in the same transaction as the state change it describes, awaiting delivery to
 * MongoDB and, for {@link TradeEventType#EXPIRED} events, to Kafka. Delivery times are set only through
 * {@link OutboxEventRepository}'s per-destination updates.
 */
@Entity
@Table(name = "outbox_event")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class OutboxEventEntity {

    @Id
    @EqualsAndHashCode.Include
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "trade_id", nullable = false, updatable = false)
    private String tradeId;

    @Column(nullable = false, updatable = false)
    private int version;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, updatable = false)
    private TradeEventType type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, updatable = false)
    private EventSource source;

    @Column(updatable = false)
    private String reason;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, updatable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "mongo_delivered_at")
    private Instant mongoDeliveredAt;

    @Column(name = "kafka_delivered_at")
    private Instant kafkaDeliveredAt;
}
