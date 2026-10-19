package com.tradestore.persistence.sql.entity;

import com.tradestore.enums.MessageOutcome;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Records every processed message id with its outcome, so redelivered messages take effect only once.
 */
@Entity
@Table(name = "processed_message")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ProcessedMessageEntity {

    @Id
    @EqualsAndHashCode.Include
    @Column(name = "message_id", nullable = false, updatable = false)
    private String messageId;

    @Column(name = "trade_id", nullable = false)
    private String tradeId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private MessageOutcome outcome;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
