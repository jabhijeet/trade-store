package com.tradestore.persistence.sql;

import com.tradestore.domain.Trade;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Current version of a trade. One row per trade id.
 */
@Entity
@Table(name = "trades")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class TradeEntity {

    @Id
    @EqualsAndHashCode.Include
    @Column(name = "trade_id", nullable = false, updatable = false)
    private String tradeId;

    @Column(nullable = false)
    private int version;

    @Column(name = "counter_party_id", nullable = false)
    private String counterPartyId;

    @Column(name = "book_id", nullable = false)
    private String bookId;

    @Column(name = "maturity_date", nullable = false)
    private LocalDate maturityDate;

    @Column(name = "created_date", nullable = false)
    private LocalDate createdDate;

    @Column(nullable = false)
    private boolean expired;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public Trade toDomain() {
        return new Trade(tradeId, version, counterPartyId, bookId, maturityDate, createdDate, expired);
    }
}
