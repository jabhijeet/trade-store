package com.tradestore.persistence.mongo;

import java.time.LocalDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Read-optimised projection of a trade's current state, keyed by trade id and indexed by book. Maintained by the
 * outbox relay from trade events ({@link TradeViewProjector}), so it is eventually consistent with PostgreSQL.
 * The collection and its index are created by the Liquibase MongoDB changelog.
 */
@Document(collection = TradeViewDocument.COLLECTION)
public record TradeViewDocument(
        @Id String tradeId,
        int version,
        String counterPartyId,
        String bookId,
        LocalDate maturityDate,
        LocalDate createdDate,
        boolean expired) {

    public static final String COLLECTION = "trade_views";
}
