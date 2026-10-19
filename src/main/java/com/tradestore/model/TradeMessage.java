package com.tradestore.model;

import java.time.LocalDate;

/**
 * JSON contract of a message on the {@code trades} topic.
 *
 * @param schemaVersion version of this message contract; see {@link #SUPPORTED_SCHEMA_VERSION}
 * @param maturityDate  ISO-8601 date, for example {@code 2030-05-20}
 */
public record TradeMessage(
        Integer schemaVersion,
        String tradeId,
        Integer version,
        String counterPartyId,
        String bookId,
        LocalDate maturityDate) {

    public static final int SUPPORTED_SCHEMA_VERSION = 1;
}
