package com.tradestore.domain;

import java.time.LocalDate;

/**
 * The current, stored version of a trade.
 */
public record Trade(
        String tradeId,
        int version,
        String counterPartyId,
        String bookId,
        LocalDate maturityDate,
        LocalDate createdDate,
        boolean expired) {
}
