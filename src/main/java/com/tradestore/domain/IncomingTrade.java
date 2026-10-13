package com.tradestore.domain;

import java.time.LocalDate;

/**
 * A trade as received from a producer, before validation. The store assigns the created date and expiry flag.
 */
public record IncomingTrade(
        String tradeId,
        int version,
        String counterPartyId,
        String bookId,
        LocalDate maturityDate) {
}
