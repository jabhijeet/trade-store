package com.tradestore.domain;

/**
 * Optional filters for listing trades. A {@code null} field means "no filter".
 */
public record TradeFilter(String bookId, String counterPartyId, Boolean expired) {
}
