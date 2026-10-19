package com.tradestore.model;

import com.tradestore.domain.Trade;
import java.time.LocalDate;

public record TradeResponse(
        String tradeId,
        int version,
        String counterPartyId,
        String bookId,
        LocalDate maturityDate,
        LocalDate createdDate,
        boolean expired) {

    public static TradeResponse from(Trade trade) {
        return new TradeResponse(trade.tradeId(), trade.version(), trade.counterPartyId(), trade.bookId(),
                trade.maturityDate(), trade.createdDate(), trade.expired());
    }
}
