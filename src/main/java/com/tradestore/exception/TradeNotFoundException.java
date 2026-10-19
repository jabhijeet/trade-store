package com.tradestore.exception;

public class TradeNotFoundException extends RuntimeException {

    public TradeNotFoundException(String tradeId) {
        super("Trade not found: " + tradeId);
    }
}
