package com.tradestore.adapter.rest;

public class TradeNotFoundException extends RuntimeException {

    public TradeNotFoundException(String tradeId) {
        super("Trade not found: " + tradeId);
    }
}
