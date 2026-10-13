package com.tradestore.domain;

import lombok.Getter;

/**
 * Base type for business rejections. These are never retried.
 */
@Getter
public abstract class TradeRejectedException extends RuntimeException {

    private final String tradeId;
    private final int version;

    protected TradeRejectedException(String tradeId, int version, String message) {
        super(message);
        this.tradeId = tradeId;
        this.version = version;
    }

    /**
     * The outcome to record for the rejected message.
     */
    public abstract MessageOutcome outcome();
}
