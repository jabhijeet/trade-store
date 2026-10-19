package com.tradestore.exception;

import com.tradestore.enums.MessageOutcome;

/**
 * Raised when a trade arrives with a lower version than the stored version.
 */
public class LowerVersionTradeException extends TradeRejectedException {

    public LowerVersionTradeException(String tradeId, int version, int storedVersion) {
        super(tradeId, version, "Trade %s version %d is lower than stored version %d"
                .formatted(tradeId, version, storedVersion));
    }

    @Override
    public MessageOutcome outcome() {
        return MessageOutcome.REJECTED_LOWER_VERSION;
    }
}
