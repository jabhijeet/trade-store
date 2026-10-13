package com.tradestore.domain;

import java.time.LocalDate;

/**
 * Raised when a trade arrives with a maturity date earlier than today.
 */
public class MaturityDateInPastException extends TradeRejectedException {

    public MaturityDateInPastException(String tradeId, int version, LocalDate maturityDate, LocalDate today) {
        super(tradeId, version, "Trade %s version %d has maturity date %s earlier than today %s"
                .formatted(tradeId, version, maturityDate, today));
    }

    @Override
    public MessageOutcome outcome() {
        return MessageOutcome.REJECTED_MATURITY_IN_PAST;
    }
}
