package com.tradestore.validation;

import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.Trade;
import java.time.LocalDate;
import java.util.Optional;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Rejects a trade whose maturity date is earlier than today. A maturity date equal to today is accepted.
 */
@Component
@Order(1)
public class MaturityDateRule implements TradeValidationRule {

    @Override
    public void validate(IncomingTrade incoming, Optional<Trade> current, LocalDate today) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
