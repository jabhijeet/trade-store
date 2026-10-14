package com.tradestore.validation;

import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.Trade;
import java.time.LocalDate;
import java.util.Optional;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Rejects a trade whose version is lower than the stored version. The same or a higher version is accepted.
 */
@Component
@Order(2)
public class VersionRule implements TradeValidationRule {

    @Override
    public void validate(IncomingTrade incoming, Optional<Trade> current, LocalDate today) {
        current.filter(stored -> incoming.version() < stored.version())
                .ifPresent(stored -> {
                    throw new LowerVersionTradeException(incoming.tradeId(), incoming.version(), stored.version());
                });
    }
}
