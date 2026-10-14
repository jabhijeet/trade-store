package com.tradestore.validation;

import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.Trade;
import com.tradestore.domain.TradeRejectedException;
import java.time.LocalDate;
import java.util.Optional;

/**
 * A business rule applied to every incoming trade. Rules run inside the processing transaction, after the stored
 * trade has been locked, in {@link org.springframework.core.annotation.Order} sequence.
 * Add a rule by implementing this interface and registering it as a Spring bean.
 */
public interface TradeValidationRule {

    /**
     * @param incoming the trade being processed
     * @param current  the stored version of the trade, if any
     * @param today    the current business date
     * @throws TradeRejectedException if the trade breaks the rule
     */
    void validate(IncomingTrade incoming, Optional<Trade> current, LocalDate today);
}
