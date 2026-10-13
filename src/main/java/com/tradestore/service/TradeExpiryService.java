package com.tradestore.service;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.persistence.sql.TradeExpiryRepository;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Expires one batch of matured trades per transaction, appending an {@code EXPIRED} event for each.
 */
@Service
@RequiredArgsConstructor
public class TradeExpiryService {

    private final TradeExpiryRepository tradeExpiryRepository;
    private final OutboxWriter outboxWriter;
    private final TradeStoreProperties properties;

    /**
     * @return the number of trades expired in this batch; {@code 0} when none are left
     */
    @Transactional
    public int expireNextBatch(LocalDate today) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
