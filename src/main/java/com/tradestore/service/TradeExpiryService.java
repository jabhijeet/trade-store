package com.tradestore.service;

import com.tradestore.config.TradeStoreProperties;
import com.tradestore.domain.EventSource;
import com.tradestore.domain.Trade;
import com.tradestore.domain.TradeEventType;
import com.tradestore.persistence.sql.TradeExpiryRepository;
import java.time.LocalDate;
import java.util.List;
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
        List<Trade> expired = tradeExpiryRepository.expireMaturedTrades(today, properties.expiry().batchSize());
        expired.forEach(trade -> outboxWriter.append(trade.tradeId(), trade.version(), TradeEventType.EXPIRED,
                EventSource.SCHEDULER, null, trade));
        return expired.size();
    }
}
