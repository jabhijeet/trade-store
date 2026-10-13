package com.tradestore.service;

import com.tradestore.domain.Trade;
import com.tradestore.domain.TradeFilter;
import com.tradestore.domain.TradeHistoryEvent;
import com.tradestore.persistence.mongo.TradeEventMongoRepository;
import com.tradestore.persistence.sql.TradeJpaRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Read side: current trades from PostgreSQL, history from MongoDB.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class TradeQueryService {

    private final TradeJpaRepository tradeRepository;
    private final TradeEventMongoRepository tradeEventRepository;

    public Optional<Trade> findTrade(String tradeId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Page<Trade> findTrades(TradeFilter filter, Pageable pageable) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public Page<TradeHistoryEvent> findHistory(String tradeId, Pageable pageable) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
