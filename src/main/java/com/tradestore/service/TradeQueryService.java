package com.tradestore.service;

import com.tradestore.domain.Trade;
import com.tradestore.domain.TradeFilter;
import com.tradestore.domain.TradeHistoryEvent;
import com.tradestore.persistence.mongo.TradeEventDocument;
import com.tradestore.persistence.mongo.TradeEventMongoRepository;
import com.tradestore.persistence.sql.TradeEntity;
import com.tradestore.persistence.sql.TradeJpaRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
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
        return tradeRepository.findById(tradeId).map(TradeEntity::toDomain);
    }

    public Page<Trade> findTrades(TradeFilter filter, Pageable pageable) {
        List<Specification<TradeEntity>> conditions = new ArrayList<>();
        if (filter.bookId() != null) {
            conditions.add(attributeEquals("bookId", filter.bookId()));
        }
        if (filter.counterPartyId() != null) {
            conditions.add(attributeEquals("counterPartyId", filter.counterPartyId()));
        }
        if (filter.expired() != null) {
            conditions.add(attributeEquals("expired", filter.expired()));
        }
        return tradeRepository.findAll(Specification.allOf(conditions), pageable).map(TradeEntity::toDomain);
    }

    public Page<TradeHistoryEvent> findHistory(String tradeId, Pageable pageable) {
        return tradeEventRepository.findByTradeIdOrderByOccurredAtAsc(tradeId, pageable)
                .map(TradeQueryService::toHistoryEvent);
    }

    private static Specification<TradeEntity> attributeEquals(String attribute, Object value) {
        return (root, query, builder) -> builder.equal(root.get(attribute), value);
    }

    private static TradeHistoryEvent toHistoryEvent(TradeEventDocument document) {
        return new TradeHistoryEvent(document.id(), document.tradeId(), document.version(), document.type(),
                document.source(), document.reason(), document.occurredAt(), document.payload());
    }
}
