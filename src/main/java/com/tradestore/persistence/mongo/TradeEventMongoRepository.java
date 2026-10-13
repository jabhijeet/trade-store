package com.tradestore.persistence.mongo;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TradeEventMongoRepository extends MongoRepository<TradeEventDocument, String> {

    Page<TradeEventDocument> findByTradeIdOrderByOccurredAtAsc(String tradeId, Pageable pageable);
}
