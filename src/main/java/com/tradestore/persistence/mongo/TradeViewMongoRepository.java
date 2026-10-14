package com.tradestore.persistence.mongo;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TradeViewMongoRepository extends MongoRepository<TradeViewDocument, String> {

    Page<TradeViewDocument> findByBookId(String bookId, Pageable pageable);
}
