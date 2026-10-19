package com.tradestore.persistence.mongo.repository;

import com.tradestore.persistence.mongo.document.TradeViewDocument;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TradeViewMongoRepository extends MongoRepository<TradeViewDocument, String> {

    Page<TradeViewDocument> findByBookId(String bookId, Pageable pageable);
}
