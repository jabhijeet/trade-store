package com.tradestore.service.outbox;

import com.tradestore.domain.Trade;
import com.tradestore.enums.TradeEventType;
import com.tradestore.persistence.mongo.document.TradeViewDocument;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Applies a trade event to the {@code trade_views} projection. Every update is conditional on the version, so
 * applying an event twice, or an event older than the stored view, never moves the view backwards:
 * <ul>
 *   <li>{@code CREATED}, {@code UPDATED}, {@code REPLACED} – upsert the trade if the view's version is the same or
 *       lower. When the view already holds a higher version, the upsert's insert collides on {@code _id} and the
 *       stale event is ignored;</li>
 *   <li>{@code EXPIRED} – mark the view expired only if it still holds the expired version;</li>
 *   <li>{@code REJECTED} – no change, a rejected trade was never stored.</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class TradeViewProjector {

    private final MongoTemplate mongoTemplate;
    private final JsonMapper jsonMapper;

    /**
     * @param payload the event's JSON payload; for every event type except {@code REJECTED} it is a {@link Trade}
     */
    public void apply(TradeEventType type, String payload) {
        switch (type) {
            case CREATED, UPDATED, REPLACED -> upsertIfNotOlder(jsonMapper.readValue(payload, Trade.class));
            case EXPIRED -> markExpired(jsonMapper.readValue(payload, Trade.class));
            case REJECTED -> {
            }
        }
    }

    private void upsertIfNotOlder(Trade trade) {
        var query = Query.query(Criteria.where("_id").is(trade.tradeId()).and("version").lte(trade.version()));
        var update = new Update()
                .set("version", trade.version())
                .set("counterPartyId", trade.counterPartyId())
                .set("bookId", trade.bookId())
                .set("maturityDate", trade.maturityDate())
                .set("createdDate", trade.createdDate())
                .set("expired", trade.expired());
        try {
            mongoTemplate.upsert(query, update, TradeViewDocument.class);
        } catch (DuplicateKeyException newerVersionStored) {
            // The view already holds a higher version: this event is stale.
        }
    }

    private void markExpired(Trade trade) {
        var query = Query.query(Criteria.where("_id").is(trade.tradeId()).and("version").is(trade.version()));
        mongoTemplate.updateFirst(query, Update.update("expired", true), TradeViewDocument.class);
    }
}
