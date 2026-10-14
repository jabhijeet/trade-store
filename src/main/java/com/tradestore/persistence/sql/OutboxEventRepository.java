package com.tradestore.persistence.sql;

import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    /**
     * Events that still have at least one pending destination, oldest first.
     */
    @Query("""
            select e from OutboxEventEntity e
            where e.mongoDeliveredAt is null
               or (e.type = com.tradestore.domain.TradeEventType.EXPIRED and e.kafkaDeliveredAt is null)
            order by e.createdAt
            """)
    List<OutboxEventEntity> findUndelivered(Limit limit);
}
