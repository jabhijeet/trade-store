package com.tradestore.persistence.sql;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * Outbox events and their delivery state. Each destination has its own pending query (backed by its own partial
 * index) and its own delivery marker, so a backlog for one destination never delays the other.
 */
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    /**
     * Events not yet delivered to MongoDB, oldest first.
     */
    @Query("""
            select e from OutboxEventEntity e
            where e.mongoDeliveredAt is null
            order by e.createdAt
            """)
    List<OutboxEventEntity> findPendingMongoDelivery(Limit limit);

    /**
     * {@code EXPIRED} events not yet published to the {@code trade-status} topic, oldest first.
     */
    @Query("""
            select e from OutboxEventEntity e
            where e.type = com.tradestore.domain.TradeEventType.EXPIRED
              and e.kafkaDeliveredAt is null
            order by e.createdAt
            """)
    List<OutboxEventEntity> findPendingKafkaDelivery(Limit limit);

    @Query("select count(e) from OutboxEventEntity e where e.mongoDeliveredAt is null")
    long countPendingMongoDelivery();

    @Query("""
            select count(e) from OutboxEventEntity e
            where e.type = com.tradestore.domain.TradeEventType.EXPIRED
              and e.kafkaDeliveredAt is null
            """)
    long countPendingKafkaDelivery();

    /**
     * Sets only the MongoDB delivery time, so it cannot overwrite a concurrent Kafka delivery marker.
     */
    @Transactional
    @Modifying
    @Query("update OutboxEventEntity e set e.mongoDeliveredAt = :deliveredAt where e.id = :id")
    void markMongoDelivered(@Param("id") UUID id, @Param("deliveredAt") Instant deliveredAt);

    /**
     * Sets only the Kafka delivery time, so it cannot overwrite a concurrent MongoDB delivery marker.
     */
    @Transactional
    @Modifying
    @Query("update OutboxEventEntity e set e.kafkaDeliveredAt = :deliveredAt where e.id = :id")
    void markKafkaDelivered(@Param("id") UUID id, @Param("deliveredAt") Instant deliveredAt);
}
