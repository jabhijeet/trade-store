package com.tradestore.persistence.sql;

import java.time.Instant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessageEntity, String> {

    /**
     * Deletes up to {@code batchSize} rows processed before {@code cutoff}, in one transaction. Uses the
     * {@code processed_at} index.
     *
     * @return the number of rows deleted
     */
    @Transactional
    @Modifying
    @Query(value = """
            DELETE FROM processed_message
            WHERE message_id IN (
                SELECT message_id
                FROM processed_message
                WHERE processed_at < :cutoff
                LIMIT :batchSize
            )
            """, nativeQuery = true)
    int deleteProcessedBefore(@Param("cutoff") Instant cutoff, @Param("batchSize") int batchSize);
}
