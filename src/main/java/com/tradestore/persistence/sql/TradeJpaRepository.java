package com.tradestore.persistence.sql;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface TradeJpaRepository extends JpaRepository<TradeEntity, String>, JpaSpecificationExecutor<TradeEntity> {

    /**
     * Loads a trade and locks its row until the end of the transaction, so the version check and the write
     * cannot interleave with another update to the same trade.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select t from TradeEntity t where t.tradeId = :tradeId")
    Optional<TradeEntity> findByIdForUpdate(@Param("tradeId") String tradeId);
}
