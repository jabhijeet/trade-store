package com.tradestore.persistence.sql;

import com.tradestore.domain.Trade;
import java.time.LocalDate;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Marks matured trades as expired with a single conditional update, which is safe to rerun.
 */
@Repository
@RequiredArgsConstructor
public class TradeExpiryRepository {

    private final JdbcClient jdbcClient;

    /**
     * Marks up to {@code batchSize} trades with {@code maturity_date < today} and {@code expired = false} as expired.
     *
     * @return the trades that were marked expired by this call
     */
    public List<Trade> expireMaturedTrades(LocalDate today, int batchSize) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
