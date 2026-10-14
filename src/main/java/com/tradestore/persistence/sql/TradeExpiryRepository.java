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

    private static final String EXPIRE_MATURED_TRADES = """
            WITH matured AS (
                SELECT trade_id
                FROM trades
                WHERE expired = FALSE
                  AND maturity_date < :today
                ORDER BY maturity_date, trade_id
                LIMIT :batchSize
                FOR UPDATE
            )
            UPDATE trades t
            SET expired = TRUE,
                updated_at = now()
            FROM matured
            WHERE t.trade_id = matured.trade_id
            RETURNING t.trade_id, t.version, t.counter_party_id, t.book_id, t.maturity_date, t.created_date,
                      t.expired
            """;

    private final JdbcClient jdbcClient;

    /**
     * Marks up to {@code batchSize} trades with {@code maturity_date < today} and {@code expired = false} as expired.
     * Rows are locked before the update, so a trade being processed concurrently is not expired mid-update.
     *
     * @return the trades that were marked expired by this call
     */
    public List<Trade> expireMaturedTrades(LocalDate today, int batchSize) {
        return jdbcClient.sql(EXPIRE_MATURED_TRADES)
                .param("today", today)
                .param("batchSize", batchSize)
                .query((rs, rowNum) -> new Trade(
                        rs.getString("trade_id"),
                        rs.getInt("version"),
                        rs.getString("counter_party_id"),
                        rs.getString("book_id"),
                        rs.getObject("maturity_date", LocalDate.class),
                        rs.getObject("created_date", LocalDate.class),
                        rs.getBoolean("expired")))
                .list();
    }
}
