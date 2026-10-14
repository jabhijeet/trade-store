package com.tradestore.service;

import static com.tradestore.TradeFixtures.TODAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tradestore.TestcontainersConfiguration;
import com.tradestore.domain.EventSource;
import com.tradestore.domain.IncomingTrade;
import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.MaturityDateInPastException;
import com.tradestore.domain.TradeCommand;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

/**
 * A rejection is recorded in the processing transaction itself, so processing a trade never needs more than one
 * pool connection. With a single-connection pool, a rejection that needed a second connection would wait for the
 * connection timeout and fail instead of being recorded.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "spring.datasource.hikari.maximum-pool-size=1",
        "spring.datasource.hikari.minimum-idle=1",
        "trade-store.scheduling.enabled=false"
})
class RejectionSingleConnectionIT {

    @Autowired
    private TradeProcessingService tradeProcessingService;
    @Autowired
    private JdbcClient jdbcClient;

    @BeforeEach
    void cleanUp() {
        jdbcClient.sql("DELETE FROM outbox_event").update();
        jdbcClient.sql("DELETE FROM processed_message").update();
        jdbcClient.sql("DELETE FROM trades").update();
    }

    @Test
    void lowerVersion_IsRejectedAndRecordedWithASingleConnection() {
        tradeProcessingService.process(command("R1", 2, TODAY.plusYears(1)));

        assertThatThrownBy(() -> tradeProcessingService.process(command("R1", 1, TODAY.plusYears(1))))
                .isInstanceOf(LowerVersionTradeException.class);

        assertThat(outcomes("R1")).containsExactlyInAnyOrder("CREATED", "REJECTED_LOWER_VERSION");
        assertThat(eventTypes("R1")).containsExactlyInAnyOrder("CREATED", "REJECTED");
        assertThat(storedVersion("R1")).isEqualTo(2);
    }

    @Test
    void maturityDateInPast_IsRejectedAndRecordedWithoutStoringTheTrade() {
        assertThatThrownBy(() -> tradeProcessingService.process(command("R2", 1, TODAY.minusDays(1))))
                .isInstanceOf(MaturityDateInPastException.class);

        assertThat(outcomes("R2")).containsExactly("REJECTED_MATURITY_IN_PAST");
        assertThat(eventTypes("R2")).containsExactly("REJECTED");
        assertThat(jdbcClient.sql("SELECT count(*) FROM trades WHERE trade_id = 'R2'").query(Long.class).single())
                .isZero();
    }

    private static TradeCommand command(String tradeId, int version, LocalDate maturity) {
        return new TradeCommand("rest:" + UUID.randomUUID(), EventSource.REST,
                new IncomingTrade(tradeId, version, "CP-1", "B1", maturity));
    }

    private List<String> outcomes(String tradeId) {
        return jdbcClient.sql("SELECT outcome FROM processed_message WHERE trade_id = :id")
                .param("id", tradeId).query(String.class).list();
    }

    private List<String> eventTypes(String tradeId) {
        return jdbcClient.sql("SELECT type FROM outbox_event WHERE trade_id = :id")
                .param("id", tradeId).query(String.class).list();
    }

    private int storedVersion(String tradeId) {
        return jdbcClient.sql("SELECT version FROM trades WHERE trade_id = :id")
                .param("id", tradeId).query(Integer.class).single();
    }
}
