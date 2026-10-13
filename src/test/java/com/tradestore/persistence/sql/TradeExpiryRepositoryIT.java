package com.tradestore.persistence.sql;

import static com.tradestore.TradeFixtures.TODAY;
import static org.assertj.core.api.Assertions.assertThat;

import com.tradestore.TestcontainersConfiguration;
import com.tradestore.domain.Trade;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "trade-store.scheduling.enabled=false")
class TradeExpiryRepositoryIT {

    @Autowired
    private TradeExpiryRepository tradeExpiryRepository;
    @Autowired
    private TradeJpaRepository tradeRepository;

    @BeforeEach
    void setUp() {
        tradeRepository.deleteAll();
    }

    @Test
    void expireMaturedTrades_MarksOnlyTradesMaturedBeforeToday() {
        save("MATURED", TODAY.minusDays(1), false);
        save("MATURES_TODAY", TODAY, false);
        save("FUTURE", TODAY.plusDays(1), false);
        save("ALREADY_EXPIRED", TODAY.minusYears(1), true);

        List<Trade> expired = tradeExpiryRepository.expireMaturedTrades(TODAY, 100);

        assertThat(expired).extracting(Trade::tradeId).containsExactly("MATURED");
        assertThat(expired.getFirst().expired()).isTrue();
        assertThat(tradeRepository.findById("MATURED")).get().extracting(TradeEntity::isExpired).isEqualTo(true);
        assertThat(tradeRepository.findById("MATURES_TODAY")).get().extracting(TradeEntity::isExpired)
                .isEqualTo(false);
        assertThat(tradeRepository.findById("FUTURE")).get().extracting(TradeEntity::isExpired).isEqualTo(false);
    }

    @Test
    void expireMaturedTrades_RespectsBatchSizeAndIsSafeToRerun() {
        save("M1", TODAY.minusDays(3), false);
        save("M2", TODAY.minusDays(2), false);
        save("M3", TODAY.minusDays(1), false);

        assertThat(tradeExpiryRepository.expireMaturedTrades(TODAY, 2)).hasSize(2);
        assertThat(tradeExpiryRepository.expireMaturedTrades(TODAY, 2)).hasSize(1);
        assertThat(tradeExpiryRepository.expireMaturedTrades(TODAY, 2)).isEmpty();
    }

    private void save(String tradeId, LocalDate maturityDate, boolean expired) {
        tradeRepository.save(new TradeEntity(tradeId, 1, "CP-1", "B1", maturityDate, TODAY.minusYears(2), expired,
                Instant.parse("2026-01-01T00:00:00Z")));
    }
}
