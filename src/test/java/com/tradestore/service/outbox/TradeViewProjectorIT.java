package com.tradestore.service.outbox;

import static com.tradestore.TradeFixtures.FUTURE_MATURITY;
import static com.tradestore.TradeFixtures.TODAY;
import static org.assertj.core.api.Assertions.assertThat;

import com.tradestore.TestcontainersConfiguration;
import com.tradestore.domain.Trade;
import com.tradestore.enums.TradeEventType;
import com.tradestore.persistence.mongo.document.TradeViewDocument;
import com.tradestore.persistence.mongo.repository.TradeViewMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "trade-store.scheduling.enabled=false")
class TradeViewProjectorIT {

    @Autowired
    private TradeViewProjector projector;
    @Autowired
    private TradeViewMongoRepository tradeViewRepository;
    @Autowired
    private JsonMapper jsonMapper;

    @BeforeEach
    void setUp() {
        tradeViewRepository.deleteAll();
    }

    @Test
    void apply_CreatedThenUpdated_StoresLatestVersion() {
        apply(TradeEventType.CREATED, trade(1, "CP-1"));
        apply(TradeEventType.UPDATED, trade(2, "CP-2"));

        assertThat(view()).isEqualTo(new TradeViewDocument("T1", 2, "CP-2", "B1", FUTURE_MATURITY, TODAY, false));
    }

    @Test
    void apply_WhenEventIsOlderThanView_LeavesViewUnchanged() {
        apply(TradeEventType.UPDATED, trade(2, "CP-2"));

        apply(TradeEventType.CREATED, trade(1, "CP-1"));

        assertThat(view().version()).isEqualTo(2);
        assertThat(view().counterPartyId()).isEqualTo("CP-2");
    }

    @Test
    void apply_WhenSameVersionReplaced_OverwritesView() {
        apply(TradeEventType.CREATED, trade(1, "CP-1"));

        apply(TradeEventType.REPLACED, trade(1, "CP-9"));

        assertThat(view().counterPartyId()).isEqualTo("CP-9");
    }

    @Test
    void apply_WhenEventRedelivered_IsIdempotent() {
        apply(TradeEventType.CREATED, trade(1, "CP-1"));
        apply(TradeEventType.CREATED, trade(1, "CP-1"));

        assertThat(tradeViewRepository.count()).isEqualTo(1);
        assertThat(view().version()).isEqualTo(1);
    }

    @Test
    void apply_Expired_MarksOnlyTheExpiredVersion() {
        apply(TradeEventType.CREATED, trade(2, "CP-1"));

        apply(TradeEventType.EXPIRED, trade(1, "CP-1"));
        assertThat(view().expired()).isFalse();

        apply(TradeEventType.EXPIRED, trade(2, "CP-1"));
        assertThat(view().expired()).isTrue();
    }

    @Test
    void apply_Rejected_DoesNotCreateView() {
        projector.apply(TradeEventType.REJECTED, """
                {"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2020-05-20"}""");

        assertThat(tradeViewRepository.count()).isZero();
    }

    private void apply(TradeEventType type, Trade trade) {
        projector.apply(type, jsonMapper.writeValueAsString(trade));
    }

    private TradeViewDocument view() {
        return tradeViewRepository.findById("T1").orElseThrow();
    }

    private static Trade trade(int version, String counterPartyId) {
        return new Trade("T1", version, counterPartyId, "B1", FUTURE_MATURITY, TODAY, false);
    }
}
