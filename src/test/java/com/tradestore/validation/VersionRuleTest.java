package com.tradestore.validation;

import static com.tradestore.TradeFixtures.TODAY;
import static com.tradestore.TradeFixtures.incoming;
import static com.tradestore.TradeFixtures.stored;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tradestore.exception.LowerVersionTradeException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class VersionRuleTest {

    private final VersionRule rule = new VersionRule();

    @Test
    void validate_WhenNoStoredTrade_Accepts() {
        assertThatCode(() -> rule.validate(incoming("T1", 1), Optional.empty(), TODAY))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_WhenVersionHigherThanStored_Accepts() {
        assertThatCode(() -> rule.validate(incoming("T2", 2), Optional.of(stored("T2", 1)), TODAY))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_WhenVersionSameAsStored_Accepts() {
        assertThatCode(() -> rule.validate(incoming("T2", 2), Optional.of(stored("T2", 2)), TODAY))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_WhenVersionLowerThanStored_ThrowsLowerVersionTradeException() {
        assertThatThrownBy(() -> rule.validate(incoming("T2", 1), Optional.of(stored("T2", 2)), TODAY))
                .isInstanceOf(LowerVersionTradeException.class)
                .hasMessageContaining("T2")
                .hasMessageContaining("version 1")
                .hasMessageContaining("stored version 2");
    }
}
