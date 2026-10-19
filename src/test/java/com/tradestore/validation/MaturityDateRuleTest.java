package com.tradestore.validation;

import static com.tradestore.TradeFixtures.TODAY;
import static com.tradestore.TradeFixtures.incoming;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tradestore.exception.MaturityDateInPastException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MaturityDateRuleTest {

    private final MaturityDateRule rule = new MaturityDateRule();

    @Test
    void validate_WhenMaturityDateBeforeToday_ThrowsMaturityDateInPastException() {
        var trade = incoming("T1", 1, TODAY.minusDays(1));

        assertThatThrownBy(() -> rule.validate(trade, Optional.empty(), TODAY))
                .isInstanceOf(MaturityDateInPastException.class)
                .hasMessageContaining("T1")
                .hasMessageContaining(TODAY.minusDays(1).toString());
    }

    @Test
    void validate_WhenMaturityDateIsToday_Accepts() {
        var trade = incoming("T1", 1, TODAY);

        assertThatCode(() -> rule.validate(trade, Optional.empty(), TODAY)).doesNotThrowAnyException();
    }

    @Test
    void validate_WhenMaturityDateAfterToday_Accepts() {
        var trade = incoming("T1", 1, TODAY.plusDays(1));

        assertThatCode(() -> rule.validate(trade, Optional.empty(), TODAY)).doesNotThrowAnyException();
    }
}
