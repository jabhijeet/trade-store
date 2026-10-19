package com.tradestore.model;

import com.tradestore.domain.IncomingTrade;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.LocalDate;

/**
 * Request body of {@code POST /api/v1/trades}. Dates are ISO-8601, for example {@code 2030-05-20}.
 */
public record TradeRequest(
        @NotBlank @Size(max = 64) String tradeId,
        @NotNull @Min(1) Integer version,
        @NotBlank @Size(max = 64) String counterPartyId,
        @NotBlank @Size(max = 64) String bookId,
        @NotNull LocalDate maturityDate) {

    public IncomingTrade toIncomingTrade() {
        return new IncomingTrade(tradeId, version, counterPartyId, bookId, maturityDate);
    }
}
