package com.tradestore.adapter.rest;

import com.tradestore.service.TradeQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only access to current trades and their history.
 */
@RestController
@RequestMapping("/api/v1/trades")
@RequiredArgsConstructor
public class TradeQueryController {

    private final TradeQueryService tradeQueryService;

    @GetMapping
    public PageResponse<TradeResponse> getTrades(
            @RequestParam(required = false) String bookId,
            @RequestParam(required = false) String counterPartyId,
            @RequestParam(required = false) Boolean expired,
            @PageableDefault(size = 50, sort = "tradeId") Pageable pageable) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @GetMapping("/{tradeId}")
    public TradeResponse getTrade(@PathVariable String tradeId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @GetMapping("/{tradeId}/history")
    public PageResponse<TradeEventResponse> getTradeHistory(
            @PathVariable String tradeId,
            @PageableDefault(size = 50) Pageable pageable) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
