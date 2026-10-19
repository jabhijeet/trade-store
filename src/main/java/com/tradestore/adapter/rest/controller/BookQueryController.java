package com.tradestore.adapter.rest.controller;

import com.tradestore.model.PageResponse;
import com.tradestore.model.TradeResponse;
import com.tradestore.service.query.TradeQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only access to the trades of a book, served from the MongoDB {@code trade_views} projection. Eventually
 * consistent with {@code GET /api/v1/trades?bookId=}, which reads PostgreSQL.
 */
@RestController
@RequestMapping("/api/v1/books")
@RequiredArgsConstructor
public class BookQueryController {

    private final TradeQueryService tradeQueryService;

    @GetMapping("/{bookId}/trades")
    public PageResponse<TradeResponse> getBookTrades(
            @PathVariable String bookId,
            @PageableDefault(size = 50, sort = "tradeId") Pageable pageable) {
        return PageResponse.from(tradeQueryService.findTradesByBook(bookId, pageable), TradeResponse::from);
    }
}
