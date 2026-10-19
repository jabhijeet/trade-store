package com.tradestore.adapter.rest.controller;

import static com.tradestore.TradeFixtures.stored;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tradestore.adapter.rest.RestSliceTestConfiguration;
import com.tradestore.enums.EventSource;
import com.tradestore.enums.TradeEventType;
import com.tradestore.domain.TradeFilter;
import com.tradestore.domain.TradeHistoryEvent;
import com.tradestore.service.query.TradeQueryService;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.JwtRequestPostProcessor;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest({TradeQueryController.class, BookQueryController.class})
@Import(RestSliceTestConfiguration.class)
class TradeQueryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TradeQueryService tradeQueryService;

    @Test
    void getTrade_WhenFound_Returns200WithTrade() throws Exception {
        when(tradeQueryService.findTrade("T1")).thenReturn(Optional.of(stored("T1", 1)));

        mockMvc.perform(get("/api/v1/trades/T1").with(reader()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tradeId").value("T1"))
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.expired").value(false));
    }

    @Test
    void getTrade_WhenNotFound_Returns404() throws Exception {
        when(tradeQueryService.findTrade("T9")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/trades/T9").with(reader()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void getTrades_PassesFiltersAndPagingAndReturnsPage() throws Exception {
        when(tradeQueryService.findTrades(any(), any()))
                .thenReturn(new PageImpl<>(List.of(stored("T1", 1)), PageRequest.of(0, 10), 1));

        mockMvc.perform(get("/api/v1/trades").with(reader())
                        .param("bookId", "B1")
                        .param("expired", "false")
                        .param("page", "0")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].tradeId").value("T1"))
                .andExpect(jsonPath("$.page").value(0))
                .andExpect(jsonPath("$.size").value(10))
                .andExpect(jsonPath("$.totalElements").value(1));

        var filter = ArgumentCaptor.forClass(TradeFilter.class);
        var pageable = ArgumentCaptor.forClass(Pageable.class);
        verify(tradeQueryService).findTrades(filter.capture(), pageable.capture());
        assertThat(filter.getValue()).isEqualTo(new TradeFilter("B1", null, false));
        assertThat(pageable.getValue().getPageSize()).isEqualTo(10);
    }

    @Test
    void getTradeHistory_ReturnsEventsOfTrade() throws Exception {
        var event = new TradeHistoryEvent("e1", "T2", 1, TradeEventType.CREATED, EventSource.KAFKA, null,
                Instant.parse("2026-10-13T09:00:00Z"), Map.of("bookId", "B1"));
        when(tradeQueryService.findHistory(eq("T2"), any()))
                .thenReturn(new PageImpl<>(List.of(event), PageRequest.of(0, 50), 1));

        mockMvc.perform(get("/api/v1/trades/T2/history").with(reader()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].id").value("e1"))
                .andExpect(jsonPath("$.content[0].type").value("CREATED"))
                .andExpect(jsonPath("$.content[0].payload.bookId").value("B1"));
    }

    @Test
    void getBookTrades_ReturnsTradesOfBookFromProjection() throws Exception {
        when(tradeQueryService.findTradesByBook(eq("B1"), any()))
                .thenReturn(new PageImpl<>(List.of(stored("T1", 1)), PageRequest.of(0, 50), 1));

        mockMvc.perform(get("/api/v1/books/B1/trades").with(reader()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].tradeId").value("T1"))
                .andExpect(jsonPath("$.content[0].bookId").value("B1"))
                .andExpect(jsonPath("$.totalElements").value(1));
    }

    @Test
    void getTrade_WhenNoToken_Returns401() throws Exception {
        mockMvc.perform(get("/api/v1/trades/T1")).andExpect(status().isUnauthorized());
    }

    @Test
    void getTrade_WhenTokenLacksReadScope_Returns403() throws Exception {
        mockMvc.perform(get("/api/v1/trades/T1")
                        .with(jwt().authorities(new SimpleGrantedAuthority("SCOPE_trades:write"))))
                .andExpect(status().isForbidden());
    }

    private static JwtRequestPostProcessor reader() {
        return jwt().authorities(new SimpleGrantedAuthority("SCOPE_trades:read"));
    }
}
