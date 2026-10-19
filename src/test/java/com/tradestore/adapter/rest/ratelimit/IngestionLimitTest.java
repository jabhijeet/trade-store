package com.tradestore.adapter.rest.ratelimit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tradestore.adapter.rest.RestSliceTestConfiguration;
import com.tradestore.adapter.rest.controller.TradeIngestionController;
import com.tradestore.adapter.rest.controller.TradeQueryController;
import com.tradestore.enums.SaveOutcome;
import com.tradestore.observability.TradeMetrics;
import com.tradestore.service.ingestion.TradeProcessingService;
import com.tradestore.service.query.TradeQueryService;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.JwtRequestPostProcessor;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * Rate limiting and request size limits of {@code POST /api/v1/trades}. Each test uses its own client id, because
 * the buckets live in the shared application context. The rate is one request per second, so a second request sent
 * straight after the first is always over the limit.
 */
@WebMvcTest({TradeIngestionController.class, TradeQueryController.class})
@Import(RestSliceTestConfiguration.class)
@TestPropertySource(properties = {
        "trade-store.rest.requests-per-second=1",
        "trade-store.rest.max-request-size=1KB"
})
class IngestionLimitTest {

    private static final String VALID_BODY = """
            {"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2027-05-20"}""";

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private MeterRegistry meterRegistry;

    @MockitoBean
    private TradeProcessingService tradeProcessingService;
    @MockitoBean
    private TradeQueryService tradeQueryService;

    @Test
    void ingestTrade_WhenClientExceedsItsRate_Returns429WithRetryAfterWithoutProcessing() throws Exception {
        when(tradeProcessingService.process(any())).thenReturn(SaveOutcome.CREATED);

        postTrade("client-burst", VALID_BODY).andExpect(status().isCreated());
        postTrade("client-burst", VALID_BODY)
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.status").value(429))
                .andExpect(jsonPath("$.title").value("Rate limit exceeded"));

        verify(tradeProcessingService, times(1)).process(any());
        Assertions.assertThat(meterRegistry.counter(TradeMetrics.RATE_LIMITED).count()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void ingestTrade_WhenAnotherClientIsThrottled_StillAdmitsThisClient() throws Exception {
        when(tradeProcessingService.process(any())).thenReturn(SaveOutcome.CREATED);
        for (int i = 0; i < 2; i++) {
            postTrade("client-noisy", VALID_BODY);
        }

        postTrade("client-quiet", VALID_BODY).andExpect(status().isCreated());
    }

    @Test
    void readEndpoints_AreNotRateLimited() throws Exception {
        when(tradeQueryService.findTrade("T1")).thenReturn(Optional.empty());

        for (int i = 0; i < 5; i++) {
            mockMvc.perform(get("/api/v1/trades/T1")
                            .with(jwt().jwt(token -> token.subject("client-reader"))
                                    .authorities(new SimpleGrantedAuthority("SCOPE_trades:read"))))
                    .andExpect(status().isNotFound());
        }
    }

    @Test
    void ingestTrade_WhenBodyLargerThanLimit_Returns413WithoutProcessing() throws Exception {
        String oversized = VALID_BODY.replace("\"B1\"", "\"" + "B".repeat(2_000) + "\"");

        postTrade("client-large", oversized).andExpect(status().isContentTooLarge());

        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void ingestTrade_WhenContentLengthUnknown_Returns411WithoutProcessing() throws Exception {
        mockMvc.perform(post("/api/v1/trades")
                        .with(producer("client-chunked"))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isLengthRequired());

        verify(tradeProcessingService, never()).process(any());
    }

    private ResultActions postTrade(String clientId, String body) throws Exception {
        return mockMvc.perform(post("/api/v1/trades")
                .with(producer(clientId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));
    }

    private static JwtRequestPostProcessor producer(String clientId) {
        return jwt().jwt(token -> token.subject(clientId))
                .authorities(new SimpleGrantedAuthority("SCOPE_trades:write"));
    }
}
