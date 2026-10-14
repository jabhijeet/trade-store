package com.tradestore.adapter.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.LowerVersionTradeException;
import com.tradestore.domain.MaturityDateInPastException;
import com.tradestore.domain.SaveOutcome;
import com.tradestore.domain.TradeCommand;
import com.tradestore.service.TradeProcessingService;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.JwtRequestPostProcessor;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

@WebMvcTest(TradeIngestionController.class)
@Import(RestSliceTestConfiguration.class)
class TradeIngestionControllerTest {

    private static final String VALID_BODY = """
            {"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2027-05-20"}""";

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TradeProcessingService tradeProcessingService;

    @Test
    void ingestTrade_WhenNewTrade_Returns201WithLocation() throws Exception {
        when(tradeProcessingService.process(any())).thenReturn(SaveOutcome.CREATED);

        postTrade(VALID_BODY)
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "http://localhost/api/v1/trades/T1"))
                .andExpect(jsonPath("$.tradeId").value("T1"))
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.outcome").value("CREATED"));
    }

    @Test
    void ingestTrade_WhenHigherVersion_Returns200() throws Exception {
        when(tradeProcessingService.process(any())).thenReturn(SaveOutcome.UPDATED);

        postTrade(VALID_BODY)
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("UPDATED"));
    }

    @Test
    void ingestTrade_WhenSameVersion_Returns200() throws Exception {
        when(tradeProcessingService.process(any())).thenReturn(SaveOutcome.REPLACED);

        postTrade(VALID_BODY)
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("REPLACED"));
    }

    @Test
    void ingestTrade_WhenIdempotencyKeySent_UsesItAsMessageId() throws Exception {
        when(tradeProcessingService.process(any())).thenReturn(SaveOutcome.CREATED);

        mockMvc.perform(post("/api/v1/trades")
                        .with(producer())
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "abc-123")
                        .content(VALID_BODY))
                .andExpect(status().isCreated());

        TradeCommand command = capturedCommand();
        assertThat(command.messageId()).isEqualTo("rest:abc-123");
        assertThat(command.source()).isEqualTo(EventSource.REST);
        assertThat(command.trade().maturityDate()).isEqualTo(LocalDate.of(2027, 5, 20));
    }

    @Test
    void ingestTrade_WhenNoIdempotencyKey_GeneratesUniqueRestMessageId() throws Exception {
        when(tradeProcessingService.process(any())).thenReturn(SaveOutcome.CREATED);

        postTrade(VALID_BODY).andExpect(status().isCreated());

        assertThat(capturedCommand().messageId()).startsWith("rest:").hasSizeGreaterThan("rest:".length());
    }

    @Test
    void ingestTrade_WhenRequiredFieldMissing_Returns400WithoutProcessing() throws Exception {
        postTrade("""
                {"version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2027-05-20"}""")
                .andExpect(status().isBadRequest());

        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void ingestTrade_WhenVersionNotPositive_Returns400WithoutProcessing() throws Exception {
        postTrade(VALID_BODY.replace("\"version\":1", "\"version\":0"))
                .andExpect(status().isBadRequest());

        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void ingestTrade_WhenBodyMalformed_Returns400() throws Exception {
        postTrade("{not json").andExpect(status().isBadRequest());

        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void ingestTrade_WhenLowerVersion_Returns409Problem() throws Exception {
        when(tradeProcessingService.process(any())).thenThrow(new LowerVersionTradeException("T1", 1, 2));

        postTrade(VALID_BODY)
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.detail").value("Trade T1 version 1 is lower than stored version 2"));
    }

    @Test
    void ingestTrade_WhenMaturityDateInPast_Returns422Problem() throws Exception {
        when(tradeProcessingService.process(any())).thenThrow(new MaturityDateInPastException(
                "T1", 1, LocalDate.of(2020, 5, 20), LocalDate.of(2026, 10, 13)));

        postTrade(VALID_BODY)
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.status").value(422));
    }

    @Test
    void ingestTrade_WhenDatabaseTemporarilyUnavailable_Returns503() throws Exception {
        when(tradeProcessingService.process(any())).thenThrow(new QueryTimeoutException("lock timeout"));

        postTrade(VALID_BODY).andExpect(status().isServiceUnavailable());
    }

    @Test
    void ingestTrade_WhenNoToken_Returns401WithoutProcessing() throws Exception {
        mockMvc.perform(post("/api/v1/trades").contentType(MediaType.APPLICATION_JSON).content(VALID_BODY))
                .andExpect(status().isUnauthorized());

        verify(tradeProcessingService, never()).process(any());
    }

    @Test
    void ingestTrade_WhenTokenLacksWriteScope_Returns403WithoutProcessing() throws Exception {
        mockMvc.perform(post("/api/v1/trades")
                        .with(jwt().authorities(new SimpleGrantedAuthority("SCOPE_trades:read")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isForbidden());

        verify(tradeProcessingService, never()).process(any());
    }

    private ResultActions postTrade(String body) throws Exception {
        return mockMvc.perform(post("/api/v1/trades")
                .with(producer())
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));
    }

    private static JwtRequestPostProcessor producer() {
        return jwt().authorities(new SimpleGrantedAuthority("SCOPE_trades:write"));
    }

    private TradeCommand capturedCommand() {
        var captor = ArgumentCaptor.forClass(TradeCommand.class);
        verify(tradeProcessingService).process(captor.capture());
        return captor.getValue();
    }
}
