package com.tradestore.adapter.rest;

import com.tradestore.domain.SaveOutcome;

/**
 * Response body of {@code POST /api/v1/trades} when the trade is stored.
 */
public record IngestionResponse(String tradeId, int version, SaveOutcome outcome) {
}
