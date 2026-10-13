package com.tradestore.domain;

/**
 * A request to process one incoming trade.
 *
 * @param messageId unique id of the message that carried the trade, used for idempotency
 *                  ({@code kafka:<topic>:<partition>:<offset>} or {@code rest:<idempotency key>})
 * @param source    the ingestion path the trade arrived on
 * @param trade     the incoming trade
 */
public record TradeCommand(String messageId, EventSource source, IncomingTrade trade) {
}
