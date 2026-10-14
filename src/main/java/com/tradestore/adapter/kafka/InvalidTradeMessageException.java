package com.tradestore.adapter.kafka;

/**
 * A message that cannot be turned into a trade: malformed JSON, missing fields or an unsupported schema version.
 * Never retried; the message goes to the dead-letter topic.
 */
public class InvalidTradeMessageException extends RuntimeException {

    public InvalidTradeMessageException(String message) {
        super(message);
    }

    public InvalidTradeMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
