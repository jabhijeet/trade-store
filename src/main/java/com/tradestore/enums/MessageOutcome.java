package com.tradestore.enums;

/**
 * Outcome recorded for a processed message, so a duplicate message can be answered without reprocessing.
 */
public enum MessageOutcome {
    CREATED,
    UPDATED,
    REPLACED,
    REJECTED_LOWER_VERSION,
    REJECTED_MATURITY_IN_PAST
}
