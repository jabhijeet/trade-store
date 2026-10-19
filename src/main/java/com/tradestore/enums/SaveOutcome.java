package com.tradestore.enums;

/**
 * Result of successfully storing a trade.
 */
public enum SaveOutcome {
    /** No trade with this id was stored before. */
    CREATED,
    /** A higher version replaced the stored version. */
    UPDATED,
    /** The same version replaced the stored record. */
    REPLACED
}
