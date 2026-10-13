package com.tradestore.domain;

/**
 * Where a trade event originated.
 */
public enum EventSource {
    KAFKA,
    REST,
    SCHEDULER
}
