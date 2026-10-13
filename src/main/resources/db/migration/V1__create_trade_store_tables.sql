-- Current version of each trade
CREATE TABLE trades (
    trade_id         VARCHAR(64)  PRIMARY KEY,
    version          INTEGER      NOT NULL CHECK (version > 0),
    counter_party_id VARCHAR(64)  NOT NULL,
    book_id          VARCHAR(64)  NOT NULL,
    maturity_date    DATE         NOT NULL,
    created_date     DATE         NOT NULL,
    expired          BOOLEAN      NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_trades_expiry ON trades (expired, maturity_date);
CREATE INDEX idx_trades_book ON trades (book_id);
CREATE INDEX idx_trades_counter_party ON trades (counter_party_id);

-- Idempotency: one row per processed message
CREATE TABLE processed_message (
    message_id   VARCHAR(255) PRIMARY KEY,
    trade_id     VARCHAR(64)  NOT NULL,
    outcome      VARCHAR(32)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_processed_message_processed_at ON processed_message (processed_at);

-- Transactional outbox: events awaiting delivery to MongoDB and Kafka
CREATE TABLE outbox_event (
    id                 UUID         PRIMARY KEY,
    trade_id           VARCHAR(64)  NOT NULL,
    version            INTEGER      NOT NULL,
    type               VARCHAR(16)  NOT NULL,
    source             VARCHAR(16)  NOT NULL,
    reason             VARCHAR(512),
    payload            JSONB        NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL,
    mongo_delivered_at TIMESTAMPTZ,
    kafka_delivered_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_mongo_pending ON outbox_event (created_at) WHERE mongo_delivered_at IS NULL;
CREATE INDEX idx_outbox_kafka_pending ON outbox_event (created_at) WHERE type = 'EXPIRED' AND kafka_delivered_at IS NULL;

-- ShedLock: distributed locks for scheduled jobs
CREATE TABLE shedlock (
    name       VARCHAR(64)  PRIMARY KEY,
    lock_until TIMESTAMPTZ  NOT NULL,
    locked_at  TIMESTAMPTZ  NOT NULL,
    locked_by  VARCHAR(255) NOT NULL
);
