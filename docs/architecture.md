# Trade Store – Architecture

This document describes the target architecture of the trade store. It is the reference for implementation and is updated as design decisions are made. Parts of the design that are not implemented yet are marked **(planned)**; §12 lists them.

## 1. Overview

Kafka is the primary entry point for trades. A REST endpoint (rate limiting and authentication planned) is a secondary entry point, used as a backup when Kafka is unavailable and for producers that integrate directly over HTTP. Both entry points are thin adapters over one **Trade Processing Service**, which owns all validation and is the only component that writes trade state.

State changes and their events are written to PostgreSQL in the same transaction (**transactional outbox**). An **outbox relay** then delivers the events to MongoDB (history and projections) and to Kafka (status events). Read-only REST endpoints serve current state from PostgreSQL and history from MongoDB.

```
                    ┌──────────────────────────── Spring Boot Trade Store App ────────────────────────────┐
Producers ──► Kafka ──► Trade Consumer ──┐                                                               │
             (trades,                    ├──► Trade Processing Service ──► PostgreSQL ◄── Expiry Scheduler │
              key = trade id)            │    (validation rules,           (trades,         (ShedLock)     │
Producers ──► Rate Limiter ──► POST ─────┘     version check,               processed_message,             │
             (per client, 429)  /api/v1/trades  idempotency)                outbox_event)                  │
                                                                                 │                         │
                                                                           Outbox Relay ──► MongoDB         │
                                                                                 └────────► Kafka           │
                                                                                            (trade-status)  │
Clients ──► GET /api/v1/trades ... ──► Trade Query API ──► PostgreSQL (current) / MongoDB (history)       │
                    └──────────────────────────────────────────────────────────────────────────────────────┘
```

The PlantUML source for every diagram lives in [`diagrams/`](diagrams):

| Diagram | File |
|---------|------|
| Component view | [`component.puml`](diagrams/component.puml) |
| Trade ingestion sequence (Kafka) | [`sequence-ingestion.puml`](diagrams/sequence-ingestion.puml) |
| Trade ingestion sequence (REST) | [`sequence-rest-ingestion.puml`](diagrams/sequence-rest-ingestion.puml) |
| Trade expiry sequence | [`sequence-expiry.puml`](diagrams/sequence-expiry.puml) |
| Outbox relay sequence | [`sequence-outbox-relay.puml`](diagrams/sequence-outbox-relay.puml) |
| Domain class model | [`class-domain.puml`](diagrams/class-domain.puml) |

## 2. Components

### 2.1 Trade Producers

Upstream systems that send trades to the store:

- **normally** by publishing to the Kafka `trades` topic, using the trade id as the message key;
- **as a fallback** by calling `POST /api/v1/trades` when Kafka is unavailable, or when the producer can only integrate over HTTP.

### 2.2 Kafka – Primary Ingestion

- **Partition by trade id** – every version of a trade lands on the same partition, so versions of one trade are consumed in the order they were published, while different trades are processed in parallel.
- **Buffering** – bursts of thousands of trades are absorbed by the topic; a slow database shows up as consumer lag instead of failed requests.
- **Durability** – topics use replication factor 3 and `min.insync.replicas=2`; producers use `acks=all` and `enable.idempotence=true`.
- **Message contract** – trades are JSON documents with a `schemaVersion` field (currently `1`). The consumer checks the schema version and the required fields; a message with an unknown schema version, a missing or oversized field, or a record key that differs from the trade id is sent to the dead-letter topic. (A published JSON Schema, or Avro with a Schema Registry, is an alternative if stricter compatibility checks are needed.)
- **Partition count** – sized up front for the expected peak throughput. Adding partitions later changes the key-to-partition mapping and temporarily breaks per-trade ordering, so it is done only during a controlled drain.
- **Reprocessing** – consumers can re-read a topic from an earlier offset to reprocess messages after a fix, within the topic's retention period. Replay is **not** a state-rebuild mechanism: the maturity rule depends on the processing date, so replaying old messages would not reproduce the original outcomes (see §9).

### 2.3 Trade Consumer (Kafka adapter)

A thin adapter (`TradeConsumer`). It reads the record value as a string, parses and checks it (§2.2; invalid messages throw `InvalidTradeMessageException`), builds a message id `kafka:<topic>:<partition>:<offset>`, calls the processing service and acknowledges the offset **after** the database transaction has committed. It applies the error-handling policy in §6, configured in `KafkaConfig`.

### 2.4 Trade Ingestion API (REST adapter, backup / direct)

`POST /api/v1/trades` – a thin adapter over the same processing service, so every rule is applied identically on both paths. Because its purpose is to work when Kafka is down, it writes through the processing service directly and does **not** depend on Kafka.

| Outcome | HTTP status |
|---------|-------------|
| New trade id stored | `201 Created` |
| Higher version stored, or same version replaced | `200 OK` |
| Malformed request | `400 Bad Request` |
| Missing or invalid credentials **(planned)** | `401 Unauthorized` / `403 Forbidden` |
| Lower version than stored | `409 Conflict` |
| Maturity date earlier than today | `422 Unprocessable Entity` |
| Rate limit exceeded **(planned)** | `429 Too Many Requests` with `Retry-After` |
| Database unavailable | `503 Service Unavailable` with `Retry-After` |

Errors are returned as RFC 9457 problem details by `RestExceptionHandler`.

- **Idempotency** – callers may send an `Idempotency-Key` header (at most 200 characters). It becomes the message id (`rest:<key>`); a repeated key returns the originally recorded outcome without reprocessing. Without the header, a random id is used, so a retried request is processed again.
- **Rate limiter (planned)** – Bucket4j token buckets keyed by the authenticated client id, so one noisy producer cannot starve others. Requests over the limit fail fast with `429` instead of queueing. By default buckets are held in memory, so the limit applies **per instance** (configured as the desired global limit divided by the instance count). A Redis-backed Bucket4j store gives an exact global limit when needed, and an API gateway is an alternative in larger deployments.

### 2.5 Trade Processing Service

The core of the application and the only writer of trade state. Its `process(command)` method (`TradeProcessingService`), in one PostgreSQL transaction:

1. Looks up the message id in `processed_message`. If it exists, the message is a duplicate: nothing is written and the recorded outcome is returned (a recorded rejection is rethrown).
2. Loads the current trade for the trade id with `SELECT … FOR UPDATE`.
3. Runs every registered `TradeValidationRule` against the incoming trade, the current trade and today's date. The first failing rule throws a `TradeRejectedException` subclass (`MaturityDateInPastException`, `LowerVersionTradeException`).
4. Inserts the trade, or updates the existing row (higher version or same version).
5. Appends the matching event (`CREATED`, `UPDATED` or `REPLACED`) to `outbox_event` (`OutboxWriter`).
6. Inserts the message id into `processed_message` with the outcome.

If the same message id is processed twice concurrently, the second commit fails on the `processed_message` primary key and is retried, at which point step 1 finds the recorded outcome.

On a rejection, the main transaction is rolled back and the rejection is recorded in a **separate** transaction (`RejectionRecorder`): the message id goes into `processed_message` with outcome `REJECTED_LOWER_VERSION` or `REJECTED_MATURITY_IN_PAST`, and a `REJECTED` event with the reason goes into `outbox_event`. The exception is then rethrown to the adapter, which satisfies the requirement that a lower version "generates an exception".

If two first versions of the same new trade id arrive concurrently (one per ingestion path), one insert fails on the primary key. On the Kafka path the message is retried, at which point the row exists and the normal version check applies; on the REST path the request currently fails with `500` and the caller retries (see §10).

### 2.6 PostgreSQL

The **authoritative trade state** and the write-side store for idempotency and the outbox. Schema changes are managed with Liquibase. See §3 for the data model and migrations.

### 2.7 Outbox Relay

A scheduled poller (single active instance via ShedLock) that reads undelivered rows from `outbox_event` in creation order and delivers them:

- **to MongoDB** – upserts the event into `trade_events` using the outbox event id as `_id`, so redelivery is idempotent (updating query projections is planned);
- **to Kafka** – publishes `EXPIRED` events to the `trade-status` topic, keyed by trade id.

Each destination is tracked separately (`mongo_delivered_at`, `kafka_delivered_at`), and failed deliveries are retried on the next poll. A short Kafka outage does not hold up MongoDB history; a long one currently can (see §10). Debezium (change data capture on the outbox table) is a drop-in alternative to the poller if lower latency is needed.

### 2.8 MongoDB

The **trade event history** and read side.

- `trade_events` – one immutable document per event (created, updated, replaced, rejected with reason, expired), including the full trade payload and the event source.
- **Projections (planned)** – read-optimised views (for example, trades by book or by counter-party), updated by the relay from events. They are eventually consistent with PostgreSQL, typically within one relay poll interval.

### 2.9 Expiry Scheduler

Runs daily shortly after midnight UTC (cron in configuration), with ShedLock so only one instance runs it. In one transaction per batch it:

1. marks matured trades expired with a conditional update – `UPDATE trades SET expired = true WHERE expired = false AND maturity_date < :today … RETURNING trade_id, version` – which is safe to rerun;
2. inserts an `EXPIRED` event into `outbox_event` for every returned row.

Because the events are written in the same transaction as the update, a crash cannot leave a trade expired without its event. Large volumes are processed in fixed-size batches.

### 2.10 Trade Query API (read only)

| Endpoint | Source |
|----------|--------|
| `GET /api/v1/trades?bookId=&counterPartyId=&expired=&page=&size=` | PostgreSQL, paginated and filtered |
| `GET /api/v1/trades/{tradeId}` | PostgreSQL, current version |
| `GET /api/v1/trades/{tradeId}/history` | MongoDB, all events of the trade, paginated |

### 2.11 Security

- **REST (planned)** – OAuth2 resource server with JWT bearer tokens. `POST /api/v1/trades` requires the `trades:write` scope; read endpoints require `trades:read`. Request size limits are planned as well. Implemented today: request bodies are validated with Bean Validation (identifiers at most 64 characters, version ≥ 1, maturity date required).
- **Kafka (planned)** – `SASL_SSL` authentication and topic ACLs: producers may only write to `trades`; the application may read `trades` and write `trade-status` and the dead-letter topic.
- **Secrets** – credentials are supplied through environment variables (`POSTGRES_PASSWORD`, `MONGODB_URI`, …) and never committed to the repository.

### 2.12 Monitoring

Implemented: Spring Boot Actuator `health`, `info` and `metrics` endpoints, including the connection pool metrics (`hikaricp.connections.*`, `mongodb.driver.pool.*`). Logs carry the trade id and message id of every stored, rejected or duplicate trade.

Planned: application metrics for trades received per source, created / updated / replaced / rejected (by reason), expired, consumer lag, rate-limited requests and outbox backlog (undelivered events per destination), and structured (JSON) logs.

## 3. Data Model

### PostgreSQL

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `trades` | Current version of each trade | `trade_id` (PK), `version`, `counter_party_id`, `book_id`, `maturity_date`, `created_date`, `expired`, `updated_at` |
| `processed_message` | Idempotency (inbox) | `message_id` (PK), `trade_id`, `outcome`, `processed_at` |
| `outbox_event` | Events awaiting delivery | `id` (PK, UUID), `trade_id`, `version`, `type`, `source`, `payload` (JSONB), `created_at`, `mongo_delivered_at`, `kafka_delivered_at` |
| `shedlock` | Distributed locks for scheduled jobs | `name` (PK), `lock_until`, `locked_at`, `locked_by` |

Indexes: `trades (expired, maturity_date)` for the expiry scheduler, `trades (book_id)` and `trades (counter_party_id)` for queries, and partial indexes on `outbox_event` for undelivered rows. A scheduled job that purges `processed_message` rows older than the Kafka retention period is planned; the `processed_at` index exists for it.

### MongoDB

| Collection | Purpose |
|------------|---------|
| `trade_events` | Full event history, `_id` = outbox event id, indexed by `tradeId` and `occurredAt` |
| projection collections **(planned)** | Read-optimised views, for example by book or by counter-party |

### Schema migrations

Both databases are migrated with Liquibase at startup, from one changelog tree:

```
src/main/resources/db/changelog/
├── postgres/
│   ├── db.changelog-master.yaml
│   └── changes/001-create-trade-store-tables.sql   (formatted SQL with rollback statements)
└── mongodb/
    ├── db.changelog-master.yaml
    └── changes/001-create-trade-events.yaml        (collection and index change sets)
```

- **PostgreSQL** – applied by Spring Boot's Liquibase auto-configuration (`spring.liquibase.change-log`) before JPA starts, so Hibernate's schema validation runs against the migrated schema.
- **MongoDB** – applied with the Liquibase MongoDB extension by `MongoLiquibaseMigration`, because Spring Boot only auto-configures Liquibase for JDBC. It reuses Spring's MongoDB connection details and database name, and runs while the application context is created, before Kafka listeners and scheduled jobs start. Disabled with `trade-store.mongodb.liquibase.enabled=false`.
- Each database keeps its own change history (`databasechangelog` table, `DATABASECHANGELOG` collection) and lock. Applied change sets are never edited; every change is a new numbered file.

### Connection pools

Each database has its own, independently sized pool, configured in `DatabaseConfig` and `application.yml`:

| Database | Pool | Settings | Defaults |
|----------|------|----------|----------|
| PostgreSQL | HikariCP (`trade-store-postgres`) | `spring.datasource.hikari.*` | max 10, min idle 2, 5 s connection timeout, 60 s leak detection |
| MongoDB | MongoDB driver pool (MongoDB does not use JDBC, so HikariCP does not apply) | `trade-store.mongodb.pool.*` | max 20, min 2, 5 s wait time |

- The PostgreSQL pool is kept small: every write is a short transaction, and the only writers are the Kafka consumer, REST ingestion and the two scheduled jobs. A larger pool would only increase lock contention on hot trades.
- Pool sizes can be overridden per environment with `POSTGRES_POOL_MAX_SIZE`, `POSTGRES_POOL_MIN_IDLE`, `MONGODB_POOL_MAX_SIZE` and `MONGODB_POOL_MIN_SIZE`.
- The MongoDB pool settings take precedence over pool options in the connection string.
- Both pools publish Micrometer metrics (`hikaricp.connections.*`, `mongodb.driver.pool.*`) through Actuator.

## 4. Business Rules and Definitions

| Topic | Decision |
|-------|----------|
| **Time zone** | Everything is UTC: business dates, timestamps, the expiry schedule and the JVM default zone (set at startup, so the PostgreSQL session zone does not depend on the host). |
| **Today** | The current UTC date, taken from an injected `java.time.Clock` (`Clock.systemUTC()`). Tests use a fixed clock. |
| **Maturity rule boundary** | A trade with maturity date **equal to** today is accepted. A trade with maturity date **before** today is rejected. |
| **Expiry boundary** | A trade becomes expired on the first scheduler run after its maturity date, that is, when `maturity_date < today`. |
| **Version rule** | Lower version than stored: rejected with `LowerVersionTradeException`. Same version: replaces the stored record. Higher version: stored as the new current record. |
| **New version of an expired trade** | Accepted if it passes the rules (its maturity date is therefore today or later) and resets `expired` to `N`. A lower version is still rejected. |
| **Created date** | The date on which that version of the trade was received by the store. |
| **Same trade id and version on both paths** | Both are valid replacements; the last committed write wins. |

### Mapping to the sample data

The sample table in the assignment shows both T2 version 1 and T2 version 2. In this design:

- **PostgreSQL** holds the current version of each trade: T1 v1, T2 v2 and T3 v3.
- **MongoDB** holds every version ever accepted, so T2 v1 (created 14/03/2015) remains available in the trade's history.

Together they represent the sample table: current state plus history. Trades such as T1 and T3, whose maturity dates are in the past, were accepted while their maturity dates were still in the future; the expiry scheduler marks them expired once those dates pass.

## 5. Processing Flows

### Kafka (primary)

1. A producer publishes a trade to `trades`, keyed by trade id.
2. The consumer deserializes it and calls the processing service with message id `kafka:<topic>:<partition>:<offset>`.
3. The processing service runs the steps in §2.5.
4. The consumer acknowledges the offset after the transaction commits, or applies the error policy in §6.

### REST (backup / direct)

1. A producer calls `POST /api/v1/trades` with a JWT (planned) and, optionally, an `Idempotency-Key`.
2. The rate limiter (planned) admits the request or rejects it with `429`.
3. The request body is validated; a malformed request is rejected with `400`.
4. The processing service runs the steps in §2.5.
5. The outcome is returned as an HTTP status (see §2.4).

### Event delivery

1. The outbox relay reads undelivered events.
2. It upserts them into MongoDB and publishes `EXPIRED` events to `trade-status`.
3. It marks each destination as delivered.

### Expiry

1. The scheduler acquires its ShedLock lock.
2. It marks matured trades expired and inserts `EXPIRED` outbox events in the same transaction.
3. The relay delivers the events.

## 6. Error-Handling Policy

| Failure | Kafka path | REST path |
|---------|------------|-----------|
| Message cannot be deserialized or has an unknown schema version | Sent to the dead-letter topic, offset committed | `400 Bad Request` |
| Business rejection (`TradeRejectedException`) | **Not retried.** Rejection recorded (§2.5), offset committed | `409` or `422` |
| Duplicate message id | Recorded outcome reused, offset committed | Recorded outcome returned |
| Transient failure (database unavailable, lock timeout) | Retried with exponential backoff (1 s doubling to 60 s, for up to 10 minutes). The partition is not skipped, so per-trade ordering is preserved; consumer lag grows | `503 Service Unavailable` with `Retry-After`, caller retries |
| Primary key race on a new trade id | Retried as above | `500 Internal Server Error`, caller retries (§10) |
| Unexpected error after retries are exhausted | Sent to the dead-letter topic with the exception details | `500 Internal Server Error` |

Business rejections are excluded from retries (configured as not-retryable in Spring Kafka's `DefaultErrorHandler`), so a rejected trade never blocks the trades behind it on the same partition.

## 7. Validation Rules Mapping

| Rule | Where it is enforced |
|------|----------------------|
| Lower version is rejected with an exception; same version replaces | `VersionRule`, inside the PostgreSQL transaction, after the row lock (both paths) |
| Maturity date earlier than today is rejected | `MaturityDateRule`, inside the same transaction (both paths) |
| Trade marked expired once maturity date has passed | Expiry scheduler |

## 8. Extensibility

| Change | How the design absorbs it |
|--------|---------------------------|
| New validation rule | Implement `TradeValidationRule` and register it as a Spring bean; the processing service runs every registered rule in `@Order`. No existing code changes. |
| New ingestion channel (for example files or another message broker) | Add a thin adapter that builds a `TradeCommand` with its own message id and calls the processing service. |
| New downstream consumer or read model | Consume `trade-status`, or add a projection updated by the outbox relay from existing events. |
| New event destination | Add a delivery target to the outbox relay with its own delivered-at column. |
| New trade field | Liquibase change set (PostgreSQL column, and MongoDB index if queried), a new message `schemaVersion`, and a new field on the event payload. Consumers accept the previous schema version during the transition. |
| Different expiry policy (for example intraday cut-off) | The scheduler's cron is configuration (evaluated in UTC); the expiry condition is isolated in the repository query. |
| Higher throughput | Add application instances up to the partition count; scale partitions during a controlled drain (§2.2). |

## 9. Design Notes and Trade-offs

- **Why Kafka is the primary path** – per-trade ordering, buffering of bursts, and separation of producers from database availability.
- **Why a REST ingestion endpoint as well** – Kafka is configured to be highly available (§2.2), but the REST path removes it as a single point of failure for ingestion and supports producers that cannot publish to Kafka. The cost is a second write path; it is contained by making both paths thin adapters over one processing service, with the same rules, idempotency and outbox.
- **Ordering across the two paths** – Kafka cannot order a Kafka message against a REST call. Correctness does not depend on that ordering: the version check runs after the row lock, so a lower version than the stored one is always rejected whichever path delivers it.
- **Why a transactional outbox** – writing PostgreSQL and then MongoDB or Kafka directly is a dual write: a crash in between loses the event. With the outbox, the state change and its event commit together, and delivery is retried until it succeeds. The same mechanism keeps expiry events flowing to MongoDB while Kafka is down.
- **Why an idempotency table** – Kafka delivers at least once and REST callers retry. Recording each message id in the same transaction as its effect makes every message take effect exactly once, including its event.
- **Why replay is not used to rebuild state** – the maturity rule depends on the processing date, so replaying old messages would reject trades that were accepted when they first arrived, and would record spurious rejections. History is rebuilt from MongoDB events; reprocessing from Kafka is reserved for messages that failed because of a defect.
- **Why a rate limiter (planned)** – the REST path has no Kafka buffer in front of the database. The limiter caps the load each client can put on PostgreSQL and fails fast with `429`.
- **Why both SQL and NoSQL** – PostgreSQL gives strong consistency for the current trade state, the version rule, idempotency and the outbox. MongoDB holds the append-only history and read models, where schema flexibility matters more than relational constraints.

## 10. Known Limitations

- With in-memory Bucket4j buckets (once implemented), the rate limit is per instance; an exact global limit needs the Redis-backed store.
- **Outbox backlog during a long Kafka outage** – the relay reads the oldest events that have *any* destination pending. If Kafka is down while a full batch of `EXPIRED` events is waiting for it, every poll reads the same events, each waiting for the Kafka send timeout, and newer events do not reach MongoDB until Kafka recovers.
- **Primary key race on the REST path** – when two first versions of a new trade id are stored concurrently, the losing REST request gets `500` instead of `503`.
- **Connections per rejection** – recording a rejection uses a second transaction, and so a second pool connection, while the processing transaction is still open. Listener concurrency plus REST threads must stay well below the PostgreSQL pool size.
- MongoDB history is eventually consistent with PostgreSQL, behind by up to one relay poll interval.
- Between scheduler runs, a trade whose maturity date has just passed is still shown as not expired. Running the scheduler shortly after midnight keeps this window small.
- When the same trade id and version arrive on both paths with different contents, which one is kept depends on commit order.
- A Kafka send that times out in the outbox relay may still be delivered by the producer later, so `trade-status` is at-least-once. Consumers deduplicate by `eventId`.

## 11. Test Strategy

| Layer | Tests | What they prove |
|-------|-------|-----------------|
| Unit (`*Test`, `mvn test`) | Validation rules, processing service, consumer, controllers, expiry, outbox relay, configuration | Business rules and error mapping, with external services mocked |
| Integration (`*IT`, `mvn verify`, Testcontainers) | `TradeStoreFlowIT`, `TradeExpiryRepositoryIT`, `TradeQueryServiceIT`, `SchemaMigrationIT`, `DatabasePoolIT` | SQL, MongoDB queries, Kafka consumption and Liquibase migrations against real services |
| Consistency (`TradeStoreConsistencyIT`) | Real outages, simulated by pausing the containers | PostgreSQL and MongoDB stay consistent under partial failure and retries |

Mocks can show that the code calls MongoDB after PostgreSQL, but not that the two stores agree after a failure. `TradeStoreConsistencyIT` covers that with real services:

| Scenario | Expected result |
|----------|-----------------|
| MongoDB down while a trade is processed | Trade committed in PostgreSQL; event stays pending in the outbox; after recovery, exactly one history event with the outbox event id |
| PostgreSQL down while a Kafka message arrives | Message retried with backoff; after recovery, exactly one trade, one processed message, one outbox event and one history event |
| Relay crashes after writing to MongoDB but before marking delivery | Redelivery upserts the same document; history is not duplicated |
| Same message delivered twice | One trade, one processed message, one outbox event, one history event |
| PostgreSQL transaction fails part-way | No trade, no outbox event, no processed message, no history |
| Kafka down during expiry | History reaches MongoDB; the status event stays pending and is published after recovery |

Integration tests start their own containers with Testcontainers (`TestcontainersConfiguration`), using the same images as `compose.yml`. Test JVMs run with `-Duser.timezone=UTC` (set in `pom.xml`).

## 12. Implementation Status

| Area | Status |
|------|--------|
| Kafka ingestion, dead-letter topic, retries with backoff | Implemented |
| REST ingestion with idempotency key, problem-detail errors | Implemented |
| Validation rules (maturity date, version), rejection recording | Implemented |
| PostgreSQL state, idempotency table, transactional outbox | Implemented |
| Outbox relay to MongoDB history and the `trade-status` topic | Implemented (see the backlog limitation in §10) |
| Expiry scheduler with ShedLock | Implemented |
| Read-only query API (trades, trade, history) | Implemented |
| Liquibase migrations for PostgreSQL and MongoDB, connection pools | Implemented |
| Local infrastructure (`compose.yml`) | Implemented |
| OAuth2/JWT security for REST, Kafka SASL_SSL and ACLs | Planned |
| Bucket4j rate limiter and request size limits | Planned |
| MongoDB projections | Planned |
| Purge job for old `processed_message` rows | Planned |
| Application metrics and structured logs | Planned |
