# Trade Store – Architecture

This document describes the target architecture of the trade store. It is the reference for implementation and is updated as design decisions are made.

## 1. Overview

Kafka is the primary entry point for trades. A rate-limited REST endpoint is a secondary entry point, used as a backup when Kafka is unavailable and for producers that integrate directly over HTTP. Both entry points are thin adapters over one **Trade Processing Service**, which owns all validation and is the only component that writes trade state.

State changes and their events are written to PostgreSQL in the same transaction (**transactional outbox**). An **outbox relay** then delivers the events to MongoDB (history and projections) and to Kafka (status events). Read-only REST endpoints serve current state from PostgreSQL and history from MongoDB.

```
                    ┌──────────────────────────── Spring Boot Trade Store App ────────────────────────────┐
Producers ──► Kafka ──► Trade Consumer ──┐                                                               │
             (trades,                    ├──► Trade Processing Service ──► PostgreSQL ◄── Expiry Scheduler │
              key = trade id)            │    (validation rules,           (trades,         (ShedLock)     │
Producers ──► Rate Limiter ──► POST ─────┘     version check,               processed_message,             │
             (per client, 429)  /trades         idempotency)                outbox_event)                  │
                                                                                 │                         │
                                                                           Outbox Relay ──► MongoDB         │
                                                                                 └────────► Kafka           │
                                                                                            (trade-status)  │
Clients ──► GET /trades ... ──► Trade Query API ──► PostgreSQL (current) / MongoDB (history)              │
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
- **as a fallback** by calling `POST /trades` when Kafka is unavailable, or when the producer can only integrate over HTTP.

### 2.2 Kafka – Primary Ingestion

- **Partition by trade id** – every version of a trade lands on the same partition, so versions of one trade are consumed in the order they were published, while different trades are processed in parallel.
- **Buffering** – bursts of thousands of trades are absorbed by the topic; a slow database shows up as consumer lag instead of failed requests.
- **Durability** – topics use replication factor 3 and `min.insync.replicas=2`; producers use `acks=all` and `enable.idempotence=true`.
- **Message contract** – trades are JSON documents validated against a versioned JSON Schema and carry a `schemaVersion` field. A message with an unknown schema version is sent to the dead-letter topic. (Avro with a Schema Registry is a drop-in alternative if stricter compatibility checks are needed.)
- **Partition count** – sized up front for the expected peak throughput. Adding partitions later changes the key-to-partition mapping and temporarily breaks per-trade ordering, so it is done only during a controlled drain.
- **Reprocessing** – consumers can re-read a topic from an earlier offset to reprocess messages after a fix, within the topic's retention period. Replay is **not** a state-rebuild mechanism: the maturity rule depends on the processing date, so replaying old messages would not reproduce the original outcomes (see §9).

### 2.3 Trade Consumer (Kafka adapter)

A thin adapter. It deserializes the message (`ErrorHandlingDeserializer`), builds a message id `kafka:<topic>:<partition>:<offset>`, calls the processing service and acknowledges the offset **after** the database transaction has committed. It applies the error-handling policy in §6.

### 2.4 Trade Ingestion API (REST adapter, backup / direct)

`POST /trades` – a thin adapter over the same processing service, so every rule is applied identically on both paths. Because its purpose is to work when Kafka is down, it writes through the processing service directly and does **not** depend on Kafka.

| Outcome | HTTP status |
|---------|-------------|
| New trade id stored | `201 Created` |
| Higher version stored, or same version replaced | `200 OK` |
| Malformed request | `400 Bad Request` |
| Missing or invalid credentials | `401 Unauthorized` / `403 Forbidden` |
| Lower version than stored | `409 Conflict` |
| Maturity date earlier than today | `422 Unprocessable Entity` |
| Rate limit exceeded | `429 Too Many Requests` with `Retry-After` |
| Database unavailable | `503 Service Unavailable` |

- **Idempotency** – callers may send an `Idempotency-Key` header. It becomes the message id (`rest:<key>`); a repeated key returns the originally recorded outcome without reprocessing.
- **Rate limiter** – Bucket4j token buckets keyed by the authenticated client id, so one noisy producer cannot starve others. Requests over the limit fail fast with `429` instead of queueing. By default buckets are held in memory, so the limit applies **per instance** (configured as the desired global limit divided by the instance count). A Redis-backed Bucket4j store gives an exact global limit when needed, and an API gateway is an alternative in larger deployments.

### 2.5 Trade Processing Service

The core of the application and the only writer of trade state. Its `process(command)` method, in one PostgreSQL transaction:

1. Inserts the message id into `processed_message`. If the id already exists, the message is a duplicate: the transaction ends and the recorded outcome is returned.
2. Loads the current trade for the trade id with `SELECT … FOR UPDATE`.
3. Runs every registered `TradeValidationRule` against the incoming trade, the current trade and today's date. The first failing rule throws a `TradeRejectedException` subclass (`MaturityDateInPastException`, `LowerVersionTradeException`).
4. Inserts the trade, or updates the existing row (higher version or same version).
5. Inserts the matching event (`CREATED`, `UPDATED` or `REPLACED`) into `outbox_event`.

On a rejection, the main transaction is rolled back and the rejection is recorded in a **separate** transaction: the message id goes into `processed_message` with outcome `REJECTED`, and a `REJECTED` event with the reason goes into `outbox_event`. The exception is then rethrown to the adapter, which satisfies the requirement that a lower version "generates an exception".

If two first versions of the same new trade id arrive concurrently (one per ingestion path), one insert fails on the primary key; that is classified as a transient error and retried, at which point the row exists and the normal version check applies.

### 2.6 PostgreSQL

The **authoritative trade state** and the write-side store for idempotency and the outbox. Schema changes are managed with Flyway. See §3 for the data model.

### 2.7 Outbox Relay

A scheduled poller (single active instance via ShedLock) that reads undelivered rows from `outbox_event` in creation order and delivers them:

- **to MongoDB** – upserts the event into `trade_events` using the outbox event id as `_id`, so redelivery is idempotent, and updates the query projections;
- **to Kafka** – publishes `EXPIRED` events to the `trade-status` topic, keyed by trade id.

Each destination is tracked separately (`mongo_delivered_at`, `kafka_delivered_at`), so a Kafka outage does not hold up MongoDB history. Failed deliveries are retried on the next poll. Debezium (change data capture on the outbox table) is a drop-in alternative to the poller if lower latency is needed.

### 2.8 MongoDB

The **trade event history** and read side.

- `trade_events` – one immutable document per event (created, updated, replaced, rejected with reason, expired), including the full trade payload and the event source.
- **Projections** – read-optimised views (for example, trades by book or by counter-party), updated by the relay from events. They are eventually consistent with PostgreSQL, typically within one relay poll interval.

### 2.9 Expiry Scheduler

Runs daily shortly after midnight in the configured business time zone (cron and zone in configuration), with ShedLock so only one instance runs it. In one transaction per batch it:

1. marks matured trades expired with a conditional update – `UPDATE trades SET expired = true WHERE expired = false AND maturity_date < :today … RETURNING trade_id, version` – which is safe to rerun;
2. inserts an `EXPIRED` event into `outbox_event` for every returned row.

Because the events are written in the same transaction as the update, a crash cannot leave a trade expired without its event. Large volumes are processed in fixed-size batches.

### 2.10 Trade Query API (read only)

| Endpoint | Source |
|----------|--------|
| `GET /trades?bookId=&counterPartyId=&expired=&page=&size=` | PostgreSQL, paginated and filtered |
| `GET /trades/{tradeId}` | PostgreSQL, current version |
| `GET /trades/{tradeId}/history` | MongoDB, all events of the trade, paginated |

### 2.11 Security

- **REST** – OAuth2 resource server with JWT bearer tokens. `POST /trades` requires the `trades:write` scope; read endpoints require `trades:read`. Request bodies are validated with Bean Validation and limited in size.
- **Kafka** – `SASL_SSL` authentication and topic ACLs: producers may only write to `trades`; the application may read `trades` and write `trade-status` and the dead-letter topic.
- **Secrets** – credentials are supplied through environment variables or a secrets manager and never committed to the repository.

### 2.12 Monitoring

Spring Boot Actuator health checks and Micrometer metrics: trades received per source, created / updated / replaced / rejected (by reason), expired, consumer lag, rate-limited requests, and outbox backlog (undelivered events per destination). Logs are structured and carry the trade id and message id.

## 3. Data Model

### PostgreSQL

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `trades` | Current version of each trade | `trade_id` (PK), `version`, `counter_party_id`, `book_id`, `maturity_date`, `created_date`, `expired`, `updated_at` |
| `processed_message` | Idempotency (inbox) | `message_id` (PK), `trade_id`, `outcome`, `processed_at` |
| `outbox_event` | Events awaiting delivery | `id` (PK, UUID), `trade_id`, `version`, `type`, `source`, `payload` (JSONB), `created_at`, `mongo_delivered_at`, `kafka_delivered_at` |
| `shedlock` | Distributed locks for scheduled jobs | `name` (PK), `lock_until`, `locked_at`, `locked_by` |

Indexes: `trades (expired, maturity_date)` for the expiry scheduler, `trades (book_id)` and `trades (counter_party_id)` for queries, and partial indexes on `outbox_event` for undelivered rows. `processed_message` rows older than the Kafka retention period are purged by a scheduled job.

### MongoDB

| Collection | Purpose |
|------------|---------|
| `trade_events` | Full event history, `_id` = outbox event id, indexed by `tradeId` and `occurredAt` |
| projection collections | Read-optimised views, for example by book or by counter-party |

## 4. Business Rules and Definitions

| Topic | Decision |
|-------|----------|
| **Today** | The current date in the configured business time zone, taken from an injected `java.time.Clock`. Tests use a fixed clock. |
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

1. A producer calls `POST /trades` with a JWT and, optionally, an `Idempotency-Key`.
2. The rate limiter admits the request or rejects it with `429`.
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
| Transient failure (database unavailable, lock timeout, primary key race) | Retried with exponential backoff. The partition is not skipped, so per-trade ordering is preserved; consumer lag grows and is alerted on | `503 Service Unavailable`, caller retries |
| Unexpected error after retries are exhausted | Sent to the dead-letter topic with the exception details, alert raised | `500 Internal Server Error` |

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
| New trade field | Flyway migration, new JSON Schema version (`schemaVersion`), and a new field on the event payload. Consumers accept the previous schema version during the transition. |
| Different expiry policy (for example intraday cut-off) | The scheduler's cron and time zone are configuration; the expiry condition is isolated in the repository query. |
| Higher throughput | Add application instances up to the partition count; scale partitions during a controlled drain (§2.2). |

## 9. Design Notes and Trade-offs

- **Why Kafka is the primary path** – per-trade ordering, buffering of bursts, and separation of producers from database availability.
- **Why a REST ingestion endpoint as well** – Kafka is configured to be highly available (§2.2), but the REST path removes it as a single point of failure for ingestion and supports producers that cannot publish to Kafka. The cost is a second write path; it is contained by making both paths thin adapters over one processing service, with the same rules, idempotency and outbox.
- **Ordering across the two paths** – Kafka cannot order a Kafka message against a REST call. Correctness does not depend on that ordering: the version check runs after the row lock, so a lower version than the stored one is always rejected whichever path delivers it.
- **Why a transactional outbox** – writing PostgreSQL and then MongoDB or Kafka directly is a dual write: a crash in between loses the event. With the outbox, the state change and its event commit together, and delivery is retried until it succeeds. The same mechanism keeps expiry events flowing to MongoDB while Kafka is down.
- **Why an idempotency table** – Kafka delivers at least once and REST callers retry. Recording each message id in the same transaction as its effect makes every message take effect exactly once, including its event.
- **Why replay is not used to rebuild state** – the maturity rule depends on the processing date, so replaying old messages would reject trades that were accepted when they first arrived, and would record spurious rejections. History is rebuilt from MongoDB events; reprocessing from Kafka is reserved for messages that failed because of a defect.
- **Why a rate limiter** – the REST path has no Kafka buffer in front of the database. The limiter caps the load each client can put on PostgreSQL and fails fast with `429`.
- **Why both SQL and NoSQL** – PostgreSQL gives strong consistency for the current trade state, the version rule, idempotency and the outbox. MongoDB holds the append-only history and read models, where schema flexibility matters more than relational constraints.

## 10. Known Limitations

- With in-memory Bucket4j buckets, the rate limit is per instance; an exact global limit needs the Redis-backed store.
- MongoDB history and projections are eventually consistent with PostgreSQL, behind by up to one relay poll interval.
- Between scheduler runs, a trade whose maturity date has just passed is still shown as not expired. Running the scheduler shortly after midnight keeps this window small.
- When the same trade id and version arrive on both paths with different contents, which one is kept depends on commit order.
