# Trade Store – Architecture

This document describes the architecture of the trade store. It is the reference for implementation and is updated as design decisions are made. §12 lists the implementation status of each area.

## 1. Overview

Kafka is the primary entry point for trades. A REST endpoint, secured with OAuth2 bearer tokens and rate limited per client, is a secondary entry point, used as a backup when Kafka is unavailable and for producers that integrate directly over HTTP. Both entry points are thin adapters over one **Trade Processing Service**, which owns all validation and is the only component that writes trade state.

State changes and their events are written to PostgreSQL in the same transaction (**transactional outbox**). An **outbox relay** then delivers the events to MongoDB (history and the `trade_views` projection) and to Kafka (status events). Read-only REST endpoints serve current state from PostgreSQL, and history and the by-book view from MongoDB.

```
                    ┌──────────────────────────── Spring Boot Trade Store App ────────────────────────────┐
Producers ──► Kafka ──► Trade Consumer ──┐                                                               │
             (trades,                    ├──► Trade Processing Service ──► PostgreSQL ◄── Expiry Scheduler │
              key = trade id)            │    (validation rules,           (trades,         (ShedLock)     │
Producers ──► JWT + Rate ──► POST ────────┘     version check,               processed_message,             │
             Limiter (429)   /api/v1/trades  idempotency)                outbox_event)                  │
                                                                                 │                         │
                                                                           Outbox Relay ──► MongoDB         │
                                                                                 └────────► Kafka           │
                                                                                            (trade-status)  │
Clients ──► GET /api/v1/... (JWT) ──► Trade Query API ──► PostgreSQL (current) / MongoDB (history, views)│
                    └──────────────────────────────────────────────────────────────────────────────────────┘
```

The PlantUML source for every diagram lives in [`diagrams/`](diagrams):

| Diagram | File |
|---------|------|
| High-level design | [`design.puml`](diagrams/design.puml) |
| Target architecture, v2 (proposal, see [architecture-v2.md](architecture-v2.md)) | [`design-v2.puml`](diagrams/design-v2.puml) |
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
| Missing or invalid token / token without `trades:write` | `401 Unauthorized` / `403 Forbidden` |
| Lower version than stored | `409 Conflict` |
| Maturity date earlier than today | `422 Unprocessable Content` |
| Body without `Content-Length` | `411 Length Required` |
| Body larger than `trade-store.rest.max-request-size` (16 KB) | `413 Content Too Large` |
| Rate limit exceeded | `429 Too Many Requests` with `Retry-After` |
| Transient database failure (lock or query timeout, concurrent first insert of a trade id) | `503 Service Unavailable` with `Retry-After: 5` |
| Database unreachable (no connection within 5 s) | `500 Internal Server Error` (see §6.1) |

Errors are returned as RFC 9457 problem details by `RestExceptionHandler`.

- **Idempotency** – callers may send an `Idempotency-Key` header (at most 200 characters). It becomes the message id (`rest:<key>`); a repeated key returns the originally recorded outcome without reprocessing. Without the header, a random id is used, so a retried request is processed again.
- **Rate limiter** – Bucket4j token buckets keyed by the authenticated client (the JWT subject), so one noisy producer cannot starve others (`ClientRateLimiter`). Each bucket allows `trade-store.rest.requests-per-second` requests per second (default 100, `REST_REQUESTS_PER_SECOND`) with a burst of the same size. Requests over the limit fail fast with `429` and a `Retry-After` header instead of queueing. Buckets are held in memory, so the limit applies **per instance** (configure the desired global limit divided by the instance count). A Redis-backed Bucket4j store gives an exact global limit when needed, and an API gateway is an alternative in larger deployments. Read endpoints are not rate limited.
- **Request size limit** – the body must declare its `Content-Length` and stay within `trade-store.rest.max-request-size` (16 KB; a trade is about 150 bytes), so an oversized body is rejected before it is read or parsed.
- Both checks run in `IngestionLimitInterceptor`, after authentication and before the body is read.

### 2.5 Trade Processing Service

The core of the application and the only writer of trade state. Its `process(command)` method (`TradeProcessingService`), in one PostgreSQL transaction:

1. Looks up the message id in `processed_message`. If it exists, the message is a duplicate: nothing is written and the recorded outcome is returned (a recorded rejection is rethrown).
2. Loads the current trade for the trade id with `SELECT … FOR UPDATE`.
3. Runs every registered `TradeValidationRule` against the incoming trade, the current trade and today's date. The first failing rule throws a `TradeRejectedException` subclass (`MaturityDateInPastException`, `LowerVersionTradeException`).
4. Inserts the trade, or updates the existing row (higher version or same version).
5. Appends the matching event (`CREATED`, `UPDATED` or `REPLACED`) to `outbox_event` (`OutboxWriter`).
6. Inserts the message id into `processed_message` with the outcome.

If the same message id is processed twice concurrently, the second commit fails on the `processed_message` primary key and is retried, at which point step 1 finds the recorded outcome.

On a rejection, the rejection is recorded in the **same** transaction (`RejectionRecorder`): the message id goes into `processed_message` with outcome `REJECTED_LOWER_VERSION` or `REJECTED_MATURITY_IN_PAST`, and a `REJECTED` event with the reason goes into `outbox_event`. Nothing else has been written when a rule fails, and `process` is declared `noRollbackFor = TradeRejectedException`, so the transaction commits just the rejection record, and processing a trade never needs more than one pool connection. The exception is then rethrown to the adapter, which satisfies the requirement that a lower version "generates an exception".

If two first versions of the same new trade id arrive concurrently (for example one per ingestion path), there is no row to lock yet, so both pass validation and one insert fails on the primary key. The trade is written with an immediate flush, and a unique violation on a new trade id is reported as a transient `ConcurrencyFailureException`: the Kafka path retries the message and the REST path answers `503`. On retry the row exists and the normal version check applies. Other integrity violations (for example a value too long for its column) are permanent and are not retried as transient.

### 2.6 PostgreSQL

The **authoritative trade state** and the write-side store for idempotency and the outbox. Schema changes are managed with Liquibase. See §3 for the data model and migrations.

### 2.7 Outbox Relay

A scheduled poller (single active instance via ShedLock). Each poll reads the oldest pending events **separately for each destination** (each query is backed by its own partial index) and delivers them:

- **to MongoDB** – upserts the event into `trade_events` using the outbox event id as `_id`, so redelivery is idempotent, and applies it to the `trade_views` projection (§2.8);
- **to Kafka** – publishes `EXPIRED` events to the `trade-status` topic, keyed by trade id.

Each destination is read, delivered and marked independently (`mongo_delivered_at`, `kafka_delivered_at`, each set by its own `UPDATE`), so an outage or backlog of one destination never holds up the other. Within a poll, the first failed delivery to a destination stops further deliveries to it until the next poll. This bounds a poll to one timeout per destination, well inside the ShedLock lease, and keeps per-trade order on `trade-status`. Failed deliveries are retried on the next poll. Debezium (change data capture on the outbox table) is a drop-in alternative to the poller if lower latency is needed.

### 2.8 MongoDB

The **trade event history** and read side.

- `trade_events` – one immutable document per event (created, updated, replaced, rejected with reason, expired), including the full trade payload and the event source.
- `trade_views` – a read-optimised projection holding the current state of each trade (`_id` = trade id), indexed by book. The relay applies every event to it (`TradeViewProjector`): `CREATED`, `UPDATED` and `REPLACED` upsert the trade only if the view holds the same or a lower version; `EXPIRED` marks the view expired only if it still holds the expired version; `REJECTED` changes nothing. Every update is conditional on the version, so redelivered or stale events never move a view backwards. It serves `GET /api/v1/books/{bookId}/trades` and is eventually consistent with PostgreSQL, typically within one relay poll interval. Further views (for example by counter-party) are added the same way.

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
| `GET /api/v1/books/{bookId}/trades?page=&size=` | MongoDB `trade_views` projection, paginated; eventually consistent |

All read endpoints require a token with the `trades:read` scope.

The REST API is described by an OpenAPI 3 document at `/v3/api-docs` and can be browsed and called from Swagger UI at `/swagger-ui.html` (springdoc-openapi, `OpenApiConfig`). The document declares one HTTP bearer (JWT) security scheme and applies it to every operation, so Swagger UI sends the token entered under **Authorize** with each request.

### 2.11 Security

- **REST** – OAuth2 resource server with JWT bearer tokens (`SecurityConfig`). Tokens are verified against the issuer's published keys (`spring.security.oauth2.resourceserver.jwt.issuer-uri`, set with `OAUTH2_ISSUER_URI`), and the issuer claim is checked. `POST /api/v1/trades` requires the `trades:write` scope; read endpoints require `trades:read`; Actuator `health` and `info` are public, other Actuator endpoints need any valid token. The OpenAPI document and Swagger UI (§2.10) are public, because they only describe the API; calls made from Swagger UI carry the caller's own token. `API_DOCS_ENABLED=false` turns both off. The API is stateless, so sessions and CSRF protection are disabled. Request bodies are size limited (§2.4) and validated with Bean Validation (identifiers at most 64 characters, version ≥ 1, maturity date required). For local runs, `compose.yml` starts Keycloak with a `trade-store` realm and two service-account clients (`trade-producer`, `trade-reader`).
- **Kafka** – the `kafka-sasl` Spring profile (`application-kafka-sasl.yml`) switches the clients to `SASL_SSL` with SCRAM-SHA-512. Credentials and the trust store come from `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TRUSTSTORE_LOCATION` and `KAFKA_TRUSTSTORE_PASSWORD`; the protocol is fixed, so a missing secret makes the connection fail instead of falling back to an unauthenticated one. Topic ACLs are set on the cluster: producers may only write to `trades`; the application may read `trades` (consumer group `trade-store`) and write `trade-status` and `trades-dlt`:

  ```shell
  kafka-acls.sh --bootstrap-server <broker> --command-config admin.properties --add \
    --allow-principal User:<producer> --operation Write --operation Describe --topic trades
  kafka-acls.sh --bootstrap-server <broker> --command-config admin.properties --add \
    --allow-principal User:trade-store --operation Read --operation Describe --topic trades --group trade-store
  kafka-acls.sh --bootstrap-server <broker> --command-config admin.properties --add \
    --allow-principal User:trade-store --operation Write --operation Describe --topic trade-status --topic trades-dlt
  ```

  Topics are created by the platform team when ACLs are enforced, because the application is not granted `Create`.
- **Secrets** – credentials are supplied through environment variables (`POSTGRES_PASSWORD`, `MONGODB_URI`, …) and never committed to the repository.

### 2.12 Monitoring and Observability

Observability today consists of Micrometer metrics, structured logs and Actuator health checks. There is no distributed tracing and no metrics export to a monitoring backend yet; [architecture-v2.md §4](architecture-v2.md#4-observability-and-monitoring) proposes both.

#### How metrics are produced

All metrics go into the Micrometer `MeterRegistry` that Spring Boot Actuator creates (`spring-boot-starter-actuator`). Three kinds of metric feed it:

1. **Application counters** – `TradeMetrics` (package `observability`) is a Spring bean that the services call at the point where something happens:
   - `TradeProcessingService` calls `tradeProcessed(source, outcome)` for every message. The call is registered as an after-commit transaction callback, so a transaction that is rolled back and retried is counted once, and a rejection is counted only once its rejection record has committed. Duplicates are counted with outcome `DUPLICATE`.
   - `ExpiryScheduler` calls `tradesExpired(n)` after each batch, `ProcessedMessagePurgeScheduler` calls `processedMessagesPurged(n)`, and `IngestionLimitInterceptor` calls `rateLimited()` for every `429`.
2. **Application gauge** – `OutboxBacklogMetrics` implements Micrometer's `MeterBinder` and registers `tradestore.outbox.pending` twice, tagged `destination=mongodb` and `destination=kafka`. A gauge is evaluated when it is read, so each read runs a `COUNT` on `outbox_event` that is backed by the destination's partial index. When PostgreSQL is down the read fails and Micrometer reports `NaN`.
3. **Built-in metrics** registered automatically by Spring Boot: HTTP server requests (`http.server.requests`), JVM and process metrics, the HikariCP pool (`hikaricp.connections.*`), the MongoDB driver pool (`mongodb.driver.pool.*`), Kafka client metrics including consumer lag (`kafka.consumer.*`), Kafka listener timings and scheduled task executions.

#### How metrics are read

Actuator exposes the registry at `/actuator/metrics` and `/actuator/metrics/{name}?tag=key:value` (JSON, pull-based, requires any valid JWT, §2.11). No Prometheus or OTLP registry is on the classpath, so metrics are not scraped or pushed anywhere; adding `micrometer-registry-prometheus` (and exposing `prometheus`) is the step that connects them to Prometheus and Grafana.

Application metrics:

| Metric | Type | Meaning |
|--------|------|---------|
| `tradestore.trades.processed` | Counter, tags `source`, `outcome` | Processed messages: `CREATED`, `UPDATED`, `REPLACED`, `REJECTED_LOWER_VERSION`, `REJECTED_MATURITY_IN_PAST` or `DUPLICATE`. Counted after the transaction commits, so retried transactions are counted once. The sum per source is the number received. |
| `tradestore.trades.expired` | Counter | Trades marked expired by the expiry scheduler |
| `tradestore.rest.rate.limited` | Counter | REST ingestion requests rejected with `429` |
| `tradestore.processed.messages.purged` | Counter | Idempotency records deleted by the purge job |
| `tradestore.outbox.pending` | Gauge, tag `destination` (`mongodb`, `kafka`) | Outbox events not yet delivered; a growing value means a destination is down or the relay is behind |
| `kafka.consumer.fetch.manager.records.lag.max` | Gauge | Consumer lag, published by the Kafka client through Spring Boot's Micrometer integration |

#### Logs

Logs are structured JSON in Elastic Common Schema (`logging.structured.format.console: ecs`), so log shippers can index them without parsing. Every stored, rejected or duplicate trade is logged with its trade id and message id. Delivery failures in the outbox relay are logged at `WARN` with the outbox event id and trade id, transient database failures on the REST path at `WARN`, and each scheduled job logs a summary line when it completes (`expiredTrades=`, `purgedRecords=`).

#### Health

`/actuator/health` is public and reports only the overall status. It includes Spring Boot's PostgreSQL (`db`) and MongoDB (`mongo`) health indicators, so it returns `503` with status `DOWN` when either database is unreachable; Spring Boot has no Kafka health indicator. On Kubernetes, Spring Boot also exposes `/actuator/health/liveness` and `/actuator/health/readiness`, which do not include the databases. Use those for the probes, so that a database outage does not cause pod restarts.

#### What to watch

| Symptom | Metric or signal |
|---------|------------------|
| Kafka ingestion falling behind or stalled (for example PostgreSQL down) | `kafka.consumer.fetch.manager.records.lag.max` rising |
| MongoDB or Kafka delivery stopped | `tradestore.outbox.pending{destination=...}` rising |
| Unusual rejection rate | `tradestore.trades.processed{outcome=REJECTED_*}` |
| A noisy REST producer | `tradestore.rest.rate.limited` |
| Messages dead-lettered | records arriving on `trades-dlt` (no metric yet) |
| PostgreSQL pool exhausted | `hikaricp.connections.pending` > 0, `hikaricp.connections.timeout` rising |
| Expiry job did not run | no `Expiry run completed` log line after 00:05 UTC; `tradestore.trades.expired` flat |

## 3. Data Model

### PostgreSQL

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `trades` | Current version of each trade | `trade_id` (PK), `version`, `counter_party_id`, `book_id`, `maturity_date`, `created_date`, `expired`, `updated_at` |
| `processed_message` | Idempotency (inbox) | `message_id` (PK), `trade_id`, `outcome`, `processed_at` |
| `outbox_event` | Events awaiting delivery | `id` (PK, UUID), `trade_id`, `version`, `type`, `source`, `payload` (JSONB), `created_at`, `mongo_delivered_at`, `kafka_delivered_at` |
| `shedlock` | Distributed locks for scheduled jobs | `name` (PK), `lock_until`, `locked_at`, `locked_by` |

Indexes: `trades (expired, maturity_date)` for the expiry scheduler, `trades (book_id)` and `trades (counter_party_id)` for queries, and partial indexes on `outbox_event` for undelivered rows. A daily job (`ProcessedMessagePurgeScheduler`, ShedLock `purge-processed-messages`, 00:30 UTC) deletes `processed_message` rows older than `trade-store.purge.processed-message-retention` (14 days, longer than Kafka's default retention of 7 days) in batches of 1,000, using the `processed_at` index. A message redelivered after its record was purged would be processed again, so the retention must always exceed the Kafka retention and the longest period a REST client retries with the same `Idempotency-Key`.

### MongoDB

| Collection | Purpose |
|------------|---------|
| `trade_events` | Full event history, `_id` = outbox event id, indexed by `tradeId` and `occurredAt` |
| `trade_views` | Current state of each trade, `_id` = trade id, indexed by `bookId` (§2.8) |

### Schema migrations

Both databases are migrated with Liquibase at startup, from one changelog tree:

```
src/main/resources/db/changelog/
├── postgres/
│   ├── db.changelog-master.yaml
│   └── changes/001-create-trade-store-tables.sql   (formatted SQL with rollback statements)
└── mongodb/
    ├── db.changelog-master.yaml
    ├── changes/001-create-trade-events.yaml        (collection and index change sets)
    └── changes/002-create-trade-views.yaml         (projection collection and book index)
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

1. A producer calls `POST /api/v1/trades` with a JWT bearer token carrying the `trades:write` scope and, optionally, an `Idempotency-Key`. A missing or invalid token is rejected with `401`, a token without the scope with `403`.
2. The rate limiter admits the request or rejects it with `429`; a body without a length or over the size limit is rejected with `411` or `413`.
3. The request body is validated; a malformed request is rejected with `400`.
4. The processing service runs the steps in §2.5.
5. The outcome is returned as an HTTP status (see §2.4).

### Event delivery

1. The outbox relay reads undelivered events.
2. It upserts them into MongoDB (`trade_events` and `trade_views`) and publishes `EXPIRED` events to `trade-status`.
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
| Transient failure (database unavailable, lock timeout, primary key race on a new trade id) | Retried with exponential backoff (1 s doubling to 60 s, for up to 10 minutes). The partition is not skipped, so per-trade ordering is preserved; consumer lag grows | Lock or query timeout and the primary key race: `503 Service Unavailable` with `Retry-After`, caller retries. Database unreachable: `500` (§6.1) |
| Unexpected error after retries are exhausted | Sent to the dead-letter topic with the exception details | `500 Internal Server Error` |

Business rejections are excluded from retries (configured as not-retryable in Spring Kafka's `DefaultErrorHandler`), so a rejected trade never blocks the trades behind it on the same partition. Every other exception on the Kafka path is retried, whatever its type.

The sections below describe what happens when each dependency is unavailable. In every case, no trade is lost and no state is half-written: a trade, its outbox event and its idempotency record commit together or not at all (§2.5), and every delivery after the commit is retried and idempotent.

Each table lists, per component, the exception that reaches application code, whether it is handled and by which class, and the result. "Not handled" means no application class catches it: it propagates to the framework, which applies its default behaviour (named in the table). §6.6 lists every exception handler in one place.

### 6.1 PostgreSQL down

PostgreSQL holds the authoritative state, so nothing can be written while it is down. HikariCP waits `connection-timeout` (5 s) for a connection and then fails with `SQLTransientConnectionException`. Which Spring exception the application sees depends on where the connection is needed:

- at the start of a transaction (every `@Transactional` method and every Spring Data repository call): `CannotCreateTransactionException` (package `org.springframework.transaction`; it is **not** a `DataAccessException`);
- when an open connection is lost inside a transaction: `DataAccessResourceFailureException` (a non-transient `DataAccessException`);
- in ShedLock, which uses `JdbcTemplate`: `CannotGetJdbcConnectionException`.

A lock wait or a slow statement would raise `CannotAcquireLockException` or `QueryTimeoutException` (both `TransientDataAccessException`), but only if PostgreSQL's `lock_timeout` or `statement_timeout` is set; `application.yml` sets neither, so by default a blocked row lock waits indefinitely.

| Component | Exception | Handled? By | Result |
|-----------|-----------|-------------|--------|
| Kafka ingestion (`TradeConsumer` → `TradeProcessingService.process`) | `CannotCreateTransactionException`, `DataAccessResourceFailureException` | **Handled** by `DefaultErrorHandler` (bean in `KafkaConfig`). Neither exception is in its not-retryable list, so the record is retried with backoff (1 s doubling to 60 s). After 10 minutes, `DeadLetterPublishingRecoverer` (also in `KafkaConfig`) publishes the record to `trades-dlt`. | The partition waits and consumer lag grows; per-trade order is kept. An outage longer than 10 minutes dead-letters about one record per partition every 10 minutes. Re-publishing them after recovery is safe, because processing is idempotent and version-checked. |
| REST ingestion (`TradeIngestionController`) | `TransientDataAccessException` and subclasses: lock or statement timeout, and the `ConcurrencyFailureException` that `TradeProcessingService.save` throws for a concurrent first insert | **Handled** by `RestExceptionHandler.handleTransientDataAccess` | `503 Service Unavailable` with `Retry-After: 5`, logged at `WARN`. |
| REST ingestion (`TradeIngestionController`) | `CannotCreateTransactionException`, `DataAccessResourceFailureException` | **Not handled.** `RestExceptionHandler` has no handler for them, and its base class `ResponseEntityExceptionHandler` covers only Spring MVC exceptions. They fall through to Spring Boot's default error handling. | `500 Internal Server Error` after the 5 s connection timeout. |
| Query API, current state (`TradeQueryController` → `TradeQueryService`) | `CannotCreateTransactionException`, `DataAccessResourceFailureException` | **Not handled** (as above). | `500`. |
| Query API, history and by-book | none: these read MongoDB | – | Keep working, serving data up to the last delivered event. |
| Outbox relay, reading pending events (`OutboxRelay.deliverPendingEvents`) | `CannotCreateTransactionException` | **Not handled** by `OutboxRelay`, which only catches failures of the MongoDB and Kafka deliveries. It propagates out of the `@Scheduled` method, and Spring's scheduler error handler logs it at `ERROR` and suppresses it. | Poll skipped; the next poll (1 s later) tries again. |
| Outbox relay, marking an event delivered (`markMongoDelivered`, `markKafkaDelivered`) | `CannotCreateTransactionException`, `DataAccessResourceFailureException` | **Not handled** (as above). | The poll stops; the event was delivered but not marked, so it is delivered again on a later poll. Both destinations are idempotent for redelivery (§6.3, §10). |
| Expiry and purge jobs (`ExpiryScheduler`, `ProcessedMessagePurgeScheduler`) | `CannotGetJdbcConnectionException` from ShedLock, or `CannotCreateTransactionException` from a batch | **Not handled** by the jobs; logged and suppressed by Spring's scheduler error handler. | The run stops; batches committed before the failure are kept. The next attempt is the next daily run, which catches up because expiry is idempotent (`maturity_date < today`), so trades are marked expired up to one day late. There is no manual trigger; restarting with `TRADE_STORE_EXPIRY_CRON` set to run sooner is the workaround. |
| Outbox backlog gauge (`OutboxBacklogMetrics`) | `CannotCreateTransactionException` from the count query | **Handled** by Micrometer, which catches exceptions thrown by a gauge function | `tradestore.outbox.pending` reads `NaN`. |
| Health (`/actuator/health`) | – | **Handled** by Spring Boot's `DataSourceHealthIndicator` | Status `DOWN`, HTTP `503`. |
| Application start | Liquibase exception, wrapped in `BeanCreationException` | **Not handled**, on purpose: startup fails fast | The application does not start until PostgreSQL is back. |

### 6.2 Kafka down

Kafka is the primary ingestion path and the destination of status events. The REST path does not depend on it.

| Component | Exception | Handled? By | Result |
|-----------|-----------|-------------|--------|
| Producers | – | – | Cannot publish to `trades`; this is the case the REST endpoint exists for. Producers switch to `POST /api/v1/trades`. |
| Kafka consumer, fetching | none reaches application code | **Handled** inside the Kafka client, which logs connection warnings and reconnects with its own backoff | Consumption pauses and resumes from the last committed offset. |
| Kafka consumer, committing the offset after the transaction committed | `org.apache.kafka.common.errors.TimeoutException` or `CommitFailedException` | **Handled** by the Spring Kafka listener container, which logs the failure | The record is delivered again after recovery. `TradeProcessingService.process` finds its message id in `processed_message` and returns the recorded outcome without writing anything; the metric counts it as `DUPLICATE`. |
| Dead-letter publishing (`DeadLetterPublishingRecoverer`) | `KafkaException` when the dead-letter send fails | **Handled** by `DefaultErrorHandler`: a failed recovery leaves the record uncommitted, so it is retried | The record is not lost. |
| Outbox relay, Kafka destination (`OutboxRelay.deliverToKafka`) | `ExecutionException` (wrapping the producer's `TimeoutException` when no broker answers), `java.util.concurrent.TimeoutException` (the relay's 10 s wait), Spring's `KafkaException`, `InterruptedException` | **Handled** by `OutboxRelay.deliverToKafka`, which logs at `WARN` and stops Kafka deliveries for that poll | The event stays pending and `tradestore.outbox.pending{destination=kafka}` grows. Events are delivered in order once Kafka is back. A send that timed out may still have reached Kafka, so `trade-status` is at-least-once and consumers deduplicate by `eventId`. |
| Outbox relay, MongoDB destination | none | – | Still delivered, because each destination is read and marked separately. Both are delivered on one scheduler thread with a fixed delay, though, so each poll waits for the Kafka timeout: 10 s, or up to the producer's `max.block.ms` (60 s) if the topic metadata is not cached. MongoDB then receives one batch (200 events) per 10–60 s instead of per second. |
| REST ingestion | none | – | Unaffected: trades are written to PostgreSQL and their events wait in the outbox. |
| Application start | topic creation fails in `KafkaAdmin` | **Handled** by Spring Kafka's `KafkaAdmin`, which logs the error (`fatalIfBrokerNotAvailable` is `false`) | The application starts; the consumer keeps reconnecting. |

### 6.3 MongoDB down

MongoDB is not on the write path, so ingestion is unaffected. History and views fall behind until it is back. The driver waits for its server selection timeout (30 s by default, not configured) and throws `MongoTimeoutException`, which Spring Data translates into a `DataAccessException` (`DataAccessResourceFailureException`).

| Component | Exception | Handled? By | Result |
|-----------|-----------|-------------|--------|
| Kafka and REST ingestion | none | – | Unaffected. |
| Outbox relay, MongoDB destination (`TradeEventMongoRepository.save`, `TradeViewProjector.apply`) | `DataAccessResourceFailureException` | **Handled** by `OutboxRelay.deliverToMongo`, which catches every `DataAccessException`, logs at `WARN` and stops MongoDB deliveries for that poll. `TradeViewProjector` itself catches only `DuplicateKeyException`, which is its version guard, not a failure. | Events stay pending and `tradestore.outbox.pending{destination=mongodb}` grows. After recovery they are upserted with the outbox event id as `_id`, and view updates are version-guarded, so redelivery never duplicates history or moves a view backwards. |
| Outbox relay, Kafka destination | none | – | Still delivered, but each poll first waits for the 30 s MongoDB timeout, so status events are published about every 30 s. |
| Query API, history and by-book (`TradeQueryService` → `TradeEventMongoRepository`, `TradeViewMongoRepository`) | `DataAccessResourceFailureException` | **Not handled**: `RestExceptionHandler` handles only `TransientDataAccessException`, and this one is non-transient | `500` after the 30 s timeout. |
| Query API, current state | none: reads PostgreSQL | – | Unaffected. |
| Health | – | **Handled** by Spring Boot's `MongoHealthIndicator` | Status `DOWN`, HTTP `503`. |
| Application start (`MongoLiquibaseMigration.afterPropertiesSet`) | Liquibase exception, wrapped in `BeanCreationException` | **Not handled**, on purpose: startup fails fast | The application does not start. Set `trade-store.mongodb.liquibase.enabled=false` to start without MongoDB once the schema is in place. |

### 6.4 Application instance down

Instances share no in-memory state except the rate-limiter buckets, so losing an instance loses no data. The crashed instance raises nothing; the effects show up in the components that remain.

| Component | Exception | Handled? By | Result |
|-----------|-----------|-------------|--------|
| In-flight transaction | none | **Handled** by PostgreSQL, which rolls back the open transaction | Nothing is half-written. |
| Kafka ingestion on the remaining instances | `CommitFailedException` if a rebalance happens while a record is being processed | **Handled** by the Spring Kafka listener container | The crashed instance's partitions are reassigned after the consumer session times out. Uncommitted records are delivered again; those already committed are answered from `processed_message` by `TradeProcessingService.process`. |
| REST clients | connection reset, seen by the client | Not visible to the application | A retry with the same `Idempotency-Key` returns the original outcome; a retry without a key is applied again, which for the same version is a harmless `REPLACED`. |
| Outbox relay | none | ShedLock: the `outbox-relay` lock expires after `lockAtMostFor` (10 minutes) | Delivery pauses for up to 10 minutes, then another instance takes over. Events delivered but not yet marked are delivered again, idempotently. |
| Expiry and purge jobs | none | ShedLock, as above | Batches already committed are kept; the rest is done by the next daily run. |
| All instances down | – | – | Kafka keeps the published trades (7 days' retention) and consumption resumes from the committed offsets. The REST endpoint is unavailable. |

### 6.5 OAuth2 issuer down

The application validates JWTs locally with the issuer's public keys, which the JWT decoder fetches on the first request and caches. While the issuer is down, requests keep working as long as the keys are cached. Kafka ingestion, the outbox relay and the scheduled jobs do not use the issuer, so producers that cannot obtain a token can publish to Kafka instead.

| Component | Exception | Handled? By | Result |
|-----------|-----------|-------------|--------|
| REST, keys not cached or unknown key id | `JwtException` from the decoder when it cannot fetch the keys, which `JwtAuthenticationProvider` turns into `AuthenticationServiceException` | **Not handled** by the application. Spring Security treats it as a server-side failure, not a bad token, and does not map it to `401`. | `500` until the issuer is reachable again. |
| REST, invalid or expired token | `InvalidBearerTokenException` | **Handled** by Spring Security's `BearerTokenAuthenticationEntryPoint` | `401` with `WWW-Authenticate: Bearer error="invalid_token"`. |

### 6.6 Exception handlers in one place

| Exception | Raised by | Handled by | Result |
|-----------|-----------|------------|--------|
| `LowerVersionTradeException` | `VersionRule` | REST: `RestExceptionHandler.handleLowerVersion`. Kafka: `TradeConsumer.onTrade` (logs, completes normally) | REST `409`. Kafka: offset committed, partition not blocked |
| `MaturityDateInPastException` | `MaturityDateRule` | REST: `RestExceptionHandler.handleMaturityDateInPast`. Kafka: `TradeConsumer.onTrade` | REST `422`. Kafka: offset committed |
| `InvalidTradeMessageException` | `TradeConsumer` (bad JSON, schema version, fields, key) | `DefaultErrorHandler` in `KafkaConfig`: not retryable, recovered by `DeadLetterPublishingRecoverer` | Published to `trades-dlt` immediately |
| Invalid REST body (`MethodArgumentNotValidException`, `HttpMessageNotReadableException`) | Spring MVC | `ResponseEntityExceptionHandler`, the base class of `RestExceptionHandler` | `400` problem detail |
| `RateLimitExceededException` | `IngestionLimitInterceptor` | `RestExceptionHandler.handleRateLimitExceeded` | `429` with `Retry-After` |
| `TradeNotFoundException` | `TradeQueryController` | `RestExceptionHandler.handleTradeNotFound` | `404` |
| `DataIntegrityViolationException` (unique violation on a new trade id) | `tradeRepository.saveAndFlush` | `TradeProcessingService.save`, which rethrows it as `ConcurrencyFailureException` | Treated as transient, see next row |
| `TransientDataAccessException` (incl. `ConcurrencyFailureException`, `QueryTimeoutException`, `CannotAcquireLockException`) | JPA / JDBC | REST: `RestExceptionHandler.handleTransientDataAccess`. Kafka: `DefaultErrorHandler` | REST `503` with `Retry-After: 5`. Kafka: retried with backoff |
| `DuplicateKeyException` | `TradeViewProjector` (stale event) | `TradeViewProjector.apply` | Stale event ignored; the view keeps the newer version |
| `DataAccessException` while delivering to MongoDB | MongoDB repositories, `MongoTemplate` | `OutboxRelay.deliverToMongo` | Event left pending, retried next poll |
| Kafka send failures (`ExecutionException`, `TimeoutException`, `KafkaException`, `InterruptedException`) | `KafkaTemplate` | `OutboxRelay.deliverToKafka` | Event left pending, retried next poll |
| Any other exception on the Kafka path | anything below `TradeConsumer` | `DefaultErrorHandler` | Retried for 10 minutes, then `trades-dlt` |
| **Not handled:** `CannotCreateTransactionException`, `DataAccessResourceFailureException` (PostgreSQL or MongoDB) on the REST path | JPA, MongoDB repositories | Spring Boot default error handling | `500` |
| **Not handled:** `AuthenticationServiceException` (issuer unreachable) | Spring Security | Spring Security rethrows it | `500` |
| **Not handled:** any exception in a scheduled job or in the outbox relay outside the two delivery methods | `ExpiryScheduler`, `ProcessedMessagePurgeScheduler`, `OutboxRelay` | Spring's scheduler error handler | Logged at `ERROR`; the run or poll is abandoned |
| **Not handled:** `JacksonException` when an outbox payload cannot be parsed (`OutboxRelay.toDocument`) | `JsonMapper` | Spring's scheduler error handler | That event fails on every poll and blocks MongoDB delivery behind it. Payloads are written by the application itself, so this needs a defect to happen. |

### 6.7 Known gaps

- **Database unreachable on the REST path returns `500`, not `503`** (§6.1, §6.3). Mapping `CannotCreateTransactionException` and `DataAccessResourceFailureException` to `503` with `Retry-After` in `RestExceptionHandler` would tell callers to retry.
- **No `lock_timeout` or `statement_timeout`.** A request blocked on a row lock waits indefinitely, and the `503` path for lock timeouts is never taken.
- **Long PostgreSQL outages dead-letter Kafka records** after 10 minutes per record (§6.1). A longer or unlimited retry window for connection failures would keep them on the partition instead.
- **One slow destination slows the other in the outbox relay**, because both are delivered on the same scheduler thread (§6.2, §6.3).
- **MongoDB reads hang for 30 s** before failing, because the server selection timeout is the driver default.
- **A failed daily job waits a day** for its next run (§6.1).
- **An unreachable OAuth2 issuer returns `500`, not `503`** (§6.5).
- **An outbox event with an unreadable payload blocks MongoDB delivery** (§6.6).
- **No metric or alert for the dead-letter topic.**

[architecture-v2.md §9](architecture-v2.md#9-failure-handling) describes how the target architecture closes each gap.

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
- **Why a rate limiter** – the REST path has no Kafka buffer in front of the database. The limiter caps the load each client can put on PostgreSQL and fails fast with `429`.
- **Why both SQL and NoSQL** – PostgreSQL gives strong consistency for the current trade state, the version rule, idempotency and the outbox. MongoDB holds the append-only history and read models, where schema flexibility matters more than relational constraints.

## 10. Known Limitations

- With in-memory Bucket4j buckets, the rate limit is per instance; an exact global limit needs the Redis-backed store.
- The `trade_views` projection and `GET /api/v1/books/{bookId}/trades` lag PostgreSQL by up to one relay poll interval; `GET /api/v1/trades?bookId=` reads PostgreSQL and is always current.
- A message redelivered after its `processed_message` record was purged is processed again. The retention (14 days) is chosen to exceed the Kafka retention.
- MongoDB history is eventually consistent with PostgreSQL, behind by up to one relay poll interval.
- Between scheduler runs, a trade whose maturity date has just passed is still shown as not expired. Running the scheduler shortly after midnight keeps this window small.
- When the same trade id and version arrive on both paths with different contents, which one is kept depends on commit order.
- A Kafka send that times out in the outbox relay may still be delivered by the producer later, so `trade-status` is at-least-once. Consumers deduplicate by `eventId`.

## 11. Test Strategy

| Layer | Tests | What they prove |
|-------|-------|-----------------|
| Unit (`*Test`, `mvn test`) | Validation rules, processing service, consumer, controllers (with the real security rules and mock JWTs), rate limiter and size limits, expiry, purge, outbox relay, metrics, configuration | Business rules, error mapping, authorization and limits, with external services mocked |
| Integration (`*IT`, `mvn verify`, Testcontainers) | `TradeStoreFlowIT`, `TradeExpiryRepositoryIT`, `TradeQueryServiceIT`, `TradeViewProjectorIT`, `ProcessedMessagePurgeIT`, `SchemaMigrationIT`, `DatabasePoolIT`, `OpenApiDocsIT` | SQL, MongoDB queries and projections, Kafka consumption, Liquibase migrations and the published OpenAPI document against real services |
| Sample data (`SampleDataScenarioIT`) | The assignment's sample table, end to end | Each row is reproduced by receiving the trade while its maturity date was still in the future (a movable test clock); the lower-version, same-version, past-maturity and expiry rules on the same data |
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
| Kafka down with more pending status events than one relay batch | Newer events still reach MongoDB; all status events are published after recovery |
| Two first versions of a new trade id stored concurrently | The loser gets a transient `ConcurrencyFailureException`; its retry is applied as a same-version replacement |

`RejectionSingleConnectionIT` runs with a one-connection PostgreSQL pool and proves that rejections are recorded without a second connection.

Integration tests start their own containers with Testcontainers (`TestcontainersConfiguration`), using the same images as `compose.yml`. Test JVMs run with `-Duser.timezone=UTC` (set in `pom.xml`).

## 12. Implementation Status

| Area | Status |
|------|--------|
| Kafka ingestion, dead-letter topic, retries with backoff | Implemented |
| REST ingestion with idempotency key, problem-detail errors | Implemented |
| Validation rules (maturity date, version), rejection recording | Implemented |
| PostgreSQL state, idempotency table, transactional outbox | Implemented |
| Outbox relay to MongoDB history and the `trade-status` topic | Implemented |
| Expiry scheduler with ShedLock | Implemented |
| Read-only query API (trades, trade, history) | Implemented |
| Liquibase migrations for PostgreSQL and MongoDB, connection pools | Implemented |
| Local infrastructure (`compose.yml`) | Implemented |
| OAuth2/JWT security for REST, Kafka SASL_SSL profile and ACLs | Implemented (ACLs are applied on the cluster, §2.11) |
| Bucket4j rate limiter and request size limits | Implemented |
| MongoDB `trade_views` projection and by-book endpoint | Implemented |
| Purge job for old `processed_message` rows | Implemented |
| Application metrics and structured (ECS JSON) logs | Implemented |
| OpenAPI document and Swagger UI | Implemented |
