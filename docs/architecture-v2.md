# Trade Store – Target Architecture (v2)

This document proposes the next version of the trade store architecture. It reviews the current design ([architecture.md](architecture.md), diagram [`design.puml`](diagrams/design.puml)) against enterprise requirements for scalability, resiliency, observability, security and operations, and describes the target design drawn in [`design-v2.puml`](diagrams/design-v2.puml).

None of this is implemented. The core of the current design is kept: Kafka as the primary ingestion path, REST as a backup, one Trade Processing Service that owns all validation, and the transactional outbox. Most changes are about how the system is deployed, scaled and observed, not about the business rules.

## 1. Summary of Changes

| Area | Current (v1) | Target (v2) | Main gain |
|------|--------------|-------------|-----------|
| Event delivery | Outbox relay polls `outbox_event`; one active instance (ShedLock) | Debezium CDC reads the outbox from the PostgreSQL WAL and publishes to `trade-events` | No polling, no single-instance bottleneck, lower latency |
| MongoDB projection | Written by the outbox relay | Separate Projection Service consuming `trade-events` | Scales by partition; MongoDB outages no longer touch the write path |
| Status events | Only `EXPIRED` events on `trade-status` | Every event on `trade-events`, keyed by trade id | Downstream systems get the full lifecycle |
| Deployment | One Spring Boot application | Ingestion, query, projection and jobs deployed and scaled separately on Kubernetes across 3 AZs | Independent scaling and failure isolation |
| Autoscaling | Manual instance count | KEDA scales Kafka ingestion on consumer lag; HPA on CPU for REST and query | Absorbs bursts without manual action |
| Rate limiting | In-memory Bucket4j, per instance | API gateway with a global limit per client (or Bucket4j on Redis) | Exact limit regardless of instance count |
| Edge security | JWT validated in the application | API gateway with WAF, TLS termination and JWT validation; the application still checks scopes | Defence in depth; abusive traffic stopped before the service |
| Message contract | JSON with `schemaVersion`, checked in code | Schema Registry (Avro or JSON Schema) with backward-compatibility checks | Incompatible producers are rejected at publish time |
| PostgreSQL | Single instance | Primary with synchronous standby (Patroni or managed), PgBouncer, read replicas | No data loss on failover; reads off the primary |
| Idempotency purge | Batched `DELETE` | `processed_message` partitioned by day; old partitions dropped | Constant-time purge, no table bloat |
| Expiry | Daily job; trade shows as not expired until it runs | Daily job kept, plus `expired` computed at read time (`maturity_date < today`) | Removes the window in which a matured trade appears active |
| Dead-letter topic | Messages land in `trades-dlt`; no metric | DLT metrics and alerts; replay tool to inspect, fix and re-publish; `trade-events-dlt` for the projection | Defined, visible recovery procedure |
| Dependency failures | Several end in `500`, a 30 s hang, a dead-lettered record or a job skipped for a day ([architecture.md §6.7](architecture.md#67-known-gaps)) | Every failure is retried or answered with `503` + `Retry-After`; database timeouts, circuit breakers, unlimited retry for connection failures (§9) | Predictable behaviour during outages |
| Observability | Actuator metrics, ECS logs | OpenTelemetry traces, metrics and logs; SLO dashboards and burn-rate alerts | End-to-end trace of a trade; alerts on user impact |
| Secrets | Environment variables | Vault or cloud KMS with rotation | No long-lived static secrets |
| Service-to-service | Plain connections | Service mesh with mTLS | Encrypted, authenticated internal traffic |
| Disaster recovery | None | Warm standby region: MirrorMaker 2, PostgreSQL async replica, MongoDB secondary | Documented RPO and RTO |
| Delivery | CI pipeline | GitOps (Argo CD), canary releases, infrastructure as code (Terraform) | Repeatable environments, safer releases |

## 2. Scalability

- **Write path.** Kafka ingestion scales horizontally up to the partition count, and KEDA adds consumers when lag grows. The partition count is sized for peak throughput up front, because adding partitions breaks per-trade ordering (architecture.md §2.2).
- **Database writes.** PgBouncer in transaction mode lets many application pods share a small number of PostgreSQL connections. The row lock stays per trade id, so contention only grows with traffic on the same trade.
- **Event delivery.** Debezium reads the outbox from the WAL instead of polling, and publishes with the trade id as key, so per-trade order is preserved. Outbox rows can be deleted soon after they are written, which keeps the table small. The Projection Service scales with the partitions of `trade-events`.
- **Read path (CQRS).** The query service reads current state from PostgreSQL read replicas and history from MongoDB secondaries, so read load never competes with ingestion. If MongoDB grows beyond one replica set, `trade_views` is sharded by `bookId`, its main query key.
- **Rate limiting.** A global limit at the gateway replaces the per-instance limit (a known limitation in v1).

## 3. Resiliency

- **Zone failure.** Every stateful component runs across three availability zones: Kafka with replication factor 3 and `min.insync.replicas=2`, PostgreSQL with a synchronous standby and automatic failover, MongoDB as a three-member replica set. Kubernetes PodDisruptionBudgets and topology spread keep services available during node maintenance.
- **Failure isolation.** Because the projection is a separate consumer, a MongoDB outage only delays history and views; ingestion and the outbox are unaffected. A Kafka outage still leaves the REST path writing directly to PostgreSQL, as in v1, and Debezium resumes from its WAL position when Kafka returns.
- **Timeouts and retries.** Calls to dependencies have explicit timeouts and bounded retries with backoff, enforced by the service mesh or Resilience4j. Readiness probes take a pod out of rotation when its database is unreachable, instead of failing requests.
- **Poison messages.** The DLT replay tool gives operators a defined way to inspect a dead-lettered message, fix the cause and re-publish it.
- **Disaster recovery.** A warm standby region receives Kafka topics through MirrorMaker 2 (or Cluster Linking) and asynchronous replicas of PostgreSQL and MongoDB. Target: RPO of minutes, RTO under one hour. Failover is a documented, rehearsed runbook; it is not automatic, because promoting a region on a false alarm risks split-brain writes.

## 4. Observability and Monitoring

- **Tracing.** Every service is instrumented with OpenTelemetry. The trace context travels in Kafka headers and is stored on the outbox event, so one trace shows a trade from the producer through processing, CDC and projection.
- **Metrics.** The existing `tradestore.*` metrics are kept and exported through the OpenTelemetry Collector to Prometheus. New lag metrics cover each hop: Kafka consumer lag, Debezium replication lag, and projection lag (time between commit and the view update).
- **Logs.** The structured ECS JSON logs are shipped to Elastic or Loki and carry the trace id, trade id and message id.
- **Service level objectives.**

  | SLO | Target (proposal) |
  |-----|-------------------|
  | Ingestion availability (REST, non-4xx responses) | 99.9 % |
  | Ingestion latency, Kafka publish to commit | p99 < 2 s |
  | Projection freshness, commit to `trade_views` | p99 < 5 s |
  | Expiry job completes by 00:30 UTC | 100 % of days |

- **Alerting.** Alertmanager pages on SLO burn rate rather than raw thresholds, plus on a growing DLT, a stuck CDC connector and a failed expiry run.

## 5. Security

- API gateway with WAF and TLS in front of both REST paths; the application keeps validating JWT scopes, so a gateway misconfiguration does not expose the API.
- mTLS between services through the service mesh; Kafka keeps SASL_SSL and topic ACLs.
- Secrets in Vault or cloud KMS with rotation; encryption at rest for PostgreSQL, MongoDB and Kafka.
- Schema Registry rejects messages that do not match a registered, compatible schema.
- Audit: the MongoDB event history is already an immutable record of every accepted and rejected trade; retention is set by the compliance policy.

## 6. Delivery and Operations

- GitOps with Argo CD; canary releases with automatic rollback when the SLOs degrade.
- Infrastructure as code with Terraform for clusters, topics, ACLs and databases.
- Liquibase migrations run as a pre-deployment job instead of at application startup, so a failed migration stops the release before any pod starts.
- Contract tests for the `trades` and `trade-events` schemas; regular chaos tests (zone loss, broker loss, database failover) based on the scenarios already covered by `TradeStoreConsistencyIT`.

## 7. Trade-offs

- **More moving parts.** Debezium, Kafka Connect, a gateway, a service mesh and a second region all need to be operated. Each is justified only at the scale and availability targets above; for a single team with modest volume, the v1 deployment remains appropriate.
- **Topic contract change.** `trade-events` replaces `trade-status`. Both are published during a transition period until every consumer has moved.
- **Eventual consistency.** History and views stay eventually consistent with PostgreSQL, as in v1; the projection-freshness SLO makes the lag visible and bounded.
- **Synchronous standby.** It adds write latency (one network round trip per commit) in exchange for zero data loss on failover.

## 8. Migration Path

Each step is independently deployable and reversible:

1. Observability first: OpenTelemetry, SLO dashboards and alerts, so the effect of every later step can be measured.
2. PostgreSQL HA, PgBouncer and read replicas; point the query endpoints at the replicas.
3. Read-time expiry check and partitioned `processed_message`.
4. API gateway with global rate limiting; remove the in-memory limiter.
5. Debezium CDC publishing to `trade-events`, run in parallel with the outbox relay and compared; then the Projection Service; then retire the relay.
6. Split the application into separately deployed ingestion, query, projection and job workloads, with KEDA autoscaling.
7. Schema Registry, Vault, service mesh.
8. DR region and the failover runbook, rehearsed before it is relied on.

The failure-handling fixes in §9 that need no new infrastructure (marked v1-compatible) can be made at any time, ahead of the steps above.

## 9. Failure Handling

[architecture.md §6](architecture.md#6-error-handling-policy) lists, for each component that can be down, which exceptions v1 handles and which it does not, and ends with a list of gaps (§6.7). This section shows how v2 closes each gap. The principle: **every dependency failure is either retried until it succeeds, or turned into an explicit, retryable response; nothing ends in an unexplained `500` or a silent skip.**

### 9.1 Gap by gap

| v1 gap | v2 handling | Owner | v1-compatible |
|--------|-------------|-------|---------------|
| PostgreSQL or MongoDB unreachable returns `500` on REST | `RestExceptionHandler` maps `CannotCreateTransactionException`, `DataAccessResourceFailureException` and `CannotGetJdbcConnectionException` to `503` with `Retry-After`, like `TransientDataAccessException` today. The gateway passes `503` through and does not retry writes itself. | `RestExceptionHandler` | Yes |
| No `lock_timeout` / `statement_timeout`; a blocked request waits indefinitely | Set `lock_timeout` (2 s) and `statement_timeout` (10 s) on the application's database role, so lock waits end in `CannotAcquireLockException` / `QueryTimeoutException` and take the existing `503` and Kafka-retry paths. | Database role, Liquibase change set | Yes |
| Long PostgreSQL outages dead-letter Kafka records after 10 minutes | `DefaultErrorHandler` classifies connection failures (`CannotCreateTransactionException`, `DataAccessResourceFailureException`) separately: they are retried without a time limit (backoff capped at 60 s), so the partition waits for the database instead of dead-lettering. Only unknown errors keep the 10-minute limit. Consumer lag alerts tell operators the partition is waiting. | `KafkaConfig` | Yes |
| Kafka and MongoDB delivery slow each other down in the outbox relay | Removed: Debezium CDC publishes every event to `trade-events`, and the Projection Service consumes it independently. A MongoDB outage only stops the projection consumer; a Kafka outage only pauses CDC, which resumes from its WAL position. | Debezium, Projection Service | No (CDC). In v1, scheduling each destination on its own thread removes most of the effect |
| MongoDB reads hang for 30 s | MongoDB `serverSelectionTimeoutMS` set to 2 s and a socket timeout on reads; the query service wraps MongoDB reads in a Resilience4j circuit breaker, so during an outage history and by-book requests fail in milliseconds with `503`, and PostgreSQL-backed endpoints are unaffected. | `TradeQueryService`, MongoDB client settings | Yes |
| A failed daily job waits a day | Jobs run as Kubernetes CronJobs with `backoffLimit` (retries with backoff) and a `startingDeadlineSeconds` window; a job that still fails raises an alert. The expiry query is idempotent, so a retry is always safe. The read-time expiry check (§1) keeps query results correct while a job is late. | Scheduled Jobs | Partly: a retry in the scheduler works in v1 |
| An unreachable OAuth2 issuer returns `500` | The API gateway validates tokens with cached keys and returns `503` if it cannot reach the issuer and has no keys; the services keep validating scopes. The issuer runs highly available, and the key cache is configured to serve stale keys during an issuer outage. | API gateway, IdP | Partly: a custom `AuthenticationEntryPoint` can map the failure to `503` in v1 |
| An outbox event with an unreadable payload blocks MongoDB delivery | The Projection Service treats a message it cannot deserialize as a poison message: it is sent to `trade-events-dlt` after a few retries, and the consumer moves on. Per-trade order is kept for all other trades. | Projection Service | No |
| No metric or alert for dead-lettered messages | Kafka exporter metrics on `trades-dlt` and `trade-events-dlt`; an alert fires on any new message. The DLT Replay Tool shows the original exception (from the dead-letter headers) and re-publishes after a fix. | Observability, DLT Replay Tool | Partly: a counter in `DeadLetterPublishingRecoverer` works in v1 |

### 9.2 Failure handling by component in v2

| Component down | Write path | Read path | Event delivery | Detected by |
|----------------|------------|-----------|----------------|-------------|
| PostgreSQL primary | Synchronous standby is promoted (seconds); until then REST answers `503` + `Retry-After` and Kafka records wait on their partition without being dead-lettered | Current state served from read replicas (stale by replication lag); history and views unaffected | CDC pauses and resumes from its WAL position on the new primary | Health `DOWN`, consumer lag, failover event |
| Kafka cluster (all zones) | REST path continues; producers fall back to REST | Unaffected | Debezium pauses; events stay in the WAL / outbox and are published on recovery | Producer errors, CDC lag |
| MongoDB replica set | Unaffected | History and by-book return `503` immediately (circuit breaker open); current state unaffected | Projection consumer pauses and resumes from its committed offset | Projection lag, circuit breaker state metric |
| Projection Service | Unaffected | Views stop updating; reads still served | Messages accumulate on `trade-events` | Consumer lag on `trade-events` |
| Ingestion pod | Other pods take over its partitions; REST traffic is routed elsewhere | Unaffected | Unaffected | Pod restarts, rebalances |
| OAuth2 issuer | Gateway validates with cached keys; `503` only if keys are unavailable | Same | Unaffected | Gateway auth error rate |
| Whole region | Manual failover to the DR region (runbook, §3) | Same | Same | Synthetic probes, multi-region health checks |
