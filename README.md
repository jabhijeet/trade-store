# trade-store

A trade store that receives a high volume of trades (thousands, over any transmission channel), validates them, and stores each trade in a defined sequence based on its trade id and version.

> **Status:** Kafka and REST ingestion, the validation rules, automatic expiry, the transactional outbox to MongoDB and Kafka, the read-only query API, OAuth2/JWT security, per-client rate limiting, the MongoDB `trade_views` projection, the idempotency purge job, application metrics and structured logs are implemented and tested; see [Implementation Status](docs/architecture.md#12-implementation-status).

## Problem Statement

Thousands of trades are sent to a single store. The store must organize and store each trade in a specific sequence while enforcing the validation rules listed below.

### Trade model

| Field           | Description                                         |
|-----------------|-----------------------------------------------------|
| Trade Id        | Business identifier of the trade                    |
| Version         | Version of the trade; higher version is newer       |
| Counter-Party Id| Counter-party of the trade                          |
| Book-Id         | Book the trade belongs to                           |
| Maturity Date   | Date on which the trade matures                     |
| Created Date    | Date the trade record was created                   |
| Expired         | `Y` once the maturity date has passed, otherwise `N` |

### Sample data

| Trade Id | Version | Counter-Party Id | Book-Id | Maturity Date | Created Date   | Expired |
|----------|---------|------------------|---------|---------------|----------------|---------|
| T1       | 1       | CP-1             | B1      | 20/05/2020    | `<today date>` | N       |
| T2       | 2       | CP-2             | B1      | 20/05/2021    | `<today date>` | N       |
| T2       | 1       | CP-1             | B1      | 20/05/2021    | 14/03/2015     | N       |
| T3       | 3       | CP-3             | B2      | 20/05/2014    | `<today date>` | Y       |

### Validation rules

1. **Version check** – If a trade is received with a lower version than the one already stored for the same trade id, the store rejects it and raises an exception. A trade with the same version replaces the existing record.
2. **Maturity date check** – The store rejects any trade whose maturity date is earlier than today's date.
3. **Automatic expiry** – When a stored trade's maturity date has passed, the store automatically marks the trade as expired (`Expired = Y`).

## Architecture

Trades are ingested primarily through Kafka, partitioned by trade id. A REST endpoint (`POST /api/v1/trades`) is available as a backup when Kafka is unavailable and for direct ingestion. It requires an OAuth2 JWT bearer token with the `trades:write` scope, and each client is rate limited (Bucket4j, `429` with `Retry-After`). Both paths are thin adapters over one Spring Boot processing service, which applies the validation rules, guarantees idempotency and is the only writer of trade state.

PostgreSQL holds the authoritative trade state. State changes and their events are committed together through a transactional outbox, and an outbox relay delivers the events to MongoDB (trade history) and Kafka (status events). Everything runs in UTC: "today" for the maturity and expiry rules is the current UTC date. An expiry scheduler marks matured trades as expired. Read-only, paginated REST endpoints (scope `trades:read`) expose stored trades, their history, and the trades of a book from a MongoDB projection.

See [docs/architecture.md](docs/architecture.md) for details and PlantUML diagrams.

## Technical Requirements

- [x] Java application built with **Spring Boot**
- [x] A **streaming tool** for trade ingestion (Kafka)
- [x] Both a **SQL** and a **NoSQL** database (PostgreSQL and MongoDB)
- [x] All functionality covered by **JUnit** tests, developed using **Test-Driven Development (TDD)**
- [x] **Deployment pipeline** using GitHub Actions (`.github/workflows/ci.yml`), including:
  - [x] Automated regression testing (`mvn verify`: unit and integration tests)
  - [x] Open Source Software (OSS) vulnerability scan that **fails the build** on critical or blocker vulnerabilities (OWASP Dependency-Check, CVSS ≥ 9)
- [x] **PlantUML** sequence, class, and/or design diagrams where feasible ([docs/diagrams](docs/diagrams))
- [ ] If AI tools are used, all prompts are shared in a separate document ([docs/ai-prompts.md](docs/ai-prompts.md); this session's prompts are recorded, the earlier sessions' prompts still have to be pasted in)

## Build and Test

Prerequisites: JDK 21, Maven 3.9+, and Docker (for integration tests).

Tech stack: Spring Boot 4.1, Spring Security (OAuth2 resource server), Kafka, PostgreSQL, MongoDB, Liquibase (schema migrations for both databases), ShedLock, Bucket4j, Micrometer, JUnit 5, Mockito, Testcontainers.

Run unit tests (`*Test`, no external services needed):

```shell
mvn -f pom.xml test
```

Run unit and integration tests (`*IT`, starts PostgreSQL, MongoDB and Kafka with Testcontainers):

```shell
mvn -f pom.xml verify
```

The integration tests start their own containers; they do not use `compose.yml`. `SampleDataScenarioIT` replays the assignment's sample table end to end.

## Run Locally

`compose.yml` starts PostgreSQL, MongoDB, Kafka and Keycloak (the token issuer) on localhost with the ports and names that `application.yml` expects:

```shell
docker compose -f compose.yml up -d
```

The application reads the PostgreSQL password from `POSTGRES_PASSWORD`. The local compose default is `tradestore`:

```powershell
$env:POSTGRES_PASSWORD = "tradestore"
mvn -f pom.xml spring-boot:run
```

Logs are written as structured JSON (Elastic Common Schema).

Stop with `docker compose -f compose.yml down`; add `-v` to delete the PostgreSQL and MongoDB data.

### Calling the REST API

Every REST call needs a bearer token from the local Keycloak realm. The `trade-producer` client has the `trades:write` and `trades:read` scopes; `trade-reader` has only `trades:read`. Their secrets are in `compose/keycloak/trade-store-realm.json` and are for local development only.

```powershell
$token = (Invoke-RestMethod -Method Post -Uri "http://localhost:8180/realms/trade-store/protocol/openid-connect/token" -Body @{ grant_type = "client_credentials"; client_id = "trade-producer"; client_secret = "trade-producer-local-secret" }).access_token
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/v1/trades" -Headers @{ Authorization = "Bearer $token"; "Idempotency-Key" = "T1-v1" } -ContentType "application/json" -Body '{"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2030-05-20"}'
```

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/api/v1/books/B1/trades" -Headers @{ Authorization = "Bearer $token" }
```

| Setting | Environment variable | Default |
|---------|----------------------|---------|
| Token issuer | `OAUTH2_ISSUER_URI` | `http://localhost:8180/realms/trade-store` |
| REST requests per second, per client and instance | `REST_REQUESTS_PER_SECOND` | `100` |
| Kafka over SASL_SSL | `SPRING_PROFILES_ACTIVE=kafka-sasl` with `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TRUSTSTORE_LOCATION`, `KAFKA_TRUSTSTORE_PASSWORD` | off (PLAINTEXT) |

Application metrics are listed under `/actuator/metrics` (any valid token); see [Monitoring](docs/architecture.md#212-monitoring).

## License

See [LICENSE](LICENSE).
