# trade-store

A trade store that receives a high volume of trades (thousands, over any transmission channel), validates them, and stores each trade in a defined sequence based on its trade id and version.

> **Status:** Work in progress. Kafka and REST ingestion, the validation rules, automatic expiry, the transactional outbox to MongoDB and Kafka, and the read-only query API are implemented and tested. Security, rate limiting, MongoDB projections and application metrics are planned; see [Implementation Status](docs/architecture.md#12-implementation-status).

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

Trades are ingested primarily through Kafka, partitioned by trade id. A REST endpoint (`POST /api/v1/trades`) is available as a backup when Kafka is unavailable and for direct ingestion; authentication and rate limiting for it are planned. Both paths are thin adapters over one Spring Boot processing service, which applies the validation rules, guarantees idempotency and is the only writer of trade state.

PostgreSQL holds the authoritative trade state. State changes and their events are committed together through a transactional outbox, and an outbox relay delivers the events to MongoDB (trade history) and Kafka (status events). Everything runs in UTC: "today" for the maturity and expiry rules is the current UTC date. An expiry scheduler marks matured trades as expired. Read-only, paginated REST endpoints expose stored trades and their history.

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
- [ ] If AI tools are used, all prompts are shared in a separate document

## Build and Test

Prerequisites: JDK 21, Maven 3.9+, and Docker (for integration tests).

Tech stack: Spring Boot 4.1, Kafka, PostgreSQL, MongoDB, Liquibase (schema migrations for both databases), ShedLock, JUnit 5, Mockito, Testcontainers.

Run unit tests (`*Test`, no external services needed):

```shell
mvn -f pom.xml test
```

Run unit and integration tests (`*IT`, starts PostgreSQL, MongoDB and Kafka with Testcontainers):

```shell
mvn -f pom.xml verify
```

The integration tests start their own containers; they do not use `compose.yml`.

## Run Locally

`compose.yml` starts PostgreSQL, MongoDB and Kafka on localhost with the ports and database names that `application.yml` expects:

```shell
docker compose -f compose.yml up -d
```

The application reads the PostgreSQL password from `POSTGRES_PASSWORD`. The local compose default is `tradestore`:

```powershell
$env:POSTGRES_PASSWORD = "tradestore"
mvn -f pom.xml spring-boot:run
```

Stop with `docker compose -f compose.yml down`; add `-v` to delete the PostgreSQL and MongoDB data.

## License

See [LICENSE](LICENSE).
