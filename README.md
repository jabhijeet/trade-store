# trade-store

A trade store that receives a high volume of trades (thousands, over any transmission channel), validates them, and stores each trade in a defined sequence based on its trade id and version.

> **Status:** Work in progress. The repository currently contains the initial Maven project skeleton. The sections below describe the assignment and its acceptance criteria. Implementation details are added as the work proceeds.

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

Trades are ingested primarily through Kafka, partitioned by trade id. A rate-limited, authenticated REST endpoint (`POST /trades`) is available as a backup when Kafka is unavailable and for direct ingestion. Both paths are thin adapters over one Spring Boot processing service, which applies the validation rules, guarantees idempotency and is the only writer of trade state.

PostgreSQL holds the authoritative trade state. State changes and their events are committed together through a transactional outbox, and an outbox relay delivers the events to MongoDB (trade history and projections) and Kafka (status events). An expiry scheduler marks matured trades as expired. Read-only, paginated REST endpoints expose stored trades and their history.

See [docs/architecture.md](docs/architecture.md) for details and PlantUML diagrams.

## Technical Requirements

- [ ] Java application built with **Spring Boot**
- [ ] A **streaming tool** for trade ingestion
- [ ] Both a **SQL** and a **NoSQL** database
- [ ] All functionality covered by **JUnit** tests, developed using **Test-Driven Development (TDD)**
- [ ] **Deployment pipeline** using GitHub Actions (alternatively Jenkins), including:
  - [ ] Automated regression testing
  - [ ] Open Source Software (OSS) vulnerability scan that **fails the build** on critical or blocker vulnerabilities
- [ ] **PlantUML** sequence, class, and/or design diagrams where feasible
- [ ] If AI tools are used, all prompts are shared in a separate document

## Build and Test

Prerequisites: JDK 17 and Maven.

```shell
mvn -f pom.xml clean install
```

Run tests only:

```shell
mvn -f pom.xml test
```

## License

See [LICENSE](LICENSE).
