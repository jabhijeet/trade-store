# trade-store

A trade store that receives a high volume of trades (thousands, over any transmission channel), validates them, and stores each trade in a defined sequence based on its trade id and version.

> **Status:** Kafka and REST ingestion, the validation rules, automatic expiry, the transactional outbox to MongoDB and Kafka, the read-only query API, OAuth2/JWT security, per-client rate limiting, the MongoDB `trade_views` projection, the idempotency purge job, application metrics, structured logs and the Kafka SASL_SSL profile are implemented and tested; see [Implementation Status](docs/architecture.md#12-implementation-status).

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

## Project Structure

Code is grouped by responsibility under `com.tradestore` (`src/main/java`); each test sits in the package of the class it tests.

| Package | Contents |
|---------|----------|
| `adapter.kafka` | Kafka consumer of the `trades` topic |
| `adapter.rest.controller` | REST controllers: ingestion, trade queries, book queries |
| `adapter.rest.ratelimit` | Per-client rate limiter and the ingestion limit interceptor |
| `service.ingestion` | Trade processing service (the single writer of trade state) and rejection recording |
| `service.query` | Read side over PostgreSQL and MongoDB |
| `service.expiry` | Expiry service and its scheduler |
| `service.outbox` | Outbox writer, outbox relay and the MongoDB `trade_views` projector |
| `service.housekeeping` | Purge of old idempotency records |
| `validation` | Pluggable validation rules (version, maturity date) |
| `domain` | Core records: `Trade`, `IncomingTrade`, `TradeCommand`, `TradeFilter`, `TradeHistoryEvent` |
| `enums` | `EventSource`, `MessageOutcome`, `SaveOutcome`, `TradeEventType` |
| `model` | REST request and response DTOs, Kafka message records (`TradeMessage`, `TradeStatusMessage`) |
| `exception` | Business and adapter exceptions, and the REST problem-detail handler |
| `persistence.sql.entity`, `persistence.sql.repository` | JPA entities and repositories (PostgreSQL) |
| `persistence.mongo.document`, `persistence.mongo.repository` | MongoDB documents and repositories |
| `config` | Security, Kafka, web, OpenAPI, clock, scheduling and application properties |
| `config.database` | PostgreSQL pool and MongoDB pool and Liquibase configuration |
| `observability` | Application metrics |

## Technical Requirements

- [x] Java application built with **Spring Boot**
- [x] A **streaming tool** for trade ingestion (Kafka)
- [x] Both a **SQL** and a **NoSQL** database (PostgreSQL and MongoDB)
- [x] All functionality covered by **JUnit** tests, developed using **Test-Driven Development (TDD)**
- [x] **Deployment pipeline** using GitHub Actions (`.github/workflows/ci.yml`), including:
  - [x] Automated regression testing (`mvn verify`: unit and integration tests)
  - [x] Open Source Software (OSS) vulnerability scan that **fails the build** on critical or blocker vulnerabilities (OWASP Dependency-Check, CVSS ≥ 9)
- [x] **PlantUML** sequence, class, and/or design diagrams where feasible ([docs/diagrams](docs/diagrams))
- [x] If AI tools are used, all prompts are shared in a separate document ([docs/ai-prompts.md](docs/ai-prompts.md))

## Build and Test

Prerequisites: JDK 21, Maven 3.9+, and Docker (for integration tests).

Tech stack: Spring Boot 4.1, Spring Security (OAuth2 resource server), Kafka, PostgreSQL, MongoDB, Liquibase (schema migrations for both databases), ShedLock, Bucket4j, Micrometer, springdoc-openapi (OpenAPI document and Swagger UI), JUnit 5, Mockito, Testcontainers.

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

### 1. Start the infrastructure

`compose.yml` starts PostgreSQL (5432), MongoDB (27017), Kafka (9092) and Keycloak (8180, the OAuth2 token issuer), bound to localhost, with the names that `application.yml` expects:

```powershell
docker compose -f compose.yml up -d
```

Keycloak imports the `trade-store` realm from `compose/keycloak/trade-store-realm.json` on startup, which takes about 20–30 seconds. It is ready when this returns the realm's issuer:

```powershell
(Invoke-RestMethod -Uri "http://localhost:8180/realms/trade-store/.well-known/openid-configuration").issuer
```

### 2. Start the application

The application reads the PostgreSQL password from `POSTGRES_PASSWORD`; the compose default is `tradestore`:

```powershell
$env:POSTGRES_PASSWORD = "tradestore"
mvn -f pom.xml spring-boot:run
```

The API listens on `http://localhost:8080`, and Swagger UI on `http://localhost:8080/swagger-ui.html` ([Swagger UI](#swagger-ui)). Liquibase creates the PostgreSQL tables and MongoDB collections on startup, and the Kafka topics (`trades`, `trades-dlt`, `trade-status`) are created automatically. Logs are structured JSON (Elastic Common Schema).

| Setting | Environment variable | Default |
|---------|----------------------|---------|
| Token issuer | `OAUTH2_ISSUER_URI` | `http://localhost:8180/realms/trade-store` |
| REST requests per second, per client and instance | `REST_REQUESTS_PER_SECOND` | `100` |
| Expiry scheduler (cron, UTC) | `TRADE_STORE_EXPIRY_CRON` | `0 5 0 * * *` (00:05 daily) |
| PostgreSQL / MongoDB / Kafka | `POSTGRES_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `MONGODB_URI`, `KAFKA_BOOTSTRAP_SERVERS` | the compose services |
| OpenAPI document and Swagger UI (public) | `API_DOCS_ENABLED` | `true` |
| Kafka over SASL_SSL | `SPRING_PROFILES_ACTIVE=kafka-sasl` with `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TRUSTSTORE_LOCATION`, `KAFKA_TRUSTSTORE_PASSWORD` | off (PLAINTEXT) |

### 3. Stop

Stop the application with `Ctrl+C`, then the infrastructure:

```powershell
docker compose -f compose.yml down
```

Add `-v` to also delete the PostgreSQL and MongoDB data.

## Testing the Workflows Locally

The commands assume PowerShell 7 (`-SkipHttpErrorCheck` shows error responses instead of throwing) and the application running as described above. Environment variables set with `$env:` stay set for the rest of the PowerShell session; remove an override with, for example, `Remove-Item Env:REST_REQUESTS_PER_SECOND`.

### OAuth2 tokens

The realm has two service-account clients (OAuth2 client-credentials grant). Their secrets are in the realm file and work only against the local Keycloak:

| Client | Secret | Scopes | Can |
|--------|--------|--------|-----|
| `trade-producer` | `trade-producer-local-secret` | `trades:write trades:read` | submit and read trades |
| `trade-reader` | `trade-reader-local-secret` | `trades:read` | read trades only |

A token is requested from Keycloak's token endpoint with the client id and secret. Tokens are valid for 5 minutes (`expires_in: 300`); request a new one when calls start returning `401`.

To see the response, call the token endpoint directly:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8180/realms/trade-store/protocol/openid-connect/token" -Body @{ grant_type = "client_credentials"; client_id = "trade-producer"; client_secret = "trade-producer-local-secret" } | Select-Object token_type, expires_in, scope, access_token | Format-List
```

```text
token_type   : Bearer
expires_in   : 300
scope        : trades:write trades:read
access_token : eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ...
```

The same request with `curl`, which works in any shell (on Windows, `curl.exe`):

```shell
curl -s -X POST "http://localhost:8180/realms/trade-store/protocol/openid-connect/token" -d "grant_type=client_credentials" -d "client_id=trade-producer" -d "client_secret=trade-producer-local-secret"
```

The commands in this guide keep a token for each client in a variable:

```powershell
$producer = (Invoke-RestMethod -Method Post -Uri "http://localhost:8180/realms/trade-store/protocol/openid-connect/token" -Body @{ grant_type = "client_credentials"; client_id = "trade-producer"; client_secret = "trade-producer-local-secret" }).access_token
```

```powershell
$reader = (Invoke-RestMethod -Method Post -Uri "http://localhost:8180/realms/trade-store/protocol/openid-connect/token" -Body @{ grant_type = "client_credentials"; client_id = "trade-reader"; client_secret = "trade-reader-local-secret" }).access_token
```

Print a token to copy it (for example into Swagger UI), or check that it works by calling the API with it (`200`):

```powershell
$producer
```

```powershell
(Invoke-WebRequest -Uri "http://localhost:8080/api/v1/trades?size=1" -Headers @{ Authorization = "Bearer $producer" }).StatusCode
```

To see what the application checks, decode the token's claims (`iss` must equal the issuer, `scope` holds the scopes, `sub` identifies the client for rate limiting):

```powershell
$payload = $producer.Split(".")[1].Replace('-', '+').Replace('_', '/'); $payload += "=" * ((4 - $payload.Length % 4) % 4); [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload)) | ConvertFrom-Json | Select-Object iss, scope, sub, exp
```

Security scenarios:

| Scenario | Command | Expected |
|----------|---------|----------|
| No token | `Invoke-WebRequest -Uri "http://localhost:8080/api/v1/trades/T1" -SkipHttpErrorCheck` | `401` |
| Read with `trade-reader` | `Invoke-RestMethod -Uri "http://localhost:8080/api/v1/trades" -Headers @{ Authorization = "Bearer $reader" }` | `200` |
| Write with `trade-reader` (no `trades:write`) | `Invoke-WebRequest -Method Post -Uri "http://localhost:8080/api/v1/trades" -Headers @{ Authorization = "Bearer $reader" } -ContentType "application/json" -Body '{"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2030-05-20"}' -SkipHttpErrorCheck` | `403` |
| Tampered or expired token | `Invoke-WebRequest -Uri "http://localhost:8080/api/v1/trades" -Headers @{ Authorization = "Bearer $($reader)x" } -SkipHttpErrorCheck` | `401` |
| Actuator health (public) | `Invoke-RestMethod -Uri "http://localhost:8080/actuator/health"` | `200`, `UP` |
| OpenAPI document (public) | `Invoke-RestMethod -Uri "http://localhost:8080/v3/api-docs"` | `200`, the API description |

### Swagger UI

Swagger UI lists every REST endpoint with its parameters, request body and responses, and can call them. The OpenAPI document it is built from is at `http://localhost:8080/v3/api-docs`. Both are public; the endpoints still need a token.

1. Open `http://localhost:8080/swagger-ui.html`.
2. Get a token and copy it to the clipboard. `trade-producer` can call every endpoint, `trade-reader` only the `GET` endpoints:

   ```powershell
   (Invoke-RestMethod -Method Post -Uri "http://localhost:8180/realms/trade-store/protocol/openid-connect/token" -Body @{ grant_type = "client_credentials"; client_id = "trade-producer"; client_secret = "trade-producer-local-secret" }).access_token | Set-Clipboard
   ```

   To see the token instead, leave out `| Set-Clipboard` and copy the printed value (one long line starting with `eyJ`). [OAuth2 tokens](#oauth2-tokens) shows the full response and a `curl` version.

3. Select **Authorize**, paste the token into **Value** without the `Bearer ` prefix (Swagger UI adds it), select **Authorize** again, then **Close**.
4. Open an endpoint, select **Try it out**, fill in the parameters or body, and select **Execute**. Swagger UI shows the `curl` command, the status code and the response.

For example, `POST /api/v1/trades` with this body returns `201` and `CREATED` for a new trade id, then `200` and `REPLACED` when sent again with a new (or no) `Idempotency-Key`; the [REST section](#rest-ingestion-and-validation-rules) lists the other outcomes:

```json
{"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2030-05-20"}
```

Tokens expire after 5 minutes. When calls start returning `401`, run step 2 again, select **Authorize**, **Logout**, and paste the new token. Set `API_DOCS_ENABLED=false` to turn off the OpenAPI document and Swagger UI where the API description should not be exposed.

### REST ingestion and validation rules

Submit trades as `trade-producer`. Each row is one command; run them in order:

| Scenario | Body | Expected |
|----------|------|----------|
| New trade | `{"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2030-05-20"}` | `201`, outcome `CREATED` |
| Higher version | `{"tradeId":"T1","version":2,"counterPartyId":"CP-2","bookId":"B1","maturityDate":"2030-05-20"}` | `200`, outcome `UPDATED` |
| Same version replaces | `{"tradeId":"T1","version":2,"counterPartyId":"CP-9","bookId":"B1","maturityDate":"2030-05-20"}` | `200`, outcome `REPLACED` |
| Lower version is rejected | `{"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2030-05-20"}` | `409`, "version 1 is lower than stored version 2" |
| Maturity date before today | `{"tradeId":"T9","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2020-05-20"}` | `422` |
| Missing fields | `{"version":1}` | `400` |

```powershell
Invoke-WebRequest -Method Post -Uri "http://localhost:8080/api/v1/trades" -Headers @{ Authorization = "Bearer $producer" } -ContentType "application/json" -Body '{"tradeId":"T1","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2030-05-20"}' -SkipHttpErrorCheck
```

Idempotency: send the same request twice with the same `Idempotency-Key`. The second call returns the original outcome (`201 CREATED`) without storing anything again:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/v1/trades" -Headers @{ Authorization = "Bearer $producer"; "Idempotency-Key" = "T5-v1" } -ContentType "application/json" -Body '{"tradeId":"T5","version":1,"counterPartyId":"CP-1","bookId":"B1","maturityDate":"2030-05-20"}'
```

Request size limit: a body over 16 KB is rejected with `413` before it is parsed:

```powershell
Invoke-WebRequest -Method Post -Uri "http://localhost:8080/api/v1/trades" -Headers @{ Authorization = "Bearer $producer" } -ContentType "application/json" -Body ('{"tradeId":"' + ("X" * 20000) + '"}') -SkipHttpErrorCheck
```

### Reading trades (PostgreSQL and MongoDB)

| Endpoint | Store | Command |
|----------|-------|---------|
| Current trade | PostgreSQL | `Invoke-RestMethod -Uri "http://localhost:8080/api/v1/trades/T1" -Headers @{ Authorization = "Bearer $reader" }` |
| Filtered, paginated list | PostgreSQL | `Invoke-RestMethod -Uri "http://localhost:8080/api/v1/trades?bookId=B1&expired=false&page=0&size=20" -Headers @{ Authorization = "Bearer $reader" }` |
| History of a trade | MongoDB `trade_events` | `(Invoke-RestMethod -Uri "http://localhost:8080/api/v1/trades/T1/history" -Headers @{ Authorization = "Bearer $reader" }).content` |
| Trades of a book | MongoDB `trade_views` | `(Invoke-RestMethod -Uri "http://localhost:8080/api/v1/books/B1/trades" -Headers @{ Authorization = "Bearer $reader" }).content` |

The MongoDB endpoints are filled by the outbox relay, so they show a change about one second after it is committed. After the ingestion scenarios above, the history of `T1` is `CREATED v1`, `UPDATED v2`, `REPLACED v2`, `REJECTED v1`.

The databases can also be inspected directly:

```powershell
docker exec trade-store-postgres-1 psql -U tradestore -d tradestore -c "SELECT trade_id, version, counter_party_id, book_id, maturity_date, created_date, expired FROM trades ORDER BY trade_id;"
```

```powershell
docker exec trade-store-mongo-1 mongosh tradestore --quiet --eval "db.trade_views.find().forEach(printjson)"
```

### Rate limiting

The default limit (100 requests per second per client) is hard to reach by hand. To see it, restart the application with a low limit:

```powershell
$env:REST_REQUESTS_PER_SECOND = "2"
mvn -f pom.xml spring-boot:run
```

Then send five requests in a row as the same client. The first two are admitted; the rest get `429 Too Many Requests` with `Retry-After: 1`:

```powershell
1..5 | ForEach-Object { $r = Invoke-WebRequest -Method Post -Uri "http://localhost:8080/api/v1/trades" -Headers @{ Authorization = "Bearer $producer" } -ContentType "application/json" -Body ('{"tradeId":"RL-' + $_ + '","version":1,"counterPartyId":"CP-1","bookId":"RL","maturityDate":"2030-05-20"}') -SkipHttpErrorCheck; "request $_ -> $($r.StatusCode) Retry-After=$($r.Headers['Retry-After'])" }
```

Each client has its own budget, so requests from another client are still admitted, and read endpoints are never rate limited. The number of rejected requests is published as a metric:

```powershell
(Invoke-RestMethod -Uri "http://localhost:8080/actuator/metrics/tradestore.rest.rate.limited" -Headers @{ Authorization = "Bearer $reader" }).measurements
```

### Kafka ingestion

Kafka is the primary ingestion path. Messages go to the `trades` topic, keyed by trade id, as JSON with `schemaVersion: 1`. Publish with the console producer inside the Kafka container; each line is `key|value`:

```powershell
'K1|{"schemaVersion":1,"tradeId":"K1","version":1,"counterPartyId":"CP-1","bookId":"BK","maturityDate":"2030-05-20"}' | docker exec -i trade-store-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic trades --property parse.key=true --property "key.separator=|"
```

| Scenario | Message (`key\|value`) | Expected |
|----------|-----------------------|----------|
| New trade | `K1\|{"schemaVersion":1,"tradeId":"K1","version":1,...}` | stored, history `CREATED` |
| Higher version | `K1\|{"schemaVersion":1,"tradeId":"K1","version":2,...}` | stored, history `UPDATED` |
| Lower version | `K1\|{"schemaVersion":1,"tradeId":"K1","version":1,...}` | not stored; history `REJECTED` with the reason; offset committed, the partition is not blocked |
| Maturity date before today | `K2\|{... "tradeId":"K2", "maturityDate":"2020-05-20"}` | not stored; `GET /api/v1/trades/K2` returns `404` |
| Key differs from trade id | `WRONG\|{... "tradeId":"K3", ...}` | sent to `trades-dlt` |
| Malformed JSON | `K4\|not json` | sent to `trades-dlt` |

Check the results with the read endpoints above (history entries have `source` `KAFKA`), and read the dead-letter topic:

```powershell
docker exec trade-store-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trades-dlt --from-beginning --timeout-ms 8000 --property print.key=true
```

The consumer stops with a `TimeoutException` after printing the messages; that is the `--timeout-ms` taking effect.

### Expiry and the `trade-status` topic

The expiry scheduler runs daily at 00:05 UTC and publishes an `EXPIRED` event to `trade-status` for every trade it expires. To test it locally, run the scheduler every minute:

```powershell
$env:TRADE_STORE_EXPIRY_CRON = "0 * * * * *"
mvn -f pom.xml spring-boot:run
```

A trade cannot be submitted with a past maturity date, so store one with a future date and then move its maturity date into the past directly in PostgreSQL (test shortcut only):

```powershell
docker exec trade-store-postgres-1 psql -U tradestore -d tradestore -c "UPDATE trades SET maturity_date = DATE '2020-01-01' WHERE trade_id = 'T1';"
```

Within a minute `T1` shows `"expired": true` in `GET /api/v1/trades/T1` and in `GET /api/v1/books/B1/trades`, its history ends with `EXPIRED` (source `SCHEDULER`), and the event is on `trade-status`:

```powershell
docker exec trade-store-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trade-status --from-beginning --timeout-ms 8000 --property print.key=true
```

Because the shortcut changes PostgreSQL directly instead of sending a trade, the MongoDB view keeps the original maturity date; only the expired flag changes.

### Kafka over SASL_SSL

The compose broker uses PLAINTEXT. The `kafka-sasl` profile is covered by `KafkaSaslProfileTest`; to use it against a cluster with SCRAM-SHA-512 users and TLS, set the profile and credentials before starting the application:

```powershell
$env:SPRING_PROFILES_ACTIVE = "kafka-sasl"
$env:KAFKA_BOOTSTRAP_SERVERS = "<broker>:9093"
$env:KAFKA_USERNAME = "trade-store"
$env:KAFKA_PASSWORD = "<password>"
$env:KAFKA_TRUSTSTORE_LOCATION = "file:C:/certs/kafka-truststore.p12"
$env:KAFKA_TRUSTSTORE_PASSWORD = "<truststore password>"
mvn -f pom.xml spring-boot:run
```

The topic ACLs the application needs are listed in [docs/architecture.md §2.11](docs/architecture.md#211-security).

### Metrics

Metrics need any valid token. List them, then read one:

```powershell
(Invoke-RestMethod -Uri "http://localhost:8080/actuator/metrics" -Headers @{ Authorization = "Bearer $reader" }).names | Where-Object { $_ -like "tradestore.*" }
```

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/actuator/metrics/tradestore.trades.processed?tag=outcome:REJECTED_LOWER_VERSION" -Headers @{ Authorization = "Bearer $reader" }
```

| Metric | Shows |
|--------|-------|
| `tradestore.trades.processed` (tags `source`, `outcome`) | processed messages per path and outcome, including `DUPLICATE` |
| `tradestore.trades.expired` | trades expired by the scheduler |
| `tradestore.rest.rate.limited` | requests rejected with `429` |
| `tradestore.outbox.pending` (tag `destination`) | events not yet delivered to MongoDB or Kafka; stop MongoDB (`docker stop trade-store-mongo-1`), submit a trade, and watch it grow, then start MongoDB again and watch it drain |
| `tradestore.processed.messages.purged` | idempotency records removed by the purge job |

## License

See [LICENSE](LICENSE).
