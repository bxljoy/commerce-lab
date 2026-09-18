# Commerce Lab

A greenfield e-commerce microservices system built **step-by-step on local Docker
Compose** to turn best-practice theory into verified practice. The companion plan
and the note-by-phase mapping live in the Obsidian vault:
`Obsidian-notes/tech-decisions/notes/commerce-lab-phased-build-plan.md`.

> **Guiding scenario:** *Place an order → reserve inventory → confirm or reject; cancel and release stock when needed.*

The core lab finishes with two services, one compensation path, recovery from
failures, and a small observable demo. Shipping and the Phase 7 menu are optional.
See [ADR-0004](docs/adr/0004-evidence-driven-learning-roadmap.md) for the revised scope.

## Services

| Service | Owns | Status |
|---|---|---|
| `order-service` | Order aggregate; later confirmation/rejection and cancellation | Phase 2 complete — place + get backed by Postgres |
| `inventory-service` | Stock and reservations per SKU (reserve / get / release) | Phase 3A complete — PostgreSQL contention and rollback proofs |
| `frontend` | React SPA to place orders and watch them confirm | not started (Phase 6) |

## Tech stack

- **Java 21**, **Spring Boot 3.3**, **Maven**
- **Postgres + Flyway** (Phase 2), **Kafka via Redpanda** (Phase 4)
- **OpenTelemetry → Tempo + Prometheus + Grafana** (Phase 5)
- **React + Vite** (Phase 6)

## Prerequisites

- JDK 21 (`java -version`)
- Maven 3.9+ (`mvn -version`)
- Docker + Docker Compose with a running daemon (e.g. Docker Desktop or OrbStack)

## Quick start

```bash
make test                   # both services' fast Surefire suites
make test-order             # order-service fast suite only
make test-inventory         # inventory-service fast suite only
make verify                 # both full suites, including Testcontainers
make verify-order           # order-service full suite only
make verify-inventory       # inventory-service full suite only
make verify-restart         # isolated order image/restart proof
make verify-inventory-image # isolated inventory image reserve/release proof
make up                     # build images and start both service/database pairs
make ps                     # show service health
make logs                   # tail logs
make down                   # stop the stack and retain database volumes
docker compose down -v      # stop the stack and reset both local databases
```

Each Maven project also builds independently:

```bash
mvn -f order-service/pom.xml -B verify
mvn -f inventory-service/pom.xml -B verify
```

Run `make` with no target for the full list.

> **Integration tests & Docker (local note):** `make verify` runs Testcontainers-backed
> integration tests (`*IT`) against a throwaway Postgres, so it needs a Docker daemon.
> The build pins `-Dapi.version` for the test JVM because docker-java's negotiation probes
> Docker API v1.32, which modern daemons (Docker Engine 25+, OrbStack, Colima) reject. If
> Testcontainers can't find your daemon's socket (e.g. OrbStack/Colima don't expose
> `/var/run/docker.sock`), point it there in `~/.testcontainers.properties`:
> `docker.host=unix:///Users/<you>/.orbstack/run/docker.sock`. On standard Docker / CI this
> isn't needed.

### Local ports and ownership

| Component | Host port | Ownership |
|---|---:|---|
| `order-service` | `8080` | `orderdb`, `order-pgdata`, `order-network` |
| order PostgreSQL | `5432` | Used only by `order-service` |
| `inventory-service` | `8081` | `inventory`, `inventory-pgdata`, `inventory-network` |
| inventory PostgreSQL | `5433` | Used only by `inventory-service` |

The explicit Compose networks are disjoint. Starting an isolated verification target
brings up only its service/database pair and removes that project's containers,
volume, and network on exit.

### Verify Phase 0

```bash
make up
make health     # -> {"status":"UP", ...}
```

### Verify Phase 1 — place + get an order

`order-service` is OpenAPI-first: `order-service/openapi.yaml` is the source of truth,
and the API interface + DTOs are generated from it at build time. With the service
running (`make up`, or start Postgres with `docker compose up -d postgres` and
then run `mvn -f order-service/pom.xml spring-boot:run`):

```bash
# place an order -> 201 Created, with a Location header
curl -i -X POST http://localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","currency":"EUR",
       "lines":[{"sku":"SKU-APPLE","quantity":2,"unitPrice":9.99}]}'

# fetch it back -> 200
curl http://localhost:8080/api/v1/orders/<id-from-Location>

# invalid body -> 400 RFC-7807 problem+json with a per-field `errors` map
curl -i -X POST http://localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' -d '{"currency":"EU","lines":[]}'
```

### Verify Phase 2 — orders persist (Postgres + Flyway, survive a restart)

As of Phase 2, orders are stored in **Postgres** (Flyway-migrated schema, JPA with
`open-in-view: false` and `ddl-auto: validate`) behind the same `OrderRepository`
interface. The controller mapping stayed stable; the service gained transaction
boundaries. To see an order survive a restart:

```bash
make up                                  # starts postgres + order-service
# place an order (see Phase 1), note the id, then:
docker compose restart order-service
# wait for order-service to become healthy, then:
curl http://localhost:8080/api/v1/orders/<id>   # still 200 — stored in Postgres
```

The Testcontainers suite (`make verify`) proves database round trips, migration from
V1 to V2, stable line order, supported price boundaries, assigned-ID insert behavior,
detached lazy-loading behavior, and that OSIV remains disabled. Run
`make verify-restart` for the separate image-and-process check: it creates an order,
restarts only `order-service`, and fetches the same value and line sequence. The named
Postgres volume also retains data across `docker compose down` / `up` (without `-v`).
Exact claims and limits are in the [scoreboard](docs/notes-verification.md).

### Verify Phase 3A — reserve and release inventory

`inventory-service` is independently deployable and owns a separate PostgreSQL
database. Flyway seeds two demo rows: `SKU-APPLE` with 10 available units and
`SKU-BANANA` with 5. Start the stack, choose a fresh order ID, and exercise the
complete public inventory API:

```bash
docker compose down -v # optional fresh demo; removes both local database volumes
make up
ORDER_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')

# reserve both demo SKUs -> 201 Created and status RESERVED
curl -i -X POST http://localhost:8081/api/v1/reservations \
  -H 'Content-Type: application/json' \
  -d "{\"orderId\":\"${ORDER_ID}\",\"lines\":[{\"sku\":\"SKU-APPLE\",\"quantity\":2},{\"sku\":\"SKU-BANANA\",\"quantity\":1}]}"

# retrieve the reservation -> 200, preserving request line order
curl http://localhost:8081/api/v1/reservations/${ORDER_ID}

# read current stock -> 200 with availableQuantity 8
curl http://localhost:8081/api/v1/stock/SKU-APPLE

# release once, then repeat -> both 200 RELEASED; stock is restored only once
curl -i -X PUT http://localhost:8081/api/v1/reservations/${ORDER_ID}/release
curl -i -X PUT http://localhost:8081/api/v1/reservations/${ORDER_ID}/release

# restored demo stock -> availableQuantity 10
curl http://localhost:8081/api/v1/stock/SKU-APPLE
```

Duplicate SKUs are rejected before database mutation. A reservation request for an
existing `orderId` returns `409 Conflict`, even when its payload matches or the
reservation was released. Payload-aware replay and lost-response recovery belong to
Phase 3B; Phase 3A does not claim reservation idempotency.

On real PostgreSQL, the integration suite proves the implemented contention shapes:
ascending pessimistic stock locks prevent two distinct orders from overselling the
tested final unit, an insufficient multi-SKU request rolls back every stock and
reservation change, and a reservation-row-first release restores stock at most once
under the tested sequential and concurrent calls. These are bounded proofs, not a
claim about every isolation level, deadlock shape, or SQL anomaly. See
[ADR-0006](docs/adr/0006-inventory-reservation-correctness.md) and the
[scoreboard](docs/notes-verification.md).

### Generated API code

Each service's API interface and DTOs are generated from its `openapi.yaml` into
its `target/generated-sources/openapi` directory at build time and are **not committed**.
The Maven plugin adds that directory to the compile source roots, so the generated
`...generated.api` / `...generated.model` types import like any other class — but only
**after** a build has run. Regeneration never appears in a git diff; the reviewable
artifacts are `order-service/openapi.yaml` and `inventory-service/openapi.yaml`.

```bash
# after a fresh clone, run once so the IDE can resolve the generated types:
mvn -f order-service/pom.xml compile
mvn -f inventory-service/pom.xml compile
```

CI (`make verify`) and the Docker build regenerate automatically — never copy or hand-edit
generated code.

## Layout

```
commerce-lab/
├── docker-compose.yml      # the local stack (grows each phase)
├── Makefile                # up / down / test / build / logs / health
├── docs/
│   ├── adr/                # one ADR per phase
│   └── notes-verification.md   # checklist: which vault note each phase proves
├── order-service/          # Phase 0+ (Spring Boot)
├── inventory-service/      # Phase 3A (Spring Boot)
└── frontend/               # planned — Phase 6
```

> `frontend/` is listed for orientation; it is created when Phase 6 begins.

## How this repo is meant to grow

Each phase is **independently demoable**. Predict a failure, reproduce it, make a
regression test pass, and explain the result and its limits without reading the
implementation. Record the exact claim, test/command, and environment in the
[scoreboard](docs/notes-verification.md), then update the relevant vault note.
Write an ADR for meaningful decisions; smaller experiments need only a short note.

| Next milestone | What it proves |
|---|---|
| Phase 3B | Synchronous reservations are idempotent and recover from lost responses; network calls stay outside DB transactions |
| Phase 4A | Committed events survive publisher failure through an outbox |
| Phase 4B | Both services recover from duplicates, rejection, cancellation, and delayed events |
| Phase 5 | Logs, metrics, and traces explain successful and failed orders |
| Phase 6 | A small UI and E2E test demonstrate the completed flow |

Basic correlation logging starts in Phase 3; the full dashboard stack stays in
Phase 5. A separate weekly `playground/` exercise covers Java/concurrency topics
without blocking service milestones. The directory is created with its first exercise.
