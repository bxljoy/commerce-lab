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
| `order-service` | Order identity, confirmation/rejection, pending recovery | Phase 3B implemented; hosted CI pending |
| `inventory-service` | Stock, reservations and durable attempt replay | Phase 3B implemented; hosted CI pending |
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
- Python 3 (stdlib only) and curl for image verification scripts

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
make verify-sync-recovery   # real response loss, stock effects and restart recovery
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

Applications share `service-network`; neither database joins it. Each database
retains its private service network. Order depends only on its own database at
startup, not inventory. Compose sets `INVENTORY_BASE_URL=http://inventory-service:8081`;
local JVM runs default to `http://localhost:8081`.

Image verification targets use unique Compose projects and loopback-only ephemeral
ports, so they can coexist with the normal stack. The service-specific targets
start only their own service/database pair. All three verify removal of their own
containers, volumes and networks on success or failure, without pruning other stacks.
`VERIFY_FAIL_AFTER_START=1 bash scripts/verify-sync-recovery.sh` deliberately exits
97 after startup to exercise cleanup (also supported by both service-specific scripts).

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
# New intent: generate a key once; retain it for retries of this exact payload.
ORDER_KEY=$(uuidgen)
# place -> 201 CONFIRMED/REJECTED, or 202 PENDING_INVENTORY; includes Location
curl -i -X POST http://localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${ORDER_KEY}" \
  -d '{"customerId":"cust-1","currency":"EUR",
       "lines":[{"sku":"SKU-APPLE","quantity":2,"unitPrice":9.99}]}'

# fetch it back -> 200
curl http://localhost:8080/api/v1/orders/<id-from-Location>

# invalid body -> 400 RFC-7807 problem+json with a per-field `errors` map
curl -i -X POST http://localhost:8080/api/v1/orders \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' -d '{"currency":"EU","lines":[]}'
```

### Verify Phase 2 — orders persist (Postgres + Flyway, survive a restart)

As of Phase 2, orders are stored in **Postgres** (Flyway-migrated schema, JPA with
`open-in-view: false` and `ddl-auto: validate`) behind the same `OrderRepository`
interface. The controller mapping stayed stable; the service gained transaction
boundaries. To see an order survive a restart:

```bash
make up                                  # starts both service/database pairs
# place an order (see Phase 1), note the id, then:
docker compose restart order-service
# wait for order-service to become healthy, then:
curl http://localhost:8080/api/v1/orders/<id>   # still 200 — stored in Postgres
```

The Testcontainers suite (`make verify`) proves database round trips, migration from
V1 to V2, stable line order, supported price boundaries, assigned-ID insert behavior,
detached lazy-loading behavior, and that OSIV remains disabled. Run
`make verify-restart` for the separate image-and-process check: with inventory
unavailable, it creates a pending order, restarts only `order-service`, and fetches
the same pending ID, value and line sequence without starting inventory. The named
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

Duplicate SKUs are rejected before database mutation. As of Phase 3B, repeating an
equivalent SKU/quantity map with the same `orderId` returns `200` and the current
reservation (including terminal `RELEASED`), without reserving again. Different
payloads return `409 reservation-payload-conflict`. Stock rejection commits attempt
metadata, not stock or reservation changes: matching POST and GET replay its stored
`409 stock-unavailable` snapshot even after availability changes. Unknown attempts
return `404` from GET. These supersede Phase 3A's duplicate-ID conflict semantics.

On real PostgreSQL, the integration suite proves the implemented contention shapes:
ascending pessimistic stock locks prevent two distinct orders from overselling the
tested final unit, an insufficient multi-SKU request leaves stock and reservations
unchanged while committing rejection metadata, and a reservation-row-first release
restores stock at most once under the tested sequential and concurrent calls. These are bounded proofs, not a
claim about every isolation level, deadlock shape, or SQL anomaly. See
[ADR-0006](docs/adr/0006-inventory-reservation-correctness.md) and the
[scoreboard](docs/notes-verification.md).

### Verify Phase 3B: uncertain outcomes and recovery

`make verify-sync-recovery` builds both real images and a test-only forwarding proxy.
The proxy fully reads real inventory success responses, then drops both request-path
responses (new `201`, matching replay `200`) before order receives them. The proof
asserts pending state, a real RESERVED attempt and stock `10/5 -> 8/4`, restarts
order while the fault remains active, restores traffic, and uses a 90-second polling
budget (individual HTTP socket timeout 5 seconds) for the same ID to become CONFIRMED
with stock still `8/4`.

A second scenario stops order, commits a controlled pending DB fixture with no
inventory attempt, and starts order again. Recovery GET returns 404, POST reserves
once, and the same ID confirms (`8/4 -> 7/4`). This models the durable state before
the first remote attempt; it is **not** an injected crash in an HTTP handler.
The first scenario restarts after remote commit and before local terminal recording,
not at a precisely instrumented process-kill instruction.

`Idempotency-Key` is required, globally scoped and retained for the order lifetime.
Matching pending replay returns `202` without another request-path HTTP attempt;
terminal replay returns `200`. Replay preserves identity and business effect, not
the original response bytes. Reusing a key with different valid content returns 409.
Recovery defaults to enabled, fixed delay 5000 ms and batch size 20; override through
`ORDER_RECOVERY_ENABLED`, `ORDER_RECOVERY_FIXED_DELAY_MS`, `ORDER_RECOVERY_BATCH_SIZE`.
Tests alone disable scheduling in shared test resources. Historical PLACED rows are
readable but excluded from recovery.

Timeouts and transport failures mean uncertainty, never stock rejection. The request
path permits two attempts; recovery permits one GET and at most one POST per pass.
Apache classic `responseTimeout` bounds socket waiting, **not a total wall-clock
deadline**. No slow-dribble response or DNS-bound proof is claimed.

Blocked `recoveryIssue` values represent operational inconsistencies (unexpected
RELEASED, payload conflict, unknown 4xx, invalid/mismatched responses), not business
rejection. Inspect order/reservation IDs and correlated logs; compare persisted
intent, inventory attempt and stock before deciding a repair. Pause order recovery
and quiesce order writes during an operator-reviewed repair, back up affected rows,
and repair the diagnosed data or protocol cause. Only then clear the reviewed row's
`recovery_blocked`/`last_failure_code`, set `next_attempt_at` due, and resume recovery.
Never blindly confirm, reserve a new ID, delete the attempt ledger or automatically
unblock all rows. There is no public repair endpoint or proof of arbitrary corruption
repair. Cancellation orchestration remains out of scope.

Deploy inventory replay support before order integration. Required headers, new
states and replay/GET status changes are intentional API compatibility changes;
mixed-version rolling upgrades are not claimed. API/library versions are unchanged
by the runtime verification work. Hosted CI remains pending until the branch is
published through a separately authorized action.

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
| Phase 3B closeout | Local evidence and whole-branch review complete; hosted CI remains pending |
| Phase 4A | Committed events survive publisher failure through an outbox |
| Phase 4B | Both services recover from duplicates, rejection, cancellation, and delayed events |
| Phase 5 | Logs, metrics, and traces explain successful and failed orders |
| Phase 6 | A small UI and E2E test demonstrate the completed flow |

Basic correlation logging starts in Phase 3; the full dashboard stack stays in
Phase 5. A separate weekly `playground/` exercise covers Java/concurrency topics
without blocking service milestones. The directory is created with its first exercise.
