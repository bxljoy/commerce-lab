# Commerce Lab

A greenfield e-commerce microservices system built **step-by-step on local Docker
Compose** to turn best-practice theory into verified practice. The companion plan
and the note-by-phase mapping live in the Obsidian vault:
`Obsidian-notes/tech-decisions/notes/commerce-lab-phased-build-plan.md`.

> **Guiding scenario:** *Place an order → reserve inventory → confirm (or reject) → ship.*

## Services

| Service | Owns | Status |
|---|---|---|
| `order-service` | Order aggregate + lifecycle (`PLACED → CONFIRMED → SHIPPED / CANCELLED`) | Phase 1 — place + get order (OpenAPI-first, in-memory) |
| `inventory-service` | Stock per SKU (reserve / release) | not started (Phase 3) |
| `frontend` | React SPA to place orders and watch them confirm | not started (Phase 6) |

## Tech stack

- **Java 21**, **Spring Boot 3.3**, **Maven**
- **Postgres + Flyway** (Phase 2), **Kafka via Redpanda** (Phase 4)
- **OpenTelemetry → Tempo + Prometheus + Grafana** (Phase 5)
- **React + Vite** (Phase 6)

## Prerequisites

- JDK 21 (`java -version`)
- Maven 3.9+ (`mvn -version`)
- Docker + Docker Compose (Docker Desktop running)

## Quick start

```bash
make test     # unit + slice tests (Surefire; fast, no Docker)
make verify   # all tests incl. Testcontainers integration tests (Failsafe; needs Docker)
make build    # build the order-service jar locally
make up       # build images and start the stack (needs Docker running)
make health   # curl the order-service health endpoint
make ps       # show service health
make logs     # tail logs
make down     # stop the stack
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

### Verify Phase 0

```bash
make up
make health     # -> {"status":"UP", ...}
```

### Verify Phase 1 — place + get an order

`order-service` is OpenAPI-first: `order-service/openapi.yaml` is the source of truth,
and the API interface + DTOs are generated from it at build time. With the service
running (`make up`, or `cd order-service && mvn spring-boot:run`):

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
interface — the service and controller didn't change. To see an order survive a restart:

```bash
make up                                  # starts postgres + order-service
# place an order (see Phase 1), note the id, then:
docker compose restart order-service
curl http://localhost:8080/api/v1/orders/<id>   # still 200 — data survived (named volume)
```

The automated proof is the Testcontainers suite (`make verify`): an order round-trips
through a real Postgres, and a lazy-init test demonstrates the OSIV-off handling.

### Generated API code

The API interface and DTOs are generated from `order-service/openapi.yaml` into
`order-service/target/generated-sources/openapi` at build time and are **not committed**.
The Maven plugin adds that directory to the compile source roots, so the generated
`...generated.api` / `...generated.model` types import like any other class — but only
**after** a build has run. Regeneration never appears in a git diff; the reviewable
artifact is `openapi.yaml` itself.

```bash
# after a fresh clone, run once so the IDE can resolve the generated types:
mvn -f order-service/pom.xml compile
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
├── inventory-service/      # planned — Phase 3
└── frontend/               # planned — Phase 6
```

> `inventory-service/` and `frontend/` are listed for orientation; they don't exist
> yet and are created when their phase begins.

## How this repo is meant to grow

Each phase is **independently demoable** and ends with: a passing test, an ADR in
`docs/adr/`, and an update to the source note in the vault ("verified in practice —
here's the gotcha I hit"). See `docs/notes-verification.md` for the running scoreboard.
