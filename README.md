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
make test     # run tests locally (no Docker needed)
make build    # build the order-service jar locally
make up       # build images and start the stack (needs Docker running)
make health   # curl the order-service health endpoint
make ps       # show service health
make logs     # tail logs
make down     # stop the stack
```

Run `make` with no target for the full list.

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

> Phase 1 stores orders **in memory** (they don't survive a restart). Postgres + Flyway
> persistence lands in Phase 2 behind the same `OrderRepository` interface.

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
