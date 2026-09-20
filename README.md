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
| `order-service` | Order identity, outbox publication, guarded async completion | Phase 4B slice 1 locally implemented; whole-branch review and hosted CI pending |
| `inventory-service` | Stock, reservations, inbox intake and durable result publication | Phase 4B slice 1 locally implemented; whole-branch review and hosted CI pending |
| `frontend` | React SPA to place orders and watch them confirm | not started (Phase 6) |

## Tech stack

- **Java 21**, **Spring Boot 3.3**, **Maven**
- **Postgres + Flyway** (Phase 2), **Apache Kafka 3.7.1** (Phase 4A)
- **OpenTelemetry → Tempo + Prometheus + Grafana** (Phase 5)
- **React + Vite** (Phase 6)

## Prerequisites

- JDK 21 (`java -version`)
- Maven 3.9+ (`mvn -version`)
- Docker + Docker Compose with a running daemon (e.g. Docker Desktop or OrbStack)
- Python 3 (stdlib only) and curl for image verification scripts

## Quick start

For an existing Phase 3B database, follow the [upgrade runbook](#upgrade-and-rollback-runbook)
before starting 4A. Old writers must be stopped and pending orders resolved first.
For Phase 4A databases or a clean deployment, follow the [async rollout](#async-rollout-and-recovery-runbook).

```bash
make test                   # both services' fast Surefire suites
make test-order             # order-service fast suite only
make test-inventory         # inventory-service fast suite only
make verify                 # both full suites, including Testcontainers
make verify-order           # order-service full suite only
make verify-inventory       # inventory-service full suite only
make verify-restart         # isolated order image/restart proof
make verify-inventory-image # isolated inventory image reserve/release proof
make verify-outbox-recovery # real process kills and identical-event duplicate proof
make verify-async-completion # terminal outcomes and both consumer crash boundaries
make up                     # both service/database pairs, Kafka and topic provisioning
make ps                     # show service health
make logs                   # tail logs
make down                   # stop the stack and retain database volumes
docker compose down -v      # stop the stack and reset both local databases AND broker data
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
startup, not inventory or Kafka. Kafka is internal-only at `kafka:9092`, with a
named `kafka-data` volume on `service-network`; neither database joins that network.
Order no longer calls inventory. A host JVM needs an explicitly reachable Kafka
bootstrap/listener setup; the Compose broker does not expose a host port.

Image verification targets use unique Compose projects and loopback-only ephemeral
ports, so they can coexist with the normal stack. The service-specific targets
start only their own service/database pair. All four verify removal of their own
containers, volumes and networks on success or failure, without pruning other stacks.
`VERIFY_FAIL_AFTER_START=1 bash scripts/verify-outbox-recovery.sh` deliberately exits
97 after startup to exercise cleanup (also supported by the other three scripts).
Historical restart/inventory/outbox proofs explicitly disable event consumers;
only the async proof claims full workflow completion.

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
# place -> 202 PENDING_INVENTORY; includes Location; poll GET for terminal outcome
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
`make verify-restart` for the separate image-and-process check: with the relay disabled, it creates a pending order and one immutable outbox row,
restarts only `order-service`, and verifies the same ID, value, line sequence and
event through GET and keyed replay, without starting inventory or Kafka. The named
Postgres volume also retains data across `docker compose down` / `up` (without `-v`).
Exact claims and limits are in the [scoreboard](docs/notes-verification.md).

### Verify Phase 3A — reserve and release inventory

`inventory-service` is independently deployable and owns a separate PostgreSQL
database. Flyway seeds two demo rows: `SKU-APPLE` with 10 available units and
`SKU-BANANA` with 5. Start the stack, choose a fresh order ID, and exercise the
complete public inventory API:

```bash
docker compose down -v # optional fresh demo; removes both local database volumes AND broker data
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

### Reproduce the preserved Phase 3B baseline

Phase 3B merged at `ca525d1a8b4e65fe747d60824fc3c2e517e11074`; its
[hosted CI succeeded](https://github.com/bxljoy/commerce-lab/actions/runs/35453124768).
The sync coordinator, recovery worker, proxy and sync-only scripts are retired from
the active 4A path. Reproduce their commands in a separate baseline checkout, with
isolated databases, never by running the old binary against a 4A database:

```bash
git worktree add --detach ../commerce-lab-phase3b ca525d1a8b4e65fe747d60824fc3c2e517e11074
cd ../commerce-lab-phase3b
make verify
make verify-restart
make verify-inventory-image
make verify-sync-recovery
VERIFY_FAIL_AFTER_START=1 bash scripts/verify-sync-recovery.sh
```

The baseline README/ADR-0007 retain the response-loss, fixture and operational
repair explanations. Its proof projects use isolated resources; do not use its
normal stack with the 4A project name or volumes.

### Phase 4A: pending-only orders and durable publication

This section describes the historical publication-only boundary. The active
Phase 4B workflow below adds consumers; the isolated 4A proof disables them.

New valid orders return `202 PENDING_INVENTORY` with Location and Retry-After.
Matching key/payload replay returns the same pending order and no new event;
historical terminal replay returns 200 and emits no event. Changed valid payload
under the same key returns 409; invalid input returns 400. Keys remain globally
scoped and retained for the order lifetime. GET retains its 200/404 behavior.

Order, request identity and one immutable OrderPlaced event commit atomically.
The relay publishes the stored payload to `commerce.orders.v1`, keyed by order
UUID, with a stable event ID. Broker acknowledgement records publication only,
not consumer success: orders remain pending and inventory stock is untouched.
There is no inventory consumer or automatic confirmation/rejection in 4A.

```bash
make verify-outbox-recovery
```

Case A commits through HTTP with publication disabled and no broker, kills the
application, then starts Kafka and a normal relay against the retained database.
Case B observes a selected test-only post-acknowledgement marker and Kafka record,
kills the actual application before delivery recording, then restarts without
proof configuration. After lease expiry, the identical event/key/payload appears
at a different Kafka offset. These are process-crash proofs, not SQL crash fixtures.
Only the explicit `outbox-proof` profile plus enable flag and selected event UUID
can arm the hook; never enable it in normal operation.

### Upgrade and rollback runbook

1. Stop all Phase 3B order writers and recovery workers, including extra replicas.
   Mixed-version rolling operation is unsupported. Inspect every pending order,
   including blocked ones. Resolve legitimate pending work with the baseline and
   operator review before the final stop; do not fabricate terminal states or
   delete orders to bypass the guard.
2. Back up and inspect the databases with all old writers stopped. Require zero
   `PENDING_INVENTORY` orders. Flyway V5 independently rejects any pending row and
   preserves historical nonpending rows and request identities without backfill.
   The guard cannot protect against an old writer restarted after migration.
3. Deploy 4A only after that check. Start the broker and provision the topic below;
   HTTP acceptance depends only on the order database and can queue during outage.
   Verify publication, pending state and backlog diagnostics.
4. There is no automatic downgrade after new 4A pending orders exist: the old
   worker would act on them. Stop and investigate a failed upgrade; any restore
   requires an operator-reviewed database/traffic recovery plan. Baseline
   experiments must use isolated databases, never the live upgraded data.

Run through `docker compose exec -T postgres psql -U order -d orderdb`:

```sql
SELECT status, count(*) FROM orders GROUP BY status;
SELECT event_id, order_id, attempt_count, next_attempt_at, lease_until,
       last_error_code, delivered_at
FROM order_outbox WHERE delivered_at IS NULL ORDER BY created_at;
```

### Broker and backlog operations

```bash
docker compose up -d --wait kafka
docker compose run --rm topic-init
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic commerce.orders.v1
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 --entity-type topics \
  --entity-name commerce.orders.v1 --describe
curl -fsS http://localhost:8080/actuator/metrics/outbox.pending
curl -fsS http://localhost:8080/actuator/metrics/outbox.oldest.pending.age
curl -fsS http://localhost:8080/actuator/metrics/outbox.failed.pending
```

Require 3 partitions, replication factor 1 and delete retention 604800000 ms
(seven days). `--if-not-exists` provisioning does not repair a misconfigured existing
topic: inspect its configuration. RF1 plus `acks=all` is not broker-HA proof;
data loss and retention expiry are outside the application-crash guarantee.

Defaults: poll 1000 ms, at most 20 claims/pass, lease 60000 ms, acknowledgement
wait 12000 ms, producer max.block 2000 ms, delivery.timeout 10000 ms,
request.timeout 3000 ms, linger 0, idempotence true, max.in.flight 1 and acks all.
Persisted backoff starts at 1000 ms, doubles to a 60000 ms cap, then adds 0..250 ms jitter.
Claims use database time and skip locked rows; token-conditional bookkeeping
rejects stale workers but does not fence a late Kafka send. Retries can duplicate
publication; no cross-order ordering or exactly-once consumer effect is claimed.

Metrics are cached snapshots refreshed by the scheduler, not live SQL on scrape.
Inspect `outbox.snapshot.stale` and `outbox.snapshot.age` as well as pending,
oldest pending age (seconds), failed pending count, attempts and publication timer.
A stale/unavailable snapshot is not a healthy empty queue; before the first
successful refresh, values are NaN. Snapshot age can grow during a long relay pass
even when the last refresh succeeded. When scheduling is
disabled with `OUTBOX_ENABLED=false`, snapshots are not refreshed automatically.
Use the SQL above as the authoritative inspection path.

For persistent failures, correlate event/order/correlation IDs, attempt, outcome,
latency and stable error code in relay logs. `ACK_UNCERTAIN` means retry can
duplicate; `RECORD_TOO_LARGE` and repeated `SEND_FAILED` need diagnosis of broker,
topic, producer limits, authorization/configuration and stored event validity.
Repair the diagnosed cause with operator review; no lifetime attempt cap silently
drops work. Do not arbitrarily edit payloads, delete pending rows, mark delivery,
or change identity/destination to make the backlog disappear. Delivered rows are
retained; purging and arbitrary corruption repair are outside 4A.

See [ADR-0008](docs/adr/0008-transactional-outbox-and-polling-relay.md) and the
[scoreboard](docs/notes-verification.md) for evidence and limits. Whole-branch
review and scoped fixes for Phase 4A are complete; its hosted CI passed in run
35494095946. Phase 4B whole-branch review and hosted CI remain pending; local
tests are not merge authorization.

### Async rollout and recovery runbook

Phase 4B slice 1 consumes `OrderPlaced` from `commerce.orders.v1` using group
`commerce-inventory-order-placed-v1`, then publishes `InventoryReserved` or
`InventoryRejected` to `commerce.inventory.v1`, consumed by group
`commerce-order-inventory-result-v1`. Both topics have **3 partitions, RF1,
delete retention 604800000 ms**. Keys are canonical lowercase order UUIDs.
See [event schemas and validation](contracts/README.md#inventory-result-events-v1)
and [ADR-0009](docs/adr/0009-idempotent-event-consumers.md).

| Input / current state | Committed outcome |
| --- | --- |
| Valid reserved result / PENDING_INVENTORY | CONFIRMED, accepted result identity, version increment |
| Valid rejected result / PENDING_INVENTORY | REJECTED; event INSUFFICIENT_STOCK maps to HTTP STOCK_UNAVAILABLE |
| Identical event already in inbox | Duplicate success; no repeated stock/state/version change |
| Same event ID, changed parsed content | Protocol failure; no mutation or offset skip |
| Different result identity after acceptance, contradictory terminal result, unknown order | Protocol failure; current state preserved |

New POST returns 202; matching replay returns 202 while pending and 200 once
terminal. GET observes completion. No transition back to pending is supported.
Inventory inbox, attempt, stock/reservation and result outbox commit together;
order inbox, accepted identity and transition commit together. Listener offset
commit follows the database commit, so a crash can redeliver. There is no network
I/O in either business transaction and no distributed atomic commit.

1. Stop old writers/consumers, back up both databases, and inventory pending
   orders, unpublished rows, delivered timestamps and broker retention. Phase 4A
   pending rows are valid upgrade input (unlike the earlier 3B-to-4A migration).
   Do not delete or regenerate identities to make unresolved work disappear.
2. Start databases and broker; initialize and inspect **both** topics before
   enabling listeners. For a clean deployment use:

   ```bash
   docker compose up -d --wait postgres inventory-postgres kafka
   docker compose run --rm topic-init
   for topic in commerce.orders.v1 commerce.inventory.v1; do
     docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "$topic"
     docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name "$topic" --describe
   done
   ORDER_EVENTS_ENABLED=false INVENTORY_EVENTS_ENABLED=false docker compose up -d --build --wait order-service inventory-service
   ```

   Confirm Flyway order V6 and inventory V4 applied in startup logs. Topic-init's
   `--if-not-exists` does not repair topology/config drift. Do not change partition
   count casually: assignment/error-state instrumentation assumes this topology.
3. After inspection, enable listeners by recreating services:

   ```bash
   ORDER_EVENTS_ENABLED=true INVENTORY_EVENTS_ENABLED=true docker compose up -d --force-recreate --wait order-service inventory-service
   ```

   Stable existing groups resume committed offsets; a fresh group starts at the
   earliest **retained** event. Keep the named groups; renaming is not repair.
   Original OrderPlaced rows remain for result validation. A delivered event
   deleted by retention cannot be recovered automatically, even with earliest
   reset. Report unresolved pending orders for future repair tooling. No backfill,
   inbox purge, identity regeneration, or automatic downgrade is supplied.

All four toggles default true: `ORDER_EVENTS_ENABLED`, `INVENTORY_EVENTS_ENABLED`,
`OUTBOX_ENABLED` (order relay), `INVENTORY_OUTBOX_ENABLED` (result relay).
Relay disable stops scheduling, not transactional outbox insertion. Consumer
disable stops intake, not HTTP acceptance. Services can cold-start with consumers
enabled while broker DNS is absent and retry listener startup after recovery;
HTTP health alone does not demonstrate consumer assignment or backlog progress.
Proof-only profiles and `CONSUMER_PROOF_*` settings must never remain enabled in
normal operation. Broker recovery must not require switching consumers off.

Inspect committed offsets (the next offset to consume) and lag:

```bash
for group in commerce-inventory-order-placed-v1 commerce-order-inventory-result-v1; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group "$group"
done
docker compose exec -T postgres psql -U order -d orderdb -c "SELECT id,status,version FROM orders WHERE status='PENDING_INVENTORY';"
docker compose exec -T postgres psql -U order -d orderdb -c "SELECT event_id,order_id,created_at,attempt_count,last_error_code,delivered_at FROM order_outbox ORDER BY created_at;"
docker compose exec -T postgres psql -U order -d orderdb -c "SELECT order_id,causation_id,result_event_id,result_type FROM order_inventory_results;"
docker compose exec -T postgres psql -U order -d orderdb -c "SELECT consumer_name,event_id,processed_at FROM order_event_inbox;"
docker compose exec -T inventory-postgres psql -U inventory -d inventory -c "SELECT consumer_name,event_id,processed_at FROM inventory_event_inbox;"
docker compose exec -T inventory-postgres psql -U inventory -d inventory -c "SELECT order_id,outcome FROM inventory_reservation_attempts; SELECT sku,available_quantity FROM stock ORDER BY sku;"
docker compose exec -T inventory-postgres psql -U inventory -d inventory -c "SELECT event_id,order_id,causation_id,event_type,attempt_count,last_error_code,lease_until,delivered_at FROM inventory_result_outbox ORDER BY created_at;"
docker compose logs --since=10m order-service inventory-service
curl -fsS http://localhost:8080/actuator/metrics/consumer.blocked.partitions
curl -fsS http://localhost:8081/actuator/metrics/consumer.blocked.partitions
```

Correlate metadata-only failure code/topic/partition/offset with `consumer.applied`,
`consumer.duplicate`, `consumer.retry`, `consumer.blocked` and
`consumer.infrastructure.failures`. Check inventory's `outbox.*` metrics on port
8081 as well as order's on 8080; cached snapshots are not authoritative SQL.
A paused partition can have a healthy process and healthy sibling partitions.
Auto-commit is disabled, record mode uses synchronous commits and max.poll.records=1.
Transient database failures retry with nonblocking backoff (1s doubling to 30s);
protocol/unknown failures block that partition. Assignment-generation fencing
prevents stale retry/success callbacks acting on a new owner.

Correct the diagnosed infrastructure/data cause under operator review, then
`docker compose restart inventory-service` or `docker compose restart order-service`
as appropriate. Verify the same failed offset is retried and only advances after
successful processing. **Never reset/skip offsets, delete inboxes, or fabricate
terminal state.** Unrepairable poison/unknown input and its partition followers
remain unavailable until later DLQ/replay tooling; restart alone is not a fix.
Other partitions continue. Existing inventory HTTP release is **unsupported on
async-owned reservations** until compensation exists. No cancellation, late-success
reconciliation, mixed-version rollback, broker HA, or exactly-once delivery is claimed.

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
| Phase 3B closeout | Merged at ca525d1; hosted CI succeeded |
| Phase 4A | Outbox baseline c411c8d; hosted CI passed in run 35494095946 |
| Phase 4B slice 1 | Async completion and consumer crash recovery locally implemented; whole-branch review/hosted CI pending |
| Later Phase 4B | Cancellation, compensation, delayed success after cancellation, DLQ and repaired replay |
| Phase 5 | Logs, metrics, and traces explain successful and failed orders |
| Phase 6 | A small UI and E2E test demonstrate the completed flow |

Basic correlation logging starts in Phase 3; the full dashboard stack stays in
Phase 5. A separate weekly `playground/` exercise covers Java/concurrency topics
without blocking service milestones. The directory is created with its first exercise.
