# Phase 3A Inventory Correctness Design

Date: 2026-09-07
Status: Approved in conversation; implementation pending

## Purpose

Add an independently buildable `inventory-service` that owns stock and reservation
state. Phase 3A proves two database correctness claims on real Postgres:

1. Concurrent orders cannot reserve more stock than exists.
2. A multi-SKU reservation changes every requested SKU or none of them.

The service also supports idempotent release so stock conservation can be tested
before cancellation and compensation are introduced in Phase 4.

## Scope

Phase 3A includes:

- A separate Spring Boot service, Maven build, OpenAPI contract, Postgres database,
  Flyway migrations, Docker image, and Compose services.
- Reserve, retrieve, release, and stock-read HTTP operations.
- Domain and API validation, including rejection of duplicate SKUs.
- Pessimistic row locking in deterministic SKU order.
- Unit, web-slice, persistence, concurrency, and full vertical-slice tests.
- Repository-level build commands, CI coverage, an ADR, and evidence updates.

Phase 3A excludes order-to-inventory HTTP calls, retries, timeouts, circuit breakers,
payload-aware reservation replay, cross-service contract tests, and correlation
propagation. Those belong to Phase 3B.

## Service Boundary

`inventory-service` owns its source code, OpenAPI-generated DTOs, domain model,
persistence entities, migrations, database, and Docker image. It does not depend on
`order-service` or a shared Java domain library. The only shared concepts are values
in documented HTTP contracts, such as `orderId` and SKU strings.

The service follows the existing order-service conventions:

- Java 21 and Spring Boot 3.3.5.
- API interfaces and DTOs generated during the Maven lifecycle from `openapi.yaml`.
- Handwritten controller, application service, domain records, repository port, and
  JPA adapter.
- Flyway owns schema changes; Hibernate uses `ddl-auto: validate` and OSIV is off.
- Surefire runs fast tests and Failsafe runs `*IT` tests with Testcontainers Postgres.
- Mockito's Byte Buddy agent is loaded at JVM startup.

## HTTP Contract

All errors use `application/problem+json`. Validation failures include an `errors`
map. Business conflicts use stable problem types.

### Reserve stock

`POST /api/v1/reservations`

Request:

```json
{
  "orderId": "c7c89918-f8e8-42d9-93c8-211ee1aa331f",
  "lines": [
    {"sku": "SKU-APPLE", "quantity": 2},
    {"sku": "SKU-BANANA", "quantity": 1}
  ]
}
```

Outcomes:

- `201 Created`: reservation persisted with status `RESERVED`; response includes a
  canonical `Location` header.
- `400 Bad Request`: malformed fields, empty lines, non-positive quantity, or a SKU
  repeated within the request.
- `409 Conflict`: one or more SKUs are unknown or have insufficient availability.
  The problem body includes `unavailableSkus`, mapping each SKU to its requested and
  available quantities. An unknown SKU reports availability zero.
- `409 Conflict`: `orderId` already has a reservation, whether that reservation is
  `RESERVED` or `RELEASED`. Phase 3B replaces this with payload-aware replay rules.

Duplicate SKUs are rejected rather than combined. Callers must express one quantity
per SKU, keeping intent and persistence identity unambiguous.

### Retrieve a reservation

`GET /api/v1/reservations/{orderId}`

- `200 OK`: returns the reservation in `RESERVED` or `RELEASED` state.
- `404 Not Found`: no reservation exists for the order.

### Release a reservation

`PUT /api/v1/reservations/{orderId}/release`

- `200 OK`: restores all reserved quantities and returns status `RELEASED`.
- `200 OK` on every repeat: returns the existing released result without changing
  stock again.
- `404 Not Found`: no reservation exists for the order.

The reservation remains stored after release. This is necessary to distinguish an
already-applied retry from an unknown reservation.

### Read stock

`GET /api/v1/stock/{sku}`

- `200 OK`: returns `sku` and `availableQuantity`.
- `404 Not Found`: the SKU is unknown.

There is no public stock-mutation endpoint in this phase. A Flyway migration seeds
`SKU-APPLE` with 10 units and `SKU-BANANA` with 5 units for the local demo;
integration tests insert isolated fixtures directly.

## Domain Model

- `Reservation`: `orderId`, immutable ordered reservation lines, status, creation
  time, and optional release time.
- `ReservationLine`: non-blank SKU and positive quantity.
- `ReservationStatus`: `RESERVED` or `RELEASED`.
- `Stock`: SKU and non-negative available quantity, with methods that reject an
  excessive reservation and restore positive quantities.

The `Reservation` constructor defensively copies lines and rejects empty or duplicate
SKUs. These invariants are independent of generated request validation because domain
objects can also be reconstructed or called without HTTP.

## Database Model

`stock`

- `sku VARCHAR(64) PRIMARY KEY`
- `available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0)`

`inventory_reservations`

- `order_id UUID PRIMARY KEY`
- `status VARCHAR(16) NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL`
- `released_at TIMESTAMPTZ NULL`

`inventory_reservation_lines`

- `order_id UUID NOT NULL` referencing `inventory_reservations`
- `sku VARCHAR(64) NOT NULL` referencing `stock`
- `quantity INTEGER NOT NULL CHECK (quantity > 0)`
- primary key `(order_id, sku)`

The composite primary key gives the database a second line of defense against
duplicate SKUs. The schema does not store a separate reservation identifier because
the approved invariant is one lifetime reservation per `orderId`.

## Reservation Transaction

The application service performs one short transaction:

1. Construct the domain request and reject duplicate SKUs.
2. Reject an existing reservation for `orderId`. The database primary key remains
   authoritative when two requests with the same new ID race.
3. Sort requested SKU values lexicographically.
4. Fetch and lock all matching stock rows with one
   `SELECT ... WHERE sku IN (...) ORDER BY sku FOR UPDATE` query.
5. Compare returned rows with requested SKUs and collect unknown or insufficient
   entries without mutating stock.
6. If any entry is unavailable, throw a business conflict and roll back.
7. Decrement every stock row and persist the reservation plus its lines.
8. Commit once.

Postgres row locks serialize contenders for the same SKU. Sorting lock acquisition
provides one order for overlapping multi-SKU requests and reduces deadlock risk.
Checking every line before decrementing makes the all-or-nothing behavior visible in
the application logic; the database transaction remains the final atomicity boundary.

## Release Transaction

Release also uses one short transaction:

1. Fetch and lock the reservation row by `orderId`.
2. Return it unchanged when it is already `RELEASED`.
3. Lock the reservation's stock rows in sorted SKU order.
4. Restore every stored reservation-line quantity.
5. Mark the reservation `RELEASED`, record `releasedAt`, and commit once.

Locking the reservation row ensures concurrent releases cannot both restore stock.
Release uses quantities stored with the reservation, never quantities supplied by a
caller.

## Error Translation

Domain/application exceptions map as follows:

- Duplicate SKU or malformed domain input: `400` with type `invalid-reservation`.
- Missing reservation or stock read: `404` with a resource-specific problem type.
- Unavailable stock: `409` with type `stock-unavailable` and structured SKU details.
- Existing reservation: `409` with type `reservation-already-exists`.

A primary-key violation from concurrent creation of the same `orderId` is translated
to the same existing-reservation conflict, and its stock changes roll back.

Unexpected persistence failures remain server errors. Exception translation will be
specific; the service will not treat every `IllegalArgumentException` as client input.

## Verification Strategy

Development follows red-green-refactor. Each production behavior begins with a test
that fails for the intended reason.

Fast tests:

- Domain tests for empty lines, duplicate SKUs, non-positive quantities, stock
  decrement, and restoration.
- Service tests for orchestration where database locking is not the behavior under
  test.
- MVC slice tests for successful responses and `400`/`404`/`409` problem bodies.

Real-Postgres integration tests:

- A reservation round-trip retains line data and state.
- A request with one sufficient and one insufficient SKU changes neither stock row
  and creates no reservation.
- Two distinct orders reserving simultaneously against the last unit produce exactly
  one reservation, one stock-unavailable result, and availability zero.
- Two concurrent releases of one reservation restore stock exactly once.
- Repeated sequential release returns `RELEASED` without another increment.
- A full HTTP reserve/get/release flow uses real controller, service, JPA, and Postgres
  beans.
- Flyway migration and Hibernate schema validation succeed on a fresh database.

The concurrency tests use a start gate and separate transactions on real Postgres.
Assertions focus on durable outcomes rather than which thread wins.

## Build, Compose, And CI

- `inventory-service/pom.xml` builds and verifies independently.
- Service-specific Make targets allow fast work on either service.
- Repository-level `make test` and `make verify` run both services sequentially.
- Compose adds `inventory-postgres` on host port `5433` and `inventory-service` on
  host port `8081`, with independent credentials, health checks, and named volume.
- CI verifies both Maven projects. The existing order-service image/restart check
  remains intact; Phase 3A adds an inventory image build and health/startup check.
- README documents demo stock and curl commands for reserve, inspect, and release.

## Evidence And Documentation

Implementation completion requires:

- ADR-0006 recording the pessimistic-locking and reservation-lifecycle decisions.
- Updated repository README and verification scoreboard with commands, environment,
  outcomes, and limits.
- Updates to the relevant Obsidian notes without marking broader isolation or
  idempotency topics fully verified.
- A clean `make verify`, Compose health check, and hosted GitHub Actions run before
  Phase 3A is marked complete.

## Risks And Mitigations

- Deadlocks from overlapping multi-SKU requests: acquire stock locks in sorted order
  and test reversed request ordering.
- Double restoration under concurrent release: lock the reservation row and persist
  terminal `RELEASED` state.
- Mistaking locks for idempotency: keep duplicate `orderId` as an explicit conflict
  and defer replay semantics to Phase 3B.
- Overclaiming concurrency evidence: assert one concrete last-unit race on Postgres;
  do not claim every isolation anomaly or throughput characteristic is proven.
- Scope growth from stock administration: use explicit demo seed data and defer an
  administrative API.
