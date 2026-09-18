# 6. Inventory reservation correctness with ordered pessimistic locks

Date: 2026-09-18

## Status

Accepted for Phase 3A.

## Context

Phase 3A adds an independently deployable inventory service and proves two bounded
correctness claims on PostgreSQL: distinct orders competing for the last unit do
not oversell it, and a multi-SKU reservation changes every requested stock row or
none of them. It also needs release retries to restore stock at most once.

These claims require an explicit reservation identity, lock order, and transaction
boundary. They do not establish payload-aware replay, every SQL isolation anomaly,
or a universal best concurrency-control strategy.

## Decision

- `inventory-service` owns its OpenAPI contract, domain and persistence models,
  Flyway migrations, PostgreSQL database, Docker image, named volume, and Compose
  network. It shares no database, network, or Java domain module with
  `order-service`.
- One lifetime reservation is identified by `orderId`. A second reserve request
  for the same `orderId`, whether the stored reservation is `RESERVED` or
  `RELEASED`, returns `409 Conflict`. Payload-aware reservation replay remains a
  Phase 3B decision.
- A request contains exactly one positive quantity per SKU. Duplicate SKUs are
  rejected before any database mutation rather than merged. Reservation lines keep
  caller order through the stored `line_position` while lock acquisition uses a
  separate ascending SKU order.
- Reserve runs in one transaction. It checks reservation identity, locks all
  matching stock rows pessimistically in ascending SKU order, validates every
  requested SKU and quantity, then updates stock and inserts the reservation and
  lines. Any unavailable or unknown SKU, duplicate reservation race, or persistence
  failure rolls back the whole transaction.
- Release runs in one transaction and locks the reservation row first. An already
  released reservation returns unchanged. Otherwise, it locks the reservation's
  stock rows in ascending SKU order, restores the persisted quantities, and marks
  the reservation `RELEASED` in the same commit. The locked terminal state makes
  sequential and tested concurrent releases restore stock at most once.
- Flyway seeds the local demo with `SKU-APPLE=10` and `SKU-BANANA=5`. There is no
  public stock-mutation endpoint in this phase.

## Consequences

Pessimistic stock locks serialize tested contenders on shared SKUs, and a stable
ascending lock order reduces deadlock risk for overlapping multi-SKU requests.
Validating all locked rows before mutation makes the all-or-none intent explicit;
the PostgreSQL transaction supplies the atomic commit or rollback boundary.

Locking has a contention and connection-hold cost. The tests prove the concrete
last-unit, reversed multi-SKU, rollback, duplicate-identity, and concurrent-release
shapes recorded below. They do not prove freedom from every deadlock, anomaly, or
throughput limit at every isolation level.

Optimistic versioning and conditional updates are deferred, not rejected
universally. A version column can work well when conflicts are rare and callers
have a defined retry policy; a conditional decrement can be compact and efficient
for a single stock row. For this phase, either approach would require additional
rules for coordinating multiple SKUs, reporting all unavailable lines, handling
unknown rows, and retrying partial conflicts without weakening atomicity. They
should be compared in a separate measured experiment rather than generalized from
this bounded proof.

## Evidence

- `InventoryDomainTest` proves duplicate-SKU rejection, ordered immutable lines,
  stock bounds, and a stable terminal release transition without persistence.
- `InventoryPersistenceIT` proves reservation line/state round trips, ascending
  pessimistic stock locks, blocking between PostgreSQL transactions, and a lock
  waiter observing the committed reservation state.
- `InventoryReservationIT` proves multi-SKU commit and rollback, the last-unit
  race, reversed-SKU completion, duplicate-`orderId` rollback, and sequential and
  concurrent release restoration on PostgreSQL.
- `InventoryApiControllerTest`, `InventoryApiIT`, and
  `InventoryServiceApplicationIT` cover the HTTP outcomes, the real-bean
  reserve/get/double-release flow, schema startup, health, and the OSIV guard.
- On 2026-09-18, `make verify` ran 78 tests with no failures, errors, or skips;
  `make verify-restart` proved isolated order-service restart persistence; and
  `make verify-inventory-image` proved the isolated inventory image could reserve
  both demo SKUs, release twice, restore exact seeded stock, and clean up its
  Compose resources.

Exact commands, environment, and claim limits are recorded in the
[verification scoreboard](../notes-verification.md).

## References

- [Phase 3A design](../superpowers/specs/2026-09-07-phase-3a-inventory-correctness-design.md)
- [ADR-0004](0004-evidence-driven-learning-roadmap.md)
- [ADR-0005](0005-phase-2-persistence-boundaries.md)
- [Verification scoreboard](../notes-verification.md)
