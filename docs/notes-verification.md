# Notes Verification Scoreboard

The point of Commerce Lab is to **prove the interview-prep notes in practice**. This
file maps each phase to the vault notes it verifies, and tracks status. Update the
`Status` column as work lands, and update the source note in the vault with the
real gotchas you hit ("verified in practice").

Legend: ⬜ not started · 🟡 partial / work remaining · ✅ verified for the stated claim

Status applies to the specific claim, not the whole linked note. Configuration
alone does not prove performance, isolation, or recovery behavior. Each phase exit
requires a predicted failure, a reproduction and regression test, an explanation
without reading code, and an exact evidence entry (test/command, result, environment,
limits). Write ADRs for meaningful decisions and short notes for smaller experiments.
See [ADR-0004](adr/0004-evidence-driven-learning-roadmap.md) for the revised scope.

## Phase status

| # | Goal | Status |
|---|------|--------|
| 0 | Rails: order-service skeleton, health, Dockerfile, compose, Makefile | ✅ |
| 1 | One service done right (OpenAPI-first, layered, validation, RFC-7807, unit/slice tests) | ✅ |
| 2 | Reliable Postgres persistence and bounded cleanup | ✅ |
| 3A | Inventory correctness under tested PostgreSQL contention shapes | ✅ |
| 3B | Sync integration, idempotency, uncertain-outcome recovery | ⬜ |
| 4A | Durable event delivery through an outbox | ⬜ |
| 4B | Workflow recovery, compensation, idempotent consumers, DLQ | ⬜ |
| 5 | Observability — logs/metrics/traces across the system | ⬜ |
| 6 | Frontend slice + E2E; core finish line | ⬜ |
| 7 | Optional capstone (gateway/auth, rate limiter, CQRS, vthreads, deploy) | ⬜ |

## Current evidence

2026-09-18, the Phase 3A closeout candidate: `make verify` passed on local macOS
26.6.2 (Apple Silicon), Java 21.0.5, Maven 3.9.14, OrbStack, and
`postgres:16-alpine`. Both independently buildable Maven projects completed with 43
Surefire and 35 Failsafe tests: 78 total, 0 failures, 0 errors, 0 skipped.

`make verify-restart` started only the order service/database pair, built the order
image, restarted `order-service`, fetched the same persisted order and line sequence,
and removed its isolated Compose containers, volume, and network. Separately,
`make verify-inventory-image` started only the inventory pair, reserved both demo
SKUs, released the reservation twice, asserted exact restoration to
`SKU-APPLE=10` and `SKU-BANANA=5`, and removed its isolated Compose resources.

Test paths below are under `order-service/src/test/java/com/commercelab/order/`.

| Claim | Evidence | Limit |
|---|---|---|
| Domain arithmetic, currency checks, defensive copying, price precision/range | `domain/OrderDomainTest` | Currency-specific minor-unit rules and concurrency are not claimed |
| Place/get orchestration | `service/OrderServiceTest` | In-memory fake; no Spring transaction behavior |
| Selected HTTP success/error responses | `api/OrderApiControllerTest` | Service mocked; not full OpenAPI conformance |
| API works through Postgres; unsupported precision/overflow rejected; line sequence retained | `api/OrderApiIT` | Selected runtime contract behavior, not exhaustive OpenAPI conformance |
| Migration, schema validation, DB health, OSIV configuration guard | `OrderServiceApplicationIT.openEntityManagerInViewRemainsDisabled` | Bean/property guard, not a phantom-write experiment |
| V1→V2 migration reaches V2 and backfill is deterministic | `FlywayMigrationIT` | Test fixture can recover only a deterministic legacy order, not the unknowable original order |
| Database round trip, explicit fetch, stable line order | `persistence/OrderPersistenceIT` | One aggregate shape; no projection or broad N+1 benchmark |
| Assigned UUID creation uses insert semantics without lookup SELECT | `OrderPersistenceIT.assignedIdInsertUsesPersistWithoutLookupSelect` | Hibernate statistics with batching disabled for this test; update semantics are intentionally a separate future operation |
| Detached lazy collection throws after repository session closes | `OrderPersistenceIT.detachedLazyCollectionThrowsOutsideTransaction` | Demonstrates detachment, not HTTP-scoped OSIV behavior |
| Built image retains an order across service restart | `make verify-restart` | Restarts the service, not Postgres or the host; not a backup/restore or durability benchmark |

Inventory test paths below are under
`inventory-service/src/test/java/com/commercelab/inventory/`.

| Claim | Evidence | Limit |
|---|---|---|
| Duplicate SKUs fail before persistence; domain lines remain immutable and ordered; release is terminal | `domain/InventoryDomainTest` | Pure domain behavior; no Spring transaction or PostgreSQL locking claim |
| Reservation persistence preserves line order/state; stock rows are returned and pessimistically locked in ascending SKU order; a waiter observes committed reservation state | `persistence/InventoryPersistenceIT` | Tested on specific PostgreSQL row-lock interactions; not every deadlock, isolation level, or anomaly |
| Multi-SKU success is atomic; insufficiency rolls back all stock and reservation changes | `service/InventoryReservationIT.reservesMultipleSkusAtomicallyAndPreservesRequestOrder`, `insufficientSecondSkuRollsBackEveryChange` | Two-SKU fixtures and the implemented transaction path; not arbitrary distributed transactions |
| Two distinct orders cannot both reserve the tested final unit | `service/InventoryReservationIT.concurrentOrdersCannotReserveTheLastUnitTwice` | One PostgreSQL last-unit race at default isolation; not a universal no-anomaly or throughput proof |
| Reversed overlapping SKU requests complete under ascending locks | `service/InventoryReservationIT.reversedSkuRequestsCompleteWithoutDeadlock` | One controlled two-request shape; does not prove deadlock freedom for every workload |
| Existing and concurrently duplicated `orderId` requests conflict and roll back stock | `service/InventoryReservationIT.existingOrderIdIsAlwaysAConflictWithoutChangingStockAgain`, `concurrentSameOrderPrimaryKeyViolationBecomesDuplicateConflictAndRollsBackStock` | Duplicate requests return `409`; payload-aware replay remains Phase 3B |
| Sequential and concurrent release restore persisted quantities at most once | `service/InventoryReservationIT.repeatedReleaseRestoresStockOnce`, `concurrentReleaseRestoresStockOnce` | One reservation-row-first implementation and controlled concurrent pair; not a general exactly-once distributed-delivery claim |
| Declared reserve/get/stock/release HTTP outcomes map to stable responses | `api/InventoryApiControllerTest` | MVC slice with mocked service; selected runtime outcomes, not exhaustive OpenAPI conformance |
| Reserve, get, double release, and exact stock restoration work through real beans and PostgreSQL | `api/InventoryApiIT.reserveGetAndDoubleReleaseThroughRealBeansAndPostgres` | One full local workflow; no order-service call, timeout, retry, or lost-response recovery |
| Fresh schema startup, health, and disabled OSIV remain valid | `InventoryServiceApplicationIT` | Startup/configuration guard, not a contention or performance experiment |
| Built inventory image reserves and double-releases seeded SKUs, then cleans up | `make verify-inventory-image` | Local Compose image proof; no host restart, backup/restore, or hosted-runtime claim |

## Phase 2 cleanup: completed 2026-09-07

These changes close the bounded Phase 2 gate. They do not mark every topic in the
linked persistence notes as fully verified.

- [x] Define supported price precision and range, enforce at API/domain boundaries,
  and reject unsupported values before persistence. Test `9.99999`, overflow,
  valid boundaries, and numeric POST/GET consistency. Choose supported currencies
  before imposing a universal two-decimal rule.
- [x] Preserve line order with a stored position and a full sequence assertion
  after a fresh read. Existing rows have no recorded original order; document
  a deterministic backfill policy.
- [x] Measure assigned-UUID insert SQL and make insert/update semantics explicit;
  assert the intended SQL behavior. The mapper creates a fresh entity each save,
  so naive `Persistable.isNew=true` or null-version handling can break updates.
- [x] Rename the detached lazy-loading test accurately and add a guard that fails
  when OSIV is enabled. Use an HTTP-scoped experiment for claims about request lifetime.
- [x] Add a repeatable service restart check with a health wait; record its result
  separately from the existing database round-trip test.
- [x] Add CI Docker image build and restart coverage through `make verify-restart`.
- [x] Correct README status and current evidence claims (2026-09-07).
- [x] Explain the completed fixes and update affected source notes with exact
  evidence, including corrections to earlier overclaims.

Follow-ups outside this bounded gate: narrow generic `IllegalArgumentException`
translation and review dependency compatibility before creating inventory.
Additional batching/propagation experiments stay unverified until observed.

## Phase 3A closeout: completed 2026-09-18

These checks close only the Phase 3A inventory gate. Reservation replay, remote-call
retries, and uncertain-outcome recovery remain Phase 3B work.

- [x] Give inventory its own service, Maven build, OpenAPI contract, PostgreSQL
  database, Flyway schema, named volume, and disjoint Compose network.
- [x] Reject duplicate SKUs before database mutation and preserve caller line order
  independently from ascending lock order.
- [x] Lock stock rows pessimistically in ascending SKU order and prove the bounded
  last-unit and reversed multi-SKU PostgreSQL contention shapes.
- [x] Keep reserve and release all-or-none in short transactions; lock the
  reservation first during release and restore stock at most once under tested
  sequential and concurrent calls.
- [x] Keep one lifetime reservation per `orderId`; return `409 Conflict` for a
  duplicate instead of claiming Phase 3B payload-aware replay.
- [x] Verify 78 aggregate Maven tests, isolated order restart, isolated inventory
  image reserve/double-release, separate networks, and clean teardown.
- [x] Record decisions and limits in ADR-0006, the README, this scoreboard, and the
  named source-note update patch.

## Future acceptance criteria

### Phase 3A: inventory correctness (completed 2026-09-18)

- [x] Inventory owns its database and stock/reservation model; no shared domain jar.
- [x] Concurrent distinct orders competing for the tested last unit cannot oversell.
- [x] Multi-SKU reservations commit all lines or none; rejection leaves stock unchanged.
- [x] Duplicate SKUs, insufficient stock, reservation identity, and release semantics are explicit.
- [x] Ascending pessimistic locks are tested on real Postgres; optimistic and
  conditional-update alternatives remain valid candidates for a separate experiment.

### Phase 3B: synchronous integration

Design preparation (2026-09-19): [draft design](superpowers/specs/2026-09-19-phase-3b-sync-integration-design.md),
[proposed ADR-0007](adr/0007-synchronous-reservation-and-recovery.md), and
[implementation plan](superpowers/plans/2026-09-19-phase-3b-sync-integration.md).
The core choices were approved in conversation; the detailed documents are ready
for review. Implementation and the evidence below remain outstanding.

- [ ] Record state transitions, public response semantics, and restart recovery in an ADR.
- [ ] Keep RestClient calls and retries outside DB transactions; prove the boundary.
- [ ] Enforce idempotency at order creation and inventory reservation: same key/payload
  repeats the outcome; conflicting payload reuse is rejected.
- [ ] Lose a successful reservation response, retry/query by stable identity, and
  recover without reserving twice. Timeout is uncertainty, not insufficient stock.
- [ ] Restart between local persistence and remote outcome recording; resolve unfinished work.
- [ ] Test timeouts, bounded retries, circuit opening and recovery; never report
  success for an unconfirmed reservation. Business rejection is not a transient failure.
- [ ] Contract tests detect an intentional breaking producer change.
- [ ] Log order/reservation/correlation IDs and propagate correlation across HTTP.
- [ ] Extend commands and CI to both services while keeping each independently buildable.

### Phase 4A: durable event delivery

- [ ] Order and outbox entry commit or roll back together.
- [ ] Restart after commit but before publish; committed work eventually reaches the broker.
- [ ] Crash after broker acknowledgement but before marking delivery; demonstrate
  possible duplicates and preserve a stable event ID.
- [ ] Document event schema, partition key, publication ordering, and relay retries.

### Phase 4B: workflow recovery

- [ ] Dedup record, stock mutation, and inventory result outbox commit atomically.
- [ ] Inventory publication survives restart; order result consumption is idempotent.
- [ ] Crash after consumer DB commit but before acknowledgement; redelivery has one business effect.
- [ ] Demonstrate confirmation, rejection, and one cancel/release compensation path.
- [ ] Delayed/duplicate reservation success after cancellation cannot reconfirm an
  order or leak reserved stock; recovery converges after services restart.
- [ ] Test concurrent transition protection; transport ordering alone does not
  protect stock shared by different orders.
- [ ] Poison messages reach a DLQ after bounded retries; repaired replay is safe.

### Phases 5 and 6: finish the core

- [ ] Phase 5: correlate order/event/reservation logs and traces; define an async
  trace propagation/linking policy and explain one success and one recovered failure.
- [ ] Phase 5: dashboard plus one measurable SLO/alert exposes backlog or stuck work.
- [ ] Phase 6: small UI shows pending, confirmed, rejected, and cancelled outcomes;
  E2E tests cover success and a failure/recovery scenario.
- [ ] Deliver repeatable demo commands, evidence links, and a short tradeoff explanation.

## Note → phase map

> Vault notes referenced as `note-slug` live in
> `Obsidian-notes/tech-decisions/notes/`.

### Phase 1 — One service done right
- ✅ `spring-rest-controller-hygiene-validation-dtos-authz` — `@Valid` DTO validation + RFC-7807 errors. (authz/`@AuthenticationPrincipal` deferred to Phase 7 — no auth yet)
- ✅ `spring-rest-jackson-and-openapi-codegen-pattern` — generation and selected responses demonstrated; codegen does not guarantee full runtime contract conformance
- 🟡 `java-records-sealed-and-pattern-matching` — records + `Money` value object verified; sealed state hierarchy + exhaustive switch deferred to Phase 4 (event-driven transitions)
- ✅ `testing-taxonomy-pyramid-contracts-e2e-and-test-design` — domain/service unit tests + `@WebMvcTest` controller slice (contract tier in Phase 3)

### Phase 2 — Persistence done right
- ✅ `jpa-entity-equals-and-hashcode` — assigned IDs/reference equality implemented; creation explicitly uses `persist`, and statistics prove no lookup SELECT on insert
- 🟡 `osiv-session-vs-transaction-and-phantom-write` — OSIV property and interceptor absence guarded; detached lazy loading demonstrated; phantom-write experiment remains
- 🟡 `jpa-fetching-projections-and-lazy-initialization` — explicit fetch and domain mapping work; projections and broad query-count/N+1 experiments remain
- 🟡 `spring-transactional-propagation-savepoints-and-self-invocation` — Phase 3A proves reserve rollback and transactional release boundaries under default propagation; savepoints, alternate propagation, self-invocation, and timeouts remain unverified
- 🟡 `postgres-write-performance-batching-and-idempotency` — batching configured, not measured; idempotency in 3B/4
- 🟡 `database-isolation-levels-mvcc-and-anomalies` — Phase 3A proves selected PostgreSQL pessimistic-lock shapes (last unit, sorted multi-SKU locks, concurrent release); broader levels and anomalies remain separate experiments
- 🟡 `money-invariant-enforcement-frontend-to-db` — positive four-decimal price range is enforced at API/domain/schema boundaries; frontend and concurrent balance/stock invariants remain

### Phase 3A/3B — Inventory correctness + sync integration
- ⬜ `restclient-http-timeouts-and-connection-pooling`
- ⬜ `circuit-breaker-retry-and-resilience4j`
- ⬜ `testing-taxonomy-pyramid-contracts-e2e-and-test-design` (contract tier)
- ⬜ `request-idempotency-keys-for-write-apis`

### Phase 4A/4B — Durable delivery + workflow recovery
- ⬜ `saga-choreography-orchestration-and-compensation`
- ⬜ `outbox-pattern-and-dual-write-problem`
- ⬜ `at-least-once-to-exactly-once-effect-and-ordered-processing`
- ⬜ `streaming-dedup-and-ordered-emission`
- ⬜ `kafka-producers-spring-boot-and-aws-msk`
- ⬜ `kafka-consumers-spring-boot-and-fargate`
- ⬜ `kafka-exactly-once-transactions-and-schema-evolution` — Kafka transaction guarantees need a separate experiment; outbox/dedup alone do not verify the whole note
- ⬜ `pubsub-topic-subscription-and-dlq-model` (DLQ analog)

### Phase 5 — Observability
- ⬜ `distributed-tracing-and-apm`
- ⬜ `metrics-emission-paths-and-custom-vs-derived`
- ⬜ `monitoring-slos-and-alerting-on-symptoms-vs-causes`
- ⬜ `cache-observability-leading-indicators-and-silent-staleness` — optional cache experiment; not verified by the core dashboard

### Phase 6 — Frontend + E2E
- ⬜ `frontend-review-drills-and-trust-pass`
- ⬜ `react-rendering-and-hooks-internals`
- ⬜ `money-invariant-enforcement-frontend-to-db`

### Phase 7 — Optional capstone
- ⬜ `oauth2-oidc-and-jwt-validation`
- ⬜ `api-rate-limiter-design-token-bucket-redis`
- ⬜ `cqrs-architecture-write-read-split-and-projections`
- ⬜ `platform-vs-virtual-threads-scheduling-internals`
- ⬜ `deploy-strategies-blue-green-canary-rolling-and-rollback-policy`

## `playground/` — pure-language notes (no infra)
Schedule one small exercise per week of active lab work, separately from service
milestones. Create the module with the first exercise. Record experimental limits:
an ordinary green JUnit run does not prove the absence of all races.
- ⬜ `java-memory-model-visibility-and-atomicity`
- ⬜ `java-generics-type-erasure-variance-and-wildcards`
- ⬜ `java-collectors-tomap-and-thread-safety`
- ⬜ `concurrenthashmap-internals-and-cache-stampede`
- ⬜ `lru-cache-linkedhashmap-and-hand-rolled-doubly-linked-list`

Shipping, additional services, and Phase 7 are optional; pick an extension only
when it answers a specific learning question. Local Kafka-compatible broker tests
do not verify AWS MSK/Fargate deployment or Pub/Sub-specific behavior.
