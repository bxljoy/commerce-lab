# Phase 3B Synchronous Integration Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development (the user's
> preserved preference) when available, or superpowers:executing-plans when executing
> inline. Complete and review one task at a time. Track progress with the checkboxes.

**Goal:** Prove that synchronous order reservation is idempotent and recovers from
lost responses and process restarts.

**Architecture:** Short order transactions surround an HTTP gateway. Inventory owns
a durable attempt ledger; order owns request identity and pending recovery. A
scheduled worker resolves uncertainty, with optimistic protection for local writes.

**Tech Stack:** Java 21, Spring Boot 3.3.5, PostgreSQL 16, JPA, Flyway, RestClient,
OpenAPI Generator 7.10.0, Testcontainers, and a compatible pinned Resilience4j version.

**Spec:** [Phase 3B design](../specs/2026-09-19-phase-3b-sync-integration-design.md).

**Status:** Draft for review. No implementation task has started.

## Global constraints

- Keep Java 21, Spring Boot 3.3.5, OpenAPI Generator 7.10.0, PostgreSQL 16 and the
  existing startup agent configuration.
- Each service retains its own domain types, database, and independently runnable build.
- Generated API classes remain untracked under `target/generated-sources/openapi`.
- No active database transaction during gateway calls, retries, or backoff.
- Historical PLACED orders are excluded from reconciliation.
- Required order keys, 201 final/202 pending, semantic inventory equality, scheduled
  recovery, and at most two request-path reserve attempts are approved decisions.
- All detailed behavior is defined by the spec, including blocked recovery cases.

## Review focus

1. Rejected inventory attempt retried after stock changes: preserve rejection (Task 2).
2. Worker and request overlap: one stock effect and no terminal regression (Tasks 4, 6).
3. Numeric scale and line order: compare intended canonical values (Tasks 1, 3).
4. Upgrade from old rows, especially RELEASED reservations: replay safely (Tasks 2, 4).
5. Lost response after remote commit: prove with real producer and dropped traffic
   rather than merely returning a timeout from a mock (Task 8).

## Shared interface map

All names below are handwritten, service-local types, independent of generated DTOs.

Inventory package roots are `com.commercelab.inventory`; order roots are
`com.commercelab.order`. `Reservation`, `Availability`, `Order`, `PlaceOrderCommand`,
and `ReserveInventoryCommand` refer to existing types within their owning service.

```java
// inventory.service
sealed interface ReservationAttemptResult {
    record Accepted(Reservation reservation, boolean created)
            implements ReservationAttemptResult {}
    record Rejected(Map<String, Availability> unavailable)
            implements ReservationAttemptResult {}
}
// InventoryService.reserve(ReserveInventoryCommand) -> ReservationAttemptResult
// InventoryService.getAttempt(UUID) -> ReservationAttemptResult
// release(UUID) remains Reservation; missing attempt throws the existing 404 type.

// order.service
record OrderCreation(Order order, boolean created) {}
// OrderCreationService.createOrReplay(String key, PlaceOrderCommand command,
//                                   String correlationId) -> OrderCreation
// OrderProgressService.load(UUID id) -> PendingOrderSnapshot
// OrderProgressService.apply(UUID id, InventoryOutcome outcome) -> Order
// OrderProgressService.findDueIds(Instant now, int limit) -> List<UUID>
// OrderProgressService.defer(UUID id, String failureCode, Instant nextAttempt) -> void
// OrderProgressService.block(UUID id, String issueCode) -> void
record PendingOrderSnapshot(Order order, int attemptCount, String correlationId) {}

// order.inventory (does not import inventory-service Java types)
record InventoryLine(String sku, int quantity) {}
record InventoryRequest(UUID orderId, List<InventoryLine> lines) {}
record StockShortage(int requested, int available) {}
sealed interface InventoryOutcome {
    record Reserved(UUID orderId, List<InventoryLine> lines) implements InventoryOutcome {}
    record Released(UUID orderId, List<InventoryLine> lines) implements InventoryOutcome {}
    record Rejected(Map<String, StockShortage> unavailable) implements InventoryOutcome {}
    record Missing() implements InventoryOutcome {}
}
interface InventoryGateway {
    InventoryOutcome reserve(InventoryRequest request, String correlationId);
    InventoryOutcome find(UUID orderId, String correlationId);
}
// TransientInventoryException: transport/timeouts/5xx/open circuit.
// InventoryProtocolException: unexpected 4xx, invalid or mismatched success payload.
// OrderReservationCoordinator.attempt(UUID id) -> Order (request-path policy)
// OrderReservationCoordinator.reconcile(UUID id) -> void (GET then optional POST)
// OrderRecoveryWorker.runOnce() -> void (bounded due-ID scan)
```

The gateway is the transport boundary. The coordinator verifies successful response
identity and lines against the pending snapshot, and routes outcomes to progress
recording. Missing is legal only from GET. Released and protocol errors block
pending work. Progress methods reload status and ignore stale updates to terminal
orders, handling optimistic conflicts outside the failed transaction.

## Task 1: Inventory payload identity

**Files:** Create `inventory-service/src/main/java/com/commercelab/inventory/service/ReservationPayload.java`
and `inventory-service/src/test/java/com/commercelab/inventory/service/ReservationPayloadTest.java`.

**Interface:** `ReservationPayload.from(ReserveInventoryCommand)` returns a value
with immutable SKU-sorted lines and `JsonNode canonicalJson()`; reject duplicates
and invalid/null lines using existing domain validation.

- [ ] Write tests with these concrete cases:

```text
[(B,2),(A,1)] equals [(A,1),(B,2)]
[(A,1)] differs from [(A,2)] and [(B,1)]
duplicate A, null line, blank SKU, zero quantity -> invalid before persistence
mutating caller list after construction does not change canonical content
```

- [ ] Run `mvn -f inventory-service/pom.xml -B -Dtest=ReservationPayloadTest test`;
  confirm failure identifies the missing payload behavior.
- [ ] Implement validation, immutable sorting, and structured JSON array/object
  construction with Jackson. Use the same sorted field content in SQL backfill.
- [ ] Rerun the focused test and `make test-inventory`; review and commit the slice.

## Task 2: Durable inventory outcomes and HTTP replay

**Files:** Add inventory `V3__reservation_attempts.sql`, persistence
`ReservationAttemptStore.java`, service `ReservationAttemptResult.java`, domain
`ReservationPayloadConflictException.java`; modify `InventoryService.java`,
`InventoryApiController.java`, `InventoryApiExceptionHandler.java`, and
`inventory-service/openapi.yaml`. Add `InventoryReplayIT.java`, `InventoryMigrationIT.java`
under inventory test package root; update existing API/reservation tests.

**Interface:** Expose reserve/getAttempt outcomes as defined above. Store claims
and results inside the same transaction as stock changes.

- [ ] Add real-Postgres red tests for concurrent same ID/same payload, reordered
  replay, different payload conflict, and replay after release. Assert one decrement.
- [ ] Add rejection replay tests: reserve too much, change stock in a test fixture,
  retry and GET the same attempt; assert original rejection snapshot. Include an
  unknown SKU and assert no reservation/stock mutation but one committed ledger row.
- [ ] Add a V2-to-V3 migration fixture containing RESERVED and RELEASED reservations
  with reversed caller line order. Assert successful semantic replays after upgrade.
- [ ] Run `mvn -f inventory-service/pom.xml -B -Dit.test=InventoryReplayIT,InventoryMigrationIT verify`;
  observe failures before implementing the new behavior.
- [ ] Implement claim/read/result SQL using structured JSON parameters:

```sql
INSERT INTO inventory_reservation_attempts
    (order_id, canonical_payload, created_at)
VALUES (:orderId, CAST(:canonicalJson AS jsonb), :createdAt)
ON CONFLICT (order_id) DO NOTHING;
```

  A zero-row insert means read/compare the winner in the subsequent statement.
  A new claim checks locked stock, returns a typed rejection after saving its
  snapshot, or saves stock/reservation/success atomically. An unexpected exception
  rolls all changes back. Backfill canonical JSON from existing lines ordered by SKU.
- [ ] Update OpenAPI/HTTP tests for 201 new, 200 matching success replay, typed 409
  conflict/rejection and GET rejected 409; keep all problem bodies documented.
- [ ] Update old duplicate-ID tests to assert the new explicit contract. Preserve
  multi-SKU atomicity, sorted locks, double release, and unrelated-integrity tests.
- [ ] Run `make verify-inventory`; review and commit.

## Task 3: Atomic order identity and pending creation

**Files:** Modify order `Order.java`, `OrderStatus.java`, `OrderEntity.java`,
`PlaceOrderCommand.java`, API/controller/exception handler, and `order-service/openapi.yaml`.
Add order `V3__order_request_identity.sql`, service `OrderCreationService.java`,
`OrderCreation.java`, `OrderPayload.java`, persistence `OrderRequestStore.java`, and
tests `OrderIdempotencyIT.java`, `service/OrderPayloadTest.java`.

**Interface:** `createOrReplay(key, command, correlationId)` as above; immutable
canonical order content is available from `OrderPayload.from(command)`.

- [ ] Write tests for required/invalid keys, same-key concurrent requests, conflicting
  payloads, numeric scale equality, reordered line conflict, null lines, and duplicate
  SKUs. Assert invalid commands leave both tables empty.
- [ ] Run `mvn -f order-service/pom.xml -B -Dit.test=OrderIdempotencyIT verify` and
  `mvn -f order-service/pom.xml -B -Dtest=OrderPayloadTest test`; observe red results.
- [ ] Add migration widening status to VARCHAR(32), new request table with named key
  constraint, canonical JSONB and SHA-256/version metadata. Preserve existing rows.
- [ ] Implement atomic pending creation and fresh-transaction replay after named
  key-race failure. Keep creation's `persist` semantics; do not merge existing orders.
- [ ] Add required key header and 200/201/202/409 contracts. Until Task 5 connects
  HTTP, new orders return 202 with Location and Retry-After; GET reads current state.
- [ ] Test the new API and update creation tests to pending. Run `make verify-order`;
  review and commit. Explicitly document this intermediate pending-only stage.

## Task 4: Safe state transitions and due work persistence

**Files:** Add order `V4__order_recovery.sql`, service `OrderProgressService.java`,
`PendingOrderSnapshot.java`, order-local `inventory/InventoryOutcome.java`,
`InventoryLine.java`, `StockShortage.java`; modify `OrderEntity.java`, domain `Order.java`,
`OrderJpaRepository.java`, controller mapping and OpenAPI response fields.
Add `OrderProgressIT.java` and extend `FlywayMigrationIT.java`.

**Interface:** load/apply/findDueIds/defer/block as in the interface map.

- [ ] Test PENDING -> CONFIRMED/REJECTED, unchanged terminal results on stale writes,
  historical PLACED exclusion, and concurrent finalization. Include pending issue
  visibility and due-time persistence after entity-manager/context recreation.
- [ ] Run `mvn -f order-service/pom.xml -B -Dit.test=OrderProgressIT,FlywayMigrationIT verify`.
- [ ] Add version (zero backfill), recovery timestamps/count/blocked/code/correlation
  columns and due-work index. Backfill eligible pending rows' due times; leave
  historical PLACED rows unscheduled. Update managed entities for transitions.
- [ ] Implement guarded state changes and optimistic-conflict reload outside the
  failed transaction. Increment recovery attempts and update due times atomically;
  ignore defer/block calls for terminal orders. Use an injectable Clock.
- [ ] Test bounded ID selection without paginating a collection fetch, then fetch
  snapshots in separate short transactions. Run `make verify-order`; review and commit.

## Task 5: Synchronous HTTP path with bounded resilience

**Files:** Add order `inventory/InventoryGateway.java`, `InventoryRequest.java`,
`RestInventoryGateway.java`, `InventoryClientConfiguration.java`, transport DTOs,
`TransientInventoryException.java`, `InventoryProtocolException.java`, service
`OrderReservationCoordinator.java`, web `CorrelationIdFilter.java`; modify order
`OrderService.java`, controller, `pom.xml`, `application.yml`. Add order tests
`inventory/InventoryGatewayTest.java`, `OrderReservationIT.java`, `web/CorrelationIdFilterTest.java`.

**Interface:** gateway and coordinator.attempt from the interface map. Rest gateway
performs one physical call; coordinator applies the two-attempt request policy.

- [ ] Write HTTP-fixture tests for successful reserve, typed rejection, timeouts,
  5xx, unexpected 4xx, invalid/mismatched success bodies, and released replay.
  Assert exactly two maximum POSTs, zero 4xx retry, no network call when circuit open.
- [ ] Verify and pin compatible HTTP pooling, Resilience4j, and HTTP test-fixture
  dependencies using official docs and dependency resolution; record versions.
- [ ] Run `mvn -f order-service/pom.xml -B -Dtest=InventoryGatewayTest,CorrelationIdFilterTest test`
  and `mvn -f order-service/pom.xml -B -Dit.test=OrderReservationIT verify`; observe red.
- [ ] Implement gateway DTO mapping, timeouts and per-call breaker; add explicit
  response verification. Implement this coordinator flow:

```text
load pending snapshot in transaction; return current state if already terminal
attempt reserve outside transaction, retry transient failure once
known valid result -> progress.apply in transaction
still transient -> progress.defer and return persisted pending order
protocol/released inconsistency -> progress.block and return persisted pending order
```

- [ ] Wire only new creations into the coordinator. Replays return stored state.
  Verify 201 CONFIRMED/REJECTED, 202 pending, and 200 terminal replay over real beans.
- [ ] Add transaction probes at gateway entry; while the HTTP fixture blocks, prove
  another transaction can update the order row. Test breaker open/half-open/closed
  with controlled state/time, including business rejection excluded from failures.
- [ ] Add correlation input/echo/propagation tests and thread-context cleanup checks.
  Run `make verify-order`; review and commit.

## Task 6: Automatic reconciliation and restart convergence

**Files:** Add order `service/OrderRecoveryWorker.java`, recovery configuration;
extend coordinator and progress service. Add `OrderRecoveryIT.java` and
`service/OrderRecoveryScheduleTest.java` under order tests.

**Interface:** coordinator.reconcile and worker.runOnce as above. Worker is enabled
by default, configurable fixed delay and batch size; disable scheduling in most tests.

- [ ] Test GET RESERVED/rejected/404/RELEASED, transient GET failure, blocked protocol
  failure, due-time cap, and maximum batch size with an injected clock.
- [ ] Test pending insert followed by context restart, and remote success followed
  by skipped local apply then context restart. Reuse the DB; assert convergence.
- [ ] Test two workers plus request finalization concurrently. Assert one terminal
  transition and safe repeated inventory intent, including optimistic reloads.
- [ ] Run `mvn -f order-service/pom.xml -B -Dit.test=OrderRecoveryIT verify` and
  `mvn -f order-service/pom.xml -B -Dtest=OrderRecoveryScheduleTest test`; confirm red.
- [ ] Implement due-ID scan, sequential snapshots, GET then optional POST, and
  persisted capped schedule. No nested HTTP retry in a recovery pass. Clear logging
  context after each order even when a call fails. Catch per-order failures so one
  bad item does not prevent processing the rest of a batch.
- [ ] Verify blocked work is visible and skipped, legacy orders never contact
  inventory, and deferred rows survive restart. Run `make verify-order`; review and commit.

## Task 7: Consumer contract evidence

**Files:** Create versioned consumer expectations in `contracts/inventory/` and
`contracts/orders/`; add `InventoryConsumerContractIT.java` to inventory tests,
`OrderPublicContractIT.java` to order tests, `inventory/InventoryContractTest.java`
to order tests, and relevant test resources/POM configuration.

**Interface:** Versioned JSON fixtures describe only the fields/status/problem types
consumers require. They remain separate from generated producer DTO expectations.

- [ ] Create accepted/new, replay/released, stock-rejected, missing, and conflict
  fixtures with the exact status and fields in the spec. Order fixtures cover
  201 confirmed/rejected, 202 pending, 200 replay and 409 key conflict.
- [ ] Make real producer API tests validate these consumer expectations, and make
  the actual order gateway deserialize inventory fixtures and classify outcomes.
- [ ] Add a negative control: remove required `orderId`, change `quantity` to text,
  and change a stock rejection's problem type. Assert contract validation fails.
  Require explicitly observed failure for an intentionally mutated producer fixture.
- [ ] Run `make verify`; confirm the normal contracts pass and negative controls
  reject mutations. Document covered operations and limits; review and commit.

## Task 8: Real two-service failure proof, CI, and evidence

**Files:** Modify `docker-compose.yml`, `Makefile`, `.github/workflows/ci.yml`,
`scripts/verify-order-restart.sh`, `scripts/verify-inventory-service.sh`, README,
`docs/notes-verification.md`, ADR-0007. Add `scripts/verify-sync-recovery.sh` and
test-only failure proxy configuration under `scripts/fixtures/`.

**Interface:** `make verify-sync-recovery` owns a unique Compose project and cleans
only its containers, networks, and volumes via exit trap. Existing service-specific
checks remain independently runnable.

- [ ] Add the application-only network and inventory base URL; preserve private DB
  networks. Assert application containers can communicate, but databases are not
  attached to the shared network. No inventory dependency for order startup.
- [ ] Update old curl/smoke requests with unique idempotency keys and new status
  assertions. The isolated order restart check uses unavailable inventory to prove
  a pending order survives restart without adding inventory containers.
- [ ] Build a test-only forwarding proxy that forwards reserve to real inventory,
  waits for its successful response, and drops the response to order. Exercise both
  request attempts being lost; verify inventory GET shows RESERVED and stock changed
  once. Keep that test hook outside production application code.
- [ ] Restart order while it remains pending; restore normal proxy traffic, poll
  within a bounded deadline, and assert the same ID is CONFIRMED with unchanged
  reserved stock. Also prove restart before the first remote attempt recovers.
- [ ] Run `make verify`, `make verify-restart`, `make verify-inventory-image`, and
  `make verify-sync-recovery`. Add the new target to hosted CI and check cleanup on
  failure paths. Record actual results and environment without reusing old counts.
- [ ] Update the scoreboard with the exact evidence and limits, including committed
  rejection metadata versus unchanged stock, request replay versus response bytes,
  and blocked operational inconsistencies. Mark ADR-0007 accepted after design review
  and verified implementation. Update vault notes separately with appropriate access.
- [ ] Review the whole branch, fix findings with focused regression checks, then
  report the ready-to-merge result. Merge/push requires the user's instruction for
  this phase; the Phase 3A merge authorization does not publish Phase 3B.

## Completion review

- [ ] Every design exit criterion maps to a passing test or recorded experiment.
- [ ] Migrations preserve legacy data; canonical SQL and Java agree.
- [ ] No gateway or sleep runs inside an active database transaction.
- [ ] Crash recovery, competing requests, and business rejection are separately proven.
- [ ] Both service builds and hosted checks pass; temporary faults are cleaned up.
- [ ] Public examples, contract fixtures, ADR, and evidence describe the same behavior.
