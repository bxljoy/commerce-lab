# Phase 4B Async Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete pending orders through durable inventory result events, with one business effect despite duplicate delivery and consumer crashes.

**Architecture:** Inventory consumes OrderPlaced inside a PostgreSQL transaction that claims an inbox identity, reuses reservation logic, and inserts a result outbox. A service-local polling relay publishes immutable results. Order consumes those results in a transaction that validates original intent and atomically records acceptance and a guarded status transition.

**Tech Stack:** Java 21, Spring Boot 3.3.5, Boot-managed Spring Kafka 3.2.4 / Kafka clients 3.7.1, PostgreSQL 16, Flyway, Testcontainers 1.20.4, apache/kafka:3.7.1, Docker Compose, Python standard-library proof harnesses.

**Spec:** [Approved design](../specs/2026-09-20-phase-4b-async-completion-design.md).

Status: approved by user; slice 1 locally implemented on codex/phase-4b-async-completion.
Task 7 final local verification passed; whole-branch review and hosted CI pending.
Baseline: c411c8d (Phase 4A); specification commit 94256bc.

## Global Constraints

- New orders still return 202 PENDING_INVENTORY. Terminal replay returns 200.
- Keep OrderPlaced v1 and its topic unchanged.
- Publish both result types to commerce.inventory.v1 with the canonical orderId UUID as key.
- Three partitions, replication factor one, seven-day delete retention.
- Inventory commits inbox, reservation attempt, stock changes, reservation, and exactly one logical result outbox row in one transaction.
- Offset acknowledgement follows successful commit.
- Network I/O stays outside the database transaction.
- No batch may commit beyond an unsuccessfully handled record.
- No automatic historical event regeneration or inventory result backfill occurs.
- Inbox and attempt records are not expired in this slice.
- No cancellation endpoint or new cancellation state is implemented here.
- No new UI or general-purpose messaging framework is needed.
- No silent skipping, default log-and-discard recoverer, forced offset reset, or purging.
- Preserve Mockito startup-agent configuration and pinned dependencies.
- Preserve pre-existing original-checkout and vault edits. Work only in an isolated implementation branch based on this design branch after approval.

## Review Focus

1. A tombstone, malformed JSON, or coercible string quantity must block without offset advance; Task 1 and Task 5.
2. Equal parsed content with different JSON object ordering is a duplicate, but reordered lines or changed envelope fields are conflicts; Task 2 and Task 4.
3. A rebalance/restart while a partition is blocked must not skip its record or stall healthy partitions; Task 5.
4. Existing HTTP reservation/release operations can overlap event intake; matching attempts replay but released reservations never publish fresh success; Task 2.
5. Fresh groups can consume retained historical events, but retention gaps cannot be fixed by earliest offsets; Task 6 and Task 7.

## File and Ownership Map

Paths below are relative to repository root. Java roots:
`I = inventory-service/src/main/java/com/commercelab/inventory`,
`O = order-service/src/main/java/com/commercelab/order`.
`IT`/`OT` replace `src/main/java` with `src/test/java` in those roots.
These abbreviations expand to exact paths, not separate modules.

- `contracts/events/inventory/v1/`: strict schemas and examples, owned by inventory.
- `I/events/`, `O/events/`: independent wire records, strict decoding, protocol exceptions.
- `I/messaging/`, `O/messaging/`: inbox and application consumers, Kafka adapters, partition failure handling, metrics, proof-only hooks.
- `I/outbox/`: local result store and publisher/relay, based on Phase 4A semantics without importing order code.
- `O/persistence/OrderCompletionStore.java`: original-intent validation reads and guarded terminal transition.
- Forward migrations: inventory V4 for inbox/outbox; order V6 for inbox/accepted result.
- Existing reservation and order HTTP contracts remain; no generated DTO edits.
- `scripts/fixtures/async-completion-proof.py` and Compose override: real-process evidence.

## Execution and Review Gates

### Progress

- [x] Task 1: contracts/decoders, 20573f2, independent review approved.
- [x] Task 2: atomic inventory intake, 3a68780, independent review approved.
- [x] Task 3: inventory relay, 1b3526e, independent review approved.
- [x] Task 4: order completion, 83dd707, independent review approved.
- [x] Task 5: Kafka listeners and partition safety, 5477ff9 + 2ff6fad, independent review and fix re-review approved.
- [x] Task 6: end-to-end and process-crash evidence, 518fcc3 + 64366a1, independent review approved.
- [x] Task 7: CI/documentation and final local verification (1005 Java, 27 Python, four images and four exit97 cleanup cases; task7-final-* logs).
- [ ] Whole-branch independent review and resolution of actionable findings.
- [ ] Hosted Phase 4B CI (no merge/push authorized or performed).

The detailed checkboxes below describe execution steps; the reviewed task status
above is authoritative while per-step evidence is recorded in task reports.

Tasks are sequential: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7. Each has a red/green test cycle and a commit; review its deliverable before proceeding. Test-only skeletons may fail compilation in the initial red step, but the final red run must demonstrate the behavioral failure before the implementation is considered proven. Do not weaken existing assertions merely to make new consumers pass.

For each Java task, run focused tests first and the owning service's full `verify` before its commit. Integration tests use real PostgreSQL; messaging tests additionally use the pinned Kafka container. A reviewer can reject transaction correctness independently from transport or proof-harness correctness.

## Task 1: Strict Event Contracts and Boundary Decoders

**Files:**
- Create `contracts/events/inventory/v1/inventory-reserved.schema.json`, `inventory-rejected.schema.json`, `inventory-reserved.json`, `inventory-rejected.json`.
- Create `I/events/OrderPlaced.java`, `InventoryResult.java`, `EventJson.java`, `EventProtocolException.java`.
- Create `O/events/InventoryResult.java`, `EventJson.java`, `EventProtocolException.java`.
- Create `IT/events/EventContractTest.java`, `OT/events/InventoryResultContractTest.java`.
- Modify both `pom.xml` test-resource includes to include `events/**/*.json`; inventory also adds Boot-managed `spring-kafka` and test-scoped Testcontainers `kafka` when Task 3 uses them.

**Interfaces:** Both service-local `EventJson` classes expose `InventoryResult readResult(String key, String value)` and `JsonNode content(String value)`; inventory additionally exposes `OrderPlaced readOrderPlaced(String key, String value)` and `String writeResult(InventoryResult result)`. Protocol exceptions have a stable `String code()` and never embed raw JSON.

```java
public record InventoryResult(UUID eventId, String eventType, int schemaVersion,
        Instant occurredAt, UUID orderId, String correlationId, UUID causationId,
        List<Line> lines, String reasonCode, List<Shortage> shortages) {
    public record Line(String sku, int quantity) {}
    public record Shortage(String sku, int requested, int available) {}
}
public record OrderPlaced(UUID eventId, String eventType, int schemaVersion,
        Instant occurredAt, UUID orderId, String correlationId, List<Line> lines) {
    public record Line(String sku, int quantity) {}
}
```

- [ ] Write fixtures using the existing OrderPlaced example's orderId and eventId as result orderId and causationId. Use a new fixed result UUID, UTC timestamp, original ordered lines, and matching correlationId. Success omits reasonCode/shortages; rejection has INSUFFICIENT_STOCK and a nonempty subset of shortage lines.
- [ ] Add tests that load those fixtures via classpath; implement test helper `fixture(String name)` using `getResourceAsStream` and UTF-8. Pin strict numeric and tombstone behavior:

```java
String valid = fixture("/contracts/events/inventory/v1/inventory-reserved.json");
String key = mapper.readTree(valid).get("orderId").textValue();
assertEquals("InventoryReserved", decoder.readResult(key, valid).eventType());
assertThrows(EventProtocolException.class, () -> decoder.readResult(key, null));
ObjectNode altered = (ObjectNode) mapper.readTree(valid);
((ObjectNode) altered.withArray("lines").get(0)).put("quantity", "1");
assertThrows(EventProtocolException.class,
        () -> decoder.readResult(key, altered.toString()));
```

- [ ] Run `mvn -f inventory-service/pom.xml -Dtest=EventContractTest test` and the order equivalent with `-Dtest=InventoryResultContractTest`; capture the red result.
- [ ] Decode to a JsonNode first. Reject null, non-object, duplicate JSON keys, unknown/missing fields, numeric coercion/overflow, noncanonical UUID key, invalid timestamps/correlation IDs, empty/duplicate SKU lines, and mismatched shortages. Validate nodes before constructing immutable records; preserve line order. Configure a dedicated strict parser, not global HTTP ObjectMapper changes. `content` returns the validated parsed event for inbox equality. UUID/timestamp validation must apply to every relevant field. Schemas mirror the same constraints; cross-field rules live in decoder tests.
- [ ] Add mutations for each required field, unsupported type/version, same key with wrong orderId, negative/zero/decimal quantities, shortage duplicates, shortage SKU absent from lines, success with rejection fields, rejection without shortages. Test serializer round-trip and object-field-order equivalence. Keep nullable success-only Java fields out of serialized success JSON.
- [ ] Run both focused suites green, then both full verifies. Commit only contract/decoder/resource changes: `feat: define inventory result event contracts`.

## Task 2: Atomic Inventory Intake and Result Creation

**Files:**
- Create `inventory-service/src/main/resources/db/migration/V4__inventory_events.sql`.
- Create `I/messaging/InboxStore.java`, `ProcessingOutcome.java`, `InventoryEventHandler.java`.
- Create `I/outbox/InventoryResultStore.java`, `InventoryResultFactory.java`.
- Create `IT/messaging/InventoryEventHandlerIT.java`, `InventoryEventMigrationIT.java`.
- Reuse `I/service/InventoryService.java`, `ReserveInventoryCommand.java`, `ReservationAttemptResult.java`, and `ReservationAttemptStore.java`; modify only if required to expose transaction-safe replay data.

**Interfaces:** `ProcessingOutcome` is enum `APPLIED, DUPLICATE`. `InventoryEventHandler.handle(String key, String value)` returns it and is transactional. `InboxStore.claim(String consumer, UUID eventId, JsonNode content)` returns true for newly inserted identity, false for equal replay, throws EventProtocolException for changed content. `InventoryResultFactory.create(OrderPlaced event, ReservationAttemptResult result)` returns InventoryResult. `InventoryResultStore.insert(InventoryResult result, String payload)` joins the caller transaction.

- [ ] Create real-Postgres tests with `@SpringBootTest`, listeners disabled, and `@DynamicPropertySource`. Inject JdbcTemplate, handler, inventory service, and decoder. Use isolated IDs and reset stock via SQL before each test. Test helper `placed(UUID orderId, UUID eventId, int quantity)` mutates the valid OrderPlaced fixture's IDs and APPLE quantity; `count(String table)` returns `SELECT count(*)` only for hardcoded test table names.

```java
String event = placed(orderId, eventId, 2);
assertEquals(ProcessingOutcome.APPLIED, handler.handle(orderId.toString(), event));
assertEquals(ProcessingOutcome.DUPLICATE, handler.handle(orderId.toString(), event));
assertEquals(1L, count("inventory_event_inbox"));
assertEquals(1L, count("inventory_result_outbox"));
assertEquals(8, inventory.getStock("APPLE").availableQuantity());
```

- [ ] Run `mvn -f inventory-service/pom.xml -Dit.test=InventoryEventHandlerIT,InventoryEventMigrationIT verify`; establish red failure.
- [ ] Add inbox primary key `(consumer_name,event_id)`, JSONB content, non-null processing timestamp; name logical consumer `inventory-order-placed-v1`. Use this atomic claim, then compare stored JSON when zero rows inserted:

```sql
INSERT INTO inventory_event_inbox(consumer_name,event_id,content,processed_at)
VALUES (?, ?, CAST(? AS jsonb), clock_timestamp())
ON CONFLICT (consumer_name,event_id) DO NOTHING;
```

- [ ] Add `inventory_result_outbox` with event_id PK, order_id UNIQUE across both result types, causation_id UNIQUE, event_type restricted to the two result names, schema_version=1, topic commerce.inventory.v1, key=orderId, immutable text payload, created_at and the same delivery/lease columns and partial due index as order V5. Enforce payload identity with explicit `IS TRUE` checks so SQL NULL cannot bypass constraints. order_id references inventory_reservation_attempts(order_id), not reservations, because rejection has no reservation. Do not copy V5's pending-order migration guard.
- [ ] Implement claim -> reserve -> validate RESERVED (not RELEASED) or rejected outcome -> create immutable result -> outbox insert. Map lines without sorting the original event; reserve payload may normalize separately. A conflicting order/causation uniqueness insert throws and rolls back the entire transaction. Retain one result ID across relay retries by generating only inside initial result creation.

```java
@Transactional
public ProcessingOutcome handle(String key, String value) {
    OrderPlaced event = events.readOrderPlaced(key, value);
    if (!inbox.claim("inventory-order-placed-v1", event.eventId(), events.content(value)))
        return ProcessingOutcome.DUPLICATE;
    var command = new ReserveInventoryCommand(event.orderId(), event.lines().stream()
            .map(line -> new ReserveInventoryCommand.Line(line.sku(), line.quantity())).toList());
    var result = factory.create(event, inventory.reserve(command));
    results.insert(result, events.writeResult(result));
    return ProcessingOutcome.APPLIED;
}
```

- [ ] Add rollback test by making the injected result store throw before insert; verify no inbox, attempt, reservation, stock change, or outbox remains. Test rejection, same event ID changed fields, changed event ID same order, matching pre-existing HTTP accepted/rejected attempt, released attempt, reordered lines, and HTTP release/intake contention. Linearization at the existing reservation locks is required; manual release after successful intake remains explicitly unsupported.
- [ ] Use two executor threads with a bounded barrier for identical intake and for distinct orders competing for limited stock; assert one duplicate or one rejection respectively. Do not wrap the tests in a transaction that hides commits from worker threads.
- [ ] Test migration from inventory V3 with existing reserved/rejected/released attempts. Run full inventory verify green, commit `feat: atomically consume orders and record inventory results`.

## Task 3: Durable Inventory Result Relay

**Files:** Create inventory-local counterparts under `I/outbox/` for `OutboxMessage`, `OutboxClaim`, `OutboxStats`, `OutboxProperties`, `OutboxDeliveryStore`, `OutboxRetryPolicy`, `OutboxPublisher`, `OutboxPublishException`, `KafkaOutboxPublisher`, `OutboxPublicationHook`, `OutboxMetrics`, `OutboxRelay`, `OutboxConfiguration`.
Create `IT/outbox/OutboxDeliveryStoreIT.java`, `OutboxRelayTest.java`, `OutboxConfigurationTest.java`, `KafkaInventoryOutboxIT.java`, `OutboxMetricsTest.java`.
Modify `inventory-service/pom.xml` and `src/main/resources/application.yml`.

**Interfaces:** Preserve the proven local method shapes: `claimNext(Duration): Optional<OutboxClaim>`, `markDelivered(UUID, UUID): boolean`, `reschedule(UUID, UUID, Duration, String): boolean`, `stats(): OutboxStats`, `publish(OutboxMessage): void`, `runOnce(): int`, `refresh(): void`. Change owning table to inventory_result_outbox and config prefix to inventory.outbox. No dependency on order-service classes.

- [ ] Write a real database stale-token test: insert a result through Task 2, claim once, force lease expiry via SQL, claim again, and assert the old token cannot mark/reschedule the new claim. Write a publisher-failure relay test asserting retry preserves payload/ID and leaves delivered_at null.

```java
var first = store.claimNext(Duration.ofSeconds(60)).orElseThrow();
jdbc.update("UPDATE inventory_result_outbox SET lease_until=clock_timestamp()-interval '1 second'");
var second = store.claimNext(Duration.ofSeconds(60)).orElseThrow();
assertFalse(store.markDelivered(first.message().eventId(), first.token()));
assertTrue(store.markDelivered(second.message().eventId(), second.token()));
```

- [ ] Run focused tests red with `-Dtest=OutboxRelayTest,OutboxConfigurationTest,OutboxMetricsTest test` and `-Dit.test=OutboxDeliveryStoreIT,KafkaInventoryOutboxIT verify`.
- [ ] Implement table-specific SQL and relay using Phase 4A's local code as reviewed source, maintaining saturated attempts/backoff, SKIP LOCKED, no network transaction, token fencing, interrupt handling, and nonoverlapping passes. Exact defaults: poll 1000ms, 20 claims/pass, lease60000ms, ack12000ms, maxBlock2000ms, delivery10000ms, request3000ms, retry base1000ms/max60000ms plus0..250ms jitter. Validate budgets and test overflow. Producer acks=all, idempotence=true, max.in.flight=1. Disable raw-payload producer error logging. Metrics are cached, never query DB during scrape.
- [ ] Add actual Kafka publication tests for both outcomes, unavailable broker retry then recovery, duplicate send after acknowledged-but-unmarked publication, and an oversized failed record not starving later due work. Assert stored topic/key/payload are reused unchanged and producer resources close cleanly.
- [ ] Test `inventory.outbox.enabled=false` disables scheduling; default true. Keep integration tests explicit about enabled settings. Run full inventory verify; commit `feat: publish durable inventory results with leased relay`.

## Task 4: Atomic Validated Order Completion

**Files:** Create `order-service/src/main/resources/db/migration/V6__inventory_result_inbox.sql`, `O/messaging/InboxStore.java`, `ProcessingOutcome.java`, `OrderResultHandler.java`, `O/persistence/OrderCompletionStore.java`, `OT/messaging/OrderResultHandlerIT.java`, `OrderResultMigrationIT.java`.
Reuse `O/outbox/OrderOutboxStore.java` and original stored payload; do not modify historical migrations or generated DTOs.

**Interfaces:** `OrderResultHandler.handle(String key, String value): ProcessingOutcome`; inbox contract identical to Task 2 but service-local. `OrderCompletionStore.complete(InventoryResult result): void` executes all reads, validation, accepted-identity insert, and transition inside the caller transaction. It uses JdbcTemplate only, avoiding mixed stale JPA entities in this command path.

- [ ] Create pending orders through existing OrderService (not manual SQL) so request identity and OrderPlaced exist. Test helper `resultFor(UUID orderId, String type)` reads the stored event, uses a fixed/test-generated resultId, copies causation/correlation/lines, and adds valid shortages for rejection. Tests call handler, then GET or repository read in a fresh transaction.

```java
String result = resultFor(orderId, "InventoryReserved");
assertEquals(ProcessingOutcome.APPLIED, handler.handle(orderId.toString(), result));
assertEquals(ProcessingOutcome.DUPLICATE, handler.handle(orderId.toString(), result));
assertEquals("CONFIRMED", jdbc.queryForObject(
        "SELECT status FROM orders WHERE id=?", String.class, orderId));
```

- [ ] Run `mvn -f order-service/pom.xml -Dit.test=OrderResultHandlerIT,OrderResultMigrationIT verify` red.
- [ ] Create order_event_inbox with the following identity/content columns, followed by the accepted-result table. All constraints are immediate; failed acceptance rolls back both tables.

```sql
CREATE TABLE order_event_inbox (
  consumer_name TEXT NOT NULL,
  event_id UUID NOT NULL,
  content JSONB NOT NULL CHECK (jsonb_typeof(content)='object'),
  processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (consumer_name,event_id)
);
CREATE TABLE order_inventory_results (
  order_id UUID PRIMARY KEY REFERENCES orders(id),
  causation_id UUID NOT NULL UNIQUE REFERENCES order_outbox(event_id),
  result_event_id UUID NOT NULL UNIQUE,
  consumer_name TEXT NOT NULL DEFAULT 'order-inventory-result-v1'
    CHECK (consumer_name='order-inventory-result-v1'),
  result_type TEXT NOT NULL
    CHECK (result_type IN ('InventoryReserved','InventoryRejected')),
  FOREIGN KEY (consumer_name,result_event_id)
    REFERENCES order_event_inbox(consumer_name,event_id)
);
```
- [ ] Claim inbox -> lock order (`SELECT ... FOR UPDATE`) -> load original OrderPlaced -> compare orderId, eventId/causationId, correlation, ordered lines -> require pending -> insert accepted identity -> update checked status. Unknown order/intent, already-terminal new result identity, changed result content, or invalid original correlation fails and rolls back. Identical inbox replay returns before terminal checks. Increment orders.version because the table is also JPA-versioned.

```sql
UPDATE orders SET status=?, rejection_reason=?, version=version+1,
  next_attempt_at=NULL, recovery_blocked=FALSE, last_failure_code=NULL
WHERE id=? AND status='PENDING_INVENTORY';
```

- [ ] Require one updated row; derive rejection_reason STOCK_UNAVAILABLE from event reasonCode INSUFFICIENT_STOCK to preserve existing HTTP behavior. Do not call the old permissive finalizeInventory method without the explicit guards. Keep failure messages bounded and metadata-only.
- [ ] Test confirmation/rejection, original mismatch for every identity field, changed lines/order, nonexistent and historical orders, identical reordered JSON object fields, different-result-ID duplicates, conflicting concurrent outcomes, and transactional rollback after inbox insertion. Add GET and matching POST-replay assertions for both terminal states. Migration test upgrades V5 with pending orders intact.
- [ ] Run full order verify; commit `feat: complete orders from validated inventory results`.

## Task 5: Kafka Listeners With Partition-Safe Failure Handling

**Files:** Create `I/messaging/OrderPlacedListener.java`, `O/messaging/InventoryResultListener.java`; in both messaging packages create `ConsumerConfiguration.java`, `PartitionFailureHandler.java`, `ConsumerMetrics.java`, `ConsumerCommitHook.java` and tests `ConsumerConfigurationTest.java`, `PartitionFailureHandlerTest.java`, `ConsumerOffsetIT.java`. Modify both application.yml files. Add dedicated test resources for listeners disabled by default outside messaging tests.

**Interfaces:** Listener `onRecord(ConsumerRecord<String,String> record): void` calls handler, then `ConsumerCommitHook.afterDatabaseCommit(UUID eventId, ProcessingOutcome outcome): void` outside the DB transaction; default hook no-op. `PartitionFailureHandler` implements Spring Kafka CommonErrorHandler, manages failed offsets and scheduled partition resumes, and exposes metadata-only metrics. Its lifecycle closes scheduled resources and clears revoked-partition state.

**Pinned settings:** stable groups `commerce-inventory-order-placed-v1` and `commerce-order-inventory-result-v1`; enable.auto.commit=false; auto.offset.reset=earliest; StringDeserializer for key/value; max.poll.records=1; max.poll.interval.ms=300000; session.timeout.ms=10000; heartbeat.interval.ms=3000; RECORD ack mode; sync commits=true; concurrency=3; pollTimeout=1000ms. No Kafka transaction manager on containers. Consumer controls: `inventory.events.enabled`, `order.events.enabled` (default true); tests override false unless exercising Kafka. Fix partition count at three for this lab, not a new scaling promise.

- [ ] Write configuration assertions for all pinned settings. Unit-test failure-handler calls with mock Consumer and MessageListenerContainer; test fixture is a ConsumerRecord at topic partition0 offset4. Assert seek to4, pause of only partition0, and `isAckAfterHandle()==false`. Unwrap ListenerExecutionFailedException before classifying.
- [ ] Run the new unit suites red. Implement container-level handler (not an annotation error handler) using `handleOne`, default `seeksAfterHandling=false`, explicit seek back to record.offset, then pausePartition. Return true to finish the callback but override isAckAfterHandle=false. max.poll.records=1 is essential: no later buffered record can advance the same partition. Never call commitSync/ack in this error path. Do not install DefaultErrorHandler's default recoverer.

```java
TopicPartition tp = new TopicPartition(record.topic(), record.partition());
consumer.seek(tp, record.offset());
container.pausePartition(tp);
// Retryable failures schedule resumePartition(tp); protocol failures stay paused.
return true;
```

- [ ] Classify protocol exceptions as permanently blocked; transient data-access/resource failures retry with nonblocking partition resume at1,2,4,...30 seconds, capped without overflow. Unexpected exceptions remain blocked for operator investigation, not swallowed. A scheduled task may call container.resumePartition, never a KafkaConsumer API. Track assignment generation so revoked/stale scheduled work cannot resume a newly blocked partition. Clear retry state on successful processing and revocation. Keep polling active while paused; handleOtherException reports infrastructure failure without discarding input.
- [ ] Metrics: consumer outcomes (applied/duplicate/retry/blocked) counters plus currently assigned blocked-partition gauge, bounded labels consumer/topic/partition/code only. Logs include validated IDs or partition/offset for undecodable records, never message bodies. Restore/clear MDC on every exit. Tests inspect logs containing deliberately sensitive malformed input.
- [ ] Real Kafka tests publish poison at p0 then a valid p0 follower, plus valid p1 input. Assert p1 completes; p0 follower does not; AdminClient committed offset for p0 is absent or <= poison offset. Restart/rebalance while blocked and repeat. Test transient DB failure then recovery, tombstone, malformed UTF/JSON values, delayed offset commit, and broker reconnect. Verify assignment health beyond session timeout. Read offsets directly, not only application state; no test passes merely because an error was logged.
- [ ] Test listener hook observes no active DB transaction and sees committed inbox/state before listener returns. Run both full verifies and commit `feat: consume workflow events without skipping failed records`.

## Task 6: End-to-End and Real Consumer-Crash Proofs

**Files:** Modify `docker-compose.yml`, `scripts/fixtures/assert-compose.py`, existing proof overrides that require pending/no-inventory behavior. Create `scripts/verify-async-completion.sh`, `scripts/fixtures/async-completion.compose.yml`, `async-completion-proof.py`, `test-async-completion-proof.py`; add `make verify-async-completion`. Create `I/messaging/proof/ConsumerProofConfiguration.java` and order counterpart, and both `ConsumerProofConfigurationTest.java` files.

**Interfaces:** script follows existing verify-compose-common.sh lifecycle/deadlines/labels. Proof hook activated only by profile `consumer-proof` AND `consumer.proof.enabled=true`, canonical selected event UUID `consumer.proof.event-id`. No public HTTP control endpoint. Emits flushed `DB_COMMIT_BOUNDARY eventId=... outcome=...` then interruptibly waits outside DB transaction. Normal restart removes all proof settings.

- [ ] Write Python unit tests for marker identity parsing, broker offset parsing, duplicate detection, nested deadline exhaustion, SIGKILL exit inspection, and normal-config verification. Use unittest and subprocess mocks. Run `python3 -B scripts/fixtures/test-async-completion-proof.py` red before implementation.
- [ ] Add inventory broker configuration/topic init for commerce.inventory.v1 (three partitions/RF1/delete seven days). Preserve broker service-network-only access; each app depends only on its own DB. Add environment toggles for each events consumer and inventory outbox to support isolated historical proofs. Explicitly disable both consumers in Phase 4A proof so its pending/untouched-inventory claim stays valid; document why. Update restart proof to wait for terminal state only when it intentionally enables the full workflow, otherwise disable consumers explicitly.
- [ ] Implement the harness with unique Compose project labels, generated order/event IDs, bounded Docker and Kafka CLI commands, and hard aggregate deadlines. Reuse existing helper APIs rather than shelling out with unbounded subprocess.run. Verify success path APPLE stock10 ->8 and confirmed order; rejection path quantity greater than available -> rejected order with unchanged stock. Publish duplicates and compare exact DB counts.
- [ ] Inventory crash case: create order with inventory consumer disabled; query OrderPlaced eventId, enable selected proof hook, wait for marker. Observe committed inventory inbox/reservation/result outbox and source group offset not beyond selected input. SIGKILL actual container PID1, assert exit137, restart normal, observe duplicate handling and eventual terminal order with one reservation/stock effect/result row.
- [ ] Order crash case: hold order result consumer disabled, create an order and wait for result outbox. Select result eventId, enable order proof hook, observe terminal DB status/inbox plus uncommitted source offset, kill/restart, prove duplicate handling and stable terminal version/accepted identity. Ensure redelivery evidence includes group offset advance after restart, not merely existing DB state.
- [ ] Broker outage case: stop broker after inventory DB commit while its publisher is disabled; enable publisher and verify durable pending row, restart broker and verify final completion. Fresh-group case starts consumers after publishing retained Phase 4A events; confirms earliest intake without manufactured events. Keep retention-gap warning as a documented limitation, not a fabricated recovery test.
- [ ] Run `make verify-async-completion`, then `VERIFY_FAIL_AFTER_START=1 bash scripts/verify-async-completion.sh` expecting97. Assert zero owned containers/networks/volumes on both exits. Fail if selected proof environment survives normal restart. Use test IDs and metadata only in logs.
- [ ] Run proof Python suites, both Maven verifies, and existing verify-restart, verify-inventory-image, verify-outbox-recovery. Commit `test: prove asynchronous completion across consumer crashes`.

## Task 7: CI, Runbook, and Evidence Closeout

**Files:** Modify `.github/workflows/ci.yml`, `order-service/src/test/java/com/commercelab/order/CiStructureTest.java`, `README.md`, `contracts/README.md`, `docs/notes-verification.md`; create `docs/adr/0009-idempotent-event-consumers.md`. Update this plan's checkboxes and approved spec status only to reflect actual evidence. Relevant existing vault notes may be updated separately without committing unrelated vault edits.

Execution scope addition: the final image matrix exposed an absent offset-row
observation during Kafka rebalance. Fix `scripts/fixtures/async-completion-proof.py`
and cover it in `scripts/fixtures/test-async-completion-proof.py`; report/review
scope includes both. RED 12 tests/3 assertion failures, GREEN 12 tests. Missing
rows wait within the existing deadline and cannot count as an uncommitted offset;
malformed/ambiguous rows still fail. No product runtime changes. Preserve failed
run `task7-async-image.log`; fresh full matrix uses `task7-final-*` logs.

- [x] Add a failing CiStructureTest asserting new Python proof suite, real image command, and deliberate-failure cleanup membership. Run `mvn -f order-service/pom.xml -Dtest=CiStructureTest test` red (5 tests, 2 expected failures; task7-ci-red.log).
- [x] Add CI steps after service verifies and old image proofs:

```yaml
- name: Verify async proof boundaries
  run: python3 -B scripts/fixtures/test-async-completion-proof.py
- name: Verify asynchronous completion and consumer crash recovery
  run: make verify-async-completion
```

- [x] Include verify-async-completion in the existing failure-cleanup loop; do not remove startup-agent or old proof gates. Run CiStructureTest green (5 tests; task7-ci-green.log).
- [x] Write runbook with topic/group names, toggles, event schemas, transition table, DB/offset inspection commands, blocked-partition diagnosis, and restart after correction without offset skipping. Explain that unknown/poison input remains unavailable until later replay tooling. Describe clean deployment/topic initialization before listener enablement, retained backlog consumption, and inability to recover deleted delivered events automatically. Existing HTTP release is unsupported on async-owned reservations. No automatic downgrade promise.
- [x] Record rejected alternatives, transaction/offset boundary, retention/dedup lifetime, fixed topology, and limits in ADR0009. Update scoreboard specifically for this slice; leave cancellation/compensation/DLQ checklist entries incomplete. Distinguish local results from hosted CI and list exact commands/environments and stable IDs/offsets for each crash proof.
- [x] Final local verification: `make verify` (1005 tests); both Python suites (15+12); all four image proofs and all four exit97 failure-cleanup scenarios. Inspect `git diff --check`, generated artifacts, and changed file scope. Final 231-file executable manifests match; exact commands/logs/IDs/offsets and the retained failed attempt are in the scoreboard and scratch/task-7-report.md.
- [ ] Whole-branch review: resolve all actionable findings and rerun affected tests before integration. Explicitly pending, not replaced by Task 7 author self-check.
- [x] Commit `docs: record async consumer guarantees and verification`. No merge/push; separate integration choice required.
- [ ] Optional vault updates: controller-only, separate from this worker; preserve unrelated edits.

## Self-Review and Coverage Map

Contracts/validation -> Task1; inbox identity and atomic inventory -> Task2;
durable result publication -> Task3; guarded completion -> Task4;
offset/error policy, metrics, assignment safety -> Task5;
rollout topology and real crash/recovery evidence -> Task6;
retention limits, cancellation boundary, CI and notes -> Task7.
Review Focus conditions each have explicit tests or, for unrecoverable retention
gaps, an explicit limitation and runbook requirement. No product implementation
is authorized by this document until the user approves the plan.

Source checks: CommonErrorHandler and MessageListenerContainer methods were
checked with javap against the locally resolved spring-kafka-3.2.4.jar.
Spring's [3.2 exception-handling reference](https://docs.spring.io/spring-kafka/reference/3.2/kafka/annotation-error-handling.html)
documents default acknowledgement/recovery behavior; real offset tests in Task5
are the release gate for the proposed custom partition policy, not API presence.

## Execution Handoff

Preserve the user's prior subagent-driven execution preference. Ask for review
of this plan before starting. Then use a fresh implementer and reviewer for each
task, followed by a whole-branch review. If required agent tools are unavailable,
report that limitation and agree on inline execution rather than claiming
independent review occurred.
