# Phase 4A Transactional Outbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Prove atomic order/publication intent and recoverable, duplicate-tolerant publication through a PostgreSQL outbox and Kafka polling relay.

**Architecture:** Order creation writes order, request identity, and an immutable event in one local transaction. The relay claims one entry in a short transaction, publishes outside it, then conditionally records the result using its lease token. Phase 4A orders remain pending; inventory consumers and results belong to 4B.

**Tech Stack:** Java 21, existing Spring Boot 3.3.5/JPA/JdbcTemplate/Flyway, PostgreSQL 16, Boot-managed Spring Kafka 3.2.4 and Kafka clients 3.7.1, Testcontainers 1.20.4, Apache Kafka image `apache/kafka:3.7.1`, Docker Compose.

**Spec:** [Approved design](../specs/2026-09-19-phase-4a-outbox-design.md).

Status: Approved and executed with per-task and whole-branch review on 2026-09-19.
All local tasks complete; hosted CI awaits separately authorized publication.
See the acceptance scoreboard for exact test counts, image evidence and limits.

## Global Constraints

- Topic: `commerce.orders.v1`; eventType: `OrderPlaced`; schemaVersion: integer `1`.
- Kafka key: order UUID string; event ID, timestamp, key, destination, and serialized payload remain unchanged on retries.
- New order HTTP status: `202`; state: `PENDING_INVENTORY`; no reservation or confirmation in 4A.
- Same request key/payload: same order, no second event. Historical terminal replay: `200`, no retrospective event.
- No broker calls inside DB transactions. Claims and result recording use short independent transactions.
- No silent event discard, lifetime retry limit, consumer DLQ, CDC, or cancellation work.
- Migration after V4 rejects every existing `PENDING_INVENTORY` row, including blocked rows. Stop old writers before migration.
- Preserve synchronous baseline `ca525d1a8b4e65fe747d60824fc3c2e517e11074`; never silently downgrade a 4A database to that binary.
- Preserve user changes and the pending Phase 3B documentation closeout. Do not commit the vault or push without a separate instruction.
- Preserve Mockito startup-agent configuration and both independently runnable service builds.
- Use an isolated managed worktree at execution time, based on this reviewed plan commit. Inspect existing worktrees before creating one.
- Every task: record failing test evidence, make it pass, run its gate, review, and commit only scoped files. A task gate is not whole-phase completion.

## Review Focus

1. Existing V4 migration test must still prove V4 behavior, not accidentally invoke the V5 pending-order guard (Task 1).
2. A unique-key race or event insert failure must leave no orphan order/request/event; historical replay must not create an event (Task 2).
3. Lease expiry during a slow send allows duplicates but cannot let the stale worker overwrite the newer claim (Tasks 3 and 4).
4. An oversized or permanently failing event must remain visible without starving unrelated due events or overflowing backoff (Tasks 3 and 4).
5. A test crash hook must be inactive normally, and image restart must preserve DB/broker data while cleanup removes only owned resources (Tasks 5 and 6).

## Configuration Contract

Use these starting values, validated at startup and recorded in tests:

| Setting | Value |
|---|---|
| `order.outbox.enabled` | true; false for nonrelay tests |
| `order.outbox.poll-interval-ms` | 1000 |
| `order.outbox.max-attempts-per-pass` | 20 |
| `order.outbox.lease-ms` | 60000 |
| `order.outbox.ack-wait-ms` | 12000 |
| Retry policy base / maximum constants | 1000 / 60000 ms |
| Retry policy jitter constant | 250 ms, added after capped base delay |
| Kafka `max.block.ms` / `delivery.timeout.ms` | 2000 / 10000 |
| Kafka `request.timeout.ms` / `linger.ms` | 3000 / 0 |
| Kafka `acks` / `enable.idempotence` | all / true |
| Kafka `max.in.flight.requests.per.connection` | 1 |
| Topic partitions / replication factor | 3 / 1 for this single-broker lab |

Keep client retries positive (managed default), with delivery timeout bounding
their window. Require lease > max.block + ack-wait, and ack-wait > delivery timeout.
These settings are normal-operation budgets, not proof of a wall-clock deadline
through arbitrary JVM pauses or custom serialization. Use String serializers.
Topic retention is 7 days, cleanup policy delete; not indefinite consumer access.
Provision the topic explicitly in test/Compose setup, not in HTTP startup.

## File and Interface Map

Paths below are repository-relative. Java production prefix is
`order-service/src/main/java/com/commercelab/order/`; test prefix is
`order-service/src/test/java/com/commercelab/order/`. Prefix notation in tasks
means exactly these directories, not new source roots.

New `outbox/` units under the production prefix:

- `OrderPlacedEvent.java`: envelope record and nested `Line(String sku, int quantity)`.
- `OutboxMessage.java`: `record OutboxMessage(UUID eventId, UUID orderId, String topic, String messageKey, String payload)`.
- `OrderPlacedEventFactory.java`: `OutboxMessage create(Order order, String correlationId)`; uses existing Clock bean and ObjectMapper, validates/normalizes correlation before serialization.
- `OrderOutboxStore.java`: caller-transaction `void insert(OutboxMessage message)`; no independent commit.
- `OutboxClaim.java`: `record OutboxClaim(OutboxMessage message, UUID token, long attemptCount)`.
- `OutboxDeliveryStore.java`: `Optional<OutboxClaim> claimNext(Duration lease)`, `boolean markDelivered(UUID eventId, UUID token)`, `boolean reschedule(UUID eventId, UUID token, Duration delay, String errorCode)`, `OutboxStats stats()`; each method owns a short transaction.
- `OutboxStats.java`: `record OutboxStats(long pendingCount, double oldestPendingAgeSeconds, long failedPendingCount)`.
- `OutboxRetryPolicy.java`: `Duration delay(long attemptCount)`; inject `IntSupplier` for deterministic jitter tests.
- `OutboxPublisher.java`: `void publish(OutboxMessage message)`; returns only after acknowledgement, throws `OutboxPublishException` with `String code()`.
- `KafkaOutboxPublisher.java`: String KafkaTemplate adapter; wraps bounded send/wait failures.
- `OutboxPublishException.java`: stable code plus cause, never payload in message.
- `OutboxRelay.java`: `int runOnce()`; counts claims attempted; never holds a transaction across publisher/hook.
- `OutboxPublicationHook.java`: `void afterAcknowledgement(OutboxMessage message)`; normal no-op.
- `OutboxConfiguration.java`: properties, publisher wiring, scheduler registration only when enabled.
- `OutboxProperties.java`: validated configuration contract above.
- `OutboxMetrics.java`: scheduled DB snapshot for three gauges plus relay success/failure counters; no DB query on each metrics scrape.
- `proof/OutboxProofConfiguration.java`: guarded, opt-in after-ack synchronization for image experiments; no HTTP endpoint.

Modify existing OrderCreationService, OrderService, controller/OpenAPI, migrations,
tests, Compose/scripts/CI, README/scoreboard. Remove obsolete synchronous order
integration units and their exclusive dependencies/tests in Task 2; retain domain
state representation and inventory-service behavior.

## Task 1: Event Contract and Forward Schema

**Files:** Create `contracts/events/orders/v1/order-placed.schema.json` and
`order-placed.json`; create production `outbox/OrderPlacedEvent.java`,
`OutboxMessage.java`, `OrderPlacedEventFactory.java`, `OrderOutboxStore.java`;
create `order-service/src/main/resources/db/migration/V5__order_outbox.sql`;
modify `order-service/pom.xml`, `contracts/README.md`, test `FlywayMigrationIT.java`;
create test `outbox/OrderPlacedEventTest.java`, `outbox/OutboxMigrationIT.java`.

**Interfaces:** Produce event factory and insert operation from the map. The insert
participates in an existing JPA/JDBC transaction, as OrderRequestStore does.

- [x] Write migration tests using the existing isolated-schema pattern. Explicitly
  target V4 in `versionFourSchedulesExistingPendingButNotHistoricalOrders` instead
  of latest. New tests: fresh schema succeeds, terminal rows unchanged, pending
  and blocked pending fail, failed upgrade preserves rows and V4 history.
- [x] Write event tests: UUID/key consistency, fixed Clock timestamp, only approved
  fields, immutable line snapshot/order, integer quantities, missing/invalid
  correlation normalized, no customer/price/request-key fields. Add schema fixture
  tests with Jackson tree assertions for required fields/types/consts; require the
  schema and examples to agree, including deliberate missing-ID/wrong-version/string-quantity mutations.

```java
// In OrderPlacedEventTest, mapper and factory use a fixed Clock.
OutboxMessage message = factory.create(order, "phase4-proof");
JsonNode body = mapper.readTree(message.payload());
assertThat(body.get("eventId").asText()).isEqualTo(message.eventId().toString());
assertThat(message.messageKey()).isEqualTo(order.id().toString());
assertThat(body.has("customerId")).isFalse();
assertThat(body.get("lines").get(0).get("quantity").isIntegralNumber()).isTrue();
```

- [x] Run `mvn -f order-service/pom.xml -Dtest=OrderPlacedEventTest test` and
  `mvn -f order-service/pom.xml -Dtest=OrderPlacedEventTest -Dit.test=OutboxMigrationIT,FlywayMigrationIT verify`.
  Record missing schema/classes or incorrect migration failures before implementation.
- [x] Implement the envelope/factory and migration. Store exact serialized JSON
  as TEXT, with a CHECK that it parses as a JSON object; metadata columns must
  match envelope eventId/orderId/type/version. Use the existing mapper's Java time support.

```sql
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM orders WHERE status = 'PENDING_INVENTORY') THEN
    RAISE EXCEPTION 'Phase 4A requires resolving all Phase 3B pending orders before migration';
  END IF;
END $$;
CREATE TABLE order_outbox (
 event_id UUID PRIMARY KEY,
 order_id UUID NOT NULL REFERENCES orders(id),
 event_type TEXT NOT NULL CHECK (event_type = 'OrderPlaced'),
 schema_version INTEGER NOT NULL CHECK (schema_version = 1),
 topic TEXT NOT NULL CHECK (topic = 'commerce.orders.v1'),
 message_key TEXT NOT NULL CHECK (message_key = order_id::text),
 payload TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 lease_token UUID,
 lease_until TIMESTAMPTZ,
 last_error_code VARCHAR(64),
 delivered_at TIMESTAMPTZ,
 UNIQUE (order_id, event_type),
 CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
 CHECK ((jsonb_typeof(payload::jsonb) = 'object'
   AND payload::jsonb->>'eventId' = event_id::text
   AND payload::jsonb->>'orderId' = order_id::text
   AND payload::jsonb->>'eventType' = event_type
   AND payload::jsonb->'schemaVersion' = to_jsonb(schema_version)) IS TRUE)
);
CREATE INDEX ix_order_outbox_due
 ON order_outbox(next_attempt_at, created_at, event_id)
 WHERE delivered_at IS NULL;
```

- [x] Add contracts/events JSON test-resource include. `OrderOutboxStore.insert`
  uses parameterized JdbcTemplate INSERT and requires an active transaction;
  never reserializes an existing payload. Make insert-time envelope validation explicit.
- [x] Re-run both targeted commands. Gate: tests pass, V1-V4 unchanged, schema and
  example agree. Commit `feat: define order placed event and guarded outbox schema`.

## Task 2: Atomic Creation and Asynchronous API Switch

**Files:** Modify production `service/OrderCreationService.java`, `OrderService.java`,
`api/OrderApiController.java`, `order-service/openapi.yaml`, `order-service/pom.xml`,
`order-service/src/main/resources/application.yml`, test resource properties;
modify test `OrderIdempotencyIT.java`, `OrderPublicContractIT.java`,
`api/OrderApiIT.java`, `api/OrderApiControllerTest.java`, `service/OrderServiceTest.java`;
create test `outbox/OrderOutboxCreationIT.java`. Audit other order tests for new outbox FK cleanup.

**Interfaces:** Consume `create(Order,String)` and `insert(OutboxMessage)` inside
`OrderCreationService.createOrReplay`. Preserve its public signature and existing key-race detection.

- [x] Write real-Postgres tests for new HTTP202, same-ID replay, 409 conflicting
  reuse, concurrent same-key winners, all-three rollback, historical terminal
  replay without events, and zero inventory calls. For rollback, use a test spy
  that executes the real outbox insert then throws; a second case throws before
  insert after request identity exists. Assert in fresh transactions, not cached entities.

```java
// jdbc is an autowired JdbcTemplate; each test clears its owned records first.
assertThat(jdbc.queryForObject("SELECT count(*) FROM orders", Long.class)).isZero();
assertThat(jdbc.queryForObject("SELECT count(*) FROM order_requests", Long.class)).isZero();
assertThat(jdbc.queryForObject("SELECT count(*) FROM order_outbox", Long.class)).isZero();
```

- [x] Run `mvn -f order-service/pom.xml -Dtest=OrderServiceTest,OrderApiControllerTest -Dit.test=OrderOutboxCreationIT,OrderIdempotencyIT verify`; record red assertions.
- [x] Insert the event immediately after request identity in the existing transaction:

```java
Order order = orders.add(Order.place(payload.customerId(), payload.lines(), clock.instant()));
requests.insert(key, order.id(), payload, correlationId);
outbox.insert(eventFactory.create(order, correlationId));
return new OrderCreation(order, true);
// OrderService.placeOrder now returns creation.createOrReplay(...) directly.
```

- [x] Remove the active synchronous path: `service/OrderReservationCoordinator.java`,
  `OrderRecoveryWorker.java`, `OrderRecoveryConfiguration.java`, `OrderProgressService.java`,
  `PendingOrderSnapshot.java`, and the production `inventory/` package. Remove their
  exclusive tests: `OrderReservationIT`, `OrderRecoveryIT`, `OrderProgressIT`,
  `service/OrderRecoveryScheduleTest`, and test `inventory/` package. Preserve
  inventory producer contract tests/fixtures, historical domain states and migrations.
  Remove HTTP client/Resilience4j dependencies and old recovery/inventory config;
  leave unrelated domain/persistence cleanup alone. Recover history at the baseline ref.
- [x] Update POST OpenAPI to pending202/terminal-replay200 with existing errors;
  remove unreachable new-order201 behavior. Preserve Location and correlation
  headers; explain Retry-After as polling guidance, not a completion deadline.
  Move confirmed/rejected contract scenarios to historical fixtures; no active
  test may continue claiming fresh synchronous confirmation.
- [x] Ensure test cleanup deletes outbox rows before orders; normal tests set
  `order.outbox.enabled=false`. Run `make verify-order` and `make verify-inventory`.
  Gate: no inventory caller/worker bean; all new orders pending; rollback/race
  tests pass. Commit `feat: atomically enqueue order events and return pending`.

## Task 3: Lease-Safe Store and Retry Policy

**Files:** Create production `outbox/OutboxClaim.java`, `OutboxStats.java`,
`OutboxDeliveryStore.java`, `OutboxRetryPolicy.java`; create test
`outbox/OutboxDeliveryStoreIT.java`, `OutboxRetryPolicyTest.java`.

**Interfaces:** Produce store/retry signatures from the map. Claim count is the
current attempt's count, incremented in the atomic claim, never stale caller input.

- [x] Write concurrent claim tests with barriers, not sleeps. Test due/not-due,
  delivered exclusion, expired lease reclaim, different live tokens, and success
  AND failure writes rejected after reclaim. Force lease expiration by SQL in
  store tests; label this a fixture rather than a process crash.

```java
OutboxClaim first = store.claimNext(Duration.ofSeconds(60)).orElseThrow();
jdbc.update("UPDATE order_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=?",
    first.message().eventId());
OutboxClaim second = store.claimNext(Duration.ofSeconds(60)).orElseThrow();
assertThat(second.token()).isNotEqualTo(first.token());
assertThat(store.markDelivered(first.message().eventId(), first.token())).isFalse();
assertThat(store.reschedule(first.message().eventId(), first.token(), Duration.ofSeconds(1), "SEND_FAILED")).isFalse();
assertThat(store.markDelivered(second.message().eventId(), second.token())).isTrue();
```

- [x] Run `mvn -f order-service/pom.xml -Dtest=OutboxRetryPolicyTest -Dit.test=OutboxDeliveryStoreIT verify`; capture red.
- [x] Implement a REQUIRES_NEW TransactionTemplate for each store operation with
  database `clock_timestamp()` for eligibility/leases. Use this claim algorithm:

```sql
WITH candidate AS (
 SELECT event_id FROM order_outbox
 WHERE delivered_at IS NULL AND next_attempt_at <= clock_timestamp()
 AND (lease_until IS NULL OR lease_until <= clock_timestamp())
 ORDER BY next_attempt_at, created_at, event_id
 FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE order_outbox o SET lease_token=?,
 lease_until=clock_timestamp()+(? * interval '1 millisecond'),
 attempt_count=CASE WHEN attempt_count < 9223372036854775807
                   THEN attempt_count+1 ELSE attempt_count END
FROM candidate c WHERE o.event_id=c.event_id RETURNING o.*;
-- All completion updates: WHERE event_id=? AND lease_token=? AND delivered_at IS NULL.
-- Success sets delivered_at, clears lease/error; retry sets DB-now+delay,
-- sets stable error code and clears lease. Neither changes payload or identifiers.
```

- [x] Implement capped retry arithmetic before shifting/multiplication; test
  attempts 1,2,6,7, Long.MAX_VALUE with jitter 0 and250. Expected capped bases:
  1000,2000,32000,60000,60000 ms. Reject attempt<1. Test one repeatedly failing
  row is deferred so another due row is claimable. Stats must count expired and
  live claims as undelivered and return zero age on an empty table.
- [x] Re-run targeted gate plus `make verify-order`. Commit `feat: claim outbox work with recoverable leases`.

## Task 4: Bounded Kafka Publisher and Polling Relay

**Files:** Create remaining production outbox units except proof config; create test
`outbox/OutboxRelayTest.java`, `OutboxConfigurationTest.java`, `KafkaOutboxIT.java`;
modify `order-service/pom.xml` and application.yml.

**Interfaces:** Consume delivery store, retry policy, message. Produce publisher,
hook and relay signatures from the map. OutboxPublicationHook is a no-op by default.

- [x] Write unit tests with a fake publisher that asserts
  `TransactionSynchronizationManager.isActualTransactionActive()` is false.
  Test acknowledgement-before-marking, failure/reschedule, exhausted per-pass
  budget, empty queue, interrupt restoration and early stop, and a zero-row stale
  completion reported as stale rather than success. No async completion callback
  may independently update delivery state after a timeout.
- [x] Add Boot-managed `org.springframework.kafka:spring-kafka` and test-scoped
  `org.testcontainers:kafka` (versions in header). Testcontainers fixture uses
  `org.testcontainers.kafka.KafkaContainer("apache/kafka:3.7.1")` and explicit
  AdminClient topic creation. Verify resolved dependency versions with Maven tree.
- [x] Write real Kafka test: create an order with relay scheduling disabled, invoke
  runOnce manually, consume using a unique group/earliest, assert exact stored
  value/key and pending order. Add stopped-broker acceptance followed by restart
  and eventual publication, with bounded polling and the same broker storage.
- [x] Run `mvn -f order-service/pom.xml -Dtest=OutboxRelayTest,OutboxConfigurationTest -Dit.test=KafkaOutboxIT verify`; capture red before wiring.
- [x] Implement relay logic and enforce nonoverlapping calls with an AtomicBoolean
  guard released in finally; do not put @Transactional on the relay:

```text
try enter pass guard; if already running, return 0
repeat at most max-attempts-per-pass:
  claimNext(lease); if empty, break
  publish(message) outside transaction
  afterAcknowledgement(message)
  markDelivered(eventId,token); count true as delivered, false as stale
on publication failure: reschedule using current claim count and stable code
on DB bookkeeping failure: log; leave lease to expire; stop this pass
on interruption: restore interrupt; leave recoverable state; stop pass
finally release guard and clear/restore MDC
```

- [x] KafkaOutboxPublisher uses `KafkaTemplate<String,String>.send(...).get(12000, MILLISECONDS)`
  with finite producer settings from the contract. Map timeout to ACK_UNCERTAIN,
  RecordTooLargeException to RECORD_TOO_LARGE, other send failures to SEND_FAILED.
  Preserve causes internally, not raw payload in logs. A timeout can leave a send
  in flight; duplicates remain permitted. Permanent failure continues capped retries.
- [x] Configuration tests reject nonpositive budgets, invalid lease/wait relations,
  and accidental scheduler activation when disabled. Test a failed large-message
  send and a later healthy message: failed event retained, healthy event delivered.
  Assert no DB transaction during publish/hook, normal hook inert, failedPending
  and age gauges meaningful, event/order/correlation IDs logged without payloads.
- [x] Run full service gates. Commit `feat: publish outbox events with bounded retries`.

## Task 5: Reproducible Broker and Real Crash Experiments

**Files:** Modify `docker-compose.yml`, `scripts/verify-order-restart.sh`,
`scripts/fixtures/runtime-proof.py`, `scripts/fixtures/assert-compose.py`;
create `scripts/verify-outbox-recovery.sh`,
`scripts/fixtures/outbox-recovery.compose.yml`, `scripts/fixtures/outbox-proof.py`;
create production `outbox/proof/OutboxProofConfiguration.java` and test
`outbox/OutboxProofConfigurationTest.java`. Reuse `scripts/verify-compose-common.sh`.

**Interfaces:** Hook `afterAcknowledgement(OutboxMessage)` pauses only the selected
event ID. Harness reads a flushed structured ACK_BOUNDARY log marker; it controls
process termination using Docker, never a public application endpoint.

- [x] Write proof-config tests: no hook activation without BOTH Spring profile
  `outbox-proof` and `order.outbox.proof.enabled=true`; selected event only; normal
  bean remains no-op. Invalid proof configuration fails explicitly. Inject a latch
  for unit tests; image hook waits interruptibly until container kill.
- [x] Extend Compose structural assertions first. Kafka uses KRaft broker/controller,
  node1, internal client listener `kafka:9092`, controller9093, named `kafka-data`
  volume and service-network only; no DB network membership or public listener.
  Explicitly set single-node replication factors/min ISR=1 and persistent log dir.
  Add topic-init one-shot service creating 3-partition RF1 topic with 7-day delete
  retention. Order depends only on its database, never broker health/topic-init.
- [x] Configure bootstrap `kafka:9092`, remove old order inventory/recovery env;
  add finite broker healthcheck. Proof override uses unique Compose project,
  ephemeral loopback application/DB ports, `restart: "no"` for order, and guarded
  proof profile. Bound startup wait to120s and each recovery observation to180s.
- [x] Implement normal/injected proof steps with real HTTP-created orders:

```text
A. Start DB + order with relay disabled and no Kafka. POST ->202.
   Verify one immutable outbox row; kill order; start broker and provision topic.
   Recreate order with relay enabled on same DB; observe event and delivered row.
B. Disable relay, POST a fresh keyed order, obtain eventId from DB.
   Recreate order with relay enabled + hook selected for this eventId.
   Observe ACK_BOUNDARY and first Kafka record; delivered_at must still be null.
   docker compose kill -s SIGKILL order-service
   Recreate order WITHOUT proof config; retain DB and Kafka volumes.
   After lease expiry, observe second record at distinct offset, same ID/key/value.
   Assert delivered_at set; order still pending; no inventory attempt/stock change.
C. Start inventory only for the no-side-effect assertion; compare stock snapshot
   and attempt/reservation row counts before/after A/B, never call reserve in harness.
```

- [x] Consumer observation runs Kafka CLI inside the broker container, prints key,
  partition, offset, and JSON value; parse structured payloads in outbox-proof.py,
  filter by unique event ID. Do not mistake repeated reads of one offset for two
  publications. Start consumers with a fresh group/earliest and bounded timeout.
- [x] Test the marker and first-record observation before killing; otherwise the
  experiment proves an unknown crash location. Restart before first publication
  uses disabled relay rather than claiming an instruction-level HTTP crash.
- [x] Run proof-config tests and shell/Python structural tests red then green;
  run `bash scripts/verify-outbox-recovery.sh` and adapted restart proof.
  Inject `VERIFY_FAIL_AFTER_START=1`, expect exit97 and zero project-owned containers,
  networks and volumes. Preserve logs on failure, never globally prune.
- [x] Gate: two different offsets for one event, pending order, unchanged stock,
  no normal hook activation, success/failure cleanup. Commit
  `test: prove outbox recovery across real application crashes`.

## Task 6: CI, Operational Evidence, and Phase Closeout

**Files:** Modify `Makefile`, `.github/workflows/ci.yml`, `README.md`,
`docs/notes-verification.md`, `contracts/README.md`; create
`docs/adr/0008-transactional-outbox-and-polling-relay.md`; update ADR-0007 status
with a link to its replacement for the active order path, preserving history.

**Interfaces:** Publish `make verify-outbox-recovery` invoking the new script;
no change to independent `make verify-order`/`make verify-inventory` semantics.

- [x] Add a structural test that the CI normal gate and deliberate-failure loop
  include outbox proof, keep restart/inventory proof, and omit active sync proof.
  Remove current sync-specific script/fixture targets only after locating all
  references; README explains checkout at the preserved baseline for those commands.
- [x] Implement the Make/CI changes and document runbook queries:

```sql
SELECT status, count(*) FROM orders GROUP BY status;
SELECT event_id, order_id, attempt_count, next_attempt_at, lease_until,
       last_error_code, delivered_at
FROM order_outbox WHERE delivered_at IS NULL ORDER BY created_at;
```

- [x] Document stop-old-writers upgrade procedure, pending-row guard, no automatic
  downgrade, broker/topic setup, permanent-error diagnosis, no arbitrary payload
  editing/deletion, pending-count/oldest-age metrics, and 4A pending-only API.
  Topic/producer acknowledgement is not consumer success. RF1 is not broker-HA proof.
- [x] Run the final gates from the implementation worktree:

```bash
make verify
make verify-restart
make verify-inventory-image
make verify-outbox-recovery
```

- [x] Run deliberate-failure cleanup for all three image scripts. Check
  `git diff --check`; audit changed config for secret leakage; confirm startup
  agent warning guard intact. Record exact test counts/environment/log paths and
  Kafka offsets/event IDs, not expected or inherited results.
- [x] Perform whole-branch review, resolve findings with regression tests and rerun
  affected gates. Update scoreboard/ADR and relevant vault notes with actual limits;
  vault changes stay separate and require filesystem access if outside worktree.
  Gate hosted CI separately after user-authorized publication. Commit
  `docs: record phase 4a durability evidence and CI gates` only after real evidence.

## Plan Self-Review and Handoff

Coverage: Task1 event/schema/migration; Task2 atomic creation/API/history;
Task3 leases/retries/stats; Task4 Kafka/bounded calls/visibility/outages;
Task5 process crashes/isolation/cleanup; Task6 runbooks/full review/hosted gate.
All five Review Focus conditions have owner tests. Signatures in the interface map
are shared contracts; proposed helper names in an implementer's tests must be
defined in that task, not assumed to exist.

Execution used the approved subagent-driven method, with spec-compliance and
quality review after each task. Whole-branch review and the correlation logging
fix re-review are complete. Runtime poll naming and fixed retry constants above
reflect the implemented configuration; runtime retry tuning would require a
later code/config change. Hosted CI and merge/push are not claimed.

## Sources Checked During Planning

- Existing Boot 3.3.5 dependency BOM locally resolves Spring Kafka3.2.4 / clients3.7.1;
  implementation must verify with Maven dependency tree rather than override blindly.
- [Kafka 3.7 producer configuration](https://kafka.apache.org/37/configuration/producer-configs/)
  covers enqueue/delivery budgets and idempotence prerequisites.
- [Testcontainers Kafka module](https://java.testcontainers.org/modules/kafka/)
  documents the Apache Kafka container; execute the pinned image/container pairing
  as an integration gate, not a claim that it was already run during planning.
