# Phase 4B, Slice 1: Async Order Completion

Status: written spec approved by user; implementation plan awaiting review.
Date: 2026-09-20
Baseline: main at c411c8d. Phase 4A hosted CI passed in run 35494095946.

## Purpose and Scope

Commerce Lab converts technical notes into tested claims. This slice proves that
at-least-once event delivery can complete orders without repeating stock effects,
including when a consumer crashes between database commit and offset commit.
It extends Phase 4A's durable publication, not its delivery guarantees.

New orders still return 202 PENDING_INVENTORY. Inventory consumes OrderPlaced,
reserves or rejects stock, and publishes a durable result. Order consumes that
result and becomes CONFIRMED or REJECTED; clients observe completion through GET.
Existing matching POST replay behavior remains: pending returns 202 and terminal
returns 200. No synchronous inventory call is restored.

Cancellation, compensation, bounded poison-message retries, DLQ tooling, and
repaired replay are later Phase 4B slices. This is not completion of all Phase 4B.
No distributed transaction, broker HA, exactly-once delivery, or indefinite event
retention is claimed. No new UI or general-purpose messaging framework is needed.

## Architecture and Alternatives

Use consumer inboxes and service-local outboxes around existing business logic:

```text
Order DB transaction -> OrderPlaced outbox -> commerce.orders.v1
  -> inventory DB transaction: inbox + reservation attempt/stock + result outbox
  -> commerce.inventory.v1
  -> order DB transaction: inbox + guarded order transition
```

Existing InventoryService.reserve already provides transactional, orderId-based
attempt replay and sorted stock locking. A separate consumer application service
owns the outer transaction and calls that existing service through its Spring
proxy; reservation work must join the same transaction, not REQUIRES_NEW.
Order completion similarly has a transactional application boundary independent
of listener transport code. Offset acknowledgement follows successful commit.

Reservation idempotency alone was rejected because it does not atomically save a
publishable result. Kafka transactions alone were rejected because they do not
make PostgreSQL effects atomic with broker offsets. Inbox/outbox duplication is
intentional service ownership; do not extract a shared framework in this slice.

## Event Contracts

Keep OrderPlaced v1 and its topic unchanged. Add JSON schemas and examples under
contracts/events/inventory/v1 for InventoryReserved and InventoryRejected.
Publish both to commerce.inventory.v1 with the canonical orderId UUID as key.
Use the same local topic topology and retention policy as commerce.orders.v1:
three partitions, replication factor one, seven-day delete retention.

Both result types require:

| Field | Meaning |
| --- | --- |
| eventId | UUID generated once when the result is stored |
| eventType | InventoryReserved or InventoryRejected |
| schemaVersion | Integer 1 |
| occurredAt | UTC timestamp of result creation |
| orderId | Existing order and reservation identity |
| correlationId | Original OrderPlaced correlation identifier |
| causationId | Original OrderPlaced eventId |
| lines | Original ordered SKU/quantity lines |

Preserve OrderPlaced's field constraints for shared fields. Lines must have
distinct SKUs, positive integer quantities, and nonblank SKUs. Rejection adds
reasonCode INSUFFICIENT_STOCK and a nonempty shortages array containing sku,
requested, and available for each unavailable SKU. Unknown SKU is represented
with available zero, matching the existing reservation behavior. Each shortage
must refer to a requested line with matching requested quantity and
0 <= available < requested. Success carries no shortages or rejection reason.
Customer identifiers, prices, HTTP request keys, and exception text are excluded.

Stored payload, key, topic, timestamps, and identifiers are immutable on retry.
Consumers validate the entire envelope and business constraints, including key
agreement, supported version/type, and UUID/timestamp formats. Unknown fields
follow the existing strict v1 schema convention; evolution requires an explicit
contract change. Do not rely on Java deserialization alone as validation.

## Inbox Identity and Atomicity

Each consuming service has a local inbox keyed by logical consumer and eventId.
Store the validated event content, schema version, and processing time so the
same eventId with changed content is rejected, not acknowledged as a duplicate.
Compare parsed JSON content (ignoring object member order, preserving array
order); a fingerprint may accelerate comparison but is not the sole evidence.

Claim identity atomically under a database uniqueness constraint. An existence
check followed by an unguarded insert is insufficient. Concurrent claims must
wait for or observe the winner's transaction. A rollback leaves no completed
inbox record. Do not catch a constraint error and continue inside an aborted
transaction; use a conflict-safe insert or retry in a fresh transaction.

Inventory commits the inbox, reservation attempt, stock changes if accepted,
reservation, and exactly one logical result outbox row in one transaction.
Enforce one initial result per orderId across BOTH result types, as well as unique
eventId and causation identity. A second OrderPlaced identity for the same order
is a protocol conflict, not permission to emit another result.

Identical redelivery observes the committed inbox and performs no new work.
Existing reservation attempts with matching payload may supply their persisted
outcome without repeating stock changes. A released reservation is not a success
eligible for initial publication: reject processing and expose the conflict.
This preserves safety when historical HTTP operations overlap event intake.

Inventory's relay follows Phase 4A's database-time leases, SKIP LOCKED claims,
fresh lease tokens, token-conditional bookkeeping, bounded producer waits, and
durable retry behavior. Network I/O stays outside the database transaction.
Publication may duplicate; delivered means broker acknowledged, not consumed.

## Order Completion Rules

Validate each result against the persisted original OrderPlaced: causationId,
orderId, correlationId, and ordered SKU/quantity content must agree. Retain the
original event for this validation; no outbox purging is introduced here.
Unknown orders, missing original events, and invalid shortages are failures,
not acknowledgable successes. Legacy terminal orders without an OrderPlaced
are not retroactively enrolled in this workflow.

Lock the order or use a checked conditional transition, within the same
transaction as the inbox claim. Result acceptance must also be unique by
original causationId, so a different result eventId cannot silently replace an
already accepted outcome. Preserve the accepted result identity for diagnosis.

| State/event | Behavior |
| --- | --- |
| PENDING_INVENTORY + valid InventoryReserved | CONFIRMED |
| PENDING_INVENTORY + valid InventoryRejected | REJECTED with stable rejection reason |
| Identical, already processed event | No change; successful duplicate handling |
| Same eventId with different content | Failure; preserve current state |
| Different result identity after accepted completion | Protocol conflict; preserve state |
| Terminal state + contradictory/unrelated result | Failure; preserve state |

No transition back to pending exists. RejectionReason is derived from the stable
reasonCode, not arbitrary message text. Stock is owned only by inventory.
Kafka key ordering does not protect stock shared by different orders; retain
the existing database locking discipline and test cross-order contention.

## Failure and Offset Policy

Disable automatic offset commits. Use record processing with commits only after
the application transaction returns successfully. No batch may commit beyond
an unsuccessfully handled record. A crash after database commit and before
offset commit deliberately permits redelivery, handled by inbox identity.

Transient database/processing failures roll back and retry with backoff without
discarding the record. Invalid schemas, identity conflicts, and contradictory
outcomes block the affected partition without acknowledging past the failure.
Other partitions must remain able to progress. Keep consumer polling/heartbeats
healthy while a partition is paused; do not implement blocking sleeps that
silently trigger rebalances or use a default recoverer that skips messages.

For this slice, operator intervention means correcting the underlying cause and
restarting/resuming processing without advancing offsets. An unrepairable poison
record remains blocked until the later DLQ/replay slice. Document this deliberate
availability limitation; do not claim production-ready poison handling.

Record metadata-only failures and expose processing failures, duplicate outcomes,
and partition-blocked state in logs/metrics. Inventory outbox backlog/publication
metrics follow Phase 4A. Do not log raw event bodies or use event/order IDs as
metric labels. Full tracing and dashboards remain Phase 5.

## Cancellation Boundary

No cancellation endpoint or new cancellation state is implemented here. Existing
inventory reserve/release HTTP contracts remain available, but manually releasing
reservations owned by this async workflow is unsupported until compensation is
implemented. State transitions must be explicit so the next slice can guard
cancelled orders and compensate delayed success without introducing a generic
terminal-state overwrite. This slice does not claim late-success-after-cancel
recovery or released-stock reconciliation.

## Rollout and Retention

Add forward migrations for inboxes, inventory outbox, and accepted result
identity. Do not rewrite old migrations or reject valid Phase 4A pending orders.
No automatic historical event regeneration or inventory result backfill occurs.

Use stable, distinct consumer groups for inventory intake and order results.
On first startup, with no committed offsets, start at the earliest retained
records. Existing groups resume committed offsets, not the beginning. Deploy
topic/migration support before enabling listeners; keep services independently
restartable and tolerate broker downtime without losing committed DB work.

Phase 4A pending orders can complete only if their events remain available or
their unpublished outbox entries still deliver. A delivered event already lost
to retention is not recovered by earliest offset reset. The runbook must check
backlog/retention before rollout and report unresolved orders; never quietly
regenerate event identities or mark those orders completed. Inbox and attempt
records are not expired in this slice, preserving replay protection.

## Acceptance Evidence

1. Full HTTP -> Kafka -> inventory -> Kafka -> order tests demonstrate both
   confirmed and rejected outcomes, with exact expected stock changes.
2. Duplicate OrderPlaced and result delivery create one business effect and one
   logical inventory result, including concurrent duplicate processing.
3. Competing orders cannot oversell shared stock; lock ordering remains tested.
4. A forced failure during result creation rolls back inbox, stock, reservation
   attempt, and outbox together. Order inbox and status similarly roll back.
5. Process-crash proofs at each consumer's post-DB-commit/pre-offset-commit
   boundary demonstrate redelivery with no repeated stock or state effects.
   A normal restart must not retain any proof-only pause configuration.
6. Broker outage after inventory commit preserves the result; restart/recovery
   eventually completes the order. Duplicate result publication remains safe.
7. Schema, key, causation, payload, and terminal-state conflicts cannot mutate
   business state or commit past failed input. A healthy partition progresses
   while another is blocked; restart does not silently skip poison input.
8. A matching historical reservation attempt replays safely, a released one
   does not emit fresh success, and a fresh group consumes retained Phase 4A work.
9. Contract fixtures cover both result types and invalid mutations. Existing
   HTTP reserve/release, order replay, and Phase 4A durability tests remain green
   or receive explicit, justified updates for the newly active consumers.

Use real PostgreSQL/Kafka integration tests for transaction and offset claims,
and real application-process crash proofs for restart claims. Bound all proof
subprocesses and waits, and prove cleanup on success and deliberate failure.
Record exact commands, environments, and limits in the verification scoreboard;
keep local evidence distinct from hosted CI results. Update relevant existing
notes only after the claims have evidence, not merely an implementation.

## Next Approval Gate

Review this written spec. Approval permits writing the implementation plan;
it does not yet authorize product-code changes. The plan must define concrete
listener/error-handler settings, schema migrations, tests, proof harnesses, and
delivery slices compatible with the existing pinned dependencies. Review the
plan and select its execution method before implementation begins.
