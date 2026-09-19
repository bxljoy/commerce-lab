# Phase 4A Transactional Outbox Design

Date: 2026-09-19
Status: Approved, implemented, locally verified and independently reviewed on 2026-09-19.
Hosted Phase 4A CI remains pending; no merge or push is claimed.

## Purpose and scope

Turn the transactional-outbox claim into a reproducible experiment: an accepted
order must retain its publication intent across an application restart, while a
crash between broker acknowledgement and local delivery recording can cause a
duplicate. The learning goal is recoverable at-least-once publication, not
exactly-once processing or a completed asynchronous commerce workflow.

Replace synchronous order reservation with atomic order/outbox persistence and a
database-polling Kafka relay. New orders remain PENDING_INVENTORY throughout 4A.
Phase 4B owns inventory consumers, durable result publication, order completion,
consumer deduplication, cancellation/compensation, and consumer DLQs.

The approved Phase 3B baseline is commit
`ca525d1a8b4e65fe747d60824fc3c2e517e11074`. Its reproduction commands are in the
[README](../../../README.md). Preserve this reference; two permanent order paths
are not required.

## Alternatives and decision

- Choose a polling relay: its database state and crash boundaries are directly
  inspectable with the existing Spring/PostgreSQL structure.
- Defer CDC/Debezium: it is a later comparison experiment, not required here.
- Choose a full switch for new order creation instead of temporary dual delivery.
  This makes 4A's pending-only behavior explicit and avoids two reservation paths.

Each service retains its own database and build. Inventory's existing HTTP
reserve/release APIs remain available; order no longer calls them.

## Request flow and atomicity

Within the existing order-creation transaction, persist:

1. The order and ordered lines, with PENDING_INVENTORY status.
2. The required idempotency-key identity and canonical request payload.
3. One immutable OrderPlaced event and its initial outbox delivery metadata.

All three commit or roll back together. Do not publish from that transaction or
rely on an in-memory after-commit callback as the durable queue.

Retain the existing request-key validation, canonical comparison, and unique-key
race handling. A concurrent losing request rolls back its order and event before
reading the winner in a fresh transaction. Database constraints must enforce
event-ID uniqueness and at most one OrderPlaced event for an order.

| Request/outcome | HTTP behavior |
|---|---|
| New valid order | 202, pending representation and Location |
| Matching key/payload, pending order | 202, same order, no additional event |
| Matching key/payload, historical terminal order | 200, existing representation, no event |
| Key reused with different valid payload | Existing 409 problem contract |
| Invalid key or request | Existing 400 problem contract; no persisted identity/event |
| GET order | Existing 200/404 behavior |

Update OpenAPI, examples, controller tests, and runtime scripts to describe these
semantics. Publication alone must never confirm or reject an order. Broker outage
must not prevent accepting orders while their database is available; relay
initialization and broker health must not gate the HTTP creation path.

## Event contract

Publish JSON to topic `commerce.orders.v1`, with the order UUID string as the Kafka
key. Keep a versioned schema and example under the existing `contracts/` tree.

| Field | Contract |
|---|---|
| eventId | UUID, generated once per logical event and stored before commit |
| eventType | Literal OrderPlaced |
| schemaVersion | Integer 1 |
| occurredAt | UTC instant captured during event creation, not publication |
| orderId | UUID of the persisted order; must match the message key |
| correlationId | Validated/generated correlation ID from creation |
| lines | Nonempty ordered snapshot of objects containing sku and quantity |

SKU/quantity validation follows the existing accepted order contract. Exclude
customer details, prices, request idempotency keys, and HTTP headers. OrderPlaced
means accepted and persisted, not inventory reserved.

Persist the complete serialized event at creation; the relay sends that stored
payload unchanged rather than reconstructing it from current order state. Persist
the destination and message key too. Metadata columns and envelope identifiers
must agree. Retries retain the event ID, payload, timestamp, key, and destination.

There is one logical event per new order in 4A, possibly appearing multiple times
in Kafka. No cross-order ordering guarantee is made. Per-order partition keys do
not solve future multi-event ordering or shared-stock concurrency; those need
explicit treatment when later event types and consumers are introduced.

## Outbox and relay boundaries

An outbox entry contains the immutable event plus attempt count, next-attempt
time, lease token, lease expiry, last stable error code, and delivered timestamp.
Retain delivered entries for inspection; retention/purging is outside this phase.
Index the due-work query. Use database time for lease eligibility and expiration
so competing workers do not depend on synchronized application clocks.

Each attempt has three stages:

1. A short transaction atomically claims an eligible undelivered entry, assigns
   a fresh lease token/expiry, and increments the attempt count.
2. Outside any database transaction, send the stored event and wait for broker
   acknowledgement within a configured finite budget.
3. A short transaction conditionally records delivery or schedules a retry using
   the claim's token. An update with a superseded token must change no rows.

Claim selection must skip work held by other live claims, using PostgreSQL row
locking and atomic updates. Claim one entry immediately before sending it, not a
whole batch whose leases may expire while waiting. Each polling pass has a bounded
number of attempts; scheduling passes in one process must not overlap.

Set finite producer metadata/enqueue and delivery waits; choose the lease longer
than the configured normal send/wait budget. A paused process can nevertheless
outlive its lease. Tokens protect database bookkeeping, not the Kafka send: a
late worker can still publish a duplicate. Do not claim broker fencing.

Use acknowledged publication (`acks=all`) and explicit producer idempotence, but
do not treat producer idempotence as deduplication of a later outbox replay. On
uncertain acknowledgement, retain the event and retry rather than mark delivered.

The implementation plan must pin compatible broker/client versions and numeric
poll, lease, send, and backoff settings together, based on the existing build;
these are configuration choices, not permission to weaken these boundaries.

## Failures and visibility

| Failure boundary | Required result |
|---|---|
| Creation transaction fails | No order, request identity, or outbox entry commits |
| Commit succeeds; process stops before publication | Entry remains recoverable |
| Relay dies while holding a claim | Another attempt can claim after lease expiry |
| Kafka unavailable, rejects send, or acknowledgement is uncertain | Entry is not marked delivered; retry remains possible |
| Kafka acknowledges; process dies before delivery recording | Same event can publish again with the same ID |
| Delivery recording commits | Entry is excluded from later claims |
| Stale worker records success/failure after reclaim | Token-conditional update changes nothing |

Persist exponential backoff with jitter and a maximum delay. Use saturating
arithmetic so repeated failures cannot overflow the retry calculation. Cap work
per pass, not the lifetime number of attempts. Do not silently drop or delete
events after repeated errors. Permanent configuration/payload failures remain
visible and retry at the capped rate until an operator repairs the cause.

Log eventId, orderId, correlationId, attempt, outcome, latency, and stable error
code. Do not log full payloads or raw request keys. Expose pending count, oldest
undelivered age, and failure visibility through documented diagnostics. A broker
acknowledgement records publication, not consumer processing or order completion.

Eventual delivery assumes a running relay, restored database/broker availability,
valid configuration and event data, and retained durable storage. A single-broker
lab demonstrates application crash recovery, not replicated broker disaster
tolerance or indefinite consumer access beyond Kafka retention.

## Migration and old data

Add a new forward migration after V4; never rewrite applied migrations. It must
fail clearly if any pre-existing PENDING_INVENTORY order exists, including blocked
orders. Do not generate retrospective events or delete orders to pass the guard.
Historical nonpending orders and request identities remain readable/replayable.

The runbook must require stopping all Phase 3B order writers/recovery workers
before the migration. Resolve pending work with the baseline and operator review
before switching. A migration guard is not protection against an old binary
continuing to write afterward; mixed-version rolling operation is unsupported.

Remove the synchronous coordinator and recovery worker from the active order
flow. Existing recovery columns may remain inert to avoid unrelated destructive
schema changes. There is no automatic downgrade to the old binary once new 4A
pending orders exist: its worker would act on them. Use the preserved baseline
with isolated databases to reproduce Phase 3B.

## Verification and phase exit

Tests are requirements below, not claims of completed verification:

1. PostgreSQL rollback tests inject failures within creation and prove all three
   records commit together or none do.
2. Concurrent matching requests produce one order and one event; conflicting
   payloads fail without orphan orders/events. Replay of historical data emits none.
3. Migration tests cover fresh databases, terminal historical data, and rejection
   of unresolved/blocked pending orders without modifying them.
4. Contract tests validate stored and published JSON, stable payload/identity
   across retries, destination/key consistency, and deliberately invalid examples.
5. Real PostgreSQL tests exercise claim competition, lease expiration, retry
   scheduling, delivered exclusion, and stale-token success/failure updates.
6. Real Kafka tests prove broker outage does not block HTTP acceptance and that
   publication resumes when the broker returns. Assert Kafka records, not only logs.
7. Image tests stop order after a committed HTTP creation but before publication,
   restart it, and observe the original event in Kafka.
8. A test-only synchronization hook pauses the relay after Kafka acknowledgement
   but before delivery recording. The harness observes the first record, kills
   the actual application process, restarts without the hook, and verifies a
   second record at a different offset with identical event ID/key/payload.
9. Verify the order stays pending, no inventory reservation occurs, and no
   synchronous recovery runs for new 4A orders.

Fault hooks must be explicitly test-only, disabled in normal operation, and need
no public control endpoint. Use bounded waits and identifiable event IDs. Separate
fixtures from real process crashes in evidence; do not claim one proves the other.

Add a focused `make verify-outbox-recovery` image experiment and hosted CI gate.
Use isolated Compose projects, ephemeral host ports, and owned-resource cleanup
on success and deliberate failure. Retain PostgreSQL and Kafka data across the
application restart inside an experiment, then remove only its owned resources.
Move sync-specific verification out of the active 4A CI path; its full commands
remain available at the preserved baseline. Adapt restart verification to the
pending/outbox flow; inventory image verification remains relevant.

Before declaring completion, run both service suites and the applicable image
checks, review the branch, and record exact environments/counts/limits in the
[scoreboard](../../notes-verification.md). Update the roadmap and write the next
ADR explaining the replacement of ADR-0007's active synchronous order path.
Hosted CI is a separate gate after an explicitly authorized publication.

## Reference checks

- [Kafka producer configuration](https://kafka.apache.org/38/configuration/producer-configs/):
  acknowledgement, idempotence, enqueue blocking, and delivery timeout controls.
  Recheck against the resolved client version during implementation planning.
- [PostgreSQL SELECT locking](https://www.postgresql.org/docs/14/sql-select.html):
  SKIP LOCKED can support queue-like claim selection, but is not a general
  consistent snapshot and does not bypass table-level locks.

## Next approval gate

The user approved this spec and the implementation plan. Local implementation
and independent review are complete; see the scoreboard for measured evidence
and limits. Merge/push remains a separate user decision, followed by hosted CI.
