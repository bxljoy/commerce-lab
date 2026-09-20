# ADR-0009: Idempotent event consumers

Date: 2026-09-20
Status: accepted design, locally implemented; whole-branch review and hosted CI pending.

## Context

Phase 4A durably publishes OrderPlaced but leaves orders pending. Phase 4B slice 1
must complete them after inventory acceptance/rejection while surviving duplicate
delivery and a consumer crash after database commit but before offset commit.
PostgreSQL and Kafka do not share an atomic commit boundary.

## Decision

Use independent service-local inbox/outbox implementations and explicit guarded
order transitions, not a shared messaging framework. Inventory's outer application
transaction claims `(consumer_name,event_id)`, validates persisted JSON identity,
joins existing reservation/stock locking, and stores one immutable result outbox
row. Unique order and causation constraints span both reserved and rejected results.
Any failure rolls back the inbox, attempt, stock, reservation and result together.
An existing matching reservation attempt can replay; a released reservation cannot
produce fresh success. Order's transaction claims its inbox, locks the order,
validates the original OrderPlaced and records accepted identity plus terminal
transition/version increment. Identical redelivery changes neither stock nor state.
Different result identities cannot replace an accepted outcome.

Each relay uses database-time leases, SKIP LOCKED claims, fresh lease tokens and
conditional bookkeeping. Network publication occurs outside database transactions.
Broker acknowledgement means publication, not downstream processing. Ambiguous
acknowledgement or a crash before delivered-at recording permits duplicate sends.
The stored key/topic/payload/event ID remain unchanged on every retry.

Consumers use auto-commit=false, RECORD acknowledgement, sync commits and
max.poll.records=1. Database commit precedes listener return/offset commit.
The intentional crash window redelivers; durable inbox equality (parsed JSON,
object-order-insensitive but array/value-sensitive) suppresses repeated effects.
This is not exactly-once delivery or a Kafka/PostgreSQL distributed transaction.

The partition handler seeks failed input and pauses only that partition, with
ack-after-handle=false and no discard recoverer. Transient failures resume with
nonblocking capped backoff; protocol/unknown failures remain blocked. Polling and
heartbeats continue, healthy partitions progress, and assignment ownership/generation
fences stale callbacks across revoke/reassign. Consumer lifecycle retries cold
startup even if broker DNS is absent, preserving independent HTTP/DB availability.
Health alone does not prove consumer progress.

## Topology And Lifetime

OrderPlaced stays on `commerce.orders.v1`; results share `commerce.inventory.v1`.
Both use canonical order UUID keys, three partitions, RF1 and seven-day delete
retention. Stable groups are `commerce-inventory-order-placed-v1` and
`commerce-order-inventory-result-v1`; first assignment starts earliest retained
input and existing groups resume committed offsets. Fixed topology is intentional;
partition growth and broker HA require another design/verification pass.

Inbox rows, reservation attempts, accepted identities and original/result outbox
rows are not purged. Replay protection depends on retaining these records and the
same logical consumer identity. The original order event is needed to validate
results. Kafka retention is independent: earliest reset cannot recover deleted
delivered events. No automatic regeneration/backfill or indefinite recovery is
promised. Deploy migrations/topics before enabling listeners and explicitly report
unresolved retained-backlog gaps; there is no automatic downgrade contract.

## Rejected Alternatives

- Reservation idempotency alone: does not atomically retain a publishable result
  or validate a changed event identity/content.
- Kafka transactions alone: do not atomically commit PostgreSQL stock/state.
- Synchronous inventory calls: restore temporal coupling and uncertain HTTP
  outcomes instead of completing the approved asynchronous path.
- Default skip/recover handlers or offset resets: trade silent loss for apparent
  availability and violate the no-discard requirement.
- Global consumer sleeping/stopping on poison: unnecessarily blocks healthy
  partitions and risks heartbeat/rebalance failures.
- Generic shared messaging library: unnecessary coupling across service-owned
  transactions for this slice; local duplication is an explicit tradeoff.

## Limits And Evidence

Unrepairable poison and its partition followers remain unavailable until later
DLQ/replay work. Metadata-only logs and bounded metrics expose outcomes and blocked
partitions; no raw input logging, full tracing or dashboards are claimed.
Cancellation/compensation and late-success recovery are not implemented. Existing
HTTP release is unsupported on async-owned reservations. Historical terminal
orders are not retroactively enrolled. No mixed-version rollout, broker HA,
host/disk-loss recovery or general corruption repair guarantee is made.

Real PostgreSQL/Kafka tests cover atomic rollback, duplicate/conflicting claims,
stock contention, guarded transitions and actual offset/healthy-partition behavior.
The image proof kills the actual consumer JVM at both post-DB/pre-offset boundaries,
then checks DUPLICATE handling, offset advance and unchanged effects after normal
restart without proof configuration. It also covers rejection, retained backlog
and enabled-consumer cold-DNS outage recovery. Harness unit tests are not process
crash evidence. Exact local commands, counts, source hashes, stable IDs/offsets and
cleanup results are in the [scoreboard](../notes-verification.md#phase-4b-workflow-recovery).
Whole-branch review and hosted CI are separate pending gates. No merge/push is authorized.
