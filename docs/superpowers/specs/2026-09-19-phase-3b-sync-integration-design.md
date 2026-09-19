# Phase 3B Synchronous Integration Design

Date: 2026-09-19
Status: Approved, implemented, locally verified and reviewed on 2026-09-19.
Hosted CI remains pending on the unpushed branch; see the acceptance scoreboard for evidence and limits.

## Goal and scope

Connect order creation to inventory reservation over HTTP and demonstrate recovery
from a lost response and an order-service restart. Each service keeps its own
database, domain model, migrations, and independently runnable Maven build.

Phase 3B adds order creation idempotency, inventory outcome replay, short local
transactions, automatic reconciliation, bounded retry, circuit breaking, contract
tests, and correlation logging. Kafka, outbox delivery, cancellation orchestration,
authentication, stock administration, and the frontend remain later work.

## Approved decisions

- A newly created order returns `201` with `CONFIRMED` or `REJECTED` when the
  inventory result is known; an unresolved outcome returns `202` with
  `PENDING_INVENTORY`.
- `Idempotency-Key` is required on order creation.
- Inventory identifies a lifetime reservation attempt by `orderId`. Reordering
  otherwise identical SKU/quantity lines is equivalent. Stored response line order
  remains the original order. A released reservation never reserves stock again.
- A scheduled recovery worker resolves pending orders automatically.
- The request path allows one retry: at most two reserve HTTP attempts. Business
  rejection and other HTTP 4xx responses are not automatically retried.
- Existing `PLACED` orders remain historical and are excluded from recovery.

The detailed policies below make these decisions executable. They are approved
implementation requirements, not claims about verified behavior.

## Approaches considered

1. **Synchronous attempt plus durable pending recovery (chosen).** Normal requests
   receive an immediate business result; durable pending rows bridge network and
   process failures. This adds a reconciler but directly exercises Phase 3B goals.
2. **Always return 202 and let a worker reserve.** Simpler request latency, but hides
   the synchronous success path this phase is intended to teach.
3. **Reserve before saving the order.** Appears simpler but can leave an untracked
   reservation when the order insert fails. Avoid it.

## Order lifecycle and HTTP contract

New orders start in `PENDING_INVENTORY` and transition once to `CONFIRMED` or
`REJECTED`. A definitive stored stock rejection permits `REJECTED`; an observed
`RESERVED` reservation permits `CONFIRMED`. Timeout, 5xx, open circuit, malformed
response, or an unavailable database cannot establish either terminal result.

Retain existing enum values for reading historical data. `PLACED`, `SHIPPED`, and
`CANCELLED` are not created by this phase. Widen the database status column from
16 to 32 characters: `PENDING_INVENTORY` requires 17.

| Operation/outcome | Response |
|---|---|
| New order, confirmed or definitively rejected | 201 and order representation |
| New order, outcome unresolved | 202 and pending order representation |
| Same key/payload, existing terminal order | 200 and current order representation |
| Same key/payload, existing pending order | 202 and current order representation |
| Same key, different valid payload | 409 problem: `idempotency-conflict` |
| Missing/invalid key or invalid payload | 400 problem; no identity persisted |
| GET existing order, including pending/historical | 200 and current representation |
| GET absent order | 404 problem |

POST success responses include `Location: /api/v1/orders/{id}`; pending responses
also include `Retry-After: 5`. GET remains a read and does not trigger recovery.
Order responses add nullable `rejectionReason` and `recoveryIssue` fields.
`rejectionReason=STOCK_UNAVAILABLE` is the Phase 3B business rejection. A blocked
pending order exposes a stable issue code, never exception text or remote bodies.

Replay preserves identity and business effect, not a byte-for-byte initial HTTP
response. In particular, an initial 202 can later replay as 200 CONFIRMED.
Replaying an existing pending order does not initiate another request-path attempt;
the scheduled worker owns continued progress.

## Order identity and payload equality

Accept opaque, case-sensitive keys matching `[A-Za-z0-9._:-]{1,128}`. The lab has
no authenticated caller identity, so keys are globally scoped within order-service.
Do not use an untrusted customer ID as a tenant boundary. Retain keys for the life
of their orders; no expiry or reuse in Phase 3B. Clients generate a fresh key for
each new intent and reuse it only for retries.

Create an `order_requests` table with key primary key, unique order ID foreign key,
canonical request JSONB, and fingerprint version `1`. Store the order and request
record atomically. Use a SHA-256 fingerprint for diagnostics/comparison acceleration,
but compare canonical content as the authority for equality.

Canonical order content contains exact customer ID, validated uppercase currency,
and the ordered list of SKU, quantity, and normalized decimal unit price. Decimal
scale differences such as `9.9900` and `9.99` are equivalent. Do not trim or change
case of identifiers. Reordered order lines are different: their visible order is
part of the order API. Serialize with a structured JSON API and explicit field
ordering, never delimiter concatenation.

Validate the complete command before creating durable identity. Reject duplicate
SKUs at the order boundary with 400, aligning new orders with inventory's existing
one-line-per-SKU contract; historical rows remain readable. Reject null lines,
invalid currencies, unsupported prices, and invalid quantities before persistence.

For simultaneous requests with one key, the unique constraint selects a winner.
Roll back the losing transaction, then read and compare the winner in a fresh
transaction. Recognize only the named request-key constraint as a replay race;
other integrity failures propagate. No query occurs in an aborted transaction.

## Inventory outcome replay

Add an `inventory_reservation_attempts` table keyed by `order_id`, with canonical
SKU/quantity JSONB, outcome (`RESERVED` or `REJECTED`), optional unavailable-SKU
snapshot JSONB, and creation time. A nullable outcome is allowed only transiently
inside the transaction that claims the identity; application code must never commit
an unfinished attempt. Unknown SKUs can appear in rejection JSON without a stock FK.

The ledger is needed because Phase 3A throws on insufficiency and persists no result.
A rejected request must still replay that rejection after stock is replenished or
released by another order. Malformed requests are rejected before ledger creation.

Use `INSERT ... ON CONFLICT (order_id) DO NOTHING` to claim a new attempt inside
the inventory transaction. The competing insert waits for the winner to commit or
roll back. At the existing default READ COMMITTED isolation, a subsequent query
reads the committed winner. Compare canonical content before changing any stock.
After claiming a new attempt, lock stock in ascending SKU order. Atomically commit
either reservation + stock decrement + successful ledger result, or just a rejected
ledger result and its availability snapshot. Return a typed rejection result from
the transaction; mapping to HTTP 409 occurs after commit, without a rollback-causing
exception inside the transaction.

Backfill the ledger for all existing reservations, including RELEASED ones, from
their lines using SKU-sorted JSON arrays. Do not change reservation line positions.
Migration and Java canonicalization must produce equal JSON content.

| Inventory call | Result |
|---|---|
| First reserve, available stock | 201 RESERVED |
| Matching replay after success | 200 with current RESERVED or RELEASED reservation |
| First stock rejection or matching rejected replay | 409 `stock-unavailable`, stored availability snapshot |
| Reused orderId with different SKU/quantity map | 409 `reservation-payload-conflict` |
| GET successful attempt | 200 current reservation |
| GET rejected attempt | 409 `stock-unavailable`, stored availability snapshot |
| GET unknown attempt | 404 `reservation-not-found` |

Release retains the Phase 3A reservation-first, sorted-stock-lock implementation.
The attempt ledger is immutable after commit; release changes reservation state.
Release of an ID with only a rejection returns 404. Replaying a released success
returns RELEASED and never decrements stock. This makes the POST replay a current
resource view while preserving the original decision and one stock effect.

## Transaction boundaries and components

`OrderService` becomes a nontransactional coordinator. Separate Spring beans own
the database transactions, so proxy interception is explicit.

1. `OrderCreationService.createOrReplay` validates identity and atomically persists
   a pending order and request record, or returns the existing order.
2. For a newly created order, `OrderReservationCoordinator.attempt` invokes an
   `InventoryGateway` with immutable domain values and no active DB transaction.
3. `OrderProgressService.apply` loads the current order and applies the permitted
   transition in a new short transaction. Status updates modify a managed entity;
   they must not use the existing creation-only `persist` adapter operation.

Add a non-null `@Version` column to orders, backfilled with zero. Concurrent
recorders reload after an optimistic conflict. If already terminal, return the
stored result; never overwrite a terminal result with stale pending metadata.
`@Version` protects local state writes; it does not suppress duplicate remote calls.
The inventory ledger makes request/worker and worker/worker overlap safe.

If local outcome recording fails after inventory commits, the durable pending order
allows recovery. An order database outage may return a server error; clients retry
with the same key. Never manufacture an order response from uncommitted state.

## Scheduled recovery

Use a fixed-delay scan every 5 seconds and a batch size of 20 by default, configurable
and disableable in tests. Persist `next_attempt_at`, `attempt_count`, last stable
failure code, and `recovery_blocked`. Initial pending work becomes eligible after
5 seconds. Select due, nonblocked PENDING_INVENTORY rows ordered by due time then
ID. Read a bounded set of IDs without fetching paginated to-many collections, close
the transaction, then process immutable snapshots sequentially.

Recovery first GETs the attempt by orderId. RESERVED confirms, stored stock rejection
rejects, and 404 permits an idempotent POST with the original payload. A 404 says no
committed attempt was found at that read; the POST remains necessary to safely race
an in-flight original request. Do not infer rejection from absence.

Each reconciliation pass allows one GET and at most one POST, without nested retry.
On transient failure, schedule a later pass with delays 5, 10, 20, 40, then 60 seconds
(cap), with small nonnegative jitter. Persist attempts/due times across restart.
No finite retry count converts uncertainty to rejection. Stable business rejection
does not retry. Circuit-open passes use the same scheduling policy.

An unexpected RELEASED result while the order is pending, payload conflict,
unrecognized 4xx, or structurally invalid/mismatched successful response leaves the
order pending, records a specific `recoveryIssue`, and blocks automatic retries.
These are operational inconsistencies, not stock rejection. Log them distinctly;
Phase 3B documents diagnosis and an operator-reviewed repair procedure rather than
adding a public recovery endpoint. Crash/transient recovery is automatic; automatic
repair of corrupted state or manual cross-service interference is outside the claim.

## HTTP resilience and correlation

Use RestClient behind the order-owned gateway, with explicit connection acquisition,
connect, and response timeouts. Proposed local defaults: 500 ms pool acquisition,
500 ms connect, 2 seconds response; pool limits 20 total and 10 per route. Verify
the chosen request factory and dependency APIs during implementation, against the
repo's Spring Boot version, before encoding configuration.

Request-path retry is at most two POST attempts, separated by 100 ms. Retry only
transport failures/timeouts and 5xx. No automatic retry on any 4xx. Classify
`stock-unavailable` by its stable problem type; HTTP 409 alone is insufficient.
Verify returned orderId and canonical lines before accepting RESERVED.

Wrap each physical call in a shared inventory circuit breaker. Suggested lab
defaults: count window 10, minimum 5 calls, threshold 50%, open wait 10 seconds,
2 half-open probes. Expected business outcomes do not count as infrastructure
failures. An open circuit prevents HTTP calls and leaves work pending. Tests control
time or breaker state rather than sleeping for production intervals. Avoid stacked
retry mechanisms in the HTTP client, resilience wrapper, and scheduler.

Accept `X-Correlation-ID` matching `[A-Za-z0-9._:-]{1,128}` or generate a UUID when
absent/invalid. Echo and propagate it; store the originating ID for recovery logs.
Log orderId, correlationId, operation, attempt, transition, latency, and stable
failure code. Do not log raw idempotency keys, customer payloads, or full remote
error bodies. Clear logging context in a finally block, including worker threads.

## Deployment and compatibility

Keep Java 21, Spring Boot 3.3.5, OpenAPI Generator 7.10.0, PostgreSQL 16 and the
existing startup agent configuration. Choose/pin compatible new test/resilience
libraries during the relevant implementation task, recording verified versions.

Add a `service-network` shared only by the application containers. Retain each
private database network and attach no database to the shared network. Configure
order's inventory base URL as `http://inventory-service:8081` in Compose and
`http://localhost:8081` for local JVM runs. Order may start while inventory is down.
Independent service verification remains possible; inventory availability is not
a requirement for order liveness.

Deploy inventory replay support first, then order integration. The lab does not
claim mixed-version rolling upgrades: duplicate reserve changes from 409 to 200,
GET can now return a rejected attempt as 409, order POST requires a new header,
and new statuses are added. Update contracts, curl examples, scripts, tests, and
document these compatibility changes together. Keep historical ADRs intact.

## Evidence and exit criteria

- Concurrent matching keys create one order; conflicting payloads produce 409.
- Simultaneous equivalent/reordered inventory requests decrement stock once;
  changed payloads conflict, and release remains terminal.
- A stored stock rejection replays after availability changes, including unknown
  SKU cases; unexpected DB errors roll back the attempt and stock together.
- A forwarding test proxy lets inventory commit and discards its success response;
  retry and/or reconciliation confirms the same order with one stock effect.
- Restart order-service after pending creation and after remote success but before
  local recording; each converges when dependencies recover.
- Capture active-transaction state at every gateway entry and fail if true; an
  independent DB transaction must progress while HTTP is deliberately blocked.
- Race request and worker recorders; optimistic conflicts cannot regress state.
- Prove timeouts, two-attempt maximum, no business retry, circuit opening and
  recovery, and due-time scheduling without unbounded test sleeps.
- Contract checks use consumer-owned expectations against real producer responses;
  an intentional required-field/type mutation must fail. Code generation and
  regenerated mocks alone do not count as this evidence.
- Migrate Phase 3A fixtures: old PLACED orders remain untouched, existing successful
  and RELEASED reservations acquire correct replay behavior, and line order survives.
- Run both Maven suites, isolated smoke checks, a two-service recovery demonstration,
  and hosted CI. Record actual counts/environment and limitations in the scoreboard.

## Related documents

- [ADR-0007](../../adr/0007-synchronous-reservation-and-recovery.md)
- [Implementation plan](../plans/2026-09-19-phase-3b-sync-integration.md)
- [Acceptance scoreboard](../../notes-verification.md)
