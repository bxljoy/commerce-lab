# ADR-0007: Synchronous Reservation and Recovery

Date: 2026-09-19
Status: Accepted on 2026-09-19 after design review and locally verified implementation.
Whole-branch review and scoped fixes are complete. Hosted CI remains pending; not a merge authorization.

## Context

Phase 3A proves bounded stock concurrency and atomic reserve/release behavior inside
inventory's database. Its duplicate-ID conflict does not resolve a lost HTTP response.
Before Phase 3B, order creation persisted PLACED without contacting inventory. Phase 3B
must bridge the two transactions without pretending an HTTP timeout is rejection.

## Decision

Persist a new PENDING_INVENTORY order and required idempotency key atomically. Make
the inventory call outside database transactions, then record CONFIRMED or REJECTED
in a separate transaction. Return 201 for a newly created final outcome and 202 for
pending work. Matching order retries return the same order's current representation;
different payloads using the same key conflict.

Inventory stores a durable attempt result keyed by orderId, including stock
rejections. Compare SKU/quantity mappings independent of request order. Successful
replays return the current reservation, including terminal RELEASED, without another
stock effect. Rejected replays retain their original availability snapshot.

A scheduled worker queries unresolved attempts and safely resubmits absent ones.
Persist due times and failure information. A request allows one transient retry;
each recovery pass is independently bounded. Circuit breaking reduces calls during
outages. Business rejection is final, while transport uncertainty stays pending.

Protect local transitions with optimistic versioning; duplicate remote calls are
handled by inventory identity. Preserve legacy PLACED rows without reconciliation.
Keep separate databases and private database networks; add an application-only
network for HTTP. See the [design](../superpowers/specs/2026-09-19-phase-3b-sync-integration-design.md)
for exact contracts, defaults, equality rules, and test requirements.

## Alternatives

- Always-202 worker processing avoids synchronous latency but does not exercise the
  chosen immediate-result experience.
- Remote reservation before local persistence risks untracked inventory effects.
- Holding a database transaction across HTTP increases lock/connection lifetime and
  still does not make the two databases atomic.
- Retrying only successful reservations leaves rejected attempts dependent on stock
  changes between retries. Durable rejection avoids that ambiguity.

## Consequences

The design adds an inventory attempt ledger, an order request-key table, order
version/recovery columns, and a reconciler. Rejection recording changes Phase 3A's
"no writes on insufficiency" detail: stock and reservations remain unchanged, but
the rejection decision is committed. Old rollback tests must distinguish these.

Idempotency guarantees stable intent and one business effect, not immutable HTTP
bytes or exactly-once network delivery. Key retention is unbounded for this lab.
Terminal order results assume reservation release is not manually invoked during
the workflow; detecting and repairing arbitrary later cross-service changes belongs
outside Phase 3B. Unexpected release during reconciliation is visible blocked work.

Required headers, added states, replay status codes, and rejected-attempt GET behavior
are deliberate API changes. Migration preserves historical data; current scripts
and contract tests must be updated. No distributed atomicity or universal recovery
claim is made. The decisive evidence is a committed reservation with a lost response
that recovers after restart without decrementing inventory again.

## Verification and limits

On 2026-09-19, `make verify` passed 228 tests (order 144, inventory 84), with zero
failures, errors or skips. `make verify-restart` and `make verify-inventory-image`
exercise each independently runnable image. `make verify-sync-recovery` forwards to
real inventory, reads its successful 201 and replay 200, drops both responses, and
verifies pending order + RESERVED attempt + stock `10/5 -> 8/4`. Order remains pending
through its process restart; restoring traffic confirms the same ID with stock
still `8/4`. The fault proxy/configuration exists only under `scripts/fixtures`.

This is remote commit followed by response loss before local terminal recording,
then a service restart, not an instruction-level kill inside the handler. A second
proof uses a clearly labeled committed-pending DB fixture while order is stopped:
no prior remote attempt (GET 404), then startup recovery GET 404 + POST 201, same ID
CONFIRMED and one stock decrement. It demonstrates recovery from that durable state,
not an HTTP crash at the local-commit boundary. The scheduler uses production defaults
(enabled, fixed delay 5000 ms, batch size 20) in the image proofs.

Apache classic `responseTimeout` bounds socket waiting, not a total wall-clock
deadline. Tests do not prove a slow-dribble or DNS bound. Blocked operational
inconsistencies require diagnosis and operator-reviewed repair as documented in
the README; no automatic corruption repair or public unblocking endpoint is claimed.

Compose joins only applications to `service-network`, preserves private database
networks, and does not make order startup depend on inventory. Proofs use isolated
projects/ephemeral ports and verify owned-resource cleanup, including deliberate
failure paths. Deploy inventory replay support before order integration; no
mixed-version rolling-upgrade guarantee is made. Exact environment and evidence
are in the [scoreboard](../notes-verification.md). Hosted CI is configured but not
yet run on this unpushed branch.
