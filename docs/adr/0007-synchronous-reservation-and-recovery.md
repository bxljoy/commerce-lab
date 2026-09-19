# ADR-0007: Synchronous Reservation and Recovery

Date: 2026-09-19
Status: Accepted on 2026-09-19; implementation and verification in progress.

## Context

Phase 3A proves bounded stock concurrency and atomic reserve/release behavior inside
inventory's database. Its duplicate-ID conflict does not resolve a lost HTTP response.
Order creation currently persists PLACED without contacting inventory. Phase 3B
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
