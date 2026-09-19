# ADR-0008: Transactional Outbox and Polling Relay

Date: 2026-09-19
Status: Approved design, implemented and locally verified on 2026-09-19.
Whole-branch review and scoped re-review complete. Hosted CI pending. No merge/push authorization.

## Context

ADR-0007's synchronous order path cannot make local persistence and remote
inventory work atomic. Phase 4A replaces that path with recoverable at-least-once
publication, not a completed asynchronous commerce workflow. Phase 3B remains
reproducible at `ca525d1a8b4e65fe747d60824fc3c2e517e11074`; its
[hosted CI succeeded](https://github.com/bxljoy/commerce-lab/actions/runs/35453124768).
Inventory's independent HTTP reserve/release/replay behavior is retained.

## Decision

Persist the order, request-key identity and one serialized OrderPlaced event in
one PostgreSQL transaction. New orders return 202 PENDING_INVENTORY; matching
replay preserves identity without another event. Historical terminal replay stays
200 with no event. Publication never completes an order in 4A.

Poll eligible undelivered rows with database-time leases and SKIP LOCKED. Claim
one immediately before sending, outside the claim transaction. Record delivery
or persisted capped retry in a separate transaction conditional on the claim
token. Keep payload, destination, key, event ID and creation time immutable.
Tokens fence bookkeeping, not broker sends from paused workers.

Use Apache Kafka 3.7.1, managed Spring Kafka 3.2.4 / clients 3.7.1, acks all and
producer idempotence. Finite enqueue/delivery/acknowledgement waits fit inside the
normal lease budget; uncertain acknowledgement retries. A new send after process
restart may publish the same event again, despite producer idempotence. Partition
by order UUID; no cross-order or future multi-event ordering guarantee is made.

Keep delivered rows for inspection. Expose bounded metadata logs and cached backlog
snapshots, including staleness/age; do not query the database on every scrape.
Permanent errors remain visible and retry at the capped rate, never silently drop.
The [runbook](../../README.md#broker-and-backlog-operations) specifies numeric
budgets, topic setup, diagnostics and operator repair boundaries.

## Alternatives

- CDC/Debezium is deferred to a comparison experiment; polling keeps ownership
  and crash boundaries directly inspectable in the current stack.
- In-memory after-commit publication loses intent on process death.
- Publishing inside the order transaction cannot atomically commit PostgreSQL
  and Kafka and holds database resources across network waits.
- A permanent synchronous/async dual path adds duplicate-delivery ambiguity;
  preserve the old experiment in Git instead.

## Migration and Consequences

Stop all old order writers/recovery workers before V5. Resolve all pending work,
including blocked rows, with the baseline and operator review before switching.
The migration fails on any pending row; it does not backfill events or delete
history. Its guard cannot protect against a subsequently restarted old writer.
No mixed-version rolling operation or automatic downgrade is supported once 4A
pending orders exist. Use separate databases for baseline reproduction.

There is one logical event per new order but potentially multiple Kafka records.
Consumers, deduplication, inventory result outboxes, confirmation/rejection,
compensation and DLQs belong to Phase 4B. Topic acknowledgement is not consumer
success. Three partitions/RF1/seven-day retention demonstrate application crash
recovery, not broker HA, disaster tolerance or indefinite consumer availability.
Eventual delivery requires restored dependencies, valid data/configuration, a
running relay and retained durable storage. No arbitrary payload editing/deletion
or automatic corruption repair is provided.

## Evidence and Remaining Gates

The [scoreboard](../notes-verification.md#phase-4a-durable-event-delivery) records
fresh local gate counts, environment, event IDs and offsets. The image experiment
uses HTTP creation and actual SIGKILLs: before publication and after observed
broker acknowledgement but before local delivery recording. It verifies unchanged
payload/key/ID at different offsets, pending orders and no inventory effects.
Restart and independent inventory image proofs remain separate checks. Each image
script also exercises deliberate exit-97 cleanup of owned resources only.

Independent task reviews, whole-branch review and scoped re-review completed.
The final review's valid-colon correlation logging finding was fixed in `9d9b73f`.
Hosted CI remains a separate pending gate; local ARM64 runs do not prove AMD64
execution. Expected negative-test warnings and consumer idle diagnostics
remain visible, not globally suppressed. No merge or push is part of this decision.
