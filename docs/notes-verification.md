# Notes Verification Scoreboard

The point of Commerce Lab is to **prove the interview-prep notes in practice**. This
file maps each phase to the vault notes it verifies, and tracks status. Update the
`Status` column as work lands, and update the source note in the vault with the
real gotchas you hit ("verified in practice").

Legend: ⬜ not started · 🟡 partial / work remaining · ✅ verified for the stated claim

Status applies to the specific claim, not the whole linked note. Configuration
alone does not prove performance, isolation, or recovery behavior. Each phase exit
requires a predicted failure, a reproduction and regression test, an explanation
without reading code, and an exact evidence entry (test/command, result, environment,
limits). Write ADRs for meaningful decisions and short notes for smaller experiments.
See [ADR-0004](adr/0004-evidence-driven-learning-roadmap.md) for the revised scope.

## Phase status

| # | Goal | Status |
|---|------|--------|
| 0 | Rails: order-service skeleton, health, Dockerfile, compose, Makefile | ✅ |
| 1 | One service done right (OpenAPI-first, layered, validation, RFC-7807, unit/slice tests) | ✅ |
| 2 | Reliable Postgres persistence and bounded cleanup | ✅ |
| 3A | Inventory correctness under tested PostgreSQL contention shapes | ✅ |
| 3B | Sync integration, idempotency, uncertain-outcome recovery | ✅ Merged at ca525d1; hosted CI succeeded |
| 4A | Durable event delivery through an outbox | 🟡 Locally verified; final review and hosted CI pending |
| 4B | Workflow recovery, compensation, idempotent consumers, DLQ | ⬜ |
| 5 | Observability — logs/metrics/traces across the system | ⬜ |
| 6 | Frontend slice + E2E; core finish line | ⬜ |
| 7 | Optional capstone (gateway/auth, rate limiter, CQRS, vthreads, deploy) | ⬜ |

## Phase 3A historical evidence

This section records the 2026-09-18 baseline, not current test counts or replay
semantics. Phase 3B below supersedes the duplicate-ID conflict and rejection
rollback claims; historical ADRs remain unchanged.

2026-09-18, the Phase 3A closeout candidate: `make verify` passed on local macOS
26.6.2 (Apple Silicon), Java 21.0.5, Maven 3.9.14, OrbStack, and
`postgres:16-alpine`. Both independently buildable Maven projects completed with 43
Surefire and 35 Failsafe tests: 78 total, 0 failures, 0 errors, 0 skipped.

`make verify-restart` started only the order service/database pair, built the order
image, restarted `order-service`, fetched the same persisted order and line sequence,
and removed its isolated Compose containers, volume, and network. Separately,
`make verify-inventory-image` started only the inventory pair, reserved both demo
SKUs, released the reservation twice, asserted exact restoration to
`SKU-APPLE=10` and `SKU-BANANA=5`, and removed its isolated Compose resources.

Test paths below are under `order-service/src/test/java/com/commercelab/order/`.

| Claim | Evidence | Limit |
|---|---|---|
| Domain arithmetic, currency checks, defensive copying, price precision/range | `domain/OrderDomainTest` | Currency-specific minor-unit rules and concurrency are not claimed |
| Place/get orchestration | `service/OrderServiceTest` | In-memory fake; no Spring transaction behavior |
| Selected HTTP success/error responses | `api/OrderApiControllerTest` | Service mocked; not full OpenAPI conformance |
| API works through Postgres; unsupported precision/overflow rejected; line sequence retained | `api/OrderApiIT` | Selected runtime contract behavior, not exhaustive OpenAPI conformance |
| Migration, schema validation, DB health, OSIV configuration guard | `OrderServiceApplicationIT.openEntityManagerInViewRemainsDisabled` | Bean/property guard, not a phantom-write experiment |
| V1→V2 migration reaches V2 and backfill is deterministic | `FlywayMigrationIT` | Test fixture can recover only a deterministic legacy order, not the unknowable original order |
| Database round trip, explicit fetch, stable line order | `persistence/OrderPersistenceIT` | One aggregate shape; no projection or broad N+1 benchmark |
| Assigned UUID creation uses insert semantics without lookup SELECT | `OrderPersistenceIT.assignedIdInsertUsesPersistWithoutLookupSelect` | Hibernate statistics with batching disabled for this test; update semantics are intentionally a separate future operation |
| Detached lazy collection throws after repository session closes | `OrderPersistenceIT.detachedLazyCollectionThrowsOutsideTransaction` | Demonstrates detachment, not HTTP-scoped OSIV behavior |
| Built image retains an order across service restart | `make verify-restart` | Restarts the service, not Postgres or the host; not a backup/restore or durability benchmark |

Inventory test paths below are under
`inventory-service/src/test/java/com/commercelab/inventory/`.

| Claim | Evidence | Limit |
|---|---|---|
| Duplicate SKUs fail before persistence; domain lines remain immutable and ordered; release is terminal | `domain/InventoryDomainTest` | Pure domain behavior; no Spring transaction or PostgreSQL locking claim |
| Reservation persistence preserves line order/state; stock rows are returned and pessimistically locked in ascending SKU order; a waiter observes committed reservation state | `persistence/InventoryPersistenceIT` | Tested on specific PostgreSQL row-lock interactions; not every deadlock, isolation level, or anomaly |
| Multi-SKU success is atomic; insufficiency rolls back all stock and reservation changes | `service/InventoryReservationIT.reservesMultipleSkusAtomicallyAndPreservesRequestOrder`, `insufficientSecondSkuRollsBackEveryChange` | Two-SKU fixtures and the implemented transaction path; not arbitrary distributed transactions |
| Two distinct orders cannot both reserve the tested final unit | `service/InventoryReservationIT.concurrentOrdersCannotReserveTheLastUnitTwice` | One PostgreSQL last-unit race at default isolation; not a universal no-anomaly or throughput proof |
| Reversed overlapping SKU requests complete under ascending locks | `service/InventoryReservationIT.reversedSkuRequestsCompleteWithoutDeadlock` | One controlled two-request shape; does not prove deadlock freedom for every workload |
| Existing and concurrently duplicated `orderId` requests conflict and roll back stock | `service/InventoryReservationIT.existingOrderIdIsAlwaysAConflictWithoutChangingStockAgain`, `concurrentSameOrderPrimaryKeyViolationBecomesDuplicateConflictAndRollsBackStock` | Duplicate requests return `409`; payload-aware replay remains Phase 3B |
| Sequential and concurrent release restore persisted quantities at most once | `service/InventoryReservationIT.repeatedReleaseRestoresStockOnce`, `concurrentReleaseRestoresStockOnce` | One reservation-row-first implementation and controlled concurrent pair; not a general exactly-once distributed-delivery claim |
| Declared reserve/get/stock/release HTTP outcomes map to stable responses | `api/InventoryApiControllerTest` | MVC slice with mocked service; selected runtime outcomes, not exhaustive OpenAPI conformance |
| Reserve, get, double release, and exact stock restoration work through real beans and PostgreSQL | `api/InventoryApiIT.reserveGetAndDoubleReleaseThroughRealBeansAndPostgres` | One full local workflow; no order-service call, timeout, retry, or lost-response recovery |
| Fresh schema startup, health, and disabled OSIV remain valid | `InventoryServiceApplicationIT` | Startup/configuration guard, not a contention or performance experiment |
| Built inventory image reserves and double-releases seeded SKUs, then cleans up | `make verify-inventory-image` | Local Compose image proof; no host restart, backup/restore, or hosted-runtime claim |

## Phase 2 cleanup: completed 2026-09-07

These changes close the bounded Phase 2 gate. They do not mark every topic in the
linked persistence notes as fully verified.

- [x] Define supported price precision and range, enforce at API/domain boundaries,
  and reject unsupported values before persistence. Test `9.99999`, overflow,
  valid boundaries, and numeric POST/GET consistency. Choose supported currencies
  before imposing a universal two-decimal rule.
- [x] Preserve line order with a stored position and a full sequence assertion
  after a fresh read. Existing rows have no recorded original order; document
  a deterministic backfill policy.
- [x] Measure assigned-UUID insert SQL and make insert/update semantics explicit;
  assert the intended SQL behavior. The mapper creates a fresh entity each save,
  so naive `Persistable.isNew=true` or null-version handling can break updates.
- [x] Rename the detached lazy-loading test accurately and add a guard that fails
  when OSIV is enabled. Use an HTTP-scoped experiment for claims about request lifetime.
- [x] Add a repeatable service restart check with a health wait; record its result
  separately from the existing database round-trip test.
- [x] Add CI Docker image build and restart coverage through `make verify-restart`.
- [x] Correct README status and current evidence claims (2026-09-07).
- [x] Explain the completed fixes and update affected source notes with exact
  evidence, including corrections to earlier overclaims.

Follow-ups outside this bounded gate: narrow generic `IllegalArgumentException`
translation and review dependency compatibility before creating inventory.
Additional batching/propagation experiments stay unverified until observed.

## Phase 3A closeout: completed 2026-09-18

These checks close only the Phase 3A inventory gate. Reservation replay, remote-call
retries, and uncertain-outcome recovery remain Phase 3B work.

- [x] Give inventory its own service, Maven build, OpenAPI contract, PostgreSQL
  database, Flyway schema, named volume, and disjoint Compose network.
- [x] Reject duplicate SKUs before database mutation and preserve caller line order
  independently from ascending lock order.
- [x] Lock stock rows pessimistically in ascending SKU order and prove the bounded
  last-unit and reversed multi-SKU PostgreSQL contention shapes.
- [x] Keep reserve and release all-or-none in short transactions; lock the
  reservation first during release and restore stock at most once under tested
  sequential and concurrent calls.
- [x] Keep one lifetime reservation per `orderId`; return `409 Conflict` for a
  duplicate instead of claiming Phase 3B payload-aware replay.
- [x] Verify 78 aggregate Maven tests, isolated order restart, isolated inventory
  image reserve/double-release, separate networks, and clean teardown.
- [x] Record decisions and limits in ADR-0006, the README, this scoreboard, and the
  named source-note update patch.

## Future acceptance criteria

### Phase 3A: inventory correctness (completed 2026-09-18)

- [x] Inventory owns its database and stock/reservation model; no shared domain jar.
- [x] Concurrent distinct orders competing for the tested last unit cannot oversell.
- [x] Multi-SKU reservations commit all lines or none; rejection leaves stock unchanged.
- [x] Duplicate SKUs, insufficient stock, reservation identity, and release semantics are explicit.
- [x] Ascending pessimistic locks are tested on real Postgres; optimistic and
  conditional-update alternatives remain valid candidates for a separate experiment.

### Phase 3B: synchronous integration

Historical baseline: `ca525d1a8b4e65fe747d60824fc3c2e517e11074`, merged with
[successful hosted CI](https://github.com/bxljoy/commerce-lab/actions/runs/35453124768).
Counts, sync test names and commands in this section describe that baseline,
not the active 4A path. Use the README's separate baseline checkout to reproduce
retired sync scripts and operator procedures. Independent inventory proof remains active.

Binding [design](superpowers/specs/2026-09-19-phase-3b-sync-integration-design.md)
and [ADR-0007](adr/0007-synchronous-reservation-and-recovery.md).

Fresh local `make verify` on 2026-09-19, after the final recovery-rejection fix:
**239 passed, 0 failures/errors/skips**.
Order: 82 Surefire + 73 Failsafe = 155. Inventory: 48 Surefire + 36 Failsafe = 84.
Environment: macOS 26.6.2 / Apple Silicon, Amazon Corretto 21.0.5, Maven 3.9.14,
OrbStack Docker Engine 29.4.0 (linux/arm64), Docker client 29.2.1, Compose 5.1.2,
`postgres:16-alpine`. Built application images use the existing Temurin 21 Dockerfiles
(runtime observed as Java 21.0.12);
the fault proxy is test-only `python:3.13-alpine`. No API/library versions changed
in Task 8. Shared test resources disable scheduling only in tests; image runs use
enabled recovery with fixed delay 5000 ms and batch size 20.

| Exit criterion / claim | Evidence in the fresh suite | Limit |
|---|---|---|
| Matching/concurrent order keys select one identity; changed content conflicts; invalid input consumes no identity | `OrderIdempotencyIT` | Tested races, globally scoped lifetime keys; not exactly-once network delivery |
| Equivalent/reordered inventory payloads have one stock effect; changed payload conflicts; release stays terminal | `InventoryReplayIT`, `InventoryReservationIT` | PostgreSQL READ COMMITTED, bounded race fixtures |
| Rejection replays after availability changes, including unknown SKUs; unexpected DB failure rolls everything back | `InventoryReplayIT.rejectionCommitsSnapshotIncludingUnknownSkuAndReplaysAfterReplenishment`, `failureCompletingLedgerRollsBackClaimReservationAndStockThenAllowsFreshRetry` | Business rejection **commits attempt metadata** while stock/reservations stay unchanged; only unexpected failure rolls back the ledger too |
| HTTP and retry sleep stay outside local DB transactions; an independent DB writer progresses during blocked HTTP | `OrderReservationIT.blockedHttpDoesNotHoldOrderRowOrCallerTransaction`, gateway-entry transaction guards in `OrderReservationIT` / `OrderRecoveryIT` | Selected request/recovery paths, not distributed atomicity |
| Two request attempts maximum; no business retry; circuit open/half-open; transport timeout and bounded pool acquisition | `InventoryGatewayTest`, `OrderReservationIT`, `OrderRecoveryIT`, `OrderRecoveryScheduleTest` | Apache classic `responseTimeout` is socket wait, **not total wall-clock deadline**; no slow-dribble or DNS-bound proof claimed |
| Due ordering, batch size, persisted backoff, context restart and optimistic races cannot regress terminal state | `OrderRecoveryIT`, `OrderProgressIT` | Context tests use controlled gateway outcomes; real images covered separately below |
| Recovery rejects mismatched shortage SKU/quantity as a protocol issue, not a business rejection; valid requested subsets including unknown SKUs remain definitive | `OrderRecoveryIT.invalidRejectedGetStaysPendingAndBlockedWithoutPost`, `requestedShortageSubsetRemainsDefinitive`, `lateRecoveryFailureCannotOverwriteRequestFinalization`, `InventoryGatewayTest` | Real decoder with controlled HTTP responses and PostgreSQL state assertions; malformed/mismatched GET leaves pending, blocked, stable issue, and no recovery POST |
| Producer contracts match consumer-owned fixtures and reject required-field/type mutations | `InventoryConsumerContractIT`, `OrderPublicContractIT`, both `InventoryContractTest` classes | Selected contracts, not complete OpenAPI conformance or regenerated-mock evidence |
| Legacy PLACED excluded; migration backfill agrees with Java canonical content and preserves reservation line order/states | `FlywayMigrationIT`, `InventoryMigrationIT`, `OrderRecoveryIT` | Phase 3A fixtures, not mixed-version rolling deployment |
| Correlation echo/propagation/persistence and MDC cleanup | Both `CorrelationIdFilterTest` classes, `InventoryGatewayTest`, `OrderReservationIT` | Structured local logs; no distributed trace backend claim |

Matching order replay returns the **current** representation: initial 202 may later
replay as 200 CONFIRMED. Identity and business effect are stable, not response bytes.
Pending replay does no request-path HTTP; the scheduled worker owns progress.
Business rejection is final; RELEASED during pending recovery, payload conflict,
unexpected 4xx and malformed/mismatched responses block automatic recovery with a
stable `recoveryIssue`. See the README's operator-reviewed diagnosis/repair procedure;
automatic repair of corruption or manual cross-service interference is not claimed.

Runtime evidence on the same host (all commands exit 0):

The latest `make verify-sync-recovery` ran once after the final fix's full Java
green, rebuilding the changed order image
`sha256:a51a68bd593c39312d762c248b72c4a0a421e45ae1d9f0dd8b478b8cd5be85bd`.
Project `commerce-sync-commerce-proof-x4hi9bcs` verified zero owned containers,
networks and volumes on teardown. The isolated restart/inventory and deliberate
failure-cleanup evidence below remains from the image build preceding this narrow fix;
those unchanged checks were not repeated. The earlier successful sync proof used
response-loss ID `532815c4-22d1-4ec3-b3de-8bdc779937ee` and pre-attempt fixture ID
`d7e2af3e-d042-444a-9aff-b6d9303402e5`; the table records the latest fixed-image IDs.

| Command | Actual observation | Boundary / limit |
|---|---|---|
| `make verify-restart` | Only order + its DB; pending ID `23c791bc-ad48-48a7-8f8b-cefd38d2b259` survives restart; EUR 17.9999 and line sequence unchanged | Inventory deliberately unavailable at loopback port 1; not host/DB restart |
| `make verify-inventory-image` | New reserve 201, matching replay 200, GET reserved 200/rejected 409/missing 404; two releases and released replay restore exact stock 10/5 | Independent inventory image with real PostgreSQL |
| `make verify-sync-recovery` response loss | Exactly **2** real successful responses discarded, upstream **201 then 200**; order `e2f7fb01-8950-46cf-b522-c326baf71b34` pending before/after restart, then same ID CONFIRMED; APPLE/BANANA **10/5 -> 8/4 -> 8/4** | Proxy reads producer success completely before closing downstream socket; recovery GETs return proxy 503 until restored. Restart occurs after remote commit/before local terminal recording, not an instruction-level process-kill hook |
| `make verify-sync-recovery` pre-attempt state | Controlled pending ID `22e761cf-6af3-4efe-a776-f8d3f5f6b55d`, attempt_count 0, inventory GET 404; startup recovery GET404 + POST201, same ID CONFIRMED; stock **8/4 -> 7/4** | **DB fixture inserted while order is stopped**, not an HTTP crash injection before the first attempt |
| `make verify-sync-recovery` deployment/normal path | Live app-to-app DNS/HTTP works; DBs absent from shared network; new orders return 201 CONFIRMED/REJECTED and replay 200 | Separate terminal smoke after fault checkpoints; confirmation changes stock to 7/3, rejection/replays leave it unchanged |

The scripts report unique project names and verify zero owned containers (including
stopped), networks and volumes after teardown. Deliberate `VERIFY_FAIL_AFTER_START=1`
runs for all three scripts must return 97; a cleanup error instead returns failure.
Initial harness failures also exercised cleanup: chunked client bodies required
explicit proxy decoding, and ephemeral host ports required re-discovery after restart.
Both were fixed and the image proofs rerun. Two additional stdlib proxy framing
tests pass, separate from the 239 Maven tests. Cleanup cannot be guaranteed after
SIGKILL, host loss or an unavailable Docker daemon; no global prune is used.

Phase 3B whole-branch review and scoped re-review completed before its merge:
the recovered-shortage validation finding was reproduced, fixed, and verified on
that Java tree and rebuilt image. Its hosted run succeeded as linked above.
This history is not a review or hosted-CI claim for Phase 4A. Vault updates are
owned by the controller and are not part of Task 6's worktree changes.

### Phase 4A: durable event delivery

Implementation and local image evidence recorded on 2026-09-19.
**Final whole-branch review and hosted CI pending.** This is implementer
self-review, not independent review or phase-exit/merge authorization. Controller
owns task review, final review, vault updates and final evidence wording.

- [x] Order, request identity and outbox entry commit or roll back together.
- [x] Restart after HTTP commit but before publication recovers the original event.
- [x] Actual process kill after acknowledgement/before recording demonstrates
  duplicate publication with stable ID/key/payload at different offsets.
- [x] Event schema, partition key, ordering limits, retries and upgrade runbook documented.
- [x] Make/CI includes outbox, restart, independent inventory and all three cleanup failures.
- [ ] Final whole-branch review after controller task review.
- [ ] Hosted Phase 4A CI after separately authorized publication.

Fresh `make verify`: **234 passed, 0 failures/errors/skips**:
order 92 Surefire + 58 Failsafe = 150; inventory 48 + 36 = 84.
Final run followed clean targets after deleting unused sync-only 201 contract
examples: order finished 22:17:11+02:00 (01:04 min), inventory 22:17:25+02:00
(13.778 s). Historical terminal replay fixtures remain. Those removed examples
are outside both image build contexts; all 140 frozen source/proof entries matched
after the six image runs and again after the final service verification.
Four new `CiStructureTest` cases were observed red (4 failures, no errors/skips)
before Make/CI changes, then green. Fifteen Python proof/parser/deadline tests pass
separately, not included in the Maven count. Bash syntax checks pass.

Environment: macOS 26.6.2 aarch64, Corretto 21.0.5, Maven 3.9.14,
OrbStack Docker Engine 29.4.0/API 1.54 linux/arm64, client 29.2.1 and Compose 5.1.2.
Real fixtures: `postgres:16-alpine`, `apache/kafka:3.7.1`; Kafka digest
`sha256:ed74d7d115968d5e8b00ba6822ac6a384cbaaf54ca38991828647000d7089b68`.
Managed Spring Kafka 3.2.4 / clients 3.7.1; Testcontainers 1.20.4.
No AMD64 execution or broker HA claim.

| Claim | Current executable evidence | Limit |
|---|---|---|
| Atomic creation, conflict rollback and one logical event | `OrderOutboxCreationIT`, `OrderIdempotencyIT` | PostgreSQL races and injected rollback boundaries, not distributed atomicity |
| Fresh/terminal migration, pending/blocked guard, historical replay without event | `OutboxMigrationIT`, `FlywayMigrationIT`, `OrderPublicContractIT` | Stop old writers; no mixed-version upgrade or automatic downgrade |
| Immutable event, stable key/destination, deliberate invalid examples | `OrderPlacedEventTest`, `KafkaOutboxIT` | Focused assertions, not a general schema validator |
| Claim competition, expired leases, delivered exclusion and stale-token writes | `OutboxDeliveryStoreIT`, `OutboxRelayTest`, `OutboxRetryPolicyTest` | Bookkeeping tokens do not fence late Kafka sends |
| Broker outage acceptance/recovery, oversized failure isolation | `KafkaOutboxIT`, `KafkaOutboxScheduledIT` | Real single broker; publication is not consumer success |
| Cached queue snapshots and staleness, real scheduler path | `OutboxMetricsTest`, `OutboxConfigurationTest`, `KafkaOutboxScheduledIT` | Sampled age; scheduler lag visible through snapshot age |
| CI normal/failure gates and retained startup-agent guard | `CiStructureTest` | Parsed workflow structure and command assertions, not hosted execution |

All image commands below exited 0 on frozen executable files:

| Command / case | Exact current observation |
|---|---|
| `make verify-restart` | Project `commerce-restart-commerce-proof-ocjzte8v`; order `c0f8a7e3-3209-4d64-bbec-c2eaf49d974a`, event `25f8b396-7044-4908-842d-db83908c880e`; same pending ID, one immutable event, attempts 0, total 17.9999 and ordered lines after restart/keyed replay; relay disabled, no broker/inventory |
| `make verify-inventory-image` | Project `commerce-inventory-commerce-proof-slwqmxg3`; reserve201/replay200, GET200/409/404, double release and released replay restore APPLE10/BANANA5; separate inventory API exercise |
| `make verify-outbox-recovery`, A | Project `commerce-outbox-commerce-proof-ksqprz66`; HTTP202 order `a322a771-b048-40ad-8586-f98fcda14308`, event `8418a34a-9f2c-4228-b7d7-d3cb56c812ce`; actual SIGKILL exit137 before publish, recovery partition2 offset0, attempt1 delivered |
| Same experiment, B | HTTP202 order `a38ef8a0-6e35-48a8-a810-11671f99a9b7`, event `66926eb7-07be-42b4-bd66-ecbf9a7acffe`; observed first record partition2 offset1 and selected ack marker while delivered null; SIGKILL exit137, normal restart without proof configuration, partition2 offset2 with identical raw payload/key/event ID, attempt2 delivered |

A delivered at `2026-09-19T20:13:03.581544+00:00`. B's first lease expired at
`2026-09-19T20:14:27.064615+00:00`; delivery recorded at
`2026-09-19T20:14:28.212008+00:00`. Final counts: **2 HTTP-created orders,
2 outbox rows, 2 delivered rows, 3 distinct Kafka publications, 2 pending orders**.
Before/after both crash cases: stock APPLE10/BANANA5, inventory attempts,
reservations and reservation lines all zero; old order recovery attempts stay zero.
Neither case uses SQL fixture writes; A disables publication before killing,
whereas B selects the exact post-acknowledgement hook boundary.

Each `VERIFY_FAIL_AFTER_START=1 bash scripts/<script>.sh` returned **97**:
restart project `commerce-restart-commerce-proof-asv32fbn`, inventory project
`commerce-inventory-commerce-proof-vk0qhoyj`, outbox project
`commerce-outbox-commerce-proof-g4porgag`. All six success/failure projects
verified owned containers=0, networks=0, volumes=0. No global prune. Cleanup
cannot be guaranteed if the harness itself is SIGKILLed, the host is lost, or the
daemon prevents removal; bounded cleanup reports failures instead of inventing success.

Exact local logs are retained under
`/Users/bxl/.codex/worktrees/phase-4a-outbox/commerce-lab/.superpowers/sdd/2026-09-19-phase-4a-outbox/`:
`task6-verify-final.log`, `task6-clean-{order,inventory}.log`,
`task6-verify.log`, `task6-structural-{red,green}.log`, `task6-python.log`,
`task6-{restart,inventory,outbox}-image.log`,
`task6-{restart,inventory,outbox}-failure.log`,
`task6-frozen.sha256`, `task6-frozen-check.log` and `task6-frozen-final.log`.
These ignored local handoff artifacts are not hosted CI artifacts.
The report `task-6-report.md` gives full commands, final reruns and changed paths.

Minor diagnostics remain visible: expected negative-test WARN/ERROR messages
(invalid configuration, guarded migration, injected constraints, broker outage)
and the Kafka console consumer's idle TimeoutException when bounded observation
ends. The consumer exits 0 and record assertions pass. No global logging
suppression was added. The startup-agent warning guard remains active and the
local suite contains no dynamic agent attachment warning.
At-least-once replay is demonstrated, not exactly-once effects, consumer
deduplication, inventory completion, indefinite retention or replicated disaster
tolerance. See [ADR-0008](adr/0008-transactional-outbox-and-polling-relay.md).

### Phase 4B: workflow recovery

- [ ] Dedup record, stock mutation, and inventory result outbox commit atomically.
- [ ] Inventory publication survives restart; order result consumption is idempotent.
- [ ] Crash after consumer DB commit but before acknowledgement; redelivery has one business effect.
- [ ] Demonstrate confirmation, rejection, and one cancel/release compensation path.
- [ ] Delayed/duplicate reservation success after cancellation cannot reconfirm an
  order or leak reserved stock; recovery converges after services restart.
- [ ] Test concurrent transition protection; transport ordering alone does not
  protect stock shared by different orders.
- [ ] Poison messages reach a DLQ after bounded retries; repaired replay is safe.

### Phases 5 and 6: finish the core

- [ ] Phase 5: correlate order/event/reservation logs and traces; define an async
  trace propagation/linking policy and explain one success and one recovered failure.
- [ ] Phase 5: dashboard plus one measurable SLO/alert exposes backlog or stuck work.
- [ ] Phase 6: small UI shows pending, confirmed, rejected, and cancelled outcomes;
  E2E tests cover success and a failure/recovery scenario.
- [ ] Deliver repeatable demo commands, evidence links, and a short tradeoff explanation.

## Note → phase map

> Vault notes referenced as `note-slug` live in
> `Obsidian-notes/tech-decisions/notes/`.

### Phase 1 — One service done right
- ✅ `spring-rest-controller-hygiene-validation-dtos-authz` — `@Valid` DTO validation + RFC-7807 errors. (authz/`@AuthenticationPrincipal` deferred to Phase 7 — no auth yet)
- ✅ `spring-rest-jackson-and-openapi-codegen-pattern` — generation and selected responses demonstrated; codegen does not guarantee full runtime contract conformance
- 🟡 `java-records-sealed-and-pattern-matching` — records + `Money` value object verified; sealed state hierarchy + exhaustive switch deferred to Phase 4 (event-driven transitions)
- ✅ `testing-taxonomy-pyramid-contracts-e2e-and-test-design` — domain/service unit tests + `@WebMvcTest` controller slice (contract tier in Phase 3)

### Phase 2 — Persistence done right
- ✅ `jpa-entity-equals-and-hashcode` — assigned IDs/reference equality implemented; creation explicitly uses `persist`, and statistics prove no lookup SELECT on insert
- 🟡 `osiv-session-vs-transaction-and-phantom-write` — OSIV property and interceptor absence guarded; detached lazy loading demonstrated; phantom-write experiment remains
- 🟡 `jpa-fetching-projections-and-lazy-initialization` — explicit fetch and domain mapping work; projections and broad query-count/N+1 experiments remain
- 🟡 `spring-transactional-propagation-savepoints-and-self-invocation` — Phase 3A proves reserve rollback and transactional release boundaries under default propagation; savepoints, alternate propagation, self-invocation, and timeouts remain unverified
- 🟡 `postgres-write-performance-batching-and-idempotency` — batching configured, not measured; idempotency in 3B/4
- 🟡 `database-isolation-levels-mvcc-and-anomalies` — Phase 3A proves selected PostgreSQL pessimistic-lock shapes (last unit, sorted multi-SKU locks, concurrent release); broader levels and anomalies remain separate experiments
- 🟡 `money-invariant-enforcement-frontend-to-db` — positive four-decimal price range is enforced at API/domain/schema boundaries; frontend and concurrent balance/stock invariants remain

### Phase 3A/3B — Inventory correctness + sync integration
- ⬜ `restclient-http-timeouts-and-connection-pooling`
- ⬜ `circuit-breaker-retry-and-resilience4j`
- ⬜ `testing-taxonomy-pyramid-contracts-e2e-and-test-design` (contract tier)
- ⬜ `request-idempotency-keys-for-write-apis`

### Phase 4A/4B — Durable delivery + workflow recovery
- ⬜ `saga-choreography-orchestration-and-compensation`
- ⬜ `outbox-pattern-and-dual-write-problem`
- ⬜ `at-least-once-to-exactly-once-effect-and-ordered-processing`
- ⬜ `streaming-dedup-and-ordered-emission`
- ⬜ `kafka-producers-spring-boot-and-aws-msk`
- ⬜ `kafka-consumers-spring-boot-and-fargate`
- ⬜ `kafka-exactly-once-transactions-and-schema-evolution` — Kafka transaction guarantees need a separate experiment; outbox/dedup alone do not verify the whole note
- ⬜ `pubsub-topic-subscription-and-dlq-model` (DLQ analog)

### Phase 5 — Observability
- ⬜ `distributed-tracing-and-apm`
- ⬜ `metrics-emission-paths-and-custom-vs-derived`
- ⬜ `monitoring-slos-and-alerting-on-symptoms-vs-causes`
- ⬜ `cache-observability-leading-indicators-and-silent-staleness` — optional cache experiment; not verified by the core dashboard

### Phase 6 — Frontend + E2E
- ⬜ `frontend-review-drills-and-trust-pass`
- ⬜ `react-rendering-and-hooks-internals`
- ⬜ `money-invariant-enforcement-frontend-to-db`

### Phase 7 — Optional capstone
- ⬜ `oauth2-oidc-and-jwt-validation`
- ⬜ `api-rate-limiter-design-token-bucket-redis`
- ⬜ `cqrs-architecture-write-read-split-and-projections`
- ⬜ `platform-vs-virtual-threads-scheduling-internals`
- ⬜ `deploy-strategies-blue-green-canary-rolling-and-rollback-policy`

## `playground/` — pure-language notes (no infra)
Schedule one small exercise per week of active lab work, separately from service
milestones. Create the module with the first exercise. Record experimental limits:
an ordinary green JUnit run does not prove the absence of all races.
- ⬜ `java-memory-model-visibility-and-atomicity`
- ⬜ `java-generics-type-erasure-variance-and-wildcards`
- ⬜ `java-collectors-tomap-and-thread-safety`
- ⬜ `concurrenthashmap-internals-and-cache-stampede`
- ⬜ `lru-cache-linkedhashmap-and-hand-rolled-doubly-linked-list`

Shipping, additional services, and Phase 7 are optional; pick an extension only
when it answers a specific learning question. Local Kafka-compatible broker tests
do not verify AWS MSK/Fargate deployment or Pub/Sub-specific behavior.
