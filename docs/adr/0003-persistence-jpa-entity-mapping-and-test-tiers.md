# 3. Persistence — JPA entity mapping, OSIV-off, and integration-test tiers (Phase 2)

Date: 2026-06-08

## Status

Accepted

## Context

Phase 2 makes `order-service` persist to Postgres "done right", replacing the Phase 1
in-memory repository. The order aggregate is modelled as immutable Java records (ADR-0002),
but JPA entities must be mutable with a no-arg constructor — so the central question is how
persistence relates to the domain. Supporting decisions: how to map order lines, entity
identity/`equals`, fetching strategy, transaction boundaries, schema ownership, and how to
run Testcontainers-backed integration tests without making the fast `make test` loop depend
on Docker.

## Decision

- **Separate JPA entities + a mapping adapter** (not domain-as-entity). `Order`/`OrderLine`/
  `Money` stay pure records; `OrderEntity`/`OrderLineEntity` are Hibernate entities;
  `JpaOrderRepository` implements the existing `OrderRepository` port and maps entity↔domain.
  Entities never leave the adapter; the service/controller are unchanged from Phase 1.
- **Order lines as a `@OneToMany` child entity** (own table + FK), `LAZY`, with reads opting
  into an eager join-fetch via `@EntityGraph`. (Chosen over `@ElementCollection` to exercise
  lazy loading, the OSIV-off lazy-init behaviour, and explicit fetch.)
- **UUID `@Id` assigned in the domain** (`Order.place`), no `@GeneratedValue` → non-null and
  stable from construction. **No `equals`/`hashCode` override** (Hibernate L1 identity within
  a session is enough), and **`List` not `Set`** for lines — together avoiding the
  transient-entity HashSet-corruption bug. No Lombok `@Data` on entities.
- **OSIV off** (`spring.jpa.open-in-view: false`); entity→domain mapping happens inside the
  `@Transactional` boundary and the service returns records, so nothing lazy can leak to the
  controller. `@Transactional` lives on the service (write) / `readOnly=true` (read).
- **Flyway owns the schema** (`V1__init.sql`); Hibernate runs `ddl-auto: validate`. Read/write
  batch defaults set (`default_batch_fetch_size`, `jdbc.batch_size`).
- **Compose gains Postgres** with a named volume (orders survive restart) and a healthcheck;
  `order-service` waits for a healthy DB.
- **Test tiers split by Surefire/Failsafe.** `*Test` (unit + `@WebMvcTest` slice) run under
  Surefire with no Docker (`make test`); `*IT` (full `@SpringBootTest` slices on real Postgres
  via Testcontainers) run under Failsafe (`make verify`). The in-memory repository moved to a
  test fixture for the Spring-free service unit test.
- **`-Dapi.version` pinned for the test JVM.** docker-java's API negotiation probes Docker API
  v1.32, which modern daemons (Engine 25+, OrbStack, Colima) reject; pinning a modern version
  skips the failing negotiation. Daemon-socket discovery (e.g. OrbStack) is a local concern via
  `~/.testcontainers.properties`, not committed.

## Consequences

- The Phase 1 promise held: swapping the repository implementation behind `OrderRepository`
  required no change to the service or controller — the port/adapter boundary paid off.
- 21 tests pass: 15 under Surefire (fast, no Docker) + 6 under Failsafe (Testcontainers). An
  order round-trips through Postgres; a lazy-init IT demonstrates the OSIV-off handling; the
  app survives `docker compose restart` (named volume).
- `make test` stays fast and Docker-free; `make verify` (and CI) run the full Docker-backed
  suite. CI now runs `make verify`.
- A small mapping layer is the cost of keeping the domain free of Hibernate; accepted.
- Two Phase-2-listed notes are only partially exercised by CRUD persistence and are deferred:
  `database-isolation-levels-mvcc-and-anomalies` and the idempotency half of
  `postgres-write-performance-batching-and-idempotency` (need concurrent writers — Phase 3/4).

## Notes

Supersedes nothing. Immutable once accepted. The Phase 3 ADR covers the second service and
synchronous cross-service integration (RestClient + resilience4j + contract testing).
