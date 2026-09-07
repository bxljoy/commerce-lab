# 5. Phase 2 persistence boundaries and verification

Date: 2026-09-07

## Status

Accepted.

## Context

The first Phase 2 implementation persisted orders, but several boundaries were
implicit: the API accepted values Postgres could round or reject, child order was
not stored, Spring Data `save` obscured assigned-ID insert behavior, and evidence
for OSIV and restart persistence was too broad or manual.

## Decision

- Support positive unit prices with at most four fractional digits and a maximum
  of `999999999999999.9999`. Enforce precision/range before persistence in OpenAPI
  and the domain, matching the `numeric(19,4)` database storage capacity.
- Persist each order line's zero-based position. Migration V2 backfills legacy rows
  deterministically by line UUID because their original input order is unknowable,
  then adds non-negative and per-order uniqueness constraints.
- Name the repository creation operation `add` and implement it with
  `EntityManager.persist`. Updates will use a separate operation with explicit
  missing/version semantics when the aggregate becomes mutable.
- Keep associations lazy by default, explicitly fetch lines for domain mapping,
  and order the collection by persisted position.
- Guard both `spring.jpa.open-in-view=false` and the absence of Spring Boot's OSIV
  interceptor. Keep the detached-lazy test as a narrower session-lifetime example.
- Automate an image-based service restart check and run it in CI in addition to the
  Testcontainers suite.

## Consequences

- Unsupported prices fail before persistence instead of depending on database
  rounding or overflow behavior. Four decimals are a lab-wide storage contract,
  not a claim about every currency's legal minor units.
- New orders retain caller line order. Legacy rows receive a repeatable order, not
  a reconstruction of information that was never stored.
- Creation avoids the lookup SELECT that `merge` can perform for assigned IDs.
  This choice intentionally does not define update behavior yet.
- The evidence distinguishes repository-session detachment, application OSIV
  configuration, database round trips, and process restarts as separate claims.
- CI is configured to exercise Docker image construction and restart persistence,
  increasing runtime and requiring an available Docker daemon.

## Evidence

- `make verify`: 18 Surefire and 11 Failsafe tests pass on 2026-09-07 using
  macOS, Java 21.0.5, and OrbStack.
- `make verify-restart`: the built image preserves price value and line sequence
  across an `order-service` restart against the same Postgres container.
- Detailed claim limits live in the [verification scoreboard](../notes-verification.md).

## References

- [ADR-0003](0003-persistence-jpa-entity-mapping-and-test-tiers.md)
- [ADR-0004](0004-evidence-driven-learning-roadmap.md)
- [Verification scoreboard](../notes-verification.md)
