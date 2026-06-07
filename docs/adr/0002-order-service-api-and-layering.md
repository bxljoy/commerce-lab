# 2. Order service — API style, layering, and error model (Phase 1)

Date: 2026-06-07

## Status

Accepted

## Context

Phase 1 builds the first vertical slice — the `order-service` HTTP API — "done right",
as the reference for every service that follows. The guiding scenario's first step is
*place an order*; Phase 1 delivers **place + get** with no persistence yet (Phase 2) and
no second service yet (Phase 3). Decisions needed: API style (contract-first vs
code-first), internal layering, domain modelling, error format, and what to deliberately
defer so the slice stays finishable (scope sprawl is the project's #1 failure mode).

## Decision

- **OpenAPI-first (contract-first).** `order-service/openapi.yaml` is the source of truth.
  `openapi-generator-maven-plugin` (`spring`, `interfaceOnly`, `useTags`, `skipDefaultInterface`)
  generates the `OrdersApi` interface + POJO DTOs at build time; we hand-write
  `OrderApiController implements OrdersApi` returning `ResponseEntity<T>` (hence `@Controller`,
  not `@RestController`). Generated code is emitted to `target/generated-sources` and is
  **not committed** — the build regenerates it, so the spec can never drift from the code.
- **Layered: controller → service → domain → repository.** Generated DTOs are confined to
  the controller; the service works only in domain types. A thin hand-written
  `PlaceOrderCommand` is the anti-corruption boundary, keeping the service free of generated
  artifacts (so it survives the Phase 2 persistence swap unchanged).
- **Domain via records + value objects.** `Order`, `OrderLine`, and a `Money` value object
  are records with invariants in compact constructors and a defensive copy of lines;
  `OrderStatus` is an enum. The **sealed state machine + exhaustive switch is deferred to
  Phase 4**, where event-driven transitions actually need exhaustive dispatch (only `PLACED`
  is reachable in Phase 1).
- **In-memory repository** behind an `OrderRepository` interface. Postgres + Flyway + JPA
  replace the implementation in Phase 2 with no change to service/controller.
- **RFC-7807 errors.** `@RestControllerAdvice extends ResponseEntityExceptionHandler`
  returns `ProblemDetail` (`application/problem+json`): 400 for bean-validation failures
  (with a per-field `errors` map) and bad arguments, 404 for unknown ids. Validation rules
  live in `openapi.yaml` so the spec stays the single source.
- **No authentication/authorization in Phase 1.** The `@AuthenticationPrincipal`/ownership
  half of the controller-hygiene note is deferred to Phase 7 (gateway + OIDC/JWT); adding
  auth now would be security theatre against a single local service.
- **Endpoints limited to place + get.** List/cancel and lifecycle transitions arrive with
  the inventory integration (Phase 3–4).

## Consequences

- The spec is enforced at compile time: change `openapi.yaml`, regenerate, and the
  controller stops compiling until it matches — schema drift becomes impossible.
- 16 tests pass (domain + service unit tests, a `@WebMvcTest` controller slice) and the
  endpoints are verified over real HTTP (201 + `Location`, 200, 400, 404). CI continues to
  guard the Byte Buddy `-javaagent` pre-load (see ADR-0001 era work).
- The records note is only **partially** verified here (records/value objects); the sealed
  + pattern-matching part is tracked for Phase 4 in `docs/notes-verification.md`.
- Not committing generated code means a clean checkout must run `generate-sources` before
  the IDE resolves `OrdersApi` — the Maven build does this automatically.
- The `PlaceOrderCommand` mapping hop is mild extra boilerplate, accepted in exchange for a
  service layer that has zero dependency on generated OpenAPI types.

## Notes

Supersedes nothing. Immutable once accepted — superseded, never edited. The Phase 2 ADR
will record the persistence decisions (JPA entity vs domain record, OSIV-off, Testcontainers).
