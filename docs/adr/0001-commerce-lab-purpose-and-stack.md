# 1. Commerce Lab — purpose, scope, and stack

Date: 2026-06-04

## Status

Accepted

## Context

The production FOP microservices carry significant accumulated mess. Retrofitting
best practices (testing taxonomy, resilience, observability, contracts) onto a live
legacy system is high-risk, low-learning-velocity work, constrained by existing data,
deploy pipelines, and prior decisions.

Separately, there is a large body of *passive* knowledge — an Obsidian vault of ~96
interview-prep notes on Java, the JVM, Spring, system design, messaging, and
observability — that has not been exercised hands-on.

## Decision

Build **Commerce Lab**: a greenfield e-commerce microservices system on local Docker
Compose, constructed step-by-step to verify the vault notes in practice.

- **Domain slice:** `order-service` + `inventory-service` only (customer optional/later).
  Two services are the minimum that forces the interesting distributed problems
  (cross-service consistency, contract testing, tracing across hops).
- **Defining evolution:** the order→inventory interaction is built synchronously first
  (Phase 3), then migrated to async outbox + Kafka saga (Phase 4).
- **Stack:** Java 21, Spring Boot 3.3, Maven, Postgres + Flyway, Kafka via Redpanda,
  OpenTelemetry + Prometheus + Grafana, React + Vite. Chosen for transfer value and
  low yak-shaving (it mirrors the FOP stack the author already knows).
- **No shared domain jar between services.** Contracts are expressed via OpenAPI and
  event schemas only, to avoid the coupling anti-pattern.
- **Vertical slices:** one service made excellent end-to-end before adding the next.

## Consequences

- Each phase is independently demoable and ends with a passing test, an ADR, and an
  update to the source note. Progress is tracked in `docs/notes-verification.md`.
- The project doubles as a portfolio/talking-piece for the active job hunt; clusters
  most likely to come up in interviews (concurrency, idempotency, transactions, testing,
  observability) are prioritised over breadth of business features.
- Main risk is scope sprawl (rebuilding a smaller FOP mess, or never reaching a done
  milestone). Mitigations: 2 services only, timeboxed infra, vertical slices, and
  pushing pure-language verification into a `playground/` module rather than plumbing.
- Optional capstone: port one proven pattern (e.g. idempotency keys) back into FOP as a
  single real PR.

## Notes

This ADR establishes the lightweight ADR practice for the lab. Subsequent ADRs are
numbered incrementally (one per phase) and are immutable once accepted — superseded,
never edited.
