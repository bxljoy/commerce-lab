# Notes Verification Scoreboard

The point of Commerce Lab is to **prove the interview-prep notes in practice**. This
file maps each phase to the vault notes it verifies, and tracks status. Update the
`Status` column as work lands, and update the source note in the vault with the
real gotchas you hit ("verified in practice").

Legend: ⬜ not started · 🟡 in progress · ✅ verified

## Phase status

| # | Goal | Status |
|---|------|--------|
| 0 | Rails: order-service skeleton, health, Dockerfile, compose, Makefile | 🟡 |
| 1 | One service done right (OpenAPI-first, layered, validation, RFC-7807, unit/slice tests) | ✅ |
| 2 | Persistence done right (Postgres, Flyway, JPA, Testcontainers) | ⬜ |
| 3 | Second service + sync integration (RestClient, resilience4j, contract test) | ⬜ |
| 4 | Async — outbox + Kafka saga, idempotent consumers, DLQ | ⬜ |
| 5 | Observability — logs/metrics/traces across the system | ⬜ |
| 6 | Frontend slice + E2E (Playwright) | ⬜ |
| 7 | Optional capstone (gateway/auth, rate limiter, CQRS, vthreads, deploy) | ⬜ |

## Note → phase map

> Vault notes referenced as `note-slug` live in
> `Obsidian-notes/tech-decisions/notes/`.

### Phase 1 — One service done right
- ✅ `spring-rest-controller-hygiene-validation-dtos-authz` — `@Valid` DTO validation + RFC-7807 errors. (authz/`@AuthenticationPrincipal` deferred to Phase 7 — no auth yet)
- ✅ `spring-rest-jackson-and-openapi-codegen-pattern` — spec-first `openapi.yaml` → generated `OrdersApi` interface + POJO DTOs; hand-written `@Controller` returning `ResponseEntity`
- 🟡 `java-records-sealed-and-pattern-matching` — records + `Money` value object verified; sealed state hierarchy + exhaustive switch deferred to Phase 4 (event-driven transitions)
- ✅ `testing-taxonomy-pyramid-contracts-e2e-and-test-design` — domain/service unit tests + `@WebMvcTest` controller slice (contract tier in Phase 3)

### Phase 2 — Persistence done right
- ⬜ `jpa-entity-equals-and-hashcode`
- ⬜ `osiv-session-vs-transaction-and-phantom-write`
- ⬜ `jpa-fetching-projections-and-lazy-initialization`
- ⬜ `database-isolation-levels-mvcc-and-anomalies`
- ⬜ `postgres-write-performance-batching-and-idempotency`
- ⬜ `spring-transactional-propagation-savepoints-and-self-invocation`

### Phase 3 — Second service + sync integration
- ⬜ `restclient-http-timeouts-and-connection-pooling`
- ⬜ `circuit-breaker-retry-and-resilience4j`
- ⬜ `testing-taxonomy-pyramid-contracts-e2e-and-test-design` (contract tier)
- ⬜ `request-idempotency-keys-for-write-apis`

### Phase 4 — Async (the crown jewels)
- ⬜ `outbox-pattern-and-dual-write-problem`
- ⬜ `at-least-once-to-exactly-once-effect-and-ordered-processing`
- ⬜ `streaming-dedup-and-ordered-emission`
- ⬜ `kafka-producers-spring-boot-and-aws-msk`
- ⬜ `kafka-consumers-spring-boot-and-fargate`
- ⬜ `kafka-exactly-once-transactions-and-schema-evolution`
- ⬜ `pubsub-topic-subscription-and-dlq-model` (DLQ analog)

### Phase 5 — Observability
- ⬜ `distributed-tracing-and-apm`
- ⬜ `metrics-emission-paths-and-custom-vs-derived`
- ⬜ `monitoring-slos-and-alerting-on-symptoms-vs-causes`
- ⬜ `cache-observability-leading-indicators-and-silent-staleness`

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
Verified as small standalone JUnit tests, knocked out opportunistically between phases:
- ⬜ `java-memory-model-visibility-and-atomicity`
- ⬜ `java-generics-type-erasure-variance-and-wildcards`
- ⬜ `java-collectors-tomap-and-thread-safety`
- ⬜ `concurrenthashmap-internals-and-cache-stampede`
- ⬜ `lru-cache-linkedhashmap-and-hand-rolled-doubly-linked-list`
