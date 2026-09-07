# 4. Evidence-driven learning roadmap and bounded core scope

Date: 2026-09-07

## Status

Accepted. Amends the scope and phase-exit conventions in ADR-0001 and the current
interpretation of verification claims in ADR-0002/0003. Existing implementation
decisions remain in force unless a subsequent ADR changes them.

## Context

Phase 2 delivers a working Postgres-backed order API. The 2026-09-07 review ran
15 Surefire and 6 Failsafe tests successfully with Docker access outside the
Codex sandbox. Remaining issues include price precision/range, line ordering,
assigned-ID insert behavior, and evidence that overstates what tests demonstrate.

The lab exists for practical understanding, interview explanations, and a portfolio
demo. Adding patterns and marking whole notes verified can hide gaps in that
understanding. The next phases need explicit failure and recovery semantics.

## Decision

- Keep two independently buildable services, separate databases/domain models,
  OpenAPI contracts, and the synchronous-to-asynchronous evolution.
- Finish the core at Phase 6: reserve, confirm/reject, cancel/release, recover from
  failures, and demonstrate behavior with observability and a small UI. Shipping,
  additional services, and Phase 7 features remain optional.
- Close a bounded Phase 2 cleanup before inventory work. Track unfinished items in
  `docs/notes-verification.md`; passing existing tests does not close those items.
- Split Phase 3 into inventory correctness (3A) and synchronous integration (3B).
  Prove concurrent stock protection in 3A. In 3B, define reservation identity,
  idempotency, uncertain outcomes, and restart recovery; use short database
  transactions with network calls outside them. Add basic correlation logging here.
- Split Phase 4 into durable event delivery (4A) and workflow recovery (4B).
  Include reliable inventory result publication as well as order publication.
  Test crashes around commit/publication/acknowledgement, duplicates, and one
  cancellation/release compensation path. Choose the detailed state machine and
  concurrency strategy in the implementation ADRs.
- Make each exit demonstrate a predicted failure and a regression test, an
  explanation without reading code, and an exact claim with evidence and limits.
  A green status applies to that claim, not every topic in a linked note.
- Keep a weekly, separate Java/concurrency exercise. It does not gate service work.
- Write ADRs for meaningful decisions, and short notes for smaller experiments.

## Consequences

- The vault roadmap defines goals and phase sequence; the repository scoreboard
  tracks acceptance criteria and evidence. README summarizes current capabilities.
- Phase 2 is now marked cleanup pending. Historical ADRs retain the claims made
  at the time; the scoreboard records current corrections. In particular, codegen
  does not prove all runtime contract behavior, and a detached lazy-loading test
  outside a web request does not prove OSIV is disabled.
- This roadmap revision changes documentation only. Cleanup items and future
  milestones require separate implementation and verification.
- Review dependency compatibility before copying the service setup to inventory;
  verify upgrades separately without making a framework rewrite a prerequisite.

## References

- [Verification scoreboard](../notes-verification.md)
- Vault: `tech-decisions/notes/commerce-lab-phased-build-plan.md`
- [ADR-0001](0001-commerce-lab-purpose-and-stack.md)
- [ADR-0002](0002-order-service-api-and-layering.md)
- [ADR-0003](0003-persistence-jpa-entity-mapping-and-test-tiers.md)
