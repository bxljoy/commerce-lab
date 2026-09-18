# Phase 3A Final Review Fix Report

Date: 2026-09-18
Reviewed baseline: `d756589b4b32ae436a4649fa5c20956689434107`

## Status

Complete. Both final-review findings were addressed together without widening the
behavioral scope.

## Changes

- Added an MVC regression proving a request containing `"lines": [null]` returns
  `400 application/problem+json` with type
  `https://commerce-lab/errors/invalid-reservation`.
- Added explicit null-line rejection in the HTTP adapter before dereferencing the
  generated request DTO.
- Added a domain regression proving `Reservation` rejects a null line with
  `InvalidReservationException`, and added the corresponding aggregate invariant.
- Added a direct `ReserveInventoryCommand` regression and typed null-line validation
  so command construction cannot leak a raw `NullPointerException`.
- Changed the standalone datasource default from database `inventorydb` to
  `inventory`, matching the supplied Compose database.
- Added a lightweight parsed-YAML test comparing the application datasource default
  database to Compose `POSTGRES_DB`.
- Updated the superseded datasource value in the implementation plan so the
  documented runnable configuration remains accurate.

## TDD Record

RED command:

```text
mvn -f inventory-service/pom.xml -B \
  -Dtest=InventoryApiControllerTest,InventoryDomainTest,ReserveInventoryCommandTest,InventoryConfigurationTest \
  test
```

RED result: expected build failure; 29 tests ran with 3 failures and 1 error.

- MVC null-line request errored with the raw generated-line dereference NPE.
- `Reservation` null-line regression received raw NPE instead of
  `InvalidReservationException`.
- Direct command construction received raw NPE instead of
  `InvalidReservationException`.
- Configuration comparison found `inventorydb` versus Compose `inventory`.

GREEN command: the same focused command after the implementation changes.

GREEN result: build success; 29 tests, 0 failures, 0 errors, 0 skipped.

## Verification

- Focused domain and `InventoryApiControllerTest` suites: PASS as part of the
  29-test focused run.
- Direct-command and parsed configuration tests: PASS as part of the same run.
- `make verify`: PASS after granting the required local Docker socket access;
  both order and inventory unit, slice, and Testcontainers integration suites
  completed successfully. The initial sandboxed attempt was blocked only by denied
  OrbStack socket access and was rerun unchanged with access enabled.
- `make verify-inventory-image`: PASS; the real image reserved and idempotently
  released both seeded SKUs with stock restored.
- Clean teardown: PASS; the verification script's EXIT trap ran, and a subsequent
  `docker compose -p commerce-lab-inventory-test ps -a --format json` returned no
  containers.
- `git diff --check`: PASS.

## Self-Review

- Generated request DTO knowledge remains confined to the HTTP adapter package.
- No catch-all `IllegalArgumentException`, `RuntimeException`, or `Exception`
  handler was added.
- The API and domain boundaries independently reject null reservation lines.
- Configuration verification parses both YAML files instead of relying on visual
  inspection.
- No `inventorydb` references remain outside generated/build output, and the plan
  now agrees with the runnable configuration.
- No unrelated production, API schema, persistence, or Obsidian changes were made.

## Concerns

None.
