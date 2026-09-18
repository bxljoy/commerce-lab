# Phase 3A Inventory Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an independent inventory service that atomically reserves multiple SKUs, cannot oversell under concurrent requests, and releases each reservation exactly once.

**Architecture:** A contract-first Spring Boot service owns a separate Postgres schema and maps generated HTTP DTOs to immutable domain records. The application service coordinates two repository ports inside short transactions; the JPA stock adapter acquires pessimistic row locks in sorted SKU order, while the reservation row lock serializes release.

**Tech Stack:** Java 21, Spring Boot 3.3.5, Maven, OpenAPI Generator 7.10.0, Spring Data JPA/Hibernate, Flyway, Postgres 16, Testcontainers 1.20.4, Docker Compose, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-07-phase-3a-inventory-correctness-design.md`

## Global Constraints

- Keep `inventory-service` independently buildable; do not add a shared Java domain module.
- Use `orderId` as the sole lifetime reservation identity.
- Reject duplicate SKUs rather than combining quantities.
- Acquire stock locks in ascending SKU order.
- Reserve all requested lines or change none; release restores stock at most once.
- Preserve reservation request order with `line_position`.
- Keep OSIV disabled and let Flyway own the schema with Hibernate validation only.
- Keep order-to-inventory networking and reservation replay semantics out of Phase 3A.
- Write each behavior test first, run it to observe the intended failure, then add minimal production code.

## File Structure

`inventory-service/openapi.yaml` owns the public HTTP contract and generates APIs/models under `target/generated-sources/openapi`.

`inventory-service/src/main/java/com/commercelab/inventory/` contains:

- `InventoryServiceApplication.java`: boot entry point and UTC `Clock` bean.
- `domain/Reservation.java`, `ReservationLine.java`, `ReservationStatus.java`, `StockItem.java`: immutable business model and invariants.
- `domain/*Exception.java`, `domain/Availability.java`: typed failure information.
- `service/ReserveInventoryCommand.java`, `InventoryService.java`: use cases and transaction boundaries.
- `repository/StockRepository.java`, `ReservationRepository.java`: domain-facing persistence ports.
- `persistence/StockEntity.java`, `ReservationEntity.java`, `ReservationLineEntity.java`: JPA-only mutable models.
- `persistence/StockJpaRepository.java`, `ReservationJpaRepository.java`: package-private Spring Data repositories.
- `persistence/JpaStockRepository.java`, `JpaReservationRepository.java`: mapping adapters.
- `api/InventoryApiController.java`: generated-interface implementation and DTO mapping.
- `web/InventoryApiExceptionHandler.java`: RFC problem translation.

`inventory-service/src/main/resources/` contains application configuration and Flyway migrations. Tests mirror the package layout and share one Testcontainers base.

---

### Task 1: Contract-First Service Skeleton

**Files:**
- Create: `inventory-service/pom.xml`
- Create: `inventory-service/openapi.yaml`
- Create: `inventory-service/.dockerignore`
- Create: `inventory-service/Dockerfile`
- Create: `inventory-service/src/main/resources/application.yml`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/InventoryServiceApplication.java`

**Interfaces:**
- Produces generated `InventoryApi` operations `reserveInventory`, `getReservation`, `releaseReservation`, and `getStock`.
- Produces generated models `ReserveInventoryRequest`, `ReserveInventoryLineRequest`, `ReservationResponse`, `ReservationLineResponse`, `StockResponse`, `ProblemDetail`, and `AvailabilityDetail`.

- [ ] **Step 1: Define the OpenAPI contract**

Create OpenAPI 3.0.3 paths with these exact operation IDs and outcomes:

```yaml
paths:
  /api/v1/reservations:
    post:
      operationId: reserveInventory
      responses:
        '201': {description: Stock reserved}
        '400': {description: Invalid reservation request}
        '409': {description: Stock unavailable or reservation already exists}
  /api/v1/reservations/{orderId}:
    get:
      operationId: getReservation
      responses:
        '200': {description: Reservation found}
        '404': {description: Reservation not found}
  /api/v1/reservations/{orderId}/release:
    put:
      operationId: releaseReservation
      responses:
        '200': {description: Reservation released or already released}
        '404': {description: Reservation not found}
  /api/v1/stock/{sku}:
    get:
      operationId: getStock
      responses:
        '200': {description: Stock found}
        '404': {description: Stock not found}
```

Define required request fields, UUID `orderId`, non-empty lines, SKU length 1-64,
quantity minimum 1, statuses `RESERVED|RELEASED`, date-time fields, and reusable
`application/problem+json` response bodies. `ProblemDetail.unavailableSkus` is:

```yaml
unavailableSkus:
  type: object
  additionalProperties:
    $ref: '#/components/schemas/AvailabilityDetail'
AvailabilityDetail:
  type: object
  required: [requested, available]
  properties:
    requested: {type: integer, format: int32, minimum: 1}
    available: {type: integer, format: int32, minimum: 0}
```

- [ ] **Step 2: Create the Maven build and application skeleton**

Use the order-service dependency/plugin versions, with these exact inventory values:

```xml
<groupId>com.commercelab</groupId>
<artifactId>inventory-service</artifactId>
<version>0.0.1-SNAPSHOT</version>
<properties>
  <java.version>21</java.version>
  <testcontainers.version>1.20.4</testcontainers.version>
</properties>
```

Configure OpenAPI generation with:

```xml
<inputSpec>${project.basedir}/openapi.yaml</inputSpec>
<generatorName>spring</generatorName>
<apiPackage>com.commercelab.inventory.generated.api</apiPackage>
<modelPackage>com.commercelab.inventory.generated.model</modelPackage>
```

Retain the existing Spring Web, Actuator, Validation, Data JPA, Flyway Postgres,
Postgres runtime, Spring Boot test, and Testcontainers dependencies. Retain the
Surefire/Failsafe startup Byte Buddy agent configuration.

```java
@SpringBootApplication
public class InventoryServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
```

Configure port `8081`, datasource default
`jdbc:postgresql://localhost:5433/inventory`, user/password `inventory`, OSIV false,
`ddl-auto: validate`, Flyway enabled, and health/info/metrics actuator exposure.

- [ ] **Step 3: Add the Docker build files**

Use the same two-stage Maven/Temurin structure as order-service, copy
`pom.xml`, `openapi.yaml`, then `src`, expose `8081`, and run as UID 1001. Ignore
`target`, `.idea`, and `.git` in `.dockerignore`.

- [ ] **Step 4: Generate and compile the contract**

Run: `mvn -f inventory-service/pom.xml -B generate-sources`

Expected: BUILD SUCCESS and generated `InventoryApi.java` plus all named models under
`inventory-service/target/generated-sources/openapi`.

Run: `mvn -f inventory-service/pom.xml -B -DskipTests compile`

Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add inventory-service
git commit -m "feat: scaffold contract-first inventory service"
```

---

### Task 2: Domain Invariants

**Files:**
- Create: `inventory-service/src/test/java/com/commercelab/inventory/domain/InventoryDomainTest.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/ReservationLine.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/ReservationStatus.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/Reservation.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/StockItem.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/InvalidReservationException.java`

**Interfaces:**
- Produces `Reservation.reserve(UUID, List<ReservationLine>, Instant)` and `Reservation.release(Instant)`.
- Produces `StockItem.reserve(int)` and `StockItem.release(int)` returning new values.

- [ ] **Step 1: Write failing domain tests**

```java
@Test
void rejectsDuplicateSkus() {
    assertThatThrownBy(() -> Reservation.reserve(UUID.randomUUID(), List.of(
            new ReservationLine("SKU-1", 1),
            new ReservationLine("SKU-1", 2)), Instant.EPOCH))
            .isInstanceOf(InvalidReservationException.class)
            .hasMessageContaining("duplicate SKU: SKU-1");
}

@Test
void reservesAndRestoresStockWithoutMutation() {
    StockItem original = new StockItem("SKU-1", 2);
    StockItem reserved = original.reserve(2);
    assertThat(original.availableQuantity()).isEqualTo(2);
    assertThat(reserved.availableQuantity()).isZero();
    assertThat(reserved.release(2).availableQuantity()).isEqualTo(2);
}

@Test
void releaseIsAStableTerminalTransition() {
    Reservation reserved = Reservation.reserve(UUID.randomUUID(),
            List.of(new ReservationLine("SKU-1", 1)), Instant.EPOCH);
    Reservation released = reserved.release(Instant.EPOCH.plusSeconds(1));
    assertThat(released.status()).isEqualTo(ReservationStatus.RELEASED);
    assertThat(released.release(Instant.EPOCH.plusSeconds(2))).isEqualTo(released);
}
```

Also test null IDs/timestamps, empty lines, blank SKU, quantity zero, negative stock,
reserving more than available, and defensive copying.

- [ ] **Step 2: Run tests to verify RED**

Run: `mvn -f inventory-service/pom.xml -B -Dtest=InventoryDomainTest test`

Expected: test compilation fails because the domain types do not exist.

- [ ] **Step 3: Implement minimal immutable domain types**

Use compact record constructors for validation. `Reservation` enforces these state
rules:

```java
if (status == ReservationStatus.RESERVED && releasedAt != null) {
    throw new InvalidReservationException("reserved reservation cannot have releasedAt");
}
if (status == ReservationStatus.RELEASED && releasedAt == null) {
    throw new InvalidReservationException("released reservation requires releasedAt");
}
Set<String> skus = new HashSet<>();
for (ReservationLine line : lines) {
    if (!skus.add(line.sku())) {
        throw new InvalidReservationException("duplicate SKU: " + line.sku());
    }
}
lines = List.copyOf(lines);
```

`StockItem.reserve` rejects non-positive quantities and quantities above availability;
use `Math.addExact` in `release` to avoid silent integer overflow.

- [ ] **Step 4: Run tests to verify GREEN**

Run: `mvn -f inventory-service/pom.xml -B -Dtest=InventoryDomainTest test`

Expected: all domain tests pass.

- [ ] **Step 5: Commit**

```bash
git add inventory-service/src/main/java/com/commercelab/inventory/domain inventory-service/src/test/java/com/commercelab/inventory/domain
git commit -m "feat: define inventory reservation invariants"
```

---

### Task 3: Flyway Schema And Locking Repositories

**Files:**
- Create: `inventory-service/src/main/resources/db/migration/V1__init.sql`
- Create: `inventory-service/src/main/resources/db/migration/V2__seed_demo_stock.sql`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/repository/StockRepository.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/repository/ReservationRepository.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/persistence/StockEntity.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/persistence/ReservationEntity.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/persistence/ReservationLineEntity.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/persistence/StockJpaRepository.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/persistence/ReservationJpaRepository.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/persistence/JpaStockRepository.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/persistence/JpaReservationRepository.java`
- Create: `inventory-service/src/test/java/com/commercelab/inventory/AbstractPostgresIntegrationTest.java`
- Create: `inventory-service/src/test/java/com/commercelab/inventory/persistence/InventoryPersistenceIT.java`

**Interfaces:**
- `StockRepository`: `findBySku`, `lockBySkus`, and `updateAll`.
- `ReservationRepository`: `existsByOrderId`, `add`, `findByOrderId`, `lockByOrderId`, and `markReleased`.

- [ ] **Step 1: Write the persistence and lock tests**

Create a shared static `PostgreSQLContainer<>("postgres:16-alpine")` and dynamic
datasource properties. In `@BeforeEach`, execute:

```sql
TRUNCATE TABLE inventory_reservation_lines, inventory_reservations, stock CASCADE;
INSERT INTO stock (sku, available_quantity) VALUES ('SKU-1', 2), ('SKU-2', 3);
```

Test a reservation entity round trip preserves SKU order. Add a deterministic lock
test using two `TransactionTemplate` calls: transaction A locks `SKU-1` and waits on a
latch; transaction B attempts the same lock and its `Future` must remain incomplete
for 200 ms; release A and assert B completes.

- [ ] **Step 2: Run integration test to verify RED**

Run: `mvn -f inventory-service/pom.xml -B -Dit.test=InventoryPersistenceIT verify`

Expected: test compilation fails because repositories/entities do not exist.

- [ ] **Step 3: Add exact schema and seed migrations**

```sql
CREATE TABLE stock (
    sku VARCHAR(64) PRIMARY KEY,
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0)
);
CREATE TABLE inventory_reservations (
    order_id UUID PRIMARY KEY,
    status VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ
);
CREATE TABLE inventory_reservation_lines (
    order_id UUID NOT NULL REFERENCES inventory_reservations(order_id),
    sku VARCHAR(64) NOT NULL REFERENCES stock(sku),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    line_position INTEGER NOT NULL CHECK (line_position >= 0),
    PRIMARY KEY (order_id, sku),
    UNIQUE (order_id, line_position)
);
```

V2 inserts `('SKU-APPLE', 10)` and `('SKU-BANANA', 5)` with `ON CONFLICT DO NOTHING`.

- [ ] **Step 4: Implement repository ports and JPA adapters**

Use these port signatures:

```java
public interface StockRepository {
    Optional<StockItem> findBySku(String sku);
    List<StockItem> lockBySkus(List<String> sortedSkus);
    void updateAll(List<StockItem> stock);
}

public interface ReservationRepository {
    boolean existsByOrderId(UUID orderId);
    void add(Reservation reservation);
    Optional<Reservation> findByOrderId(UUID orderId);
    Optional<Reservation> lockByOrderId(UUID orderId);
    void markReleased(UUID orderId, Instant releasedAt);
}
```

The locking Spring Data methods are:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("select s from StockEntity s where s.sku in :skus order by s.sku")
List<StockEntity> lockBySkus(@Param("skus") Collection<String> skus);

@Lock(LockModeType.PESSIMISTIC_WRITE)
@EntityGraph(attributePaths = "lines")
@Query("select r from ReservationEntity r where r.orderId = :orderId")
Optional<ReservationEntity> lockByOrderId(@Param("orderId") UUID orderId);
```

Map entities to domain records inside adapter transactions. `ReservationEntity.lines`
uses `@OrderBy("position ASC")`. `JpaStockRepository.updateAll` calls
`EntityManager.find(StockEntity.class, sku)` and changes the already-managed locked
entity; missing rows throw `IllegalStateException` because the service validated the
locked set.

- [ ] **Step 5: Run integration test to verify GREEN**

Run: `mvn -f inventory-service/pom.xml -B -Dit.test=InventoryPersistenceIT verify`

Expected: round-trip and lock-blocking tests pass against Postgres.

- [ ] **Step 6: Commit**

```bash
git add inventory-service/src/main/resources/db inventory-service/src/main/java/com/commercelab/inventory/repository inventory-service/src/main/java/com/commercelab/inventory/persistence inventory-service/src/test/java/com/commercelab/inventory/AbstractPostgresIntegrationTest.java inventory-service/src/test/java/com/commercelab/inventory/persistence
git commit -m "feat: persist inventory with deterministic row locking"
```

---

### Task 4: Atomic Multi-SKU Reservation And No-Oversell Proof

**Files:**
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/Availability.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/StockUnavailableException.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/ReservationAlreadyExistsException.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/service/ReserveInventoryCommand.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/service/InventoryService.java`
- Create: `inventory-service/src/test/java/com/commercelab/inventory/service/InventoryReservationIT.java`

**Interfaces:**
- Produces `InventoryService.reserve(ReserveInventoryCommand): Reservation`.
- Produces structured `StockUnavailableException.unavailableSkus(): Map<String, Availability>`.

- [ ] **Step 1: Write failing all-or-nothing and concurrency tests**

```java
@Test
void insufficientSecondSkuRollsBackEveryChange() {
    insertStock("SKU-A", 5);
    insertStock("SKU-B", 1);

    assertThatThrownBy(() -> service.reserve(command(orderId,
            line("SKU-A", 2), line("SKU-B", 2))))
            .isInstanceOf(StockUnavailableException.class);

    assertThat(available("SKU-A")).isEqualTo(5);
    assertThat(available("SKU-B")).isEqualTo(1);
    assertThat(reservationCount(orderId)).isZero();
}

@Test
void concurrentOrdersCannotReserveTheLastUnitTwice() throws Exception {
    insertStock("SKU-LAST", 1);
    CyclicBarrier start = new CyclicBarrier(2);
    Future<Outcome> first = submitReserve(start, UUID.randomUUID(), "SKU-LAST", 1);
    Future<Outcome> second = submitReserve(start, UUID.randomUUID(), "SKU-LAST", 1);

    assertThat(List.of(first.get(), second.get()))
            .containsExactlyInAnyOrder(Outcome.RESERVED, Outcome.UNAVAILABLE);
    assertThat(available("SKU-LAST")).isZero();
    assertThat(reservationCount()).isEqualTo(1);
}
```

Also send reversed SKU orders in two concurrent multi-SKU requests and assert both
complete within a bounded timeout, proving deterministic lock order avoids this
test's deadlock shape.

- [ ] **Step 2: Run test to verify RED**

Run: `mvn -f inventory-service/pom.xml -B -Dit.test=InventoryReservationIT verify`

Expected: test compilation fails because `InventoryService` and command types do not exist.

- [ ] **Step 3: Implement reservation orchestration**

Define the command and availability value with exact immutable shapes:

```java
public record ReserveInventoryCommand(UUID orderId, List<Line> lines) {
    public ReserveInventoryCommand {
        Objects.requireNonNull(orderId, "orderId");
        lines = List.copyOf(Objects.requireNonNull(lines, "lines"));
    }

    public List<ReservationLine> toDomainLines() {
        return lines.stream().map(line -> new ReservationLine(line.sku(), line.quantity())).toList();
    }

    public record Line(String sku, int quantity) {}
}

public record Availability(int requested, int available) {}
```

```java
@Transactional
public Reservation reserve(ReserveInventoryCommand command) {
    Reservation candidate = Reservation.reserve(command.orderId(), command.toDomainLines(), clock.instant());
    if (reservations.existsByOrderId(command.orderId())) {
        throw new ReservationAlreadyExistsException(command.orderId());
    }

    List<String> skus = candidate.lines().stream().map(ReservationLine::sku).sorted().toList();
    Map<String, StockItem> locked = stocks.lockBySkus(skus).stream()
            .collect(Collectors.toMap(StockItem::sku, Function.identity()));
    Map<String, Availability> unavailable = collectUnavailable(candidate.lines(), locked);
    if (!unavailable.isEmpty()) {
        throw new StockUnavailableException(unavailable);
    }

    List<StockItem> updated = candidate.lines().stream()
            .map(line -> locked.get(line.sku()).reserve(line.quantity()))
            .toList();
    stocks.updateAll(updated);
    reservations.add(candidate);
    return candidate;
}
```

Use a `LinkedHashMap` for unavailable details in request order. Unknown SKU availability
is zero. Translate a concurrent `inventory_reservations_pkey` violation during
`add`/flush into `ReservationAlreadyExistsException`; other integrity failures propagate.

- [ ] **Step 4: Run test to verify GREEN**

Run: `mvn -f inventory-service/pom.xml -B -Dit.test=InventoryReservationIT verify`

Expected: all-or-nothing, last-unit, duplicate-order conflict, and reversed-order tests pass.

- [ ] **Step 5: Commit**

```bash
git add inventory-service/src/main/java/com/commercelab/inventory/domain inventory-service/src/main/java/com/commercelab/inventory/service inventory-service/src/test/java/com/commercelab/inventory/service inventory-service/src/main/java/com/commercelab/inventory/persistence/JpaReservationRepository.java
git commit -m "feat: reserve inventory atomically under contention"
```

---

### Task 5: Exactly-Once Stock Release

**Files:**
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/ReservationNotFoundException.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/domain/StockNotFoundException.java`
- Modify: `inventory-service/src/main/java/com/commercelab/inventory/service/InventoryService.java`
- Modify: `inventory-service/src/test/java/com/commercelab/inventory/service/InventoryReservationIT.java`

**Interfaces:**
- Produces `InventoryService.release(UUID): Reservation`.
- Produces `InventoryService.getReservation(UUID): Reservation` and `getStock(String): StockItem` for later HTTP mapping.

- [ ] **Step 1: Write failing sequential and concurrent release tests**

```java
@Test
void repeatedReleaseRestoresStockOnce() {
    UUID orderId = reserve("SKU-1", 2);
    assertThat(service.release(orderId).status()).isEqualTo(ReservationStatus.RELEASED);
    assertThat(service.release(orderId).status()).isEqualTo(ReservationStatus.RELEASED);
    assertThat(available("SKU-1")).isEqualTo(INITIAL_QUANTITY);
}

@Test
void concurrentReleaseRestoresStockOnce() throws Exception {
    UUID orderId = reserve("SKU-1", 2);
    CyclicBarrier start = new CyclicBarrier(2);
    Future<Reservation> first = submitRelease(start, orderId);
    Future<Reservation> second = submitRelease(start, orderId);
    assertThat(first.get().status()).isEqualTo(ReservationStatus.RELEASED);
    assertThat(second.get().status()).isEqualTo(ReservationStatus.RELEASED);
    assertThat(available("SKU-1")).isEqualTo(INITIAL_QUANTITY);
}
```

Also test unknown reservation and retrieval of both statuses.

- [ ] **Step 2: Run test to verify RED**

Run: `mvn -f inventory-service/pom.xml -B -Dit.test=InventoryReservationIT verify`

Expected: compilation fails because `release` and retrieval methods do not exist.

- [ ] **Step 3: Implement release and retrieval**

```java
@Transactional
public Reservation release(UUID orderId) {
    Reservation reservation = reservations.lockByOrderId(orderId)
            .orElseThrow(() -> new ReservationNotFoundException(orderId));
    if (reservation.status() == ReservationStatus.RELEASED) {
        return reservation;
    }
    List<String> skus = reservation.lines().stream().map(ReservationLine::sku).sorted().toList();
    Map<String, StockItem> locked = stocks.lockBySkus(skus).stream()
            .collect(Collectors.toMap(StockItem::sku, Function.identity()));
    stocks.updateAll(reservation.lines().stream()
            .map(line -> locked.get(line.sku()).release(line.quantity()))
            .toList());
    Instant releasedAt = clock.instant();
    reservations.markReleased(orderId, releasedAt);
    return reservation.release(releasedAt);
}
```

Make `getReservation` and `getStock` read-only transactions. Add typed
`StockNotFoundException` for unknown stock reads.

- [ ] **Step 4: Run test to verify GREEN**

Run: `mvn -f inventory-service/pom.xml -B -Dit.test=InventoryReservationIT verify`

Expected: release, concurrent release, retrieval, and prior reservation tests pass.

- [ ] **Step 5: Commit**

```bash
git add inventory-service/src/main/java/com/commercelab/inventory inventory-service/src/test/java/com/commercelab/inventory/service/InventoryReservationIT.java
git commit -m "feat: release inventory idempotently"
```

---

### Task 6: HTTP Boundary And Full Vertical Slice

**Files:**
- Create: `inventory-service/src/main/java/com/commercelab/inventory/api/InventoryApiController.java`
- Create: `inventory-service/src/main/java/com/commercelab/inventory/web/InventoryApiExceptionHandler.java`
- Create: `inventory-service/src/test/java/com/commercelab/inventory/api/InventoryApiControllerTest.java`
- Create: `inventory-service/src/test/java/com/commercelab/inventory/api/InventoryApiIT.java`
- Create: `inventory-service/src/test/java/com/commercelab/inventory/InventoryServiceApplicationIT.java`

**Interfaces:**
- Implements generated `InventoryApi` without exposing generated DTOs below the controller.
- Returns stable RFC problem types under `https://commerce-lab/errors/`.

- [ ] **Step 1: Write failing MVC slice tests**

Cover:

```java
mockMvc.perform(post("/api/v1/reservations")
        .contentType(APPLICATION_JSON).content(validBody))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", "/api/v1/reservations/" + orderId))
        .andExpect(jsonPath("$.status").value("RESERVED"));

mockMvc.perform(post("/api/v1/reservations")
        .contentType(APPLICATION_JSON).content(duplicateSkuBody))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.type").value("https://commerce-lab/errors/invalid-reservation"));
```

Also verify generated bean validation, missing reservation/stock `404`, unavailable
stock `409` with `unavailableSkus`, existing reservation `409`, and idempotent release `200`.

- [ ] **Step 2: Run slice test to verify RED**

Run: `mvn -f inventory-service/pom.xml -B -Dtest=InventoryApiControllerTest test`

Expected: compilation fails because controller and handler do not exist.

- [ ] **Step 3: Implement controller and exception handler**

Map generated request lines to `ReserveInventoryCommand.Line`, and map domain
reservations in original line order:

```java
@Override
public ResponseEntity<ReservationResponse> reserveInventory(ReserveInventoryRequest request) {
    Reservation reservation = service.reserve(toCommand(request));
    return ResponseEntity.created(URI.create("/api/v1/reservations/" + reservation.orderId()))
            .body(toResponse(reservation));
}
```

Map exceptions to:

- `InvalidReservationException` -> `400 invalid-reservation`
- `ReservationNotFoundException` -> `404 reservation-not-found`
- `StockNotFoundException` -> `404 stock-not-found`
- `StockUnavailableException` -> `409 stock-unavailable` plus details
- `ReservationAlreadyExistsException` -> `409 reservation-already-exists`

Override `handleMethodArgumentNotValid` to attach the field-message map as in
order-service, but do not add a catch-all `IllegalArgumentException` handler.

- [ ] **Step 4: Run slice test to verify GREEN**

Run: `mvn -f inventory-service/pom.xml -B -Dtest=InventoryApiControllerTest test`

Expected: all MVC slice tests pass.

- [ ] **Step 5: Write and run the full-stack tests**

Use `@SpringBootTest`, `@AutoConfigureMockMvc`, and the shared Postgres base. POST a
two-line reservation, GET it and assert line order, PUT release twice, then GET stock
and assert exact quantities. Add an application test that checks health is UP,
`spring.jpa.open-in-view=false`, and no `openEntityManagerInViewInterceptor` bean.

Run: `mvn -f inventory-service/pom.xml -B verify`

Expected: all Surefire and Failsafe tests pass.

- [ ] **Step 6: Commit**

```bash
git add inventory-service/src/main/java/com/commercelab/inventory/api inventory-service/src/main/java/com/commercelab/inventory/web inventory-service/src/test/java/com/commercelab/inventory/api inventory-service/src/test/java/com/commercelab/inventory/InventoryServiceApplicationIT.java
git commit -m "feat: expose inventory reservation API"
```

---

### Task 7: Repository Commands, Compose, And CI

**Files:**
- Modify: `Makefile`
- Modify: `docker-compose.yml`
- Modify: `.github/workflows/ci.yml`
- Create: `scripts/verify-inventory-service.sh`

**Interfaces:**
- Produces service-specific `test-order`, `test-inventory`, `verify-order`, and `verify-inventory` targets.
- Keeps aggregate `test` and `verify` targets and existing `verify-restart` behavior.

- [ ] **Step 1: Add the inventory Compose smoke script before wiring CI**

The script uses project name `commerce-lab-inventory-test`, host ports `15433` and
`18081`, traps `docker compose ... down -v --remove-orphans`, waits up to 60 seconds
for `/actuator/health`, reserves two demo SKUs, releases twice, and asserts final
stock is exactly the seeded quantity.

Run: `bash -n scripts/verify-inventory-service.sh`

Expected: syntax check passes.

Run: `bash scripts/verify-inventory-service.sh`

Expected: fails because Compose does not yet define inventory services.

- [ ] **Step 2: Extend Compose with independently owned services**

Add `inventory-postgres` using `postgres:16-alpine`, database/user/password
`inventory`, host port `${INVENTORY_POSTGRES_PORT:-5433}`, and volume
`inventory-pgdata`. Add `inventory-service`, image
`commerce-lab/inventory-service:0.0.1`, host port `${INVENTORY_SERVICE_PORT:-8081}`,
inventory datasource environment variables, health check, and dependency on healthy
inventory Postgres. Do not reuse order Postgres or its volume.

- [ ] **Step 3: Extend Make commands and CI**

```make
test: test-order test-inventory
verify: verify-order verify-inventory

test-order:
	mvn -f order-service/pom.xml -B test
test-inventory:
	mvn -f inventory-service/pom.xml -B test
verify-order:
	mvn -f order-service/pom.xml -B verify
verify-inventory:
	mvn -f inventory-service/pom.xml -B verify
verify-inventory-image:
	bash scripts/verify-inventory-service.sh
```

Keep the existing order-service Byte Buddy warning guard. Add an inventory verification
step and run `make verify-inventory-image` in CI. Both Maven projects must remain
directly runnable with `mvn -f <service>/pom.xml verify`.

- [ ] **Step 4: Verify the local stack**

Run: `make verify`

Expected: both services' Surefire and Failsafe suites pass.

Run: `make verify-restart`

Expected: existing order-service restart proof still passes.

Run: `make verify-inventory-image`

Expected: image builds, service becomes healthy, reserve/release assertions pass, and
the isolated Compose stack is removed afterward.

- [ ] **Step 5: Commit**

```bash
git add Makefile docker-compose.yml .github/workflows/ci.yml scripts/verify-inventory-service.sh
git commit -m "ci: verify both commerce services"
```

---

### Task 8: ADR, Evidence, And Phase Closeout

**Files:**
- Create: `docs/adr/0006-inventory-reservation-correctness.md`
- Modify: `README.md`
- Modify: `docs/notes-verification.md`
- Modify outside repo: `/Users/bxl/workspace/Obsidian-notes/tech-decisions/notes/commerce-lab-phased-build-plan.md`
- Modify outside repo: `/Users/bxl/workspace/Obsidian-notes/tech-decisions/notes/database-isolation-levels-mvcc-and-anomalies.md`
- Modify outside repo: `/Users/bxl/workspace/Obsidian-notes/tech-decisions/notes/spring-transactional-propagation-savepoints-and-self-invocation.md`
- Modify outside repo: `/Users/bxl/workspace/Obsidian-notes/tech-decisions/notes/README.md`

**Interfaces:**
- Records exact claims, commands, environment, outcomes, and limits without marking all isolation or idempotency topics complete.

- [ ] **Step 1: Write ADR-0006**

Record: separate database ownership, one reservation per `orderId`, duplicate-SKU
rejection, sorted pessimistic stock locks, transaction ordering, idempotent release,
demo seed data, and why optimistic/conditional-update approaches were deferred.

- [ ] **Step 2: Update runnable documentation**

README must show inventory status, ports, aggregate/service-specific commands, demo
SKUs, and curl examples for reserve, get, stock read, and repeated release. The
scoreboard must name exact test classes and explicitly limit the claim to tested
Postgres contention shapes; reservation replay remains Phase 3B.

- [ ] **Step 3: Update source notes**

Add concise `Verified in commerce-lab Phase 3A` sections describing the last-unit
race, multi-SKU rollback, sorted lock order, and concurrent release. Update each
note's `updated` date and move its index row to the top date group.

- [ ] **Step 4: Run final verification**

Run: `git diff --check`

Expected: no output.

Run: `make verify`

Expected: both Maven builds succeed with no failures or skips.

Run: `make verify-restart`

Expected: order restart proof passes.

Run: `make verify-inventory-image`

Expected: inventory image/Compose proof passes and cleans up.

- [ ] **Step 5: Commit**

```bash
git add README.md docs
git commit -m "docs: record phase 3a inventory evidence"
```

- [ ] **Step 6: Push and verify hosted CI after review approval**

Push `codex/phase-3a-inventory`, open or update its PR, and wait for the workflow.
Expected: Maven suites, Byte Buddy guard, both image checks, and cleanup pass on the
GitHub-hosted Linux runner.

## Completion Checkpoint

- [ ] Duplicate SKUs fail before database mutation.
- [ ] Multi-SKU insufficiency leaves all stock and reservations unchanged.
- [ ] Concurrent distinct orders cannot oversell the final unit.
- [ ] Sequential and concurrent release restore stock exactly once.
- [ ] Reservation line order survives persistence and HTTP round trips.
- [ ] Each service builds independently; aggregate local and hosted verification pass.
- [ ] Evidence documents the environment and does not claim Phase 3B idempotency.
