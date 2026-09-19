# Consumer contract evidence

These hand-maintained v1 JSON expectations are independent of OpenAPI generation
and producer DTOs. Inventory expectations belong to the order consumer; order
expectations represent a public API client. They are examples plus deliberately
small, explicit Jackson/AssertJ checks, not a general schema engine or a shared
production Java library.

## Covered operations

| Producer | Operations and outcomes |
| --- | --- |
| Inventory | POST reserve: 201 RESERVED, 200 RESERVED replay, 200 RELEASED replay, 409 stored stock rejection, 409 changed-payload conflict |
| Inventory | GET reservation: 200 RESERVED/RELEASED, 409 stored stock rejection, 404 missing attempt |
| Inventory | PUT release: 200 RELEASED, followed by POST/GET RELEASED |
| Orders | POST create: 201 CONFIRMED, 201 REJECTED, 202 PENDING_INVENTORY |
| Orders | POST same-key replay: 200 CONFIRMED/REJECTED, 202 still pending; same-key changed valid payload: 409 idempotency-conflict |

InventoryConsumerContractIT validates real full-context MVC responses backed by
Postgres, including rejection replay after replenishment. OrderPublicContractIT
validates the real order controller, coordination, gateway and persistence with
Postgres; only the remote inventory HTTP server is a fixture.
The order InventoryContractTest serves the same inventory JSON through
JDK HttpServer to the actual RestInventoryGateway and asserts typed outcomes.
No mocked producer response or regenerated DTO comparison substitutes for the
real-producer checks.

Checks pin HTTP status, required fields and JSON types, exact stable problem
types, reservation states, identity and line contents, and stored shortage
snapshots. Order checks also pin monetary values, nullable rejection/recovery
fields, Location and Retry-After (including absence on terminal responses).
Timestamps must parse, but their literal sample values are not requirements.
Inventory uses a fixed request identity; generated order IDs must be UUIDs and
remain stable across replay. The order HTTP fixture substitutes only that dynamic
identity into inventory response examples.

## Running independently

Both service POMs copy JSON using paths relative to `project.basedir`; tests load
classpath resources, never paths relative to the shell working directory.
Existing service-local test resources remain included (notably scheduler disable).

```sh
mvn -f inventory-service/pom.xml -B -Dtest=InventoryContractTest -Dit.test=InventoryConsumerContractIT verify
mvn -f order-service/pom.xml -B -Dtest=InventoryContractTest -Dit.test=OrderPublicContractIT verify
make verify
```

The normal suite asserts rejection of missing orderId, text quantity, and a wrong
stock-unavailable problem URI, both on mutated real inventory responses and via
the actual gateway. To observe an intentionally red producer-validation command:

```sh
mvn -f inventory-service/pom.xml -B -Dtest=InventoryContractTest -Dit.test=InventoryConsumerContractIT -Dcontract.mutationEvidence=true verify
```

This opt-in run must fail with three assertion failures. Omit the property for
green; no fixture edits or disabled tests are needed.

## Limits

This is local executable compatibility evidence, not a contract broker, exhaustive
OpenAPI validation, deployed cross-service test, or mixed-version guarantee.
Full-context MVC executes real producer code without a producer TCP listener;
gateway tests exercise real HTTP against the fixture. Required values are checked,
while unconsumed top-level fields and human-readable problem text are ignored.
Line count/order and the shortage snapshot are exact for these one-SKU examples.
Broader SKU ordering, validation boundaries, concurrency, timeouts, circuit/retry,
restart recovery and migration behavior remain in the existing specialized suites.
Order GET, historical states, blocked-recovery issue variants, correlation headers,
inventory stock lookup, and release error cases are outside these v1 expectations.
Changing a fixture is a consumer-contract decision, not an automatic regeneration
step when a producer test fails.
