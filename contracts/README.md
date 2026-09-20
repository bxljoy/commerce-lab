# Consumer contract evidence

These hand-maintained v1 JSON expectations are independent of OpenAPI generation
and producer DTOs. Inventory expectations preserve the public reserve/release API
contract; order expectations represent a public API client. The active order service does not
consume inventory HTTP responses. They are examples plus deliberately
small, explicit Jackson/AssertJ checks, not a general schema engine or a shared
production Java library.

## Covered operations

| Producer | Operations and outcomes |
| --- | --- |
| Inventory | POST reserve: 201 RESERVED, 200 RESERVED replay, 200 RELEASED replay, 409 stored stock rejection, 409 changed-payload conflict |
| Inventory | GET reservation: 200 RESERVED/RELEASED, 409 stored stock rejection, 404 missing attempt |
| Inventory | PUT release: 200 RELEASED, followed by POST/GET RELEASED |
| Orders | POST create: 202 PENDING_INVENTORY |
| Orders | POST same-key replay: historical 200 CONFIRMED/REJECTED, 202 still pending; same-key changed valid payload: 409 idempotency-conflict |

InventoryConsumerContractIT validates real full-context MVC responses backed by
Postgres, including rejection replay after replenishment. OrderPublicContractIT
validates the real order controller and atomic order/request/outbox persistence
with Postgres, without an inventory server. Historical terminal rows are deliberate
DB fixtures, not newly confirmed/rejected orders. The retired order HTTP gateway
contract suite remains reproducible at the Phase 3B baseline linked in the README.
No mocked producer response or regenerated DTO comparison substitutes for the
real-producer checks.

Checks pin HTTP status, required fields and JSON types, exact stable problem
types, reservation states, identity and line contents, and stored shortage
snapshots. Order checks also pin monetary values, nullable rejection/recovery
fields, Location and Retry-After (including absence on terminal responses).
Timestamps must parse, but their literal sample values are not requirements.
Inventory uses a fixed request identity; generated order IDs must be UUIDs and
remain stable across replay. Historical replay must retain its identity and create
no outbox row.

## Running independently

Both service POMs copy JSON using paths relative to `project.basedir`; tests load
classpath resources, never paths relative to the shell working directory.
Existing service-local test resources remain included (notably scheduler disable).

```sh
mvn -f inventory-service/pom.xml -B -Dtest=InventoryContractTest -Dit.test=InventoryConsumerContractIT verify
mvn -f order-service/pom.xml -B -Dtest=OrderPlacedEventTest -Dit.test=OrderPublicContractIT verify
make verify
```

The normal suite asserts rejection of missing orderId, text quantity, and a wrong
stock-unavailable problem URI, on mutated real inventory responses and local
inventory fixture checks. To observe an intentionally red producer-validation command:

```sh
mvn -f inventory-service/pom.xml -B -Dtest=InventoryContractTest -Dit.test=InventoryConsumerContractIT -Dcontract.mutationEvidence=true verify
```

This opt-in run must fail with three assertion failures. Omit the property for
green; no fixture edits or disabled tests are needed.

## OrderPlaced event v1

`events/orders/v1/order-placed.schema.json` and `order-placed.json` define the
accepted-and-persisted event, not inventory reservation. Destination is
`commerce.orders.v1`; the key is the `orderId` UUID string. The creation-time
envelope and ordered SKU/quantity snapshot exclude customer details, prices,
request keys, and HTTP headers. Outbox storage retains the exact serialized JSON
text; publication retries must not reconstruct it.

`OrderPlacedEventTest` checks schema fields/types/constants and applies explicit
Jackson tree assertions to both the fixture and factory output, including
missing event ID, wrong version, and string quantity mutations. These are focused
contract checks, not a general JSON Schema validator or Kafka publication proof.
`OutboxMigrationIT` checks guarded upgrades, database constraints, and insertion
in the caller's transaction with real PostgreSQL.

```sh
mvn -f order-service/pom.xml -Dtest=OrderPlacedEventTest test
mvn -f order-service/pom.xml -Dtest=OrderPlacedEventTest -Dit.test=OutboxMigrationIT,FlywayMigrationIT verify
```

## Inventory result events v1

`events/inventory/v1/inventory-reserved.schema.json` and
`inventory-rejected.schema.json`, with their adjacent JSON examples, define
`InventoryReserved` and `InventoryRejected`. Both use `commerce.inventory.v1`
and canonical lowercase `orderId` keys. The unchanged OrderPlaced v1 source is
`commerce.orders.v1`. Both topics have three partitions, RF1 and seven-day delete
retention; schemas do not imply indefinite delivery availability.

The result envelope contains `eventId`, `eventType`, integer `schemaVersion: 1`,
`occurredAt`, `orderId`, `correlationId`, `causationId` and ordered `lines`.
The causation ID is the original OrderPlaced event ID, not the result's ID.
IDs/timestamp/key/payload are generated once and persisted unchanged for retries;
new results emit UTC timestamps. Customer data, prices, HTTP request keys and
exception text are excluded. Shared UUID values accept full-width hex in either
case; incoming timestamps accept valid RFC3339 offsets, while keys remain canonical.

Reserved results have no reason/shortages. Rejected results require
`reasonCode: INSUFFICIENT_STOCK` and nonempty `shortages`: each distinct SKU must
refer to a requested line with the same `requested` quantity and
`0 <= available < requested`. Unknown stock uses available zero. Public order
rejectionReason remains `STOCK_UNAVAILABLE` for HTTP compatibility.

Service-local strict decoders reject tombstones, malformed/duplicate-key JSON,
unknown fields, unsupported type/version, missing fields, scalar coercion,
overflow, blank/duplicate SKUs and key/order disagreement. Decoders and mutation
tests enforce cross-field constraints in addition to the written schemas.
Results must match persisted causation, correlation and ordered SKU/quantity
content before acceptance. Parsed JSON inbox equality ignores object member
order but preserves arrays and actual JSON values; it is not typed UUID/time
normalization. Changed content with the same event ID is a protocol conflict.

```sh
mvn -f inventory-service/pom.xml -Dtest=EventContractTest test
mvn -f order-service/pom.xml -Dtest=InventoryResultContractTest test
python3 -B scripts/fixtures/test-async-completion-proof.py
make verify-async-completion
```

The Python suite checks proof mechanics, not actual crashes. The image proof
executes real HTTP, PostgreSQL and Kafka with two consumer SIGKILL boundaries;
see the [verification scoreboard](../docs/notes-verification.md#phase-4b-workflow-recovery)
for stable identities/offsets and limitations. Unknown/poison inputs are not
silently discarded: they block their partition pending operator correction or
future repair tooling. HTTP reserve/release remains independently tested, but
manual release of async-owned reservations is unsupported until compensation.

## HTTP Contract Limits

This is local executable compatibility evidence, not a contract broker, exhaustive
OpenAPI validation, deployed cross-service test, or mixed-version guarantee.
Full-context MVC executes real producer code without a producer TCP listener;
KafkaOutboxIT checks actual stored payload publication to a real Kafka broker;
the image recovery proof observes identical raw payloads at distinct offsets.
Required values are checked, while unconsumed top-level fields and human-readable
problem text are ignored.
Line count/order and the shortage snapshot are exact for these one-SKU examples.
Broader SKU ordering, validation boundaries, concurrency, relay timeouts/retries,
restart recovery and migration behavior remain in the specialized suites. The
retired synchronous HTTP timeout/circuit suites belong to the Phase 3B baseline.
Order GET, historical PLACED, blocked-recovery issue variants, correlation headers,
inventory stock lookup, and release error cases are outside these v1 expectations.
Changing a fixture is a consumer-contract decision, not an automatic regeneration
step when a producer test fails.
