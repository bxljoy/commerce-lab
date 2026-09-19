"""Real HTTP -> durable outbox -> Kafka proof with Docker-controlled SIGKILL."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import uuid

spec = importlib.util.spec_from_file_location("runtime", Path(__file__).with_name("runtime-proof.py"))
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


compose = runtime.compose
docker = runtime.docker
Deadline = runtime.Deadline


def sql(query, inventory=False, deadline=None):
    service, user, database = (("inventory-postgres", "inventory", "inventory") if inventory
                               else ("postgres", "order", "orderdb"))
    return compose("exec", "-T", service, "psql", "-U", user, "-d", database,
                   "-v", "ON_ERROR_STOP=1", "-Atc", query, timeout=10, deadline=deadline)


def row(order_id, deadline=None):
    rows = json.loads(sql("SELECT coalesce(json_agg(t), '[]') FROM "
                         f"(SELECT * FROM order_outbox WHERE order_id='{uuid.UUID(order_id)}') t",
                         deadline=deadline))
    assert len(rows) == 1, rows
    return rows[0]


def immutable(value):
    return {key: value[key] for key in ("event_id", "order_id", "event_type", "schema_version",
                                       "topic", "message_key", "payload", "created_at")}


def parse_records(output, event_id):
    records = {}
    for line in output.splitlines():
        if not line.startswith("Partition:"):
            continue
        partition, offset, key, value = line.split("\t", 3)
        payload = json.loads(value)
        if payload.get("eventId") != event_id:
            continue
        record = {"partition": int(partition.removeprefix("Partition:")),
                  "offset": int(offset.removeprefix("Offset:")), "key": key, "value": value}
        records[(record["partition"], record["offset"])] = record
    return list(records.values())


def ack_boundary(logs, event_id):
    for line in logs.splitlines():
        if not line.startswith("{"):
            continue
        try:
            marker = json.loads(line)
        except json.JSONDecodeError:
            continue
        if marker.get("marker") == "ACK_BOUNDARY" and marker.get("eventId") == event_id:
            return True
    return False


def require_crash_boundary(marker, records, delivered):
    assert marker and records and not delivered, (marker, records, delivered)


def observe(event_id, count, deadline=None):
    deadline = deadline or Deadline(180, f"Kafka event={event_id} expected={count}")
    while True:
        # A fresh group from earliest can reread records; only distinct coordinates count.
        output = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                         "--bootstrap-server", "kafka:9092", "--topic", "commerce.orders.v1",
                         "--group", "proof-" + str(uuid.uuid4()), "--from-beginning",
                         "--timeout-ms", "5000", "--property", "print.key=true",
                         "--property", "print.partition=true", "--property", "print.offset=true",
                         timeout=25, deadline=deadline)
        records = parse_records(output, event_id)
        if len(records) >= count:
            for record in records:
                print("KAFKA_RECORD " + json.dumps(record, sort_keys=True), flush=True)
            return records
        deadline.sleep()


def await_row(order_id, predicate, deadline=None):
    deadline = deadline or Deadline(180, f"delivery order={order_id}")
    while True:
        value = row(order_id, deadline=deadline)
        if predicate(value):
            return value
        deadline.sleep()


def logs(deadline=None):
    return compose("logs", "--no-color", "--no-log-prefix", "order-service", timeout=10, deadline=deadline)


def await_marker(event_id, deadline=None):
    deadline = deadline or Deadline(180, f"ACK_BOUNDARY event={event_id}")
    while not ack_boundary(logs(deadline=deadline), event_id):
        deadline.sleep()


def recreate(enabled, event_id=None):
    deadline = Deadline(120, "order recreation, inspection and endpoint")
    os.environ.update(OUTBOX_ENABLED=str(enabled).lower(),
                      PROOF_PROFILE="outbox-proof" if event_id else "default",
                      PROOF_ENABLED=str(event_id is not None).lower(), PROOF_EVENT_ID=event_id or "")
    # Normal recovery uses the base deployment, with no proof environment or profile at all.
    files = [] if event_id else ["-f", "docker-compose.yml"]
    compose(*files, "up", "-d", "--no-deps", "--force-recreate", "--wait", "--wait-timeout", "120",
            "order-service", deadline=deadline)
    if event_id is None:
        container = compose("ps", "-q", "order-service", timeout=10, deadline=deadline)
        environment = json.loads(docker("inspect", "--format", "{{json .Config.Env}}", container,
                                        timeout=10, deadline=deadline))
        assert not any(item.startswith(("ORDER_OUTBOX_PROOF_", "SPRING_PROFILES_ACTIVE="))
                       for item in environment), environment
        print("NORMAL_CONFIGURATION proofEnvironment=absent profile=absent", flush=True)
    return runtime.order_url(deadline=deadline)


def kill():
    deadline = Deadline(120, "kill and exit inspection")
    container = compose("ps", "-q", "order-service", timeout=10, deadline=deadline)
    compose("kill", "-s", "SIGKILL", "order-service", deadline=deadline)
    state = json.loads(docker("inspect", "--format", "{{json .State}}", container,
                              timeout=10, deadline=deadline))
    assert not state["Running"] and state["ExitCode"] == 137, state
    print("SIGKILL " + json.dumps({"container": container, "exitCode": state["ExitCode"]}), flush=True)


def create(base, label):
    key = str(uuid.uuid4())
    payload = {"customerId": "outbox-" + label, "currency": "EUR", "lines": [
        {"sku": "SKU-APPLE", "quantity": 2, "unitPrice": 9.99},
        {"sku": "SKU-BANANA", "quantity": 1, "unitPrice": 4}]}
    created, headers = runtime.request(base, "/api/v1/orders", 202, "POST", payload, key)
    assert created["status"] == "PENDING_INVENTORY", created
    assert headers["Location"] == "/api/v1/orders/" + created["id"]
    value = row(created["id"])
    assert value["attempt_count"] == 0 and value["delivered_at"] is None, value
    envelope = json.loads(value["payload"])
    assert envelope["eventId"] == value["event_id"] and envelope["orderId"] == created["id"]
    assert envelope["lines"] == [{"sku": line["sku"], "quantity": line["quantity"]} for line in payload["lines"]]
    print("HTTP_CREATED " + json.dumps({"case": label, "status": 202, "orderId": created["id"],
          "eventId": value["event_id"], "outboxRows": 1, "attemptCount": 0}), flush=True)
    return created["id"], value


def assert_records(records, original):
    for record in records:
        assert record["key"] == original["message_key"] == original["order_id"], record
        assert record["value"] == original["payload"], record
    assert len({(r["partition"], r["offset"]) for r in records}) == len(records)


def inventory_snapshot(deadline=None):
    return json.loads(sql("SELECT json_build_object('stock', (SELECT json_agg(s ORDER BY sku) FROM stock s), "
                          "'attempts', (SELECT count(*) FROM inventory_reservation_attempts), "
                          "'reservations', (SELECT count(*) FROM inventory_reservations), "
                          "'lines', (SELECT count(*) FROM inventory_reservation_lines))",
                          inventory=True, deadline=deadline))


def no_effect(base, order_ids, before, deadline=None):
    after = inventory_snapshot(deadline=deadline)
    assert after == before, (before, after)
    for order_id in order_ids:
        value = runtime.order(base, order_id, "PENDING_INVENTORY", deadline=deadline)
        assert value["recoveryIssue"] is None, value
        assert sql(f"SELECT attempt_count FROM orders WHERE id='{uuid.UUID(order_id)}'", deadline=deadline) == "0"
    print("NO_INVENTORY_EFFECT " + json.dumps(after, sort_keys=True), flush=True)


def start_broker():
    deadline = Deadline(120, "broker startup and topic provisioning")
    compose("up", "-d", "--wait", "--wait-timeout", "120", "kafka", deadline=deadline)
    compose("up", "-d", "topic-init", deadline=deadline)
    while True:
        init = compose("ps", "-aq", "topic-init", timeout=10, deadline=deadline)
        state = json.loads(docker("inspect", "--format", "{{json .State}}", init,
                                  timeout=10, deadline=deadline))
        if state["Status"] == "exited":
            assert state["ExitCode"] == 0, state
            break
        deadline.sleep()
    description = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                          "--bootstrap-server", "kafka:9092", "--describe", "--topic", "commerce.orders.v1",
                          timeout=10, deadline=deadline)
    assert "PartitionCount: 3" in description and "ReplicationFactor: 1" in description, description
    assert "retention.ms=604800000" in description and "cleanup.policy=delete" in description, description
    print("TOPIC " + description, flush=True)


def main():
    setup = Deadline(120, "initial endpoint and service inspection")
    base = runtime.order_url(deadline=setup)
    runtime.assert_services(["postgres", "order-service", "inventory-postgres", "inventory-service"], deadline=setup)
    before = inventory_snapshot(deadline=setup)
    assert before["attempts"] == before["reservations"] == before["lines"] == 0, before
    print("INVENTORY_BEFORE " + json.dumps(before, sort_keys=True), flush=True)

    # A: actual HTTP commit, relay disabled, no broker. Not an instruction-level HTTP crash.
    first_id, first = create(base, "A-before-publication")
    kill()
    assert immutable(row(first_id)) == immutable(first)
    start_broker()
    base = recreate(True)
    recovery = Deadline(180, "A publication and delivery observation")
    records = observe(first["event_id"], 1, deadline=recovery)
    assert len(records) == 1, records
    assert_records(records, first)
    delivered = await_row(first_id, lambda r: r["delivered_at"] is not None, deadline=recovery)
    assert immutable(delivered) == immutable(first)
    assert "ACK_BOUNDARY" not in logs(deadline=recovery)
    print("DELIVERED_A " + json.dumps(delivered, sort_keys=True), flush=True)
    no_effect(base, [first_id], before, deadline=recovery)

    # B: pause only the fresh selected event after broker ack, then kill the actual JVM container.
    base = recreate(False)
    second_id, second = create(base, "B-acknowledged-before-delivery")
    base = recreate(True, second["event_id"])
    boundary = Deadline(180, "B marker, first record and undelivered boundary")
    await_marker(second["event_id"], deadline=boundary)
    first_records = observe(second["event_id"], 1, deadline=boundary)
    assert len(first_records) == 1, first_records
    assert_records(first_records, second)
    paused = row(second_id, deadline=boundary)
    require_crash_boundary(ack_boundary(logs(deadline=boundary), second["event_id"]), first_records,
                           paused["delivered_at"] is not None)
    assert paused["attempt_count"] == 1 and paused["lease_token"] and paused["lease_until"], paused
    print("ACK_BOUNDARY " + json.dumps({"eventId": second["event_id"], "orderId": second_id,
          "deliveredAt": None, "attemptCount": 1, "leaseUntil": paused["lease_until"]}), flush=True)
    print("PROOF_CONTAINER_LOGS\n" + logs(deadline=boundary), flush=True)
    kill()
    base = recreate(True)
    recovery = Deadline(180, "B duplicate publication and delivery observation")
    duplicated = observe(second["event_id"], 2, deadline=recovery)
    assert len(duplicated) == 2 and len({r["partition"] for r in duplicated}) == 1, duplicated
    assert first_records[0] in duplicated
    assert_records(duplicated, second)
    delivered = await_row(second_id, lambda r: r["delivered_at"] is not None, deadline=recovery)
    assert delivered["attempt_count"] == 2, delivered
    assert immutable(delivered) == immutable(second)
    assert "ACK_BOUNDARY" not in logs(deadline=recovery), "Proof hook active in normal restart"
    print("DELIVERED_B " + json.dumps(delivered, sort_keys=True), flush=True)
    no_effect(base, [first_id, second_id], before, deadline=recovery)
    assert sql("SELECT count(*) || ':' || count(delivered_at) FROM order_outbox", deadline=recovery) == "2:2"
    print("NORMAL_CONTAINER_LOGS\n" + logs(deadline=recovery), flush=True)
    print("PASS real HTTP crash experiments: orders=2 outboxRows=2 delivered=2 KafkaRecords=3; "
          "B distinct offsets=2 identical ID/key/payload; pending=2 inventory effects=0", flush=True)


if __name__ == "__main__":
    {"run": main, "broker": start_broker}[sys.argv[1] if len(sys.argv) > 1 else "run"]()
