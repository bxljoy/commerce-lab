"""Real HTTP/Kafka/database workflow and external consumer PID1 SIGKILL proofs."""
import importlib.util
import json
import os
import subprocess
from pathlib import Path
import uuid

spec = importlib.util.spec_from_file_location("outbox", Path(__file__).with_name("outbox-proof.py"))
outbox = importlib.util.module_from_spec(spec)
spec.loader.exec_module(outbox)
runtime = outbox.runtime
compose, docker, Deadline = runtime.compose, runtime.docker, runtime.Deadline
sql = outbox.sql
TOPICS = {"inventory": "commerce.orders.v1", "order": "commerce.inventory.v1"}
GROUPS = {"inventory": "commerce-inventory-order-placed-v1", "order": "commerce-order-inventory-result-v1"}


def emit(label, **metadata):
    print(label + " " + json.dumps(metadata, sort_keys=True), flush=True)


def boundary(logs, event_id):
    return f"DB_COMMIT_BOUNDARY eventId={event_id} outcome=APPLIED" in logs.splitlines()


def parse_offset(output, group, topic, partition):
    rows = [line.split() for line in output.splitlines()]
    rows = [row for row in rows if len(row) >= 4 and row[:3] == [group, topic, str(partition)]]
    assert len(rows) == 1, (group, topic, partition, rows)
    return None if rows[0][3] == "-" else int(rows[0][3])


def require_duplicates(records):
    assert len(records) >= 2
    assert len({(r["partition"], r["offset"]) for r in records}) == len(records), records
    assert len({r["partition"] for r in records}) == 1
    assert len({(r["key"], r["value"]) for r in records}) == 1


def require_killed(state):
    assert not state["Running"] and state["ExitCode"] == 137, state


def require_totals(counts):
    assert counts == {"orders": 6, "placed": 6, "orderInbox": 6, "accepted": 6,
                      "inventoryInbox": 6, "attempts": 7, "reservations": 6, "lines": 6, "results": 6}, counts


def require_stock(stock, apple):
    assert stock == {"SKU-APPLE": apple, "SKU-BANANA": 5}, stock


def check_stock(apple, deadline):
    stock = {row["sku"]: row["available_quantity"]
             for row in outbox.inventory_snapshot(deadline=deadline)["stock"]}
    require_stock(stock, apple)
    emit("EXACT_STOCK", **stock)


def require_normal(environment):
    assert not any(item.startswith(("CONSUMER_PROOF_", "ORDER_OUTBOX_PROOF_", "SPRING_PROFILES_ACTIVE="))
                   for item in environment), "Proof environment survived normal restart"


def poll(action, predicate, deadline):
    while True:
        deadline.remaining()
        value = action()
        if predicate(value):
            return value
        deadline.sleep()


def logs(service, deadline):
    return compose("logs", "--no-color", "--no-log-prefix", service + "-service", timeout=10, deadline=deadline)


def recreate(service, deadline, enabled=True, event_id=None, publisher=True):
    prefix = service.upper()
    os.environ[prefix + "_EVENTS_ENABLED"] = str(enabled).lower()
    os.environ["INVENTORY_OUTBOX_ENABLED" if service == "inventory" else "OUTBOX_ENABLED"] = str(publisher).lower()
    os.environ.update({prefix + "_PROOF_PROFILE": "consumer-proof" if event_id else "default",
                       prefix + "_PROOF_ENABLED": str(event_id is not None).lower(),
                       prefix + "_PROOF_EVENT_ID": event_id or ""})
    # Base-only recreation removes proof settings entirely, not merely the activation flag.
    files = [] if event_id else ["-f", "docker-compose.yml"]
    compose(*files, "up", "-d", "--no-deps", "--force-recreate", "--wait", "--wait-timeout", "120",
            service + "-service", deadline=deadline)
    if event_id is None:
        container = compose("ps", "-q", service + "-service", timeout=10, deadline=deadline)
        environment = json.loads(docker("inspect", "--format", "{{json .Config.Env}}", container,
                                        timeout=10, deadline=deadline))
        require_normal(environment)
        assert "DB_COMMIT_BOUNDARY" not in logs(service, deadline)
        emit("NORMAL_CONFIGURATION", service=service, proofEnvironment="absent", profile="absent")


def kill(service, deadline):
    container = compose("ps", "-q", service + "-service", timeout=10, deadline=deadline)
    compose("kill", "-s", "SIGKILL", service + "-service", timeout=15, deadline=deadline)
    state = json.loads(docker("inspect", "--format", "{{json .State}}", container, timeout=10, deadline=deadline))
    require_killed(state)
    emit("SIGKILL", service=service, container=container, exitCode=state["ExitCode"], running=state["Running"])


def event_row(order_id, inventory, deadline):
    table = "inventory_result_outbox" if inventory else "order_outbox"
    rows = json.loads(sql(f"SELECT coalesce(json_agg(t),'[]') FROM (SELECT * FROM {table} "
                          f"WHERE order_id='{uuid.UUID(order_id)}') t", inventory=inventory, deadline=deadline))
    assert len(rows) <= 1
    return rows[0] if rows else None


def await_event(order_id, inventory, deadline, delivered=False):
    return poll(lambda: event_row(order_id, inventory, deadline),
                lambda r: r is not None and (not delivered or r["delivered_at"] is not None), deadline)


def observe(row, count, deadline):
    def read():
        output = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                         "--bootstrap-server", "kafka:9092", "--topic", row["topic"],
                         "--group", "proof-" + str(uuid.uuid4()), "--from-beginning",
                         "--timeout-ms", "5000", "--property", "print.key=true",
                         "--property", "print.partition=true", "--property", "print.offset=true",
                         timeout=25, deadline=deadline)
        return outbox.parse_records(output, row["event_id"])
    records = poll(read, lambda value: len(value) >= count, deadline)
    outbox.assert_records(records, row)
    for record in records:
        emit("KAFKA_RECORD", eventId=row["event_id"], orderId=row["order_id"], topic=row["topic"],
             partition=record["partition"], offset=record["offset"])
    return records


def group_offset(service, record, deadline):
    output = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                     "--bootstrap-server", "kafka:9092", "--describe", "--group", GROUPS[service],
                     timeout=15, deadline=deadline)
    return parse_offset(output, GROUPS[service], TOPICS[service], record["partition"])


def advanced(service, record, deadline):
    offset = poll(lambda: group_offset(service, record, deadline),
                  lambda offset: offset is not None and offset > record["offset"], deadline)
    emit("OFFSET_ADVANCED", service=service, group=GROUPS[service], partition=record["partition"],
         inputOffset=record["offset"], committedOffset=offset)


def duplicate_logged(service, event_id, deadline):
    def found(output):
        return any(f"eventId={event_id} " in line and "outcome=DUPLICATE" in line for line in output.splitlines())
    output = poll(lambda: logs(service, deadline), found, deadline)
    for line in output.splitlines():
        if f"eventId={event_id} " in line and "outcome=DUPLICATE" in line:
            print("REDELIVERY_LOG " + line, flush=True)


def snapshot(order_id, deadline):
    order_id = str(uuid.UUID(order_id))
    inventory = json.loads(sql("SELECT json_build_object("
        "'apple',(SELECT available_quantity FROM stock WHERE sku='SKU-APPLE'),"
        f"'inbox',(SELECT count(*) FROM inventory_event_inbox WHERE content->>'orderId'='{order_id}'),"
        f"'attempts',(SELECT count(*) FROM inventory_reservation_attempts WHERE order_id='{order_id}'),"
        f"'reservations',(SELECT count(*) FROM inventory_reservations WHERE order_id='{order_id}'),"
        f"'lines',(SELECT count(*) FROM inventory_reservation_lines WHERE order_id='{order_id}'),"
        f"'results',(SELECT count(*) FROM inventory_result_outbox WHERE order_id='{order_id}'))",
        inventory=True, deadline=deadline))
    order = json.loads(sql("SELECT json_build_object("
        f"'status',(SELECT status FROM orders WHERE id='{order_id}'),"
        f"'version',(SELECT version FROM orders WHERE id='{order_id}'),"
        f"'inbox',(SELECT count(*) FROM order_event_inbox WHERE content->>'orderId'='{order_id}'),"
        f"'accepted',(SELECT coalesce(json_agg(t),'[]') FROM (SELECT * FROM order_inventory_results WHERE order_id='{order_id}') t))",
        deadline=deadline))
    return {"inventory": inventory, "order": order}


def check_effect(order_id, apple, status, deadline):
    check_stock(apple, deadline)
    value = snapshot(order_id, deadline)
    inv, order = value["inventory"], value["order"]
    assert inv == {"apple": apple, "inbox": 1, "attempts": 1, "reservations": int(status == "CONFIRMED"),
                   "lines": int(status == "CONFIRMED"), "results": 1}, inv
    assert order["status"] == status and order["inbox"] == 1 and len(order["accepted"]) == 1, order
    emit("DB_EFFECT", orderId=order_id, **value)
    return value


def create(label, quantity, deadline):
    base = runtime.order_url(deadline=deadline)
    body = {"customerId": "async-proof", "currency": "EUR",
            "lines": [{"sku": "SKU-APPLE", "quantity": quantity, "unitPrice": 1}]}
    key = str(uuid.uuid4())
    created, headers = runtime.request(base, "/api/v1/orders", 202, "POST", body, key, deadline=deadline)
    assert created["status"] == "PENDING_INVENTORY" and headers["Location"].endswith(created["id"])
    row = await_event(created["id"], False, deadline, delivered=True)
    emit("HTTP_CREATED", case=label, orderId=created["id"], eventId=row["event_id"], status=202)
    return created["id"], row, body, key


def terminal(order_id, status, deadline, body=None, key=None):
    base = runtime.order_url(deadline=deadline)
    value = poll(lambda: runtime.request(base, "/api/v1/orders/" + order_id, 200, deadline=deadline)[0],
                 lambda value: value["status"] == status, deadline)
    if body is not None:
        replay, _ = runtime.request(base, "/api/v1/orders", 200, "POST", body, key, deadline=deadline)
        assert replay["id"] == order_id and replay["status"] == status
    emit("HTTP_TERMINAL", orderId=order_id, status=value["status"], rejectionReason=value.get("rejectionReason"))


def publish_duplicate(row, service, deadline):
    before = observe(row, 1, deadline)
    compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", "kafka:9092", "--topic", row["topic"],
            "--property", "parse.key=true", "--property", "key.separator=\t",
            input=row["message_key"] + "\t" + row["payload"] + "\n", timeout=20, deadline=deadline)
    records = observe(row, len(before) + 1, deadline)
    require_duplicates(records)
    advanced(service, max(records, key=lambda r: r["offset"]), deadline)
    duplicate_logged(service, row["event_id"], deadline)


def crash(service, order_id, row, deadline, apple):
    record, = observe(row, 1, deadline)
    recreate(service, deadline, event_id=row["event_id"])
    output = poll(lambda: logs(service, deadline), lambda output: boundary(output, row["event_id"]), deadline)
    for line in output.splitlines():
        if boundary(line, row["event_id"]):
            print("PROOF_LOG " + line, flush=True)
    before_offset = group_offset(service, record, deadline)
    assert before_offset is None or before_offset <= record["offset"], (before_offset, record)
    before = snapshot(order_id, deadline)
    inv = before["inventory"]
    assert inv == {"apple": apple, "inbox": 1, "attempts": 1, "reservations": 1, "lines": 1, "results": 1}, inv
    if service == "order":
        assert before["order"]["status"] == "CONFIRMED" and before["order"]["inbox"] == 1
        assert len(before["order"]["accepted"]) == 1
    result = await_event(order_id, True, deadline)
    emit("DB_COMMIT_BOUNDARY", service=service, orderId=order_id, eventId=row["event_id"],
         resultEventId=result["event_id"], partition=record["partition"], inputOffset=record["offset"],
         committedOffset=before_offset, snapshot=before)
    kill(service, deadline)
    recreate(service, deadline)
    advanced(service, record, deadline)
    duplicate_logged(service, row["event_id"], deadline)
    terminal(order_id, "CONFIRMED", deadline)
    after = check_effect(order_id, apple, "CONFIRMED", deadline)
    assert after["inventory"] == before["inventory"]
    if service == "order":
        assert after == before, (before, after)
    assert outbox.immutable(await_event(order_id, True, deadline)) == outbox.immutable(result)


def assert_dns_absent(service, deadline):
    try:
        compose("exec", "-T", service + "-service", "getent", "hosts", "kafka", timeout=10, deadline=deadline)
    except subprocess.CalledProcessError as failure:
        assert failure.returncode == 2, failure.returncode
    else:
        raise AssertionError("Stopped broker still has DNS")
    output = poll(lambda: logs(service, deadline), lambda text: "startup=retry" in text, deadline)
    for line in output.splitlines():
        if "startup=retry" in line:
            print("COLD_START_LOG " + line, flush=True)
    emit("DNS_ABSENT", service=service, hostname="kafka", getentExit=2)


def container_identity(service, deadline):
    container = compose("ps", "-q", service + "-service", timeout=10, deadline=deadline)
    inspected = json.loads(docker("inspect", "--format", "{{json .}}", container, timeout=10, deadline=deadline))
    require_normal(inspected["Config"]["Env"])
    assert service.upper() + "_EVENTS_ENABLED=true" in inspected["Config"]["Env"]
    assert inspected["State"]["Running"] and inspected["RestartCount"] == 0
    return {"id": container, "startedAt": inspected["State"]["StartedAt"], "restartCount": inspected["RestartCount"]}


def cold_http_work(deadline):
    base = runtime.order_url(deadline=deadline)
    body = {"customerId": "cold-proof", "currency": "EUR",
            "lines": [{"sku": "SKU-APPLE", "quantity": 1, "unitPrice": 1}]}
    created, _ = runtime.request(base, "/api/v1/orders", 202, "POST", body, str(uuid.uuid4()), deadline=deadline)
    placed = await_event(created["id"], False, deadline)
    assert placed["delivered_at"] is None
    runtime.order(base, created["id"], "PENDING_INVENTORY", deadline=deadline)
    emit("COLD_HTTP_COMMIT", service="order", orderId=created["id"], eventId=placed["event_id"], status=202)

    inventory = "http://" + compose("port", "inventory-service", "8081", timeout=10, deadline=deadline)
    reservation_id = str(uuid.uuid4())
    # Independent HTTP reservation, not a release of an async-workflow-owned order.
    reservation = {"orderId": reservation_id, "lines": [{"sku": "SKU-BANANA", "quantity": 1}]}
    runtime.request(inventory, "/api/v1/reservations", 201, "POST", reservation, deadline=deadline)
    stock, _ = runtime.request(inventory, "/api/v1/stock/SKU-BANANA", 200, deadline=deadline)
    assert stock["availableQuantity"] == 4
    runtime.request(inventory, "/api/v1/reservations/" + reservation_id + "/release", 200, "PUT", deadline=deadline)
    check_stock(5, deadline)
    assert sql(f"SELECT status FROM inventory_reservations WHERE order_id='{reservation_id}'",
               inventory=True, deadline=deadline) == "RELEASED"
    emit("COLD_HTTP_COMMIT", service="inventory", reservationId=reservation_id, reserveStatus=201,
         releaseStatus=200, bananaAfterReserve=4, bananaAfterRelease=5)
    return created["id"], placed


def recover_outage(order_id, pending, deadline):
    compose("stop", "-t", "10", "kafka", timeout=25, deadline=deadline)
    recreate("inventory", deadline)
    recreate("order", deadline)
    identities = {}
    for service in ("inventory", "order"):
        assert_dns_absent(service, deadline)
        identities[service] = container_identity(service, deadline)
    cold_id, placed = cold_http_work(deadline)
    failed = poll(lambda: event_row(order_id, True, deadline),
                  lambda row: row["last_error_code"] is not None, deadline)
    assert failed["delivered_at"] is None and failed["attempt_count"] >= 1
    assert outbox.immutable(failed) == outbox.immutable(pending)
    emit("BROKER_OUTAGE_PENDING", orderId=order_id, eventId=pending["event_id"],
         attempts=failed["attempt_count"], errorCode=failed["last_error_code"], deliveredAt=None)
    compose("start", "--wait", "--wait-timeout", "120", "kafka", deadline=deadline)
    terminal(order_id, "CONFIRMED", deadline)
    terminal(cold_id, "CONFIRMED", deadline)
    delivered = await_event(order_id, True, deadline, delivered=True)
    assert outbox.immutable(delivered) == outbox.immutable(pending)
    assert outbox.immutable(await_event(cold_id, False, deadline, delivered=True)) == outbox.immutable(placed)
    check_effect(order_id, 4, "CONFIRMED", deadline)
    check_effect(cold_id, 4, "CONFIRMED", deadline)
    for service in ("inventory", "order"):
        assert container_identity(service, deadline) == identities[service]
        emit("COLD_RECOVERY_NO_RESTART", service=service, eventsEnabled=True, **identities[service])


def main():
    # One hard aggregate budget is shared by every nested operation, including recreation.
    deadline = Deadline(1440, "all async completion cases")
    for topic in TOPICS.values():
        description = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                              "--bootstrap-server", "kafka:9092", "--describe", "--topic", topic,
                              timeout=15, deadline=deadline)
        assert all(part in description for part in ("PartitionCount: 3", "ReplicationFactor: 1",
                   "retention.ms=604800000", "cleanup.policy=delete")), description
        emit("TOPIC", topic=topic, partitions=3, replicationFactor=1, retentionMs=604800000)

    # Retained Phase 4A-shaped work is created by HTTP with both consumers disabled.
    check_stock(10, deadline)
    order_id, placed, body, key = create("retained-fresh-group-success", 2, deadline)
    observe(placed, 1, deadline)
    initial = snapshot(order_id, deadline)
    assert initial["inventory"]["apple"] == 10 and initial["inventory"]["inbox"] == 0
    assert initial["order"]["status"] == "PENDING_INVENTORY"
    groups = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                     "--bootstrap-server", "kafka:9092", "--list", timeout=15, deadline=deadline).splitlines()
    assert not set(GROUPS.values()).intersection(groups), groups
    emit("FRESH_GROUPS", absent=list(GROUPS.values()), retainedEventId=placed["event_id"])
    recreate("order", deadline)
    recreate("inventory", deadline)
    terminal(order_id, "CONFIRMED", deadline, body, key)
    before = check_effect(order_id, 8, "CONFIRMED", deadline)
    result = await_event(order_id, True, deadline, delivered=True)
    publish_duplicate(placed, "inventory", deadline)
    publish_duplicate(result, "order", deadline)
    assert check_effect(order_id, 8, "CONFIRMED", deadline) == before
    emit("DUPLICATES_STABLE", orderId=order_id, placedEventId=placed["event_id"], resultEventId=result["event_id"], **before)

    rejected, _, body, key = create("rejection", 99, deadline)
    terminal(rejected, "REJECTED", deadline, body, key)
    check_effect(rejected, 8, "REJECTED", deadline)

    recreate("inventory", deadline, enabled=False)
    inventory_id, placed, _, _ = create("inventory-crash", 1, deadline)
    crash("inventory", inventory_id, placed, deadline, 7)

    recreate("order", deadline, enabled=False)
    order_id, _, _, _ = create("order-crash", 1, deadline)
    result = await_event(order_id, True, deadline, delivered=True)
    assert snapshot(order_id, deadline)["order"]["status"] == "PENDING_INVENTORY"
    crash("order", order_id, result, deadline, 6)

    recreate("inventory", deadline, publisher=False)
    outage_id, _, _, _ = create("broker-outage", 1, deadline)
    pending = await_event(outage_id, True, deadline)
    assert pending["delivered_at"] is None and pending["attempt_count"] == 0
    recover_outage(outage_id, pending, deadline)
    counts = {}
    for inventory, tables in ((False, {"orders": "orders", "placed": "order_outbox",
                                       "orderInbox": "order_event_inbox", "accepted": "order_inventory_results"}),
                              (True, {"inventoryInbox": "inventory_event_inbox", "attempts": "inventory_reservation_attempts",
                                      "reservations": "inventory_reservations", "lines": "inventory_reservation_lines",
                                      "results": "inventory_result_outbox"})):
        for name, table in tables.items():
            counts[name] = int(sql(f"SELECT count(*) FROM {table}", inventory=inventory, deadline=deadline))
    require_totals(counts)
    emit("EXACT_TOTALS", **counts)
    for service in ("inventory", "order"):
        assert "DB_COMMIT_BOUNDARY" not in logs(service, deadline)
    emit("PASS", orders=6, confirmed=5, rejected=1, appleBefore=10, appleAfter=4,
         realConsumerSigkills=2, freshGroupRetainedIntake=True, enabledColdDnsRecovery=True,
         limitation="Retention-expired delivered events are not recovered or regenerated")


if __name__ == "__main__":
    main()
