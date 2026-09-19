"""Assertions against built service images; stdlib only, never imported by production."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid


class Deadline:
    def __init__(self, seconds, label):
        self.end = time.monotonic() + seconds
        self.label = label

    def remaining(self, cap=None):
        remaining = self.end - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Deadline exceeded: {self.label}")
        return remaining if cap is None else min(cap, remaining)

    def sleep(self):
        time.sleep(self.remaining(1))


def docker(*args, input=None, timeout=120, deadline=None):
    deadline = deadline or Deadline(timeout, "docker " + " ".join(args[:2]))
    output = subprocess.check_output(["docker", *args], input=input, text=True,
                                     timeout=deadline.remaining(timeout))
    deadline.remaining()
    return output.strip()


def compose(*args, input=None, timeout=120, deadline=None):
    return docker("compose", *args, input=input, timeout=timeout, deadline=deadline)


def order_url(deadline=None):
    # Docker may reassign an ephemeral published port on restart/start.
    url = "http://" + compose("port", "order-service", "8080", timeout=10, deadline=deadline)
    print(f"Order endpoint: {url}", flush=True)
    return url


def request(base, path, expected, method="GET", body=None, key=None, deadline=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(base + path, method=method, headers=headers,
                                 data=None if body is None else json.dumps(body).encode())
    try:
        response = urllib.request.urlopen(req, timeout=5 if deadline is None else deadline.remaining(5))
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read()
        if deadline is not None:
            deadline.remaining()
        assert response.status == expected, (path, expected, response.status, payload)
        return json.loads(payload), response.headers


def wait_health(base, deadline=None):
    deadline = deadline or Deadline(90, "service health")
    while True:
        deadline.remaining()
        try:
            request(base, "/actuator/health", 200, deadline=deadline)
            return
        except (OSError, AssertionError):
            deadline.sleep()


def stock(base):
    return {sku: request(base, "/api/v1/stock/" + sku, 200)[0]["availableQuantity"]
            for sku in ("SKU-APPLE", "SKU-BANANA")}


def order(base, order_id, status, deadline=None):
    value = request(base, "/api/v1/orders/" + order_id, 200, deadline=deadline)[0]
    assert value["id"] == order_id and value["status"] == status, value
    return value


def await_confirmed(base, order_id):
    deadline = Deadline(90, "order confirmation")
    while True:
        value = request(base, "/api/v1/orders/" + order_id, 200, deadline=deadline)[0]
        assert value["id"] == order_id and value["recoveryIssue"] is None, value
        if value["status"] == "CONFIRMED":
            return value
        assert value["status"] == "PENDING_INVENTORY", value
        deadline.sleep()


def assert_services(expected, deadline=None):
    actual = set(compose("ps", "--services", "--status", "running", timeout=10, deadline=deadline).splitlines())
    assert actual == set(expected), actual


def restart_proof():
    base = order_url()
    assert_services(["order-service", "postgres"])
    payload = {"customerId": "restart-check", "currency": "EUR", "lines": [
        {"sku": "SKU-FIRST", "quantity": 1, "unitPrice": 9.9999},
        {"sku": "SKU-SECOND", "quantity": 2, "unitPrice": 4.00}]}
    key = str(uuid.uuid4())
    created, headers = request(base, "/api/v1/orders", 202, "POST", payload, key)
    order_id = created["id"]
    assert created["status"] == "PENDING_INVENTORY", created
    assert headers["Location"] == "/api/v1/orders/" + order_id
    assert headers["Retry-After"] == "5"
    def outbox_snapshot():
        return compose("exec", "-T", "postgres", "psql", "-U", "order", "-d", "orderdb", "-Atc",
                       f"SELECT event_id || ':' || payload FROM order_outbox WHERE order_id='{order_id}' "
                       "AND delivered_at IS NULL AND attempt_count=0", timeout=10)
    snapshot = outbox_snapshot()
    assert snapshot and len(snapshot.splitlines()) == 1, snapshot
    startup = Deadline(120, "order restart and health")
    compose("restart", "order-service", deadline=startup)
    base = order_url(deadline=startup)
    wait_health(base, deadline=startup)
    actual = order(base, order_id, "PENDING_INVENTORY")
    assert actual["currency"] == "EUR" and actual["totalAmount"] == 17.9999, actual
    assert actual["lines"] == created["lines"], actual
    replay, _ = request(base, "/api/v1/orders", 202, "POST", payload, key)
    assert replay["id"] == order_id, replay
    assert outbox_snapshot() == snapshot
    assert_services(["order-service", "postgres"])
    print(f"PASS isolated pending restart: sameID={order_id}, eventID={snapshot.split(':', 1)[0]}, "
          "outboxRows=1 attempts=0 immutable payload, total=17.9999, lines preserved; relay disabled, no broker/inventory")


def inventory_proof():
    base = os.environ["INVENTORY_URL"]
    assert_services(["inventory-service", "inventory-postgres"])
    order_id = str(uuid.uuid4())
    payload = {"orderId": order_id, "lines": [
        {"sku": "SKU-APPLE", "quantity": 3}, {"sku": "SKU-BANANA", "quantity": 2}]}
    before = stock(base)
    assert before == {"SKU-APPLE": 10, "SKU-BANANA": 5}, before
    for expected in (201, 200):
        reserved = request(base, "/api/v1/reservations", expected, "POST", payload)[0]
        assert reserved["status"] == "RESERVED" and reserved["lines"] == payload["lines"], reserved
    assert request(base, "/api/v1/reservations/" + order_id, 200)[0]["status"] == "RESERVED"
    assert stock(base) == {"SKU-APPLE": 7, "SKU-BANANA": 3}
    for _ in range(2):
        released = request(base, f"/api/v1/reservations/{order_id}/release", 200, "PUT")[0]
        assert released["status"] == "RELEASED" and released["releasedAt"], released
    assert request(base, "/api/v1/reservations", 200, "POST", payload)[0]["status"] == "RELEASED"
    rejected_id = str(uuid.uuid4())
    rejected = {"orderId": rejected_id, "lines": [{"sku": "SKU-APPLE", "quantity": 11}]}
    rejection = request(base, "/api/v1/reservations", 409, "POST", rejected)[0]
    assert rejection["type"].endswith("stock-unavailable"), rejection
    assert request(base, "/api/v1/reservations/" + rejected_id, 409)[0]["type"] == rejection["type"]
    request(base, "/api/v1/reservations/" + str(uuid.uuid4()), 404)
    assert stock(base) == before
    print(f"PASS inventory image: new201/replay200, GET200/409/404, release twice + released replay; stock restored={before}")


def network_proof():
    shared = os.environ["COMPOSE_PROJECT_NAME"] + "_service-network"
    for service in ("order-service", "inventory-service", "postgres", "inventory-postgres"):
        container = compose("ps", "-q", service)
        networks = json.loads(docker("inspect", "--format", "{{json .NetworkSettings.Networks}}", container,
                                     timeout=10))
        assert (shared in networks) == service.endswith("-service"), (service, networks)
    # Direct app-to-app DNS/HTTP, independent of the injected proxy route.
    compose("exec", "-T", "order-service", "curl", "--max-time", "5", "-fsS",
            "http://inventory-service:8081/actuator/health")
    print("PASS live networks: apps communicate, neither database attached to service-network", flush=True)


def sync_proof():
    base, inventory, proxy = (os.environ[key] for key in ("ORDER_URL", "INVENTORY_URL", "PROXY_URL"))
    network_proof()
    before = stock(inventory)
    assert before == {"SKU-APPLE": 10, "SKU-BANANA": 5}, before
    key = str(uuid.uuid4())
    payload = {"customerId": "lost-response", "currency": "EUR", "lines": [
        {"sku": "SKU-APPLE", "quantity": 2, "unitPrice": 9.99},
        {"sku": "SKU-BANANA", "quantity": 1, "unitPrice": 4}]}
    created, headers = request(base, "/api/v1/orders", 202, "POST", payload, key)
    order_id = created["id"]
    assert created["status"] == "PENDING_INVENTORY" and headers["Retry-After"] == "5", created
    stats = request(proxy, "/__control", 200)[0]
    assert [event["status"] for event in stats["dropped"]] == [201, 200], stats
    assert all(event["orderId"] == order_id for event in stats["dropped"]), stats
    assert len(stats["forwarded"]) == 2, stats
    reservation = request(inventory, "/api/v1/reservations/" + order_id, 200)[0]
    assert reservation["orderId"] == order_id and reservation["status"] == "RESERVED", reservation
    reserved = stock(inventory)
    assert reserved == {"SKU-APPLE": 8, "SKU-BANANA": 4}, reserved
    replay = request(base, "/api/v1/orders", 202, "POST", payload, key)[0]
    assert replay["id"] == order_id
    assert request(proxy, "/__control", 200)[0]["forwarded"] == stats["forwarded"]
    compose("restart", "order-service")
    base = order_url()
    wait_health(base)
    order(base, order_id, "PENDING_INVENTORY")
    assert stock(inventory) == reserved
    request(proxy, "/__control", 200, "POST", {"mode": "pass"})
    await_confirmed(base, order_id)
    terminal = request(base, "/api/v1/orders", 200, "POST", payload, key)[0]
    assert terminal["id"] == order_id and terminal["status"] == "CONFIRMED", terminal
    assert stock(inventory) == reserved
    stats = request(proxy, "/__control", 200)[0]
    assert len(stats["dropped"]) == 2, stats
    assert len([event for event in stats["forwarded"] if event["method"] == "POST"]) == 2, stats
    print(f"PASS response loss: dropped=2 upstreamStatuses=[201,200] sameID={order_id} pending->restart->confirmed stockBefore={before} stockAfterReserve={reserved} stockAfterRecovery={stock(inventory)}", flush=True)

    # A controlled DB fixture represents committed local creation before ANY HTTP.
    compose("stop", "order-service")
    fixture_id, fixture_key = str(uuid.uuid4()), str(uuid.uuid4())
    fixture_payload = {"customerId": "before-attempt-fixture", "currency": "EUR", "lines": [
        {"sku": "SKU-APPLE", "quantity": 1, "unitPrice": "2.5"}]}
    canonical = json.dumps(fixture_payload, separators=(",", ":"))
    compose("exec", "-T", "postgres", "psql", "-U", "order", "-d", "orderdb", "-v", "ON_ERROR_STOP=1",
            "-v", "id=" + fixture_id, "-v", "line_id=" + str(uuid.uuid4()), "-v", "key=" + fixture_key,
            "-v", "canonical=" + canonical, "-v", "fingerprint=" + hashlib.sha256(canonical.encode()).hexdigest(),
            input=Path("scripts/fixtures/pending-before-attempt.sql").read_text())
    assert compose("exec", "-T", "postgres", "psql", "-U", "order", "-d", "orderdb", "-Atc",
                   f"SELECT status || ':' || attempt_count FROM orders WHERE id='{fixture_id}'") == "PENDING_INVENTORY:0"
    request(inventory, "/api/v1/reservations/" + fixture_id, 404)
    assert stock(inventory) == reserved
    assert all(event.get("orderId") != fixture_id for event in stats["forwarded"])
    compose("start", "order-service")
    base = order_url()
    wait_health(base)
    await_confirmed(base, fixture_id)
    fixture_payload["lines"][0]["unitPrice"] = 2.5
    replay = request(base, "/api/v1/orders", 200, "POST", fixture_payload, fixture_key)[0]
    assert replay["id"] == fixture_id and replay["status"] == "CONFIRMED", replay
    assert request(inventory, "/api/v1/reservations/" + fixture_id, 200)[0]["status"] == "RESERVED"
    after = stock(inventory)
    assert after == {"SKU-APPLE": 7, "SKU-BANANA": 4}, after
    events = request(proxy, "/__control", 200)[0]["forwarded"]
    fixture_events = [event for event in events if event.get("orderId") == fixture_id
                      or event["path"] == "/api/v1/reservations/" + fixture_id]
    assert [(event["method"], event["status"]) for event in fixture_events] == [("GET", 404), ("POST", 201)], fixture_events
    print(f"PASS pre-attempt DB fixture (NOT HTTP crash injection): sameID={fixture_id}, attemptCountBefore=0, remoteBefore=404, recovery=GET404+POST201, stockBefore={reserved} stockAfter={after}")

    # Normal real-producer terminal paths, separate from the fault stock checkpoints.
    for quantity, expected_status in ((1, "CONFIRMED"), (100, "REJECTED")):
        intent = {"customerId": "terminal-smoke", "currency": "EUR", "lines": [
            {"sku": "SKU-BANANA", "quantity": quantity, "unitPrice": 4}]}
        terminal_key = str(uuid.uuid4())
        terminal = request(base, "/api/v1/orders", 201, "POST", intent, terminal_key)[0]
        assert terminal["status"] == expected_status, terminal
        replay = request(base, "/api/v1/orders", 200, "POST", intent, terminal_key)[0]
        assert replay["id"] == terminal["id"] and replay["status"] == expected_status, replay
        assert stock(inventory) == {"SKU-APPLE": 7, "SKU-BANANA": 3}
    assert terminal["rejectionReason"] == "STOCK_UNAVAILABLE", terminal
    print("PASS real-producer terminal order responses: new201 CONFIRMED/REJECTED, replay200, rejection/replay leave stock unchanged")


if __name__ == "__main__":
    if sys.argv[1] == "command":
        # Streaming shell commands get a wall-clock bound without swallowing their diagnostics.
        try:
            result = subprocess.run(sys.argv[3:], timeout=float(sys.argv[2]))
            sys.exit(result.returncode if result.returncode >= 0 else 128 - result.returncode)
        except subprocess.TimeoutExpired:
            print("Command deadline exceeded: " + " ".join(sys.argv[3:]), file=sys.stderr)
            sys.exit(124)
    elif sys.argv[1] == "health":
        wait_health(sys.argv[2])
    else:
        {"restart": restart_proof, "inventory": inventory_proof, "sync": sync_proof}[sys.argv[1]]()
