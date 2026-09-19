"""Assertions against built service images; stdlib only, never imported by production."""
import json
import os
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
        {"restart": restart_proof, "inventory": inventory_proof}[sys.argv[1]]()
