"""Deployment regression guard; accepts `docker compose config --format json`."""
import json
import sys

services = json.load(sys.stdin)["services"]
for name in ("order-service", "inventory-service"):
    assert "service-network" in services[name]["networks"], name
for name in ("postgres", "inventory-postgres"):
    assert "service-network" not in services[name]["networks"], name
assert set(services["order-service"]["depends_on"]) == {"postgres"}
assert services["order-service"]["environment"]["INVENTORY_BASE_URL"] == "http://inventory-service:8081"
print("Compose application network, private databases and independent order startup: PASS")
