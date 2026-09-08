#!/usr/bin/env bash
set -euo pipefail

project_name="commerce-lab-inventory-test"
service_port="18081"
order_id="00000000-0000-0000-0000-000000000001"

cleanup() {
  docker compose -p "$project_name" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

export INVENTORY_POSTGRES_PORT="15433"
export INVENTORY_SERVICE_PORT="$service_port"

wait_for_health() {
  for _ in $(seq 1 60); do
    if curl -fsS "http://localhost:${service_port}/actuator/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  docker compose -p "$project_name" ps
  docker compose -p "$project_name" logs inventory-service
  return 1
}

docker compose -p "$project_name" up --build -d inventory-service
wait_for_health

curl --fail-with-body -sS -X POST \
  "http://localhost:${service_port}/api/v1/reservations" \
  -H 'Content-Type: application/json' \
  -d "{\"orderId\":\"${order_id}\",\"lines\":[{\"sku\":\"SKU-APPLE\",\"quantity\":3},{\"sku\":\"SKU-BANANA\",\"quantity\":2}]}" \
  | python3 -c '
import json
import sys

reservation = json.load(sys.stdin)
assert reservation["status"] == "RESERVED", reservation
assert reservation["lines"] == [
    {"sku": "SKU-APPLE", "quantity": 3},
    {"sku": "SKU-BANANA", "quantity": 2},
], reservation
'

for _ in 1 2; do
  curl --fail-with-body -sS -X PUT \
    "http://localhost:${service_port}/api/v1/reservations/${order_id}/release" \
    | python3 -c '
import json
import sys

reservation = json.load(sys.stdin)
assert reservation["status"] == "RELEASED", reservation
assert reservation["releasedAt"], reservation
'
done

apple_stock="$(curl --fail-with-body -sS \
  "http://localhost:${service_port}/api/v1/stock/SKU-APPLE")"
banana_stock="$(curl --fail-with-body -sS \
  "http://localhost:${service_port}/api/v1/stock/SKU-BANANA")"

python3 - "$apple_stock" "$banana_stock" <<'PY'
import json
import sys

actual = {
    stock["sku"]: stock["availableQuantity"]
    for stock in (json.loads(sys.argv[1]), json.loads(sys.argv[2]))
}
expected = {"SKU-APPLE": 10, "SKU-BANANA": 5}
assert actual == expected, {"expected": expected, "actual": actual}
PY

echo "Inventory image reserved and idempotently released both demo SKUs with seeded stock restored."
