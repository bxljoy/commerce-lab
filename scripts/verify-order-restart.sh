#!/usr/bin/env bash
set -euo pipefail

project_name="commerce-lab-restart-test"
service_port="18080"

cleanup() {
  docker compose -p "$project_name" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

export POSTGRES_PORT="15432"
export ORDER_SERVICE_PORT="$service_port"

wait_for_health() {
  for _ in $(seq 1 60); do
    if curl -fsS "http://localhost:${service_port}/actuator/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  docker compose -p "$project_name" ps
  docker compose -p "$project_name" logs order-service
  return 1
}

docker compose -p "$project_name" up --build -d order-service
wait_for_health

created_order="$(curl --fail-with-body -sS -X POST \
  "http://localhost:${service_port}/api/v1/orders" \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"restart-check","currency":"EUR","lines":[{"sku":"SKU-FIRST","quantity":1,"unitPrice":9.9999},{"sku":"SKU-SECOND","quantity":2,"unitPrice":4.00}]}')"
order_id="$(printf '%s' "$created_order" | python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])')"

docker compose -p "$project_name" restart order-service
wait_for_health

curl --fail-with-body -sS \
  "http://localhost:${service_port}/api/v1/orders/${order_id}" \
  | python3 -c '
import json
import sys

order = json.load(sys.stdin)
assert order["status"] == "PLACED", order
assert order["currency"] == "EUR", order
assert [line["sku"] for line in order["lines"]] == ["SKU-FIRST", "SKU-SECOND"], order
assert order["totalAmount"] == 17.9999, order
'

echo "Order ${order_id} survived an order-service restart with value and line order intact."
