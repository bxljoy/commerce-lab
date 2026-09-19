#!/usr/bin/env bash
set -euo pipefail
proof_kind=sync
proof_override=scripts/fixtures/sync-recovery.compose.yml
source "$(dirname "$0")/verify-compose-common.sh"
export INVENTORY_BASE_URL=http://inventory-service:8081
export ORDER_RECOVERY_ENABLED=true ORDER_RECOVERY_FIXED_DELAY_MS=5000 ORDER_RECOVERY_BATCH_SIZE=20
python3 scripts/fixtures/test-response-drop-proxy.py
docker compose -f docker-compose.yml config --format json | python3 scripts/fixtures/assert-compose.py
docker compose up --build -d inventory-service fault-proxy order-service
export ORDER_URL="$(service_url order-service 8080)"
export INVENTORY_URL="$(service_url inventory-service 8081)"
export PROXY_URL="$(service_url fault-proxy 8082)"
wait_for_health "$ORDER_URL"
wait_for_health "$INVENTORY_URL"
check_failure_cleanup
python3 scripts/fixtures/runtime-proof.py sync
