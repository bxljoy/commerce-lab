#!/usr/bin/env bash
set -euo pipefail
proof_kind=restart
source "$(dirname "$0")/verify-compose-common.sh"
export INVENTORY_BASE_URL=http://127.0.0.1:1
export ORDER_RECOVERY_ENABLED=true ORDER_RECOVERY_FIXED_DELAY_MS=5000 ORDER_RECOVERY_BATCH_SIZE=20
docker compose up --build -d order-service
export ORDER_URL="$(service_url order-service 8080)"
wait_for_health "$ORDER_URL"
check_failure_cleanup
python3 scripts/fixtures/runtime-proof.py restart
