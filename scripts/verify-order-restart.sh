#!/usr/bin/env bash
set -euo pipefail
proof_kind=restart
source "$(dirname "$0")/verify-compose-common.sh"
export OUTBOX_ENABLED=false
docker compose build order-service
docker compose up -d --wait --wait-timeout 120 order-service
export ORDER_URL="$(service_url order-service 8080)"
wait_for_health "$ORDER_URL"
check_failure_cleanup
python3 scripts/fixtures/runtime-proof.py restart
