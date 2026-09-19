#!/usr/bin/env bash
set -euo pipefail
proof_kind=inventory
source "$(dirname "$0")/verify-compose-common.sh"
docker compose up --build -d inventory-service
export INVENTORY_URL="$(service_url inventory-service 8081)"
wait_for_health "$INVENTORY_URL"
check_failure_cleanup
python3 scripts/fixtures/runtime-proof.py inventory
