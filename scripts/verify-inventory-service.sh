#!/usr/bin/env bash
set -euo pipefail
proof_kind=inventory
source "$(dirname "$0")/verify-compose-common.sh"
# This historical HTTP reserve/release proof intentionally has no event workflow.
export INVENTORY_EVENTS_ENABLED=false INVENTORY_OUTBOX_ENABLED=false
proof_docker 600 compose build inventory-service
proof_docker 120 compose up -d --wait --wait-timeout 120 inventory-service
export INVENTORY_URL="$(service_url inventory-service 8081)"
wait_for_health "$INVENTORY_URL"
check_failure_cleanup
python3 scripts/fixtures/runtime-proof.py inventory
