#!/usr/bin/env bash
set -euo pipefail
proof_kind=outbox
proof_override=scripts/fixtures/outbox-recovery.compose.yml
source "$(dirname "$0")/verify-compose-common.sh"
export OUTBOX_ENABLED=false PROOF_PROFILE=default PROOF_ENABLED=false PROOF_EVENT_ID=
# Phase 4A proves publication with pending orders and untouched inventory, not completion.
# Keep these exports on base-config recovery too, when the proof override is removed.
export ORDER_EVENTS_ENABLED=false INVENTORY_EVENTS_ENABLED=false INVENTORY_OUTBOX_ENABLED=false
proof_docker 10 compose config --format json | python3 scripts/fixtures/assert-compose.py --proof
proof_docker 600 compose build order-service inventory-service
proof_docker 120 compose up -d --wait --wait-timeout 120 order-service inventory-service
if [[ "${VERIFY_FAIL_AFTER_START:-0}" == 1 ]]; then
  python3 -B scripts/fixtures/outbox-proof.py broker
fi
check_failure_cleanup
python3 -B scripts/fixtures/outbox-proof.py
