#!/usr/bin/env bash
set -euo pipefail
proof_kind=restart
source "$(dirname "$0")/verify-compose-common.sh"
# Isolated pending-order durability has no broker or inventory workflow.
export OUTBOX_ENABLED=false ORDER_EVENTS_ENABLED=false
proof_docker 600 compose build order-service
proof_docker 120 compose up -d --wait --wait-timeout 120 order-service
check_failure_cleanup
python3 -B scripts/fixtures/runtime-proof.py restart
