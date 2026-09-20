#!/usr/bin/env bash
set -euo pipefail
proof_kind=async
proof_override=scripts/fixtures/async-completion.compose.yml
source "$(dirname "$0")/verify-compose-common.sh"
export OUTBOX_ENABLED=true INVENTORY_OUTBOX_ENABLED=true
export ORDER_EVENTS_ENABLED=false INVENTORY_EVENTS_ENABLED=false
export INVENTORY_PROOF_PROFILE=default INVENTORY_PROOF_ENABLED=false INVENTORY_PROOF_EVENT_ID=
export ORDER_PROOF_PROFILE=default ORDER_PROOF_ENABLED=false ORDER_PROOF_EVENT_ID=
proof_docker 10 compose config --format json | python3 -B scripts/fixtures/assert-compose.py
proof_docker 600 compose build order-service inventory-service
proof_docker 120 compose up -d --wait --wait-timeout 120 kafka
proof_docker 120 compose up --no-deps --exit-code-from topic-init topic-init
proof_docker 120 compose up -d --wait --wait-timeout 120 order-service inventory-service
check_failure_cleanup
proof_command 1500 python3 -B scripts/fixtures/async-completion-proof.py
