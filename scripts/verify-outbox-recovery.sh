#!/usr/bin/env bash
set -euo pipefail
proof_kind=outbox
proof_override=scripts/fixtures/outbox-recovery.compose.yml
source "$(dirname "$0")/verify-compose-common.sh"
export OUTBOX_ENABLED=false PROOF_PROFILE=default PROOF_ENABLED=false PROOF_EVENT_ID=
docker compose config --format json | python3 scripts/fixtures/assert-compose.py --proof
docker compose build order-service inventory-service
docker compose up -d --wait --wait-timeout 120 order-service inventory-service
if [[ "${VERIFY_FAIL_AFTER_START:-0}" == 1 ]]; then
  docker compose up -d --wait --wait-timeout 120 kafka
  docker compose up -d topic-init
fi
check_failure_cleanup
python3 scripts/fixtures/outbox-proof.py
