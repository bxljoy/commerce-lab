#!/usr/bin/env bash
# Sourced by isolated image proofs, not by the developer's normal Compose stack.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
proof_root="$PWD"
proof_tmp="$(mktemp -d "${TMPDIR:-/tmp}/commerce-proof.XXXXXXXX")"
export COMPOSE_PROJECT_NAME="commerce-${proof_kind}-$(basename "$proof_tmp" | tr '[:upper:].' '[:lower:]-')"
export COMPOSE_FILE="$PWD/docker-compose.yml${proof_override:+:$PWD/$proof_override}"
# Docker allocates loopback-only ephemeral host ports, including both databases.
export POSTGRES_PORT=127.0.0.1:0 ORDER_SERVICE_PORT=127.0.0.1:0
export INVENTORY_POSTGRES_PORT=127.0.0.1:0 INVENTORY_SERVICE_PORT=127.0.0.1:0

proof_command() {
  python3 -B "$proof_root/scripts/fixtures/runtime-proof.py" command "$@"
}

proof_docker() {
  local timeout=$1
  shift
  proof_command "$timeout" docker "$@"
}

cleanup() {
  local result=$? remaining kind list_flag
  trap - EXIT
  if ((result != 0)); then
    proof_docker 10 compose ps >&2 || true
    proof_docker 10 compose logs --tail=80 >&2 || true
  fi
  if ! proof_docker 60 compose down -v --remove-orphans --timeout 10; then
    result=1
  fi
  for kind in container network volume; do
    list_flag=-q
    [[ "$kind" != container ]] || list_flag=-aq
    if ! remaining="$(proof_docker 10 "$kind" ls "$list_flag" --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME")"; then
      result=1
    elif [[ -n "$remaining" ]]; then
      echo "Cleanup FAILED: owned $kind remains: $remaining" >&2
      result=1
    else
      echo "Cleanup verified: $COMPOSE_PROJECT_NAME $kind=0"
    fi
  done
  rm -rf "$proof_tmp"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
echo "Isolated Compose project: $COMPOSE_PROJECT_NAME"

service_url() {
  local address
  address="$(proof_docker 10 compose port "$1" "$2")" || return
  printf 'http://%s' "$address"
}

wait_for_health() {
  python3 -B "$proof_root/scripts/fixtures/runtime-proof.py" health "$1"
}

check_failure_cleanup() {
  if [[ "${VERIFY_FAIL_AFTER_START:-0}" == 1 ]]; then
    echo "Deliberate verification failure after startup (cleanup test)." >&2
    exit 97
  fi
}
