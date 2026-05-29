#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.lite.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-de5-lite}"
DASHBOARD_TMUX_SESSION="${DASHBOARD_TMUX_SESSION:-de5-lite-dashboard}"

DRY_RUN=false

usage() {
  cat <<'EOF'
Usage: ./scripts/reset-local-state.sh [options]

Reset the local teaching environment to a clean state.

Default reset scope:
  - stop the DE5 Lite BI/dashboard tmux session if it exists
  - docker compose down for docker-compose.lite.yml
  - remove Lite stack volumes: Kafka, MinIO, Paimon, Iceberg, Spark Ivy cache, Airflow DB/logs

Options:
  --dry-run                        Print commands without running them.
  -h, --help                       Show this help.
EOF
}

run() {
  printf '+ %s\n' "$*"
  if [[ "${DRY_RUN}" == "false" ]]; then
    "$@"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

cd "${ROOT_DIR}"

echo "Resetting DE5 local state."
echo "Root: ${ROOT_DIR}"
echo

if command -v tmux >/dev/null 2>&1 && tmux has-session -t "${DASHBOARD_TMUX_SESSION}" 2>/dev/null; then
  run tmux kill-session -t "${DASHBOARD_TMUX_SESSION}"
else
  echo "No tmux dashboard session to stop: ${DASHBOARD_TMUX_SESSION}"
fi

if [[ -f "${COMPOSE_FILE}" ]]; then
  run docker compose \
    -p "${COMPOSE_PROJECT_NAME}" \
    -f "${COMPOSE_FILE}" \
    --profile tools \
    --profile orchestration \
    down \
    --volumes \
    --remove-orphans
else
  echo "Lite compose file not found: ${COMPOSE_FILE}"
fi

echo
echo "Reset complete."
echo
echo "Next clean-start commands:"
echo "  docker compose -p ${COMPOSE_PROJECT_NAME} -f ${COMPOSE_FILE} up -d --build"
echo "  ./scripts/smoke-test.sh"
echo "  ./scripts/start-lite-dashboard.sh"
