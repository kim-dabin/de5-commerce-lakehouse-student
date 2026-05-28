#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

echo "== Docker =="
command -v docker >/dev/null 2>&1 || fail "Docker CLI is not installed."
docker --version
docker compose version

echo
echo "== Docker daemon =="
docker info >/dev/null 2>&1 || fail "Docker daemon is not running. Start Docker Desktop, then run this script again."
docker info --format 'Server Version: {{.ServerVersion}}'
docker info --format 'CPUs: {{.NCPU}}'
docker info --format 'Total Memory: {{.MemTotal}} bytes'

echo
echo "== Host memory hint =="
case "$(uname -s)" in
  Darwin)
    mem_bytes="$(sysctl -n hw.memsize)"
    awk -v bytes="${mem_bytes}" 'BEGIN { printf "Host RAM: %.1f GiB\n", bytes / 1024 / 1024 / 1024 }'
    ;;
  Linux)
    if command -v free >/dev/null 2>&1; then
      free -h
    else
      echo "free command not found"
    fi
    ;;
  *)
    echo "Unsupported host OS for memory hint: $(uname -s)"
    ;;
esac

echo
echo "Environment check completed."
