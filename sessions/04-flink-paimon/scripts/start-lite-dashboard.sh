#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8765}"

python3 tools/lite_cycle_dashboard.py --host "${HOST}" --port "${PORT}"
