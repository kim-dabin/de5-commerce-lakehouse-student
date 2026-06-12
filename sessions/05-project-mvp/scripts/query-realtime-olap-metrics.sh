#!/usr/bin/env bash
set -euo pipefail

"${PYTHON_BIN:-python3}" tools/query_realtime_olap_metrics.py
