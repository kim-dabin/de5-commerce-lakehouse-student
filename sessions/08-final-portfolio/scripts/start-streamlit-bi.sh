#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${STREAMLIT_BI_VENV:-${ROOT_DIR}/.venv-streamlit-bi}"
PORT="${PORT:-8501}"
HOST="${HOST:-127.0.0.1}"

cd "${ROOT_DIR}"

export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
export STREAMLIT_SERVER_HEADLESS=true

if [[ ! -x "${VENV_DIR}/bin/streamlit" ]]; then
  python3 -m venv "${VENV_DIR}"
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip
  "${VENV_DIR}/bin/pip" install -r tools/streamlit-requirements.txt
fi

exec "${VENV_DIR}/bin/streamlit" run tools/streamlit_bi.py \
  --server.address "${HOST}" \
  --server.port "${PORT}" \
  --server.headless true \
  --browser.gatherUsageStats false \
  --theme.base light \
  --theme.primaryColor "#ef4444" \
  --theme.backgroundColor "#f8fafc" \
  --theme.secondaryBackgroundColor "#ffffff" \
  --theme.textColor "#0f172a"
