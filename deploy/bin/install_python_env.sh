#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${TRADING_REPO:-/opt/trading-system/repo}"
VENV_DIR="${PYTHON_VENV:-/opt/trading-system/venv}"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"

cd "$REPO_DIR"

"$PYTHON_BIN" - <<'PY'
import sys

if sys.version_info[:2] != (3, 11):
    raise SystemExit(f"python_3_11_required:{sys.version}")
PY

"$PYTHON_BIN" -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip wheel setuptools
"$VENV_DIR/bin/pip" install -r requirements.txt
