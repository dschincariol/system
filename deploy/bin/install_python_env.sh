#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${TRADING_REPO:-/opt/trading/app}"
VENV_DIR="${PYTHON_VENV:-/opt/trading/venv}"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"

cd "$REPO_DIR"

"$PYTHON_BIN" - <<'PY'
import sys

if sys.version_info[:2] != (3, 11):
    raise SystemExit(f"python_3_11_required:{sys.version}")
PY

"$PYTHON_BIN" -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip wheel setuptools
REQ_FILE="$(PYTHON_BIN="$VENV_DIR/bin/python" bash "$REPO_DIR/deploy/bin/resolve_python_requirements.sh" "$REPO_DIR")"
case "$(basename "$REQ_FILE")" in
  requirements.txt|requirements-dev.txt|requirements-nvidia-cuda.txt|requirements-amd-rocm-full.txt)
    "$VENV_DIR/bin/pip" install --require-hashes -r "$REQ_FILE"
    ;;
  requirements-amd-rocm.txt)
    "$VENV_DIR/bin/pip" install --require-hashes -r "$REPO_DIR/requirements-amd-rocm-full.txt"
    ;;
  *)
    "$VENV_DIR/bin/pip" install -r "$REQ_FILE"
    ;;
esac
