#!/usr/bin/env bash
set -euo pipefail

mkdir -p logs data

if [ -f .env ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^\s*#' .env | grep -v '^\s*$' | xargs)
fi

export DB_PATH="${DB_PATH:-$(pwd)/data/trading.db}"

PY="${OPERATOR_PYTHON:-}"
if [ -z "${PY}" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PY="python3"
  else
    PY="python"
  fi
fi

MODE="${ENGINE_MODE:-safe}"

# Run full deterministic bootstrap + dashboard
"$PY" start_system.py "$MODE"