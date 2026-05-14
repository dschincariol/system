#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

export OPERATOR_AUTO_START="${OPERATOR_AUTO_START:-1}"

if command -v python3 >/dev/null 2>&1; then
  exec python3 start_all.py
fi

exec python start_all.py