#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
UNIT_DIR="${REPO_ROOT}/ops/server/systemd"

if ! command -v systemd-analyze >/dev/null 2>&1; then
  echo "[test_systemd_units_lint] systemd-analyze not available; skipping"
else
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "${tmp_dir}"' EXIT
  cp "${UNIT_DIR}"/*.service "${UNIT_DIR}"/*.target "${tmp_dir}/"
  chmod 0644 "${tmp_dir}"/*

  if [ "$(ps -p 1 -o comm= | tr -d ' ')" != "systemd" ]; then
    echo "[test_systemd_units_lint] PID 1 is not systemd; skipping systemd-analyze verify"
  elif [ ! -x /usr/bin/node ] || [ ! -x /opt/trading/venv/bin/python ]; then
    echo "[test_systemd_units_lint] runtime executables missing; skipping systemd-analyze verify"
  else
    echo "[test_systemd_units_lint] verifying unit syntax"
    systemd-analyze verify "${tmp_dir}"/*.service "${tmp_dir}"/*.target
  fi
fi

for service in "${UNIT_DIR}"/*.service; do
  grep -q '^Restart=on-failure$' "${service}" || {
    echo "${service}: missing Restart=on-failure" >&2
    exit 1
  }
  grep -q '^NoNewPrivileges=true$' "${service}" || {
    echo "${service}: missing NoNewPrivileges=true" >&2
    exit 1
  }
  grep -q '^ProtectSystem=strict$' "${service}" || {
    echo "${service}: missing ProtectSystem=strict" >&2
    exit 1
  }
done
