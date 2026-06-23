#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${TRADING_ENV_FILE:-/etc/trading-system/trading.env}"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

TRADING_ROOT="${TRADING_ROOT:-/opt/trading-system}"
TRADING_REPO="${TRADING_REPO:-$TRADING_ROOT/repo}"
PYTHON_VENV="${PYTHON_VENV:-$TRADING_ROOT/venv}"
LOCK_FILE="${TRADING_ROOT}/upgrade.lock"
TRADING_UPGRADE_SERVICE_CONTROL="${TRADING_UPGRADE_SERVICE_CONTROL:-1}"

service_control_enabled() {
  case "$(printf '%s' "$TRADING_UPGRADE_SERVICE_CONTROL" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

systemctl_cmd() {
  if [[ "${EUID}" -eq 0 ]]; then
    systemctl "$@"
  else
    sudo -n systemctl "$@"
  fi
}

mkdir -p "$TRADING_ROOT"
exec 9>"$LOCK_FILE"
flock -n 9 || {
  echo "upgrade_already_running" >&2
  exit 1
}

cd "$TRADING_REPO"

if service_control_enabled; then
  systemctl_cmd stop trading-operator.service
  systemctl_cmd stop trading-engine.service
fi

if [[ -d .git ]]; then
  git fetch --all --prune
  if ! git pull --ff-only; then
    echo "git_pull_ff_only_failed" >&2
    exit 1
  fi
fi

"$PYTHON_VENV/bin/python" -m pip install --upgrade pip wheel setuptools
REQ_FILE="$(PYTHON_BIN="$PYTHON_VENV/bin/python" bash "$TRADING_REPO/deploy/bin/resolve_python_requirements.sh" "$TRADING_REPO")"
"$PYTHON_VENV/bin/pip" install -r "$REQ_FILE"

if [[ -f package.json ]]; then
  npm ci
fi

"$PYTHON_VENV/bin/python" -c "from engine.runtime.db_repair import repair; import json; print(json.dumps(repair(), indent=2))"

if service_control_enabled; then
  systemctl_cmd start trading-engine.service
  systemctl_cmd restart trading-operator.service
fi
