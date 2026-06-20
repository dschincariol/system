#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[run-prod-preflight] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

TRADING_USER="${TRADING_USER:-trading}"
TRADING_GROUP="${TRADING_GROUP:-trading}"
INSTALL_ROOT="${TRADING_INSTALL_ROOT:-/opt/trading}"
APP_ROOT="${TRADING_APP_ROOT:-${INSTALL_ROOT}/app}"
PYTHON_BIN="${TRADING_PYTHON_BIN:-${INSTALL_ROOT}/venv/bin/python}"
DATA_ROOT="${TRADING_DATA_ROOT:-/var/lib/trading}"
BACKUP_ROOT="${TRADING_BACKUP_ROOT:-/var/backups/trading}"
ETC_DIR="${TRADING_ETC_DIR:-/etc/trading}"
CREDSTORE_DIR="${TRADING_CREDSTORE_DIR:-/etc/credstore.encrypted}"
TRADING_ENV_FILE="${TRADING_ENV_FILE:-${ETC_DIR}/trading.env}"

log() {
  printf '[run-prod-preflight] %s\n' "$*" >&2
}

die() {
  printf '[run-prod-preflight] ERROR: %s\n' "$*" >&2
  exit 1
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    die "run_prod_preflight.sh must run as root so systemd can load encrypted credentials and switch to ${TRADING_USER}"
  fi
}

require_systemd() {
  command -v systemd-run >/dev/null 2>&1 || die "systemd-run is required"
  [ "$(ps -p 1 -o comm= | tr -d ' ')" = "systemd" ] || die "systemd must be PID 1"
}

require_file() {
  [ -r "$1" ] || die "missing readable file: $1"
}

require_executable() {
  [ -x "$1" ] || die "missing executable file: $1"
}

require_dir() {
  [ -d "$1" ] || die "missing directory: $1"
}

main() {
  require_root
  require_systemd
  require_executable "$PYTHON_BIN"
  require_executable /usr/bin/bash
  require_executable /usr/bin/env
  require_dir "$APP_ROOT"
  require_file "$TRADING_ENV_FILE"
  require_file "${CREDSTORE_DIR}/master_key.cred"
  require_file "${CREDSTORE_DIR}/pg_password_app.cred"
  require_file "${CREDSTORE_DIR}/redis_password.cred"
  require_file "${CREDSTORE_DIR}/object_store_secret_key.cred"
  require_file "${CREDSTORE_DIR}/dashboard_api_token.cred"

  log "running production preflight in a transient systemd unit"
  systemd-run --wait --collect --pipe \
    --property="User=${TRADING_USER}" \
    --property="Group=${TRADING_GROUP}" \
    --property="WorkingDirectory=${APP_ROOT}" \
    --property="LoadCredentialEncrypted=master_key:${CREDSTORE_DIR}/master_key.cred" \
    --property="LoadCredentialEncrypted=pg_password_app:${CREDSTORE_DIR}/pg_password_app.cred" \
    --property="LoadCredentialEncrypted=redis_password:${CREDSTORE_DIR}/redis_password.cred" \
    --property="LoadCredentialEncrypted=object_store_secret_key:${CREDSTORE_DIR}/object_store_secret_key.cred" \
    --property="LoadCredentialEncrypted=dashboard_api_token:${CREDSTORE_DIR}/dashboard_api_token.cred" \
    --property="NoNewPrivileges=true" \
    --property="PrivateTmp=true" \
    --property="ProtectHome=true" \
    --property="ProtectSystem=strict" \
    --property="ReadWritePaths=${DATA_ROOT} ${BACKUP_ROOT} ${APP_ROOT}/data ${APP_ROOT}/logs" \
    /usr/bin/env \
      APP_ROOT="$APP_ROOT" \
      PYTHON_BIN="$PYTHON_BIN" \
      TRADING_ENV_FILE="$TRADING_ENV_FILE" \
      TS_SERVICE_NAME=trading-prod-preflight \
      TS_SECRETS_PROVIDER=systemd-creds \
      TS_PG_ROLE=app \
      PYTHONUNBUFFERED=1 \
      /usr/bin/bash -lc 'set -euo pipefail; set -a; . "$TRADING_ENV_FILE"; set +a; export TS_SERVICE_NAME=trading-prod-preflight TS_SECRETS_PROVIDER=systemd-creds TS_PG_ROLE=app PYTHONPATH="${PYTHONPATH:-$APP_ROOT}"; cd "$APP_ROOT"; exec "$PYTHON_BIN" engine/runtime/prod_preflight.py --json'
}

main "$@"
