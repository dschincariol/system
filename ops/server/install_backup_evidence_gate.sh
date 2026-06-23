#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[install_backup_evidence_gate] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TRADING_USER="${TRADING_USER:-trading}"
TRADING_GROUP="${TRADING_GROUP:-trading}"
TRADING_OPERATOR_USER="${TRADING_OPERATOR_USER:-${SUDO_USER:-}}"
INSTALL_ROOT="${TRADING_INSTALL_ROOT:-/opt/trading}"
BACKUP_SCRIPT_DST_DIR="${TRADING_BACKUP_SCRIPT_DIR:-${INSTALL_ROOT}/ops/backup}"
BACKUP_ROOT="${TRADING_BACKUP_ROOT:-/var/backups/trading}"
BACKUP_BASE_DIR="${TRADING_BACKUP_BASE_DIR:-${BACKUP_ROOT}/base}"
BACKUP_WAL_DIR="${TRADING_BACKUP_WAL_DIR:-${BACKUP_ROOT}/wal}"
BACKUP_DRILL_DIR="${TRADING_BACKUP_DRILL_DIR:-${BACKUP_ROOT}/drills}"
BACKUP_EVIDENCE_DIR="${TRADING_BACKUP_EVIDENCE_DIR:-${BACKUP_ROOT}/evidence}"
ETC_DIR="${TRADING_ETC_DIR:-/etc/trading}"
TRADING_ENV_FILE="${TRADING_ENV_FILE:-${ETC_DIR}/trading.env}"
BACKUP_EVIDENCE_HMAC_KEY_FILE="${TRADING_BACKUP_EVIDENCE_HMAC_KEY_FILE:-${ETC_DIR}/backup_evidence.hmac.key}"
PROVIDER_ENV="${TRADING_PROVIDER_ENV:-${ETC_DIR}/provider.env}"
SYSTEMD_DST_DIR="${TRADING_SYSTEMD_DIR:-/etc/systemd/system}"
POSTGRES_VERSION="${TRADING_POSTGRES_VERSION:-}"
POSTGRES_BIN_DIR="${TRADING_POSTGRES_BIN_DIR:-}"
POSTGRES_SOCKET_DIR="${TRADING_POSTGRES_SOCKET_DIR:-/var/run/postgresql}"
POSTGRES_DB="${TRADING_POSTGRES_DB:-trading}"
COMPOSE_ENV_FILE="${TRADING_COMPOSE_ENV_FILE:-${REPO_ROOT}/deploy/compose/.env}"
COMPOSE_EXTERNAL_FILE="${TRADING_COMPOSE_EXTERNAL_FILE:-${REPO_ROOT}/deploy/compose/docker-compose.external-services.yml}"
COMPOSE_STACK_FILE="${TRADING_COMPOSE_STACK_FILE:-${REPO_ROOT}/deploy/compose/docker-compose.stack.yml}"
TIMESCALE_CONTAINER="${TRADING_TIMESCALE_CONTAINER:-trading-timescaledb}"
REDIS_CONTAINER="${TRADING_REDIS_CONTAINER:-trading-redis}"
MINIO_CONTAINER="${TRADING_MINIO_CONTAINER:-trading-minio}"
TIMESCALE_IMAGE="${TRADING_TIMESCALE_IMAGE:-}"
TIMESCALE_PORT="${TRADING_TIMESCALE_PORT:-5432}"
TIMESCALE_USER="${TRADING_TIMESCALE_USER:-trading}"
TIMESCALE_PASSWORD="${TRADING_TIMESCALE_PASSWORD:-}"
TIMESCALE_PASSWORD_FILE="${TRADING_TIMESCALE_PASSWORD_FILE:-}"
COMPOSE_POSTGRES_UID=""
COMPOSE_POSTGRES_GID=""
RESTART_POSTGRES=0
RUN_EVIDENCE=0
COMPOSE_MODE=0

log() {
  printf '[install_backup_evidence_gate] %s\n' "$*"
}

die() {
  printf '[install_backup_evidence_gate] ERROR: %s\n' "$*" >&2
  exit 1
}

read_secret_file() {
  local path="$1"
  [ -n "$path" ] || return 1
  [ -r "$path" ] || die "secret file is not readable: ${path}"
  tr -d '\r\n' < "$path"
}

resolve_compose_path() {
  local path="$1"
  [ -n "$path" ] || return 0
  case "$path" in
    /*) printf '%s\n' "$path" ;;
    *) python3 - "$COMPOSE_ENV_FILE" "$path" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

print((Path(sys.argv[1]).resolve().parent / sys.argv[2]).resolve())
PY
      ;;
  esac
}

usage() {
  cat <<'EOF'
Usage: install_backup_evidence_gate.sh [--compose] [--restart-postgres] [--run-evidence]

Installs only the backup/WAL/restore evidence production assets:
  - /opt/trading/ops/backup scripts
  - /var/backups/trading filesystem layout
  - backup/restore systemd timers
  - PostgreSQL archive config when a local cluster config exists
  - bounded WAL archive catch-up for stalled Compose archivers
  - missing Compose storage path settings, copied from the current container mounts
  - /etc/trading/trading.env evidence and PostgreSQL binary settings

Options:
  --compose           Target the Docker Compose TimescaleDB service on this host.
  --restart-postgres  Restart the local PostgreSQL cluster after writing archive settings.
                      In --compose mode, recreate/restart the timescaledb service.
  --run-evidence      Run backup_restore_evidence.sh as postgres after installation.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --compose)
      COMPOSE_MODE=1
      ;;
    --restart-postgres)
      RESTART_POSTGRES=1
      ;;
    --run-evidence)
      RUN_EVIDENCE=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
  shift
done

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    die "must run as root"
  fi
}

install_if_changed() {
  local src="$1"
  local dst="$2"
  local mode="$3"
  local owner="$4"
  local group="$5"

  if [ -f "$dst" ] && cmp -s "$src" "$dst"; then
    chmod "$mode" "$dst"
    chown "$owner:$group" "$dst"
    return 1
  fi
  install -m "$mode" -o "$owner" -g "$group" "$src" "$dst"
  return 0
}

ensure_group_membership() {
  local user="$1"
  local group="$2"
  if ! id -u "$user" >/dev/null 2>&1; then
    die "required user is missing: ${user}"
  fi
  if ! getent group "$group" >/dev/null 2>&1; then
    groupadd --system "$group"
  fi
  if ! id -nG "$user" | tr ' ' '\n' | grep -qx "$group"; then
    usermod -aG "$group" "$user"
    log "added ${user} to ${group}"
  fi
}

operator_user_exists() {
  [ -n "$TRADING_OPERATOR_USER" ] || return 1
  [ "$TRADING_OPERATOR_USER" != "root" ] || return 1
  id -u "$TRADING_OPERATOR_USER" >/dev/null 2>&1
}

ensure_trading_user() {
  if ! getent group "$TRADING_GROUP" >/dev/null 2>&1; then
    groupadd --system "$TRADING_GROUP"
  fi
  if ! id -u "$TRADING_USER" >/dev/null 2>&1; then
    useradd --system --gid "$TRADING_GROUP" --home-dir "${TRADING_DATA_ROOT:-/var/lib/trading}" --shell /usr/sbin/nologin "$TRADING_USER"
  fi
}

ensure_backup_evidence_hmac_key() {
  local key_dir tmp
  key_dir="$(dirname "$BACKUP_EVIDENCE_HMAC_KEY_FILE")"
  install -d -o root -g "$TRADING_GROUP" -m 0750 "$key_dir"
  if [ -f "$BACKUP_EVIDENCE_HMAC_KEY_FILE" ]; then
    [ -s "$BACKUP_EVIDENCE_HMAC_KEY_FILE" ] || die "backup evidence HMAC key file is empty: ${BACKUP_EVIDENCE_HMAC_KEY_FILE}"
    chown root:"$TRADING_GROUP" "$BACKUP_EVIDENCE_HMAC_KEY_FILE"
    chmod 0640 "$BACKUP_EVIDENCE_HMAC_KEY_FILE"
    log "using existing backup evidence HMAC key: ${BACKUP_EVIDENCE_HMAC_KEY_FILE}"
    return 0
  fi

  tmp="$(mktemp "${key_dir}/backup_evidence.hmac.key.tmp.XXXXXX")"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32 > "$tmp"
  else
    python3 - <<'PY' > "$tmp"
import secrets

print(secrets.token_hex(32))
PY
  fi
  install -m 0640 -o root -g "$TRADING_GROUP" "$tmp" "$BACKUP_EVIDENCE_HMAC_KEY_FILE"
  rm -f "$tmp"
  log "created backup evidence HMAC key: ${BACKUP_EVIDENCE_HMAC_KEY_FILE}"
}

detect_postgres_bin_dir() {
  local candidate version
  if [ -n "$POSTGRES_BIN_DIR" ]; then
    [ -x "${POSTGRES_BIN_DIR}/pg_basebackup" ] || die "missing pg_basebackup in ${POSTGRES_BIN_DIR}"
    [ -x "${POSTGRES_BIN_DIR}/pg_verifybackup" ] || die "missing pg_verifybackup in ${POSTGRES_BIN_DIR}"
    [ -x "${POSTGRES_BIN_DIR}/pg_ctl" ] || die "missing pg_ctl in ${POSTGRES_BIN_DIR}"
    [ -x "${POSTGRES_BIN_DIR}/pg_controldata" ] || die "missing pg_controldata in ${POSTGRES_BIN_DIR}"
    if [ -z "$POSTGRES_VERSION" ]; then
      POSTGRES_VERSION="$(basename "$(dirname "$POSTGRES_BIN_DIR")")"
    fi
    return 0
  fi

  if [ -n "$POSTGRES_VERSION" ]; then
    POSTGRES_BIN_DIR="/usr/lib/postgresql/${POSTGRES_VERSION}/bin"
    [ -x "${POSTGRES_BIN_DIR}/pg_basebackup" ] || die "missing ${POSTGRES_BIN_DIR}/pg_basebackup"
    [ -x "${POSTGRES_BIN_DIR}/pg_verifybackup" ] || die "missing ${POSTGRES_BIN_DIR}/pg_verifybackup"
    [ -x "${POSTGRES_BIN_DIR}/pg_ctl" ] || die "missing ${POSTGRES_BIN_DIR}/pg_ctl"
    [ -x "${POSTGRES_BIN_DIR}/pg_controldata" ] || die "missing ${POSTGRES_BIN_DIR}/pg_controldata"
    return 0
  fi

  while IFS= read -r candidate; do
    if [ -x "${candidate}/pg_basebackup" ] && [ -x "${candidate}/pg_verifybackup" ] && [ -x "${candidate}/pg_ctl" ] && [ -x "${candidate}/pg_controldata" ]; then
      POSTGRES_BIN_DIR="$candidate"
      version="$(basename "$(dirname "$candidate")")"
      POSTGRES_VERSION="$version"
      return 0
    fi
  done < <(find /usr/lib/postgresql -mindepth 2 -maxdepth 2 -type d -name bin 2>/dev/null | sort -Vr)

  die "could not find PostgreSQL server binaries; install postgresql-N, or set TRADING_POSTGRES_VERSION/TRADING_POSTGRES_BIN_DIR"
}

set_env_var() {
  local key="$1"
  local value="$2"
  local tmp

  install -d -o "$TRADING_USER" -g "$TRADING_GROUP" -m 0750 "$ETC_DIR"
  if [ ! -f "$TRADING_ENV_FILE" ]; then
    install -m 0640 -o "$TRADING_USER" -g "$TRADING_GROUP" /dev/null "$TRADING_ENV_FILE"
  fi

  tmp="$(mktemp)"
  awk -v key="$key" -v value="$value" '
    index($0, key "=") == 1 || index($0, "#" key "=") == 1 {
      if (!done) {
        print key "=" value
        done = 1
      }
      next
    }
    { print }
    END {
      if (!done) {
        print key "=" value
      }
    }
  ' "$TRADING_ENV_FILE" > "$tmp"
  install -m 0640 -o "$TRADING_USER" -g "$TRADING_GROUP" "$tmp" "$TRADING_ENV_FILE"
  rm -f "$tmp"
}

set_provider_env_var() {
  local key="$1"
  local value="$2"
  local tmp

  install -d -o root -g "$TRADING_GROUP" -m 0750 "$ETC_DIR"
  if [ ! -f "$PROVIDER_ENV" ]; then
    install -m 0640 -o root -g "$TRADING_GROUP" /dev/null "$PROVIDER_ENV"
  fi

  tmp="$(mktemp)"
  awk -v key="$key" -v value="$value" '
    index($0, key "=") == 1 || index($0, "#" key "=") == 1 {
      if (!done) {
        print key "=" value
        done = 1
      }
      next
    }
    { print }
    END {
      if (!done) {
        print key "=" value
      }
    }
  ' "$PROVIDER_ENV" > "$tmp"
  install -m 0640 -o root -g "$TRADING_GROUP" "$tmp" "$PROVIDER_ENV"
  rm -f "$tmp"
}

set_compose_env_var() {
  local key="$1"
  local value="$2"
  local tmp

  [ -f "$COMPOSE_ENV_FILE" ] || die "compose env file missing: ${COMPOSE_ENV_FILE}"
  tmp="$(mktemp)"
  awk -v key="$key" -v value="$value" '
    index($0, key "=") == 1 || index($0, "#" key "=") == 1 {
      if (!done) {
        print key "=" value
        done = 1
      }
      next
    }
    { print }
    END {
      if (!done) {
        print key "=" value
      }
    }
  ' "$COMPOSE_ENV_FILE" > "$tmp"
  cat "$tmp" > "$COMPOSE_ENV_FILE"
  rm -f "$tmp"
}

compose_env_value() {
  local key="$1"
  python3 - "$COMPOSE_ENV_FILE" "$key" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

path = Path(sys.argv[1])
needle = sys.argv[2]
for raw_line in path.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    if key.strip() != needle:
        continue
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    print(value)
    break
PY
}

compose_container_mount_source() {
  local container="$1"
  local destination="$2"
  if ! docker inspect "$container" >/dev/null 2>&1; then
    return 1
  fi
  docker inspect "$container" | DESTINATION="$destination" python3 -c '
import json
import os
import sys

destination = os.environ["DESTINATION"]
data = json.load(sys.stdin)
for container in data:
    for mount in container.get("Mounts") or []:
        if mount.get("Destination") == destination:
            print(mount.get("Source") or "")
            raise SystemExit(0)
raise SystemExit(1)
'
}

ensure_compose_env_mount_source() {
  local key="$1"
  local container="$2"
  local destination="$3"
  local current source

  current="$(compose_env_value "$key")"
  source="$(compose_container_mount_source "$container" "$destination" || true)"
  if [ -n "$current" ]; then
    if [ -n "$source" ] && [ "$current" != "$source" ]; then
      die "compose env ${key}=${current} does not match running ${container} mount ${destination}=${source}; refusing to move storage"
    fi
    return 0
  fi
  [ -n "$source" ] || die "compose env ${key} is missing and ${container}:${destination} mount source is unavailable"
  set_compose_env_var "$key" "$source"
  log "recorded ${key} from ${container}:${destination}"
}

ensure_compose_env_value() {
  local key="$1"
  local value="$2"
  local current

  current="$(compose_env_value "$key")"
  if [ -n "$current" ]; then
    if [ "$current" != "$value" ]; then
      die "compose env ${key}=${current} does not match required value ${value}; refusing to rewrite"
    fi
    return 0
  fi
  set_compose_env_var "$key" "$value"
  log "recorded ${key}=${value}"
}

normalize_compose_storage_env() {
  [ "$COMPOSE_MODE" -eq 1 ] || return 0
  ensure_compose_env_mount_source TRADING_TIMESCALE_DATA "$TIMESCALE_CONTAINER" /var/lib/postgresql/data
  ensure_compose_env_mount_source TRADING_REDIS_DATA "$REDIS_CONTAINER" /data
  ensure_compose_env_mount_source TRADING_MINIO_DATA "$MINIO_CONTAINER" /data
  ensure_compose_env_mount_source TRADING_BACKUP_ROOT "$TIMESCALE_CONTAINER" /var/backups/trading
  ensure_compose_env_value TRADING_BACKUP_WAL_DIR "$BACKUP_WAL_DIR"
}

load_compose_env() {
  [ -f "$COMPOSE_ENV_FILE" ] || die "compose env file missing: ${COMPOSE_ENV_FILE}"

  TIMESCALE_IMAGE="${TRADING_TIMESCALE_IMAGE:-$(compose_env_value TIMESCALE_IMAGE)}"
  TIMESCALE_IMAGE="${TIMESCALE_IMAGE:-timescale/timescaledb:latest-pg16}"
  TIMESCALE_PORT="${TRADING_TIMESCALE_PORT:-$(compose_env_value TIMESCALE_PORT)}"
  TIMESCALE_PORT="${TIMESCALE_PORT:-5432}"
  TIMESCALE_USER="${TRADING_TIMESCALE_USER:-$(compose_env_value TIMESCALE_USER)}"
  TIMESCALE_USER="${TIMESCALE_USER:-trading}"
  TIMESCALE_PASSWORD_FILE="${TRADING_TIMESCALE_PASSWORD_FILE:-$(compose_env_value TIMESCALE_PASSWORD_FILE)}"
  TIMESCALE_PASSWORD_FILE="$(resolve_compose_path "$TIMESCALE_PASSWORD_FILE")"
  if [ -z "$TIMESCALE_PASSWORD" ] && [ -n "$TIMESCALE_PASSWORD_FILE" ]; then
    TIMESCALE_PASSWORD="$(read_secret_file "$TIMESCALE_PASSWORD_FILE")"
  fi
  POSTGRES_DB="${TRADING_POSTGRES_DB:-$(compose_env_value TIMESCALE_DB)}"
  POSTGRES_DB="${POSTGRES_DB:-trading}"
  [ -n "$TIMESCALE_PASSWORD" ] || die "TIMESCALE_PASSWORD_FILE is required in compose mode"
}

compose_postgres_uid_gid() {
  docker run --rm "$TIMESCALE_IMAGE" sh -lc 'printf "%s:%s\n" "$(id -u postgres)" "$(id -g postgres)"'
}

load_compose_postgres_identity() {
  [ "$COMPOSE_MODE" -eq 1 ] || return 0
  [ -n "$COMPOSE_POSTGRES_UID" ] && [ -n "$COMPOSE_POSTGRES_GID" ] && return 0
  local pg_uid_gid
  pg_uid_gid="$(compose_postgres_uid_gid)"
  COMPOSE_POSTGRES_UID="${pg_uid_gid%%:*}"
  COMPOSE_POSTGRES_GID="${pg_uid_gid##*:}"
}

install_backup_scripts() {
  log "installing backup scripts to ${BACKUP_SCRIPT_DST_DIR}"
  local dir_mode=0750
  if [ "$COMPOSE_MODE" -eq 1 ]; then
    dir_mode=0755
  fi
  install -d -o root -g "$TRADING_GROUP" -m "$dir_mode" "${INSTALL_ROOT}/ops"
  install -d -o root -g "$TRADING_GROUP" -m "$dir_mode" "$BACKUP_SCRIPT_DST_DIR"
  install -d -o root -g "$TRADING_GROUP" -m "$dir_mode" "${INSTALL_ROOT}/tools"

  local script
  for script in \
    wal_archive.sh \
    wal_archive_catchup.sh \
    base_backup.sh \
    offsite_base_backup_stub.sh \
    state_snapshot.sh \
    artifact_snapshot.sh \
    prune.sh \
    restore.sh \
    restore_drill.sh \
    backup_restore_evidence.sh
  do
    [ -f "${REPO_ROOT}/ops/backup/${script}" ] || die "missing source script: ops/backup/${script}"
    install_if_changed "${REPO_ROOT}/ops/backup/${script}" "${BACKUP_SCRIPT_DST_DIR}/${script}" 0755 root "$TRADING_GROUP" || true
  done
  [ -f "${REPO_ROOT}/tools/restore_sanity.sql" ] || die "missing source file: tools/restore_sanity.sql"
  install_if_changed "${REPO_ROOT}/tools/restore_sanity.sql" "${INSTALL_ROOT}/tools/restore_sanity.sql" 0644 root "$TRADING_GROUP" || true
}

create_backup_layout() {
  log "creating backup directories under ${BACKUP_ROOT}"
  local dir
  for dir in \
    "$BACKUP_ROOT" \
    "$BACKUP_BASE_DIR" \
    "$BACKUP_WAL_DIR" \
    "$BACKUP_WAL_DIR/.tmp" \
    "$BACKUP_DRILL_DIR" \
    "$BACKUP_EVIDENCE_DIR"
  do
    install -d -o "$TRADING_USER" -g "$TRADING_GROUP" -m 0770 "$dir"
  done
  if [ "$COMPOSE_MODE" -eq 1 ]; then
    load_compose_postgres_identity
    chown "${COMPOSE_POSTGRES_UID}:${TRADING_GROUP}" "$BACKUP_ROOT"
    chown -R "${COMPOSE_POSTGRES_UID}:${TRADING_GROUP}" "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR"
    chmod 2750 "$BACKUP_ROOT" "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_WAL_DIR/.tmp" "$BACKUP_DRILL_DIR"
    find "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR" -type d -exec chmod 2750 {} +
    find "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR" -type f -exec chmod 0640 {} +
  fi
}

grant_operator_backup_evidence_access() {
  local acl_ok=1
  operator_user_exists || return 0
  ensure_group_membership "$TRADING_OPERATOR_USER" "$TRADING_GROUP"
  if ! command -v setfacl >/dev/null 2>&1; then
    log "setfacl unavailable; ${TRADING_OPERATOR_USER} may need a new login before reading backup evidence through ${TRADING_GROUP}"
    acl_ok=0
  else
    setfacl -m "u:${TRADING_OPERATOR_USER}:--x" "$BACKUP_ROOT" || acl_ok=0
    setfacl -m "u:${TRADING_OPERATOR_USER}:rwx" "$BACKUP_EVIDENCE_DIR" || acl_ok=0
    setfacl -d -m "u:${TRADING_OPERATOR_USER}:rwX" "$BACKUP_EVIDENCE_DIR" || acl_ok=0
    find "$BACKUP_EVIDENCE_DIR" -maxdepth 1 -type f -name '*backup_restore_evidence*.json' \
      -exec setfacl -m "u:${TRADING_OPERATOR_USER}:r--" {} + 2>/dev/null || acl_ok=0
    find "$BACKUP_EVIDENCE_DIR" -maxdepth 1 -type f -name '*backup_restore_evidence*.txt' \
      -exec setfacl -m "u:${TRADING_OPERATOR_USER}:r--" {} + 2>/dev/null || acl_ok=0
  fi
  if [ "$acl_ok" -eq 0 ]; then
    chmod o+x "$BACKUP_ROOT" || true
    chmod o+rx "$BACKUP_EVIDENCE_DIR" || true
    find "$BACKUP_EVIDENCE_DIR" -maxdepth 1 -type f -name '*backup_restore_evidence*.json' -exec chmod o+r {} + 2>/dev/null || true
    find "$BACKUP_EVIDENCE_DIR" -maxdepth 1 -type f -name '*backup_restore_evidence*.txt' -exec chmod o+r {} + 2>/dev/null || true
    log "granted backup evidence read access with non-listable backup-root fallback"
  else
    log "granted ${TRADING_OPERATOR_USER} read access to backup evidence artifacts"
  fi
}

write_runtime_env() {
  log "writing backup evidence settings to ${TRADING_ENV_FILE}"
  if [ "$COMPOSE_MODE" -eq 1 ]; then
    set_env_var PGHOST "127.0.0.1"
    set_env_var PGPORT "$TIMESCALE_PORT"
    set_env_var PGUSER "$TIMESCALE_USER"
    set_env_var PGDATABASE "$POSTGRES_DB"
    set_provider_env_var PGPASSWORD "$TIMESCALE_PASSWORD"
    set_env_var TS_BACKUP_BASE_DIR "$BACKUP_BASE_DIR"
    set_env_var TS_BACKUP_WAL_DIR "$BACKUP_WAL_DIR"
    set_env_var TS_RESTORE_DRILL_DIR "$BACKUP_DRILL_DIR"
    set_env_var TS_BACKUP_DOCKER_IMAGE ""
    set_env_var TS_BACKUP_DOCKER_EXEC_CONTAINER "$TIMESCALE_CONTAINER"
    set_env_var TS_BACKUP_DOCKER_EXEC_USER "postgres"
    set_env_var TS_BACKUP_READ_GROUP "$TRADING_GROUP"
    set_env_var TS_BACKUP_EVIDENCE_OPERATOR_USER "$TRADING_OPERATOR_USER"
    load_compose_postgres_identity
    set_env_var TS_BACKUP_WAL_TARGET_OWNER_UID "$COMPOSE_POSTGRES_UID"
    set_env_var TS_BACKUP_WAL_TARGET_GROUP "$TRADING_GROUP"
    set_env_var TS_BACKUP_WAL_TARGET_DIR_MODE "2750"
    set_env_var TS_RESTORE_DOCKER_IMAGE "$TIMESCALE_IMAGE"
    set_env_var TS_RESTORE_DOCKER_USER "postgres"
    set_env_var TS_RESTORE_DRILL_ALLOW_DIRECT "1"
    set_env_var TS_RESTORE_DB "$POSTGRES_DB"
    set_env_var TS_RESTORE_USER "$TIMESCALE_USER"
  else
    set_env_var PGBASEBACKUP_BIN "${POSTGRES_BIN_DIR}/pg_basebackup"
    set_env_var PGVERIFYBACKUP_BIN "${POSTGRES_BIN_DIR}/pg_verifybackup"
    set_env_var PGCTL_BIN "${POSTGRES_BIN_DIR}/pg_ctl"
    set_env_var PGCONTROLDATA_BIN "${POSTGRES_BIN_DIR}/pg_controldata"
  fi
  set_env_var BACKUP_EVIDENCE_PATH "${BACKUP_EVIDENCE_DIR}/latest_backup_restore_evidence.json"
  set_env_var BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S "93600"
  set_env_var BACKUP_EVIDENCE_RPO_S "120"
  set_env_var BACKUP_EVIDENCE_WAL_RPO_S "120"
  set_env_var BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S "7776000"
  set_env_var BACKUP_EVIDENCE_RTO_S "1800"
  set_env_var BACKUP_EVIDENCE_SIGNATURE_MAX_AGE_S "120"
  set_env_var BACKUP_EVIDENCE_REQUIRE_SIGNATURE "1"
  set_env_var BACKUP_EVIDENCE_HMAC_KEY_FILE "$BACKUP_EVIDENCE_HMAC_KEY_FILE"
  set_env_var TS_BACKUP_EVIDENCE_LOCK_TIMEOUT_S "30"
  set_env_var TS_BACKUP_EVIDENCE_PROBE_TIMEOUT_S "5"
  set_env_var TS_BACKUP_EVIDENCE_SYSTEMCTL_TIMEOUT_S "5"
  set_env_var TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP "0"
  set_env_var TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL "0"
  set_env_var TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP "0"
  set_env_var TS_BACKUP_EVIDENCE_BASE_BACKUP_TIMEOUT_S "7200"
  set_env_var TS_BACKUP_EVIDENCE_WAL_SWITCH_TIMEOUT_S "30"
  set_env_var TS_BACKUP_EVIDENCE_WAL_ARCHIVER_STATS_TIMEOUT_S "30"
  set_env_var TS_BACKUP_EVIDENCE_WAL_CATCHUP_TIMEOUT_S "300"
  set_env_var TS_BACKUP_EVIDENCE_RESTORE_DRILL_TIMEOUT_S "3600"
  set_env_var TS_BACKUP_EVIDENCE_SIGNATURE_TIMEOUT_S "30"
  set_env_var TS_BACKUP_EVIDENCE_PUBLISH_TIMEOUT_S "30"
  if [ "$COMPOSE_MODE" -eq 1 ]; then
    set_compose_env_var TRADING_WAL_ARCHIVE_SCRIPT "${BACKUP_SCRIPT_DST_DIR}/wal_archive.sh"
    set_compose_env_var TRADING_WAL_ARCHIVE_CATCHUP_SCRIPT "${BACKUP_SCRIPT_DST_DIR}/wal_archive_catchup.sh"
    set_compose_env_var TIMESCALE_ARCHIVE_COMMAND "/opt/trading/ops/backup/wal_archive.sh \"%p\" \"%f\""
  fi
}

wait_for_compose_timescale_exec() {
  local attempt
  for attempt in $(seq 1 60); do
    if docker exec "$TIMESCALE_CONTAINER" sh -lc 'true' >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  die "TimescaleDB container did not become exec-ready: ${TIMESCALE_CONTAINER}"
}

run_compose_archive_selftest() {
  log "running Compose WAL archive command self-test inside ${TIMESCALE_CONTAINER}"
  docker exec \
    -u postgres \
    -e TS_BACKUP_ROOT=/var/backups/trading \
    -e TS_BACKUP_WAL_DIR=/var/backups/trading/.wal-archive-selftest \
    -e TS_WAL_ARCHIVE_REQUIRE_MOUNT=1 \
    "$TIMESCALE_CONTAINER" \
    sh -lc '
      set -eu
      src="${TMPDIR:-/tmp}/wal_archive_selftest.$$"
      trap "rm -f \"$src\"; rm -rf /var/backups/trading/.wal-archive-selftest" EXIT
      printf "wal archive self-test\n" > "$src"
      /opt/trading/ops/backup/wal_archive.sh "$src" "0000000100000000000000FE.selftest"
    '
}

run_compose_wal_catchup() {
  log "running one-shot WAL archive catch-up inside ${TIMESCALE_CONTAINER}"
  docker exec \
    -u postgres \
    -e PGDATA=/var/lib/postgresql/data \
    -e TS_BACKUP_ROOT=/var/backups/trading \
    -e TS_BACKUP_WAL_DIR=/var/backups/trading/wal \
    -e TS_WAL_ARCHIVE_SCRIPT=/opt/trading/ops/backup/wal_archive.sh \
    -e TS_WAL_ARCHIVE_REQUIRE_MOUNT=1 \
    "$TIMESCALE_CONTAINER" \
    /opt/trading/ops/backup/wal_archive_catchup.sh
}

configure_compose_archive() {
  [ -f "$COMPOSE_EXTERNAL_FILE" ] || die "compose external-services file missing: ${COMPOSE_EXTERNAL_FILE}"
  grep -q '/var/backups/trading/wal' "$COMPOSE_EXTERNAL_FILE" || die "compose file missing WAL archive bind mount: ${COMPOSE_EXTERNAL_FILE}"
  grep -Eq 'archive_mode=(on|\$\{TIMESCALE_ARCHIVE_MODE:-on\})' "$COMPOSE_EXTERNAL_FILE" || die "compose file missing archive_mode=on default: ${COMPOSE_EXTERNAL_FILE}"
  grep -q 'wal_archive.sh "%p" "%f"' "$COMPOSE_EXTERNAL_FILE" || die "compose file missing audited WAL archive command: ${COMPOSE_EXTERNAL_FILE}"
  grep -q '/opt/trading/ops/backup/wal_archive.sh:ro' "$COMPOSE_EXTERNAL_FILE" || die "compose file missing WAL archive script bind mount: ${COMPOSE_EXTERNAL_FILE}"
  grep -q '/opt/trading/ops/backup/wal_archive_catchup.sh:ro' "$COMPOSE_EXTERNAL_FILE" || die "compose file missing WAL archive catch-up bind mount: ${COMPOSE_EXTERNAL_FILE}"
  if [ "$RESTART_POSTGRES" -eq 1 ]; then
    log "recreating Compose TimescaleDB service to apply WAL archive settings"
    docker compose \
      --env-file "$COMPOSE_ENV_FILE" \
      -f "$COMPOSE_EXTERNAL_FILE" \
      up -d --no-deps --force-recreate timescaledb
    wait_for_compose_timescale_exec
    run_compose_archive_selftest
    run_compose_wal_catchup
    docker inspect -f '{{.State.Health.Status}}' "$TIMESCALE_CONTAINER" >/dev/null 2>&1 || true
    log "Compose TimescaleDB restart and WAL archive catch-up completed; verify signed evidence before live promotion"
  else
    log "Compose TimescaleDB restart still required for WAL archive mount/settings"
  fi
}

configure_postgres_archive() {
  if [ "$COMPOSE_MODE" -eq 1 ]; then
    configure_compose_archive
    return 0
  fi
  local conf_dir="/etc/postgresql/${POSTGRES_VERSION}/main/conf.d"
  local main_conf="/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf"
  local target="${conf_dir}/trading-backup-evidence.conf"
  local tmp

  if [ ! -d "$conf_dir" ] && [ ! -f "$main_conf" ]; then
    log "local PostgreSQL config not found for version ${POSTGRES_VERSION}; skipping archive config"
    return 0
  fi

  install -d -o postgres -g postgres -m 0755 "$conf_dir"
  if [ -f "$main_conf" ] && ! grep -Eq "^[[:space:]]*include_dir[[:space:]]*=[[:space:]]*'conf.d'" "$main_conf"; then
    printf "\ninclude_dir = 'conf.d'\n" >> "$main_conf"
  fi

  tmp="$(mktemp)"
  cat > "$tmp" <<EOF
# Managed by ops/server/install_backup_evidence_gate.sh.
wal_level = replica
archive_mode = on
archive_command = '${BACKUP_SCRIPT_DST_DIR}/wal_archive.sh "%p" "%f"'
archive_timeout = '60s'
EOF
  if install_if_changed "$tmp" "$target" 0644 postgres postgres; then
    log "wrote PostgreSQL archive config: ${target}"
    if [ "$RESTART_POSTGRES" -eq 1 ]; then
      if command -v pg_ctlcluster >/dev/null 2>&1; then
        pg_ctlcluster "$POSTGRES_VERSION" main restart
      else
        systemctl restart postgresql
      fi
      log "restarted PostgreSQL"
    else
      log "PostgreSQL restart still required for wal_level/archive_mode changes"
    fi
  fi
  rm -f "$tmp"
}

install_systemd_units() {
  log "installing backup systemd units"
  install -d -o root -g root -m 0755 "$SYSTEMD_DST_DIR"

  local unit changed=0
  for unit in \
    trading-base-backup.service \
    trading-base-backup.timer \
    trading-backup-evidence.service \
    trading-backup-evidence.timer \
    trading-backup-prune.service \
    trading-backup-prune.timer \
    trading-restore-drill.service \
    trading-restore-drill.timer
  do
    [ -f "${SCRIPT_DIR}/systemd/${unit}" ] || die "missing source unit: ${unit}"
    if install_if_changed "${SCRIPT_DIR}/systemd/${unit}" "${SYSTEMD_DST_DIR}/${unit}" 0644 root root; then
      changed=1
    fi
  done

  if [ "$COMPOSE_MODE" -eq 1 ]; then
    install_compose_systemd_overrides
    changed=1
  fi

  if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    if [ "$changed" -eq 1 ]; then
      systemctl daemon-reload
    fi
    if [ "$RUN_EVIDENCE" -eq 1 ]; then
      systemctl enable \
        trading-base-backup.timer \
        trading-backup-evidence.timer \
        trading-backup-prune.timer \
        trading-restore-drill.timer
    else
      systemctl enable --now \
        trading-base-backup.timer \
        trading-backup-evidence.timer \
        trading-backup-prune.timer \
        trading-restore-drill.timer
    fi
  else
    log "systemd is not active; units copied but not enabled"
  fi
}

install_compose_systemd_overrides() {
  [ "$COMPOSE_MODE" -eq 1 ] || return 0
  log "installing Compose service overrides for backup evidence timers"
  local service override_dir override_file
  for service in trading-base-backup.service trading-backup-evidence.service trading-restore-drill.service; do
    override_dir="${SYSTEMD_DST_DIR}/${service}.d"
    override_file="${override_dir}/zz-compose-timescaledb.conf"
    install -d -o root -g root -m 0755 "$override_dir"
    cat > "$override_file" <<EOF
[Service]
User=root
Group=${TRADING_GROUP}
Environment=PGHOST=127.0.0.1
Environment=PGPORT=${TIMESCALE_PORT}
Environment=PGUSER=${TIMESCALE_USER}
Environment=PGDATABASE=${POSTGRES_DB}
Environment=TS_BACKUP_BASE_DIR=${BACKUP_BASE_DIR}
Environment=TS_BACKUP_WAL_DIR=${BACKUP_WAL_DIR}
Environment=TS_RESTORE_DRILL_DIR=${BACKUP_DRILL_DIR}
Environment=TS_BACKUP_DOCKER_IMAGE=
Environment=TS_BACKUP_DOCKER_EXEC_CONTAINER=${TIMESCALE_CONTAINER}
Environment=TS_BACKUP_DOCKER_EXEC_USER=postgres
Environment=TS_BACKUP_READ_GROUP=${TRADING_GROUP}
Environment=TS_BACKUP_EVIDENCE_OPERATOR_USER=${TRADING_OPERATOR_USER}
Environment=TS_BACKUP_WAL_TARGET_OWNER_UID=${COMPOSE_POSTGRES_UID}
Environment=TS_BACKUP_WAL_TARGET_GROUP=${TRADING_GROUP}
Environment=TS_BACKUP_WAL_TARGET_DIR_MODE=2750
# Deliberately check-only: backlog drain stays operator-triggered because a large
# .ready backlog can consume the backup dataset and should follow the runbook.
Environment=TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP=0
Environment=TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL=0
Environment=TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP=0
Environment=TS_BACKUP_EVIDENCE_WAL_CATCHUP_TIMEOUT_S=300
Environment=TS_RESTORE_DOCKER_IMAGE=${TIMESCALE_IMAGE}
Environment=TS_RESTORE_DOCKER_USER=postgres
Environment=TS_RESTORE_DRILL_ALLOW_DIRECT=1
Environment=TS_RESTORE_DB=${POSTGRES_DB}
Environment=TS_RESTORE_USER=${TIMESCALE_USER}
Restart=no
EOF
    chmod 0644 "$override_file"
  done
  if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    systemctl daemon-reload
  fi
}

run_evidence() {
  log "running backup/WAL/restore evidence job"
  if [ "$COMPOSE_MODE" -eq 1 ]; then
    env \
      PGHOST=127.0.0.1 \
      PGPORT="$TIMESCALE_PORT" \
      PGUSER="$TIMESCALE_USER" \
      PGPASSWORD="$TIMESCALE_PASSWORD" \
      PGDATABASE="$POSTGRES_DB" \
      TS_BACKUP_BASE_DIR="$BACKUP_BASE_DIR" \
      TS_BACKUP_WAL_DIR="$BACKUP_WAL_DIR" \
      TS_RESTORE_DRILL_DIR="$BACKUP_DRILL_DIR" \
      TS_BACKUP_DOCKER_IMAGE= \
      TS_BACKUP_DOCKER_EXEC_CONTAINER="$TIMESCALE_CONTAINER" \
      TS_BACKUP_DOCKER_EXEC_USER=postgres \
      TS_BACKUP_READ_GROUP="$TRADING_GROUP" \
      TS_BACKUP_EVIDENCE_OPERATOR_USER="$TRADING_OPERATOR_USER" \
      TS_BACKUP_WAL_TARGET_OWNER_UID="$COMPOSE_POSTGRES_UID" \
      TS_BACKUP_WAL_TARGET_GROUP="$TRADING_GROUP" \
      TS_BACKUP_WAL_TARGET_DIR_MODE=2750 \
      TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP=1 \
      TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL=1 \
      TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP=1 \
      TS_RESTORE_DOCKER_IMAGE="$TIMESCALE_IMAGE" \
      TS_RESTORE_DOCKER_USER=postgres \
      TS_RESTORE_DRILL_ALLOW_DIRECT=1 \
      TS_BACKUP_EVIDENCE_WAIT_LOCK=1 \
      TS_RESTORE_DB="$POSTGRES_DB" \
      TS_RESTORE_USER="$TIMESCALE_USER" \
      BACKUP_EVIDENCE_PATH="${BACKUP_EVIDENCE_DIR}/latest_backup_restore_evidence.json" \
      BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1 \
      BACKUP_EVIDENCE_HMAC_KEY_FILE="$BACKUP_EVIDENCE_HMAC_KEY_FILE" \
      "${BACKUP_SCRIPT_DST_DIR}/backup_restore_evidence.sh"
  else
    runuser -u postgres -- env \
      PGHOST="$POSTGRES_SOCKET_DIR" \
      PGPORT="${PGPORT:-5432}" \
      PGUSER=postgres \
      PGDATABASE="$POSTGRES_DB" \
      TS_BACKUP_BASE_DIR="$BACKUP_BASE_DIR" \
      TS_BACKUP_WAL_DIR="$BACKUP_WAL_DIR" \
      TS_RESTORE_DRILL_DIR="$BACKUP_DRILL_DIR" \
      TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP=1 \
      TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL=1 \
      TS_BACKUP_EVIDENCE_OPERATOR_USER="$TRADING_OPERATOR_USER" \
      BACKUP_EVIDENCE_PATH="${BACKUP_EVIDENCE_DIR}/latest_backup_restore_evidence.json" \
      BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1 \
      BACKUP_EVIDENCE_HMAC_KEY_FILE="$BACKUP_EVIDENCE_HMAC_KEY_FILE" \
      "${BACKUP_SCRIPT_DST_DIR}/backup_restore_evidence.sh"
  fi
}

start_backup_timers() {
  if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    systemctl start \
      trading-base-backup.timer \
      trading-backup-evidence.timer \
      trading-backup-prune.timer \
      trading-restore-drill.timer
  fi
}

main() {
  require_root
  if [ "$COMPOSE_MODE" -eq 1 ]; then
    load_compose_env
    normalize_compose_storage_env
  else
    detect_postgres_bin_dir
  fi
  ensure_trading_user
  if [ "$COMPOSE_MODE" -eq 0 ]; then
    ensure_group_membership postgres "$TRADING_GROUP"
  fi
  ensure_backup_evidence_hmac_key
  create_backup_layout
  grant_operator_backup_evidence_access
  install_backup_scripts
  write_runtime_env
  configure_postgres_archive
  install_systemd_units

  if [ "$RUN_EVIDENCE" -eq 1 ]; then
    start_backup_timers
    run_evidence
    grant_operator_backup_evidence_access
  fi

  log "installed backup evidence gate assets"
  log "verify with: ${BACKUP_SCRIPT_DST_DIR}/backup_restore_evidence.sh"
  log "preflight with: ENV=prod ENGINE_MODE=live PREFLIGHT_REQUIRE_BACKUP_EVIDENCE=1 python engine/runtime/prod_preflight.py --json"
}

main
