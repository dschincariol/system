#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[install_backup_evidence_gate] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TRADING_USER="${TRADING_USER:-trading}"
TRADING_GROUP="${TRADING_GROUP:-trading}"
INSTALL_ROOT="${TRADING_INSTALL_ROOT:-/opt/trading}"
BACKUP_SCRIPT_DST_DIR="${TRADING_BACKUP_SCRIPT_DIR:-${INSTALL_ROOT}/ops/backup}"
BACKUP_ROOT="${TRADING_BACKUP_ROOT:-/var/backups/trading}"
BACKUP_BASE_DIR="${TRADING_BACKUP_BASE_DIR:-${BACKUP_ROOT}/base}"
BACKUP_WAL_DIR="${TRADING_BACKUP_WAL_DIR:-${BACKUP_ROOT}/wal}"
BACKUP_DRILL_DIR="${TRADING_BACKUP_DRILL_DIR:-${BACKUP_ROOT}/drills}"
BACKUP_EVIDENCE_DIR="${TRADING_BACKUP_EVIDENCE_DIR:-${BACKUP_ROOT}/evidence}"
ETC_DIR="${TRADING_ETC_DIR:-/etc/trading}"
TRADING_ENV_FILE="${TRADING_ENV_FILE:-${ETC_DIR}/trading.env}"
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
TIMESCALE_IMAGE="${TRADING_TIMESCALE_IMAGE:-}"
TIMESCALE_PORT="${TRADING_TIMESCALE_PORT:-5432}"
TIMESCALE_USER="${TRADING_TIMESCALE_USER:-trading}"
TIMESCALE_PASSWORD="${TRADING_TIMESCALE_PASSWORD:-}"
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

usage() {
  cat <<'EOF'
Usage: install_backup_evidence_gate.sh [--compose] [--restart-postgres] [--run-evidence]

Installs only the backup/WAL/restore evidence production assets:
  - /opt/trading/ops/backup scripts
  - /var/backups/trading filesystem layout
  - backup/restore systemd timers
  - PostgreSQL archive config when a local cluster config exists
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

ensure_trading_user() {
  if ! getent group "$TRADING_GROUP" >/dev/null 2>&1; then
    groupadd --system "$TRADING_GROUP"
  fi
  if ! id -u "$TRADING_USER" >/dev/null 2>&1; then
    useradd --system --gid "$TRADING_GROUP" --home-dir "${TRADING_DATA_ROOT:-/var/lib/trading}" --shell /usr/sbin/nologin "$TRADING_USER"
  fi
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

load_compose_env() {
  [ -f "$COMPOSE_ENV_FILE" ] || die "compose env file missing: ${COMPOSE_ENV_FILE}"

  TIMESCALE_IMAGE="${TRADING_TIMESCALE_IMAGE:-$(compose_env_value TIMESCALE_IMAGE)}"
  TIMESCALE_IMAGE="${TIMESCALE_IMAGE:-timescale/timescaledb:latest-pg16}"
  TIMESCALE_PORT="${TRADING_TIMESCALE_PORT:-$(compose_env_value TIMESCALE_PORT)}"
  TIMESCALE_PORT="${TIMESCALE_PORT:-5432}"
  TIMESCALE_USER="${TRADING_TIMESCALE_USER:-$(compose_env_value TIMESCALE_USER)}"
  TIMESCALE_USER="${TIMESCALE_USER:-trading}"
  TIMESCALE_PASSWORD="${TRADING_TIMESCALE_PASSWORD:-$(compose_env_value TIMESCALE_PASSWORD)}"
  POSTGRES_DB="${TRADING_POSTGRES_DB:-$(compose_env_value TIMESCALE_DB)}"
  POSTGRES_DB="${POSTGRES_DB:-trading}"
  [ -n "$TIMESCALE_PASSWORD" ] || die "TIMESCALE_PASSWORD is required in compose mode"
}

compose_postgres_uid_gid() {
  docker run --rm "$TIMESCALE_IMAGE" sh -lc 'printf "%s:%s\n" "$(id -u postgres)" "$(id -g postgres)"'
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
    base_backup.sh \
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
    local pg_uid_gid
    pg_uid_gid="$(compose_postgres_uid_gid)"
    chown "$pg_uid_gid" "$BACKUP_ROOT"
    chown -R "$pg_uid_gid" "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR"
    chmod 0770 "$BACKUP_ROOT" "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR"
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
}

configure_compose_archive() {
  [ -f "$COMPOSE_EXTERNAL_FILE" ] || die "compose external-services file missing: ${COMPOSE_EXTERNAL_FILE}"
  grep -q '/var/backups/trading/wal' "$COMPOSE_EXTERNAL_FILE" || die "compose file missing WAL archive bind mount: ${COMPOSE_EXTERNAL_FILE}"
  grep -q 'archive_mode=on' "$COMPOSE_EXTERNAL_FILE" || die "compose file missing archive_mode=on: ${COMPOSE_EXTERNAL_FILE}"
  grep -q 'archive_command=' "$COMPOSE_EXTERNAL_FILE" || die "compose file missing archive_command: ${COMPOSE_EXTERNAL_FILE}"
  if [ "$RESTART_POSTGRES" -eq 1 ]; then
    log "recreating Compose TimescaleDB service to apply WAL archive settings"
    docker compose \
      --env-file "$COMPOSE_ENV_FILE" \
      -f "$COMPOSE_EXTERNAL_FILE" \
      -f "$COMPOSE_STACK_FILE" \
      up -d timescaledb
    docker inspect -f '{{.State.Health.Status}}' "$TIMESCALE_CONTAINER" >/dev/null 2>&1 || true
    log "Compose TimescaleDB restart requested; verify container health before live promotion"
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
    override_file="${override_dir}/10-compose-timescaledb.conf"
    install -d -o root -g root -m 0755 "$override_dir"
    cat > "$override_file" <<EOF
[Service]
User=root
Group=root
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
Environment=TS_RESTORE_DOCKER_IMAGE=${TIMESCALE_IMAGE}
Environment=TS_RESTORE_DOCKER_USER=postgres
Environment=TS_RESTORE_DRILL_ALLOW_DIRECT=1
Environment=TS_RESTORE_DB=${POSTGRES_DB}
Environment=TS_RESTORE_USER=${TIMESCALE_USER}
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
      TS_RESTORE_DOCKER_IMAGE="$TIMESCALE_IMAGE" \
      TS_RESTORE_DOCKER_USER=postgres \
      TS_RESTORE_DRILL_ALLOW_DIRECT=1 \
      TS_BACKUP_EVIDENCE_WAIT_LOCK=1 \
      TS_RESTORE_DB="$POSTGRES_DB" \
      TS_RESTORE_USER="$TIMESCALE_USER" \
      BACKUP_EVIDENCE_PATH="${BACKUP_EVIDENCE_DIR}/latest_backup_restore_evidence.json" \
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
      BACKUP_EVIDENCE_PATH="${BACKUP_EVIDENCE_DIR}/latest_backup_restore_evidence.json" \
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
  else
    detect_postgres_bin_dir
  fi
  ensure_trading_user
  if [ "$COMPOSE_MODE" -eq 0 ]; then
    ensure_group_membership postgres "$TRADING_GROUP"
  fi
  create_backup_layout
  install_backup_scripts
  write_runtime_env
  configure_postgres_archive
  install_systemd_units

  if [ "$RUN_EVIDENCE" -eq 1 ]; then
    start_backup_timers
    run_evidence
  fi

  log "installed backup evidence gate assets"
  log "verify with: ${BACKUP_SCRIPT_DST_DIR}/backup_restore_evidence.sh"
  log "preflight with: ENV=prod ENGINE_MODE=live PREFLIGHT_REQUIRE_BACKUP_EVIDENCE=1 python engine/runtime/prod_preflight.py --json"
}

main
