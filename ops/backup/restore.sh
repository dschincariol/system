#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: restore.sh --target-time <ISO8601|latest> --into <directory> [--allow-trade-paused] [--force]
EOF
}

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=restore %s\n' "$level" "$event" "$*"
}

die() {
  log error "$1" "${2:-}"
  exit 1
}

target_time=""
into=""
force=0
allow_trade_paused=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --target-time)
      [ "$#" -ge 2 ] || die invalid_args "missing=--target-time"
      target_time="$2"
      shift 2
      ;;
    --into)
      [ "$#" -ge 2 ] || die invalid_args "missing=--into"
      into="$2"
      shift 2
      ;;
    --allow-trade-paused)
      allow_trade_paused=1
      shift
      ;;
    --force)
      force=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die invalid_arg "arg=$1"
      ;;
  esac
done

[ -n "$target_time" ] || die invalid_args "missing=--target-time"
[ -n "$into" ] || die invalid_args "missing=--into"

base_dir="${TS_RESTORE_BASE_BACKUP_DIR:-${TS_BACKUP_BASE_DIR:-/var/backups/trading/base}}"
wal_dir="${TS_BACKUP_WAL_DIR:-/var/backups/trading/wal}"
restore_port="${TS_RESTORE_PORT:-55432}"
restore_db="${TS_RESTORE_DB:-${PGDATABASE:-trading}}"
restore_user="${TS_RESTORE_USER:-postgres}"
restore_timeout_s="${TS_RESTORE_TIMEOUT_S:-900}"
socket_dir="${TS_RESTORE_SOCKET_DIR:-}"
started_at_epoch="$(date +%s)"

quote_conf() {
  local value="$1"
  printf "'%s'" "$(printf '%s' "$value" | sed "s/'/''/g")"
}

extract_tar() {
  local archive="$1"
  local dest="$2"
  case "$archive" in
    *.tar.gz|*.tgz) tar -xzf "$archive" -C "$dest" ;;
    *.tar) tar -xf "$archive" -C "$dest" ;;
    *) die unsupported_archive "archive=${archive}" ;;
  esac
}

base_tar_for() {
  local backup_dir="$1"
  if [ -f "${backup_dir}/base.tar.gz" ]; then
    printf '%s\n' "${backup_dir}/base.tar.gz"
    return 0
  fi
  if [ -f "${backup_dir}/base.tar" ]; then
    printf '%s\n' "${backup_dir}/base.tar"
    return 0
  fi
  return 1
}

wal_tar_for() {
  local backup_dir="$1"
  if [ -f "${backup_dir}/pg_wal.tar.gz" ]; then
    printf '%s\n' "${backup_dir}/pg_wal.tar.gz"
    return 0
  fi
  if [ -f "${backup_dir}/pg_wal.tar" ]; then
    printf '%s\n' "${backup_dir}/pg_wal.tar"
    return 0
  fi
  return 1
}

verify_backup_dir() {
  local backup_dir="$1"
  local verify_log="${backup_dir}/pg_verifybackup.restore.out"
  local verify_dir base_tar wal_tar rc

  [ -f "${backup_dir}/backup_manifest" ] || die manifest_missing "backup_dir=${backup_dir}"
  base_tar="$(base_tar_for "$backup_dir")" || die base_tar_missing "backup_dir=${backup_dir}"
  wal_tar="$(wal_tar_for "$backup_dir")" || die wal_tar_missing "backup_dir=${backup_dir}"

  if "${PGVERIFYBACKUP_BIN:-pg_verifybackup}" "$backup_dir" > "$verify_log" 2>&1; then
    log info verified "backup_dir=${backup_dir} verify_log=${verify_log} verify_mode=direct"
    return 0
  fi
  printf '\n-- retrying after tar extraction for pg_verifybackup versions that require plain backups --\n' >> "$verify_log"

  verify_dir="$(mktemp -d "${TMPDIR:-/tmp}/trading-pg-verify.XXXXXX")"
  rc=0
  {
    extract_tar "$base_tar" "$verify_dir"
    mkdir -p "${verify_dir}/pg_wal"
    extract_tar "$wal_tar" "${verify_dir}/pg_wal"
    cp "${backup_dir}/backup_manifest" "${verify_dir}/backup_manifest"
    "${PGVERIFYBACKUP_BIN:-pg_verifybackup}" "$verify_dir"
  } >> "$verify_log" 2>&1 || rc=$?
  rm -rf "$verify_dir"
  if [ "$rc" -ne 0 ]; then
    die verify_failed "backup_dir=${backup_dir} verify_log=${verify_log} rc=${rc}"
  fi
  log info verified "backup_dir=${backup_dir} verify_log=${verify_log}"
}

backup_epoch() {
  local backup_dir="$1"
  local name epoch
  name="$(basename "$backup_dir")"
  epoch="$(date -u -d "${name}" +%s 2>/dev/null || true)"
  if [ -n "$epoch" ]; then
    printf '%s\n' "$epoch"
  else
    stat -c %Y "$backup_dir"
  fi
}

select_backup() {
  local target="$1"
  local selected="" selected_epoch=0 target_epoch backup epoch
  if [ "$target" = "latest" ]; then
    if [ -L "${base_dir}/latest" ] || [ -d "${base_dir}/latest" ]; then
      selected="$(readlink -f "${base_dir}/latest")"
    else
      selected="$(find "$base_dir" -mindepth 1 -maxdepth 1 -type d ! -name '.*' ! -name '*.in_progress' -printf '%T@ %p\n' | sort -rn | head -n 1 | cut -d' ' -f2-)"
    fi
    [ -n "$selected" ] || die no_backup "base_dir=${base_dir}"
    printf '%s\n' "$selected"
    return 0
  fi

  target_epoch="$(date -u -d "$target" +%s 2>/dev/null || true)"
  [ -n "$target_epoch" ] || die invalid_target_time "target_time=${target}"
  while IFS= read -r backup; do
    [ -d "$backup" ] || continue
    epoch="$(backup_epoch "$backup")"
    if [ "$epoch" -le "$target_epoch" ] && [ "$epoch" -ge "$selected_epoch" ]; then
      selected="$backup"
      selected_epoch="$epoch"
    fi
  done < <(find "$base_dir" -mindepth 1 -maxdepth 1 -type d ! -name '.*' ! -name '*.in_progress' -print)
  [ -n "$selected" ] || die no_backup_before_target "base_dir=${base_dir} target_time=${target}"
  printf '%s\n' "$selected"
}

clear_target_dir() {
  local target="$1"
  local resolved
  if ! resolved="$(realpath -m "$target" 2>/dev/null)" || [ -z "$resolved" ]; then
    resolved="$(python3 - "$target" <<'PY'
from pathlib import Path
import sys

print(Path(sys.argv[1]).expanduser().resolve(strict=False))
PY
)"
  fi
  case "$resolved" in
    /|/var|/var/lib|/var/backups|/tmp)
      die unsafe_target_dir "target=${resolved}"
      ;;
  esac
  mkdir -p "$resolved"
  if [ -n "$(find "$resolved" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    if [ "$force" -ne 1 ]; then
      die target_not_empty "target=${resolved} hint=pass_--force"
    fi
    find "$resolved" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
  printf '%s\n' "$resolved"
}

extract_backup_to_target() {
  local backup_dir="$1"
  local target="$2"
  local base_tar wal_tar
  base_tar="$(base_tar_for "$backup_dir")" || die base_tar_missing "backup_dir=${backup_dir}"
  wal_tar="$(wal_tar_for "$backup_dir")" || die wal_tar_missing "backup_dir=${backup_dir}"
  extract_tar "$base_tar" "$target"
  mkdir -p "${target}/pg_wal"
  extract_tar "$wal_tar" "${target}/pg_wal"
  cp "${backup_dir}/backup_manifest" "${target}/backup_manifest"
  rm -f "${target}/standby.signal"
  rm -f "${target}/postmaster.pid"
  chmod 0700 "$target"
}

configure_recovery() {
  local target="$1"
  local recovery_conf="${target}/postgresql.auto.conf"
  local restore_cmd
  restore_cmd="cp ${wal_dir}/%f %p"
  touch "${target}/recovery.signal"
  {
    printf '\n# Added by trading restore.sh at %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'archive_mode = off\n'
    printf 'restore_command = %s\n' "$(quote_conf "$restore_cmd")"
    printf 'recovery_target_action = %s\n' "$(quote_conf promote)"
    if [ "$target_time" != "latest" ]; then
      printf 'recovery_target_time = %s\n' "$(quote_conf "$target_time")"
    fi
  } >> "$recovery_conf"
}

psql_restore() {
  PGUSER="$restore_user" psql -X -v ON_ERROR_STOP=1 -h "$socket_dir" -p "$restore_port" -d "$restore_db" "$@"
}

write_env_line() {
  local key="$1"
  local value="$2"
  printf '%s=%q\n' "$key" "$value"
}

start_recovered_postgres() {
  local target="$1"
  local log_file="${target}/restore-postgres.log"
  mkdir -p "$socket_dir"
  "${PGCTL_BIN:-pg_ctl}" -D "$target" -o "-p ${restore_port} -h 127.0.0.1 -k ${socket_dir}" -l "$log_file" start >/dev/null
  log info postgres_started "pgdata=${target} port=${restore_port} socket_dir=${socket_dir} log=${log_file}"
}

wait_for_recovery_complete() {
  local deadline in_recovery
  deadline="$(($(date +%s) + restore_timeout_s))"
  in_recovery=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if PGUSER="$restore_user" pg_isready -h "$socket_dir" -p "$restore_port" -d "$restore_db" >/dev/null 2>&1; then
      in_recovery="$(psql_restore -Atqc "SELECT pg_is_in_recovery();" 2>/dev/null || true)"
      if [ "$in_recovery" = "f" ]; then
        return 0
      fi
    fi
    sleep 1
  done
  die recovery_timeout "timeout_s=${restore_timeout_s} in_recovery=${in_recovery:-unknown}"
}

smoke_query() {
  psql_restore -Atqc "SET search_path=trading,public; SELECT COUNT(*) FROM model_registry;" >/dev/null
}

trip_kill_switch() {
  local now_ms audit_requires_hash
  now_ms="$(date +%s%3N)"
  psql_restore <<SQL
CREATE SCHEMA IF NOT EXISTS trading;
SET search_path=trading,public;
CREATE TABLE IF NOT EXISTS kill_switch_state (
  scope TEXT NOT NULL,
  key TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 0,
  reason TEXT,
  actor TEXT NOT NULL DEFAULT 'system',
  meta_json TEXT,
  created_ts_ms BIGINT NOT NULL,
  updated_ts_ms BIGINT NOT NULL,
  PRIMARY KEY (scope, key)
);
CREATE TABLE IF NOT EXISTS kill_switch_audit (
  id BIGSERIAL PRIMARY KEY,
  ts_ms BIGINT NOT NULL,
  action TEXT NOT NULL,
  scope TEXT NOT NULL,
  key TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  actor TEXT NOT NULL,
  reason TEXT,
  meta_json TEXT
);
INSERT INTO kill_switch_state(scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms)
VALUES ('global', 'global', 1, 'restore_recovered_trade_pause', 'ops.restore', '{"restore_target_time":"${target_time}"}', ${now_ms}, ${now_ms})
ON CONFLICT(scope, key) DO UPDATE SET
  enabled=1,
  reason=excluded.reason,
  actor=excluded.actor,
  meta_json=excluded.meta_json,
  updated_ts_ms=excluded.updated_ts_ms;
SQL
  audit_requires_hash="$(
    psql_restore -Atqc "
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'trading'
          AND table_name = 'kill_switch_audit'
          AND column_name = 'row_hash'
          AND is_nullable = 'NO'
      );
    " 2>/dev/null || true
  )"
  if [ "$audit_requires_hash" = "t" ]; then
    log warn restore_audit_insert_skipped "message=kill_switch_state_is_paused audit_table_requires_runtime_hash_chain"
    return 0
  fi
  if ! psql_restore <<SQL; then
INSERT INTO kill_switch_audit(ts_ms, action, scope, key, enabled, actor, reason, meta_json)
VALUES (${now_ms}, 'RESTORE', 'global', 'global', 1, 'ops.restore', 'restore_recovered_trade_pause', '{"restore_target_time":"${target_time}"}');
SQL
    log warn restore_audit_insert_failed "message=kill_switch_state_is_paused audit_table_requires_runtime_hash_chain"
  fi
}

selected_backup="$(select_backup "$target_time")"
target_dir="$(clear_target_dir "$into")"
if [ -z "$socket_dir" ]; then
  socket_dir="${target_dir}/run"
fi

if [ "$allow_trade_paused" -ne 1 ]; then
  log warn trade_pause_implicit "message=restored_database_will_be_kill_switched"
fi

log info restore_started "backup_dir=${selected_backup} target_dir=${target_dir} target_time=${target_time} port=${restore_port}"
verify_backup_dir "$selected_backup"
extract_backup_to_target "$selected_backup" "$target_dir"
configure_recovery "$target_dir"
start_recovered_postgres "$target_dir"
wait_for_recovery_complete
smoke_query
trip_kill_switch

checkpoint_lsn="$(pg_controldata "$target_dir" 2>/dev/null | awk -F: '/Latest checkpoint location/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' || true)"
recovered_to_time="$(psql_restore -Atqc "SELECT COALESCE(pg_last_xact_replay_timestamp()::text, now()::text);" 2>/dev/null || true)"
elapsed_s="$(($(date +%s) - started_at_epoch))"

{
  write_env_line PGDATA "$target_dir"
  write_env_line PGHOST "$socket_dir"
  write_env_line PGPORT "$restore_port"
  write_env_line PGDATABASE "$restore_db"
  write_env_line PGUSER "$restore_user"
  write_env_line BACKUP_DIR "$selected_backup"
  write_env_line TARGET_TIME "$target_time"
  write_env_line CHECKPOINT_LSN "$checkpoint_lsn"
  write_env_line RECOVERED_TO_TIME "$recovered_to_time"
  write_env_line ELAPSED_S "$elapsed_s"
} > "${target_dir}/restore.env"

log info restore_complete "pgdata=${target_dir} port=${restore_port} checkpoint_lsn=${checkpoint_lsn:-unknown} recovered_to_time=${recovered_to_time:-unknown} elapsed_s=${elapsed_s} kill_switch=tripped"
