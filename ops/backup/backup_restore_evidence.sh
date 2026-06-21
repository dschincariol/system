#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=backup_restore_evidence %s\n' "$level" "$event" "$*"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

stamp="${TS_BACKUP_EVIDENCE_STAMP:-$(date -u +%Y-%m-%dT%H%M%SZ)}"
generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
evidence_dir="${TS_BACKUP_EVIDENCE_DIR:-/var/backups/trading/evidence}"
work_dir="${TS_BACKUP_EVIDENCE_WORK_DIR:-${evidence_dir}/work/${stamp}}"
report_txt="${evidence_dir}/backup_restore_evidence_${stamp}.txt"
report_json="${evidence_dir}/backup_restore_evidence_${stamp}.json"
latest_txt="${evidence_dir}/latest_backup_restore_evidence.txt"
latest_json="${BACKUP_EVIDENCE_PATH:-${evidence_dir}/latest_backup_restore_evidence.json}"

base_backup_script="${TS_BASE_BACKUP_SCRIPT:-${script_dir}/base_backup.sh}"
wal_archive_script="${TS_WAL_ARCHIVE_SCRIPT:-${script_dir}/wal_archive.sh}"
wal_catchup_script="${TS_WAL_ARCHIVE_CATCHUP_SCRIPT:-${script_dir}/wal_archive_catchup.sh}"
restore_script="${TS_RESTORE_SCRIPT:-${script_dir}/restore.sh}"
restore_drill_script="${TS_RESTORE_DRILL_SCRIPT:-${script_dir}/restore_drill.sh}"
evidence_script="${TS_BACKUP_EVIDENCE_SCRIPT:-${script_dir}/backup_restore_evidence.sh}"
base_dir="${TS_BACKUP_BASE_DIR:-/var/backups/trading/base}"
wal_dir="${TS_BACKUP_WAL_DIR:-/var/backups/trading/wal}"
drill_dir="${TS_RESTORE_DRILL_DIR:-/var/backups/trading/drills}"
systemd_unit_dir="${TS_BACKUP_SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
wal_wait_s="${TS_BACKUP_WAL_VERIFY_TIMEOUT_S:-120}"
wal_rpo_s="${BACKUP_EVIDENCE_WAL_RPO_S:-${BACKUP_EVIDENCE_RPO_S:-${BACKUP_RPO_S:-120}}}"
base_reuse_max_s="${TS_BACKUP_EVIDENCE_REUSE_BASE_MAX_AGE_S:-${BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S:-93600}}"
restore_reuse_max_s="${TS_BACKUP_EVIDENCE_REUSE_RESTORE_MAX_AGE_S:-${BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S:-7776000}}"
lock_timeout_s="${TS_BACKUP_EVIDENCE_LOCK_TIMEOUT_S:-30}"
probe_timeout_s="${TS_BACKUP_EVIDENCE_PROBE_TIMEOUT_S:-5}"
systemctl_timeout_s="${TS_BACKUP_EVIDENCE_SYSTEMCTL_TIMEOUT_S:-5}"
base_backup_timeout_s="${TS_BACKUP_EVIDENCE_BASE_BACKUP_TIMEOUT_S:-${TS_BASE_BACKUP_TIMEOUT_S:-7200}}"
wal_switch_timeout_s="${TS_BACKUP_EVIDENCE_WAL_SWITCH_TIMEOUT_S:-30}"
wal_archiver_stats_timeout_s="${TS_BACKUP_EVIDENCE_WAL_ARCHIVER_STATS_TIMEOUT_S:-30}"
wal_catchup_timeout_s="${TS_BACKUP_EVIDENCE_WAL_CATCHUP_TIMEOUT_S:-300}"
restore_drill_timeout_s="${TS_BACKUP_EVIDENCE_RESTORE_DRILL_TIMEOUT_S:-${TS_RESTORE_DRILL_TIMEOUT_S:-3600}}"
signature_timeout_s="${TS_BACKUP_EVIDENCE_SIGNATURE_TIMEOUT_S:-30}"
publish_timeout_s="${TS_BACKUP_EVIDENCE_PUBLISH_TIMEOUT_S:-30}"

script_checks_status="fail"
compose_checks_status="not_applicable"
systemd_checks_status="fail"
base_backup_status="fail"
wal_archive_status="fail"
wal_catchup_status="not_applicable"
wal_archiver_status="fail"
restore_drill_status="fail"
publish_status="not_started"
base_backup_dir=""
base_verify_log=""
base_verified_at=""
wal_verified_at=""
wal_observed_file=""
wal_catchup_verified_at=""
wal_archiver_verified_at=""
wal_archiver_archive_mode=""
wal_archiver_archive_command=""
wal_archiver_archived_count=""
wal_archiver_last_archived_wal=""
wal_archiver_last_archived_at=""
wal_archiver_last_archived_at_ts=""
wal_archiver_failed_count=""
wal_archiver_last_failed_wal=""
wal_archiver_last_failed_at=""
wal_archiver_last_failed_at_ts=""
wal_archiver_stats_reset_at=""
restore_drill_report=""
restore_drill_verified_at=""
restore_time_to_recover_s=""
signature_status="not_required"
evidence_read_group="${TS_BACKUP_EVIDENCE_READ_GROUP:-${TS_BACKUP_READ_GROUP:-}}"

mkdir -p "$evidence_dir" "$work_dir"

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

timeout_exit_code() {
  case "$1" in
    124|137) return 0 ;;
    *) return 1 ;;
  esac
}

run_with_timeout() {
  local max_s="$1"
  local output_file="$2"
  local label="$3"
  shift 3
  if ! command -v timeout >/dev/null 2>&1; then
    printf '%s_timeout_command_missing=1\n' "$label" >> "$output_file"
    return 127
  fi
  set +e
  timeout "$max_s" "$@" >> "$output_file" 2>&1
  local rc=$?
  set -e
  if timeout_exit_code "$rc"; then
    printf '%s_timeout_s=%s\n' "$label" "$max_s" >> "$output_file"
  elif [ "$rc" -ne 0 ]; then
    printf '%s_exit_code=%s\n' "$label" "$rc" >> "$output_file"
  fi
  return "$rc"
}

run_capture_with_timeout() {
  local max_s="$1"
  local stdout_file="$2"
  local stderr_file="$3"
  local label="$4"
  shift 4
  if ! command -v timeout >/dev/null 2>&1; then
    printf '%s_timeout_command_missing=1\n' "$label" >> "$stderr_file"
    return 127
  fi
  set +e
  timeout "$max_s" "$@" > "$stdout_file" 2>> "$stderr_file"
  local rc=$?
  set -e
  if timeout_exit_code "$rc"; then
    printf '%s_timeout_s=%s\n' "$label" "$max_s" >> "$stderr_file"
  elif [ "$rc" -ne 0 ]; then
    printf '%s_exit_code=%s\n' "$label" "$rc" >> "$stderr_file"
  fi
  return "$rc"
}

backup_evidence_signature_required() {
  is_truthy "${BACKUP_EVIDENCE_REQUIRE_SIGNATURE:-0}" \
    || is_truthy "${BACKUP_EVIDENCE_SIGNATURE_REQUIRED:-0}" \
    || is_truthy "${PREFLIGHT_REQUIRE_BACKUP_EVIDENCE:-0}" \
    || [ "${ENGINE_MODE:-}" = "live" ] \
    || [ "${EXECUTION_MODE:-}" = "live" ]
}

backup_evidence_signing_key_available() {
  [ "$(backup_evidence_signing_key_state)" = "available" ]
}

backup_evidence_signing_key_state() {
  if [ -n "${BACKUP_EVIDENCE_HMAC_KEY:-}" ] || [ -n "${BACKUP_EVIDENCE_SIGNING_KEY:-}" ]; then
    printf 'available\n'
    return 0
  fi
  local name path saw_file=0
  for name in BACKUP_EVIDENCE_HMAC_KEY_FILE BACKUP_EVIDENCE_SIGNING_KEY_FILE; do
    path="${!name:-}"
    [ -n "$path" ] || continue
    saw_file=1
    if [ ! -e "$path" ]; then
      printf 'missing_file\n'
      return 0
    fi
    if [ ! -f "$path" ]; then
      printf 'unreadable\n'
      return 0
    fi
    if [ ! -r "$path" ]; then
      printf 'unreadable\n'
      return 0
    fi
    if [ ! -s "$path" ]; then
      printf 'empty\n'
      return 0
    fi
    printf 'available\n'
    return 0
  done
  if [ "$saw_file" -eq 0 ]; then
    printf 'missing\n'
    return 0
  fi
  printf 'missing\n'
}

file_mtime_iso() {
  date -u -d "@$(stat -c %Y "$1")" +%Y-%m-%dT%H:%M:%SZ
}

report_value() {
  local report="$1"
  local key="$2"
  awk -F= -v key="$key" '$1==key {print $2; exit}' "$report"
}

int_seconds() {
  local value="${1:-0}"
  value="${value%%.*}"
  case "$value" in
    ''|*[!0-9]*) printf '0\n' ;;
    *) printf '%s\n' "$value" ;;
  esac
}

require_listable_dir() {
  local dir="$1"
  local label="$2"
  local output_file="$3"
  if [ ! -d "$dir" ]; then
    printf '%s_missing=%s\n' "$label" "$dir" >> "$output_file"
    return 1
  fi
  if [ ! -r "$dir" ] || [ ! -x "$dir" ]; then
    printf '%s_not_listable=%s\n' "$label" "$dir" >> "$output_file"
    printf '%s_user=%s group=%s\n' "$label" "$(id -un 2>/dev/null || id -u)" "$(id -gn 2>/dev/null || id -g)" >> "$output_file"
    return 1
  fi
  return 0
}

lock_path="${TS_BACKUP_EVIDENCE_LOCK:-${evidence_dir}/backup_restore_evidence.lock}"
exec 9>"$lock_path"
if is_truthy "${TS_BACKUP_EVIDENCE_WAIT_LOCK:-0}"; then
  if ! flock -w "$lock_timeout_s" 9; then
    log error lock_timeout "lock=${lock_path} timeout_s=${lock_timeout_s}"
    exit 75
  fi
elif ! flock -n 9; then
  log error already_running "lock=${lock_path}"
  exit 75
fi

check_scripts() {
  local missing=0
  local path
  : > "${work_dir}/script_checks.out"
  for path in "$base_backup_script" "$wal_archive_script" "$wal_catchup_script" "$restore_script" "$restore_drill_script" "$evidence_script"; do
    if [ ! -f "$path" ] || [ ! -r "$path" ]; then
      printf 'missing_or_unreadable=%s\n' "$path" >> "${work_dir}/script_checks.out"
      missing=1
    else
      printf 'ok=%s\n' "$path" >> "${work_dir}/script_checks.out"
    fi
  done
  if [ "$missing" -eq 0 ]; then
    script_checks_status="pass"
    return 0
  fi
  return 1
}

compose_mode_configured() {
  [ "${TS_BACKUP_EVIDENCE_DEPLOYMENT_MODE:-}" = "compose" ] \
    || [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ] \
    || [ -n "${TS_RESTORE_DOCKER_IMAGE:-}" ]
}

check_compose() {
  local rc=0
  : > "${work_dir}/compose_checks.out"
  if ! compose_mode_configured; then
    printf 'compose_check=not_applicable\n' >> "${work_dir}/compose_checks.out"
    compose_checks_status="not_applicable"
    return 0
  fi
  compose_checks_status="fail"
  printf 'compose_check=enabled\n' >> "${work_dir}/compose_checks.out"
  if [ -z "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
    printf 'missing=TS_BACKUP_DOCKER_EXEC_CONTAINER\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -z "${TS_BACKUP_DOCKER_EXEC_USER:-}" ]; then
    printf 'missing=TS_BACKUP_DOCKER_EXEC_USER\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -z "${TS_RESTORE_DOCKER_IMAGE:-}" ]; then
    printf 'missing=TS_RESTORE_DOCKER_IMAGE\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -z "${TS_RESTORE_DB:-}" ]; then
    printf 'missing=TS_RESTORE_DB\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -z "${TS_RESTORE_USER:-}" ]; then
    printf 'missing=TS_RESTORE_USER\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
    if ! command -v docker >/dev/null 2>&1; then
      printf 'docker=missing\n' >> "${work_dir}/compose_checks.out"
      rc=1
    elif is_truthy "${TS_BACKUP_EVIDENCE_CHECK_COMPOSE_CONTAINER:-1}"; then
      if run_with_timeout 10 "${work_dir}/compose_checks.out" compose_container docker inspect -f '{{.State.Status}}' "$TS_BACKUP_DOCKER_EXEC_CONTAINER"; then
        printf 'container_present=%s\n' "$TS_BACKUP_DOCKER_EXEC_CONTAINER" >> "${work_dir}/compose_checks.out"
      else
        printf 'container_missing_or_unavailable=%s\n' "$TS_BACKUP_DOCKER_EXEC_CONTAINER" >> "${work_dir}/compose_checks.out"
        rc=1
      fi
    fi
  fi
  if [ "$rc" -eq 0 ]; then
    compose_checks_status="pass"
  fi
  return "$rc"
}

check_systemd() {
  local unit rc=0
  local stdout_file stderr_file
  local units=(
    trading-base-backup.service
    trading-base-backup.timer
    trading-backup-evidence.service
    trading-backup-evidence.timer
    trading-backup-prune.service
    trading-backup-prune.timer
    trading-restore-drill.service
    trading-restore-drill.timer
  )
  local timers=(
    trading-base-backup.timer
    trading-backup-evidence.timer
    trading-backup-prune.timer
    trading-restore-drill.timer
  )
  : > "${work_dir}/systemd_checks.out"
  if is_truthy "${TS_BACKUP_EVIDENCE_SKIP_SYSTEMD:-0}"; then
    printf 'systemd_check=skipped\n' >> "${work_dir}/systemd_checks.out"
    systemd_checks_status="pass"
    return 0
  fi
  if ! command -v systemctl >/dev/null 2>&1; then
    printf 'systemctl=missing\n' >> "${work_dir}/systemd_checks.out"
    return 1
  fi
  for unit in "${units[@]}"; do
    stdout_file="${work_dir}/systemd_${unit}.cat.out"
    stderr_file="${work_dir}/systemd_${unit}.cat.err"
    if run_capture_with_timeout "$systemctl_timeout_s" "$stdout_file" "$stderr_file" "systemctl_cat_${unit//[^A-Za-z0-9_]/_}" systemctl cat "$unit" || [ -f "${systemd_unit_dir}/${unit}" ]; then
      printf 'unit_present=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'unit_missing=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
      cat "$stderr_file" >> "${work_dir}/systemd_checks.out"
      rc=1
    fi
  done
  for unit in "${timers[@]}"; do
    stdout_file="${work_dir}/systemd_${unit}.enabled.out"
    stderr_file="${work_dir}/systemd_${unit}.enabled.err"
    if run_capture_with_timeout "$systemctl_timeout_s" "$stdout_file" "$stderr_file" "systemctl_enabled_${unit//[^A-Za-z0-9_]/_}" systemctl is-enabled --quiet "$unit"; then
      printf 'timer_enabled=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'timer_not_enabled=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
      cat "$stderr_file" >> "${work_dir}/systemd_checks.out"
      rc=1
    fi
    stdout_file="${work_dir}/systemd_${unit}.active.out"
    stderr_file="${work_dir}/systemd_${unit}.active.err"
    if run_capture_with_timeout "$systemctl_timeout_s" "$stdout_file" "$stderr_file" "systemctl_active_${unit//[^A-Za-z0-9_]/_}" systemctl is-active --quiet "$unit"; then
      printf 'timer_active=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'timer_not_active=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
      cat "$stderr_file" >> "${work_dir}/systemd_checks.out"
      rc=1
    fi
  done
  if [ "$rc" -eq 0 ]; then
    systemd_checks_status="pass"
  fi
  return "$rc"
}

latest_file() {
  local dir="$1"
  local pattern="$2"
  local label="${3:-latest_file}"
  local stdout_file="${work_dir}/${label}.find.out"
  local stderr_file="${work_dir}/${label}.find.err"
  : > "$stdout_file"
  : > "$stderr_file"
  if ! run_capture_with_timeout "$probe_timeout_s" "$stdout_file" "$stderr_file" "$label" \
    find "$dir" -maxdepth 1 -type f -name "$pattern" -printf '%T@ %p\n'; then
    return 1
  fi
  awk '
        $1 > best {
          best = $1
          $1 = ""
          sub(/^ /, "")
          path = $0
        }
        END {
          if (path != "") {
            print path
          }
        }
      ' "$stdout_file"
}

latest_dir() {
  local dir="$1"
  local label="${2:-latest_dir}"
  local stdout_file="${work_dir}/${label}.find.out"
  local stderr_file="${work_dir}/${label}.find.err"
  : > "$stdout_file"
  : > "$stderr_file"
  if ! run_capture_with_timeout "$probe_timeout_s" "$stdout_file" "$stderr_file" "$label" \
    find "$dir" -mindepth 1 -maxdepth 1 -type d ! -name '.*' ! -name '*.in_progress' -printf '%T@ %p\n'; then
    return 1
  fi
  awk '
        $1 > best {
          best = $1
          $1 = ""
          sub(/^ /, "")
          path = $0
        }
        END {
          if (path != "") {
            print path
          }
        }
      ' "$stdout_file"
}

fresh_enough() {
  local path="$1"
  local max_age_s="$2"
  local mtime age
  [ -e "$path" ] || return 1
  max_age_s="${max_age_s%%.*}"
  [ -n "$max_age_s" ] || return 1
  mtime="$(stat -c %Y "$path")"
  age="$(($(date +%s) - mtime))"
  [ "$age" -le "$max_age_s" ]
}

publish_report_file() {
  local path="$1"
  [ -e "$path" ] || return 0
  chmod 0640 "$path"
  if [ -n "$evidence_read_group" ]; then
    chgrp "$evidence_read_group" "$path"
  fi
}

reuse_base_backup_if_fresh() {
  local latest verify_log
  require_listable_dir "$base_dir" base_dir "${work_dir}/base_backup.out" || return 1
  latest="$(readlink -f "${base_dir}/latest" 2>/dev/null || true)"
  if [ -z "$latest" ] || [ ! -d "$latest" ]; then
    if ! latest="$(latest_dir "$base_dir" base_latest_dir)"; then
      cat "${work_dir}/base_latest_dir.find.err" >> "${work_dir}/base_backup.out"
      return 1
    fi
  fi
  [ -n "$latest" ] && [ -d "$latest" ] || return 1
  verify_log="${latest}/pg_verifybackup.out"
  [ -f "${latest}/backup_manifest" ] || return 1
  [ -s "$verify_log" ] || return 1
  base_backup_dir="$latest"
  base_verify_log="$verify_log"
  base_verified_at="$(file_mtime_iso "$verify_log")"
  if ! fresh_enough "$verify_log" "$base_reuse_max_s"; then
    printf 'base_backup_stale=%s\n' "$latest" >> "${work_dir}/base_backup.out"
    printf 'base_backup_max_age_s=%s\n' "$base_reuse_max_s" >> "${work_dir}/base_backup.out"
    return 1
  fi
  base_backup_status="pass"
  printf 'base_backup_reused=%s\n' "$latest" > "${work_dir}/base_backup.out"
  return 0
}

run_base_backup_from_gate() {
  is_truthy "${TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP:-0}" || is_truthy "${TS_BACKUP_EVIDENCE_FORCE_BASE_BACKUP:-0}"
}

run_base_backup() {
  : > "${work_dir}/base_backup.out"
  if ! is_truthy "${TS_BACKUP_EVIDENCE_FORCE_BASE_BACKUP:-0}" && reuse_base_backup_if_fresh; then
    return 0
  fi
  if ! run_base_backup_from_gate; then
    if [ -n "$base_verify_log" ]; then
      base_backup_status="stale"
    else
      base_backup_status="missing"
    fi
    printf 'base_backup_gate_action=not_running\n' >> "${work_dir}/base_backup.out"
    printf 'base_backup_required_timer=trading-base-backup.timer\n' >> "${work_dir}/base_backup.out"
    return 1
  fi
  set +e
  run_with_timeout "$base_backup_timeout_s" "${work_dir}/base_backup.out" base_backup bash "$base_backup_script"
  local rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    if timeout_exit_code "$rc"; then
      base_backup_status="timeout"
    fi
    return "$rc"
  fi
  base_backup_dir="$(readlink -f "${base_dir}/latest" 2>/dev/null || true)"
  if [ -z "$base_backup_dir" ] || [ ! -d "$base_backup_dir" ]; then
    return 1
  fi
  base_verify_log="${base_backup_dir}/pg_verifybackup.out"
  if [ ! -s "$base_verify_log" ]; then
    return 1
  fi
  base_verified_at="$(file_mtime_iso "$base_verify_log")"
  base_backup_status="pass"
  return 0
}

wal_catchup_enabled() {
  is_truthy "${TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP:-0}" || is_truthy "${TS_BACKUP_EVIDENCE_FORCE_WAL_CATCHUP:-0}"
}

run_wal_archive_catchup() {
  : > "${work_dir}/wal_catchup.out"
  if ! wal_catchup_enabled; then
    wal_catchup_status="skipped"
    printf 'wal_catchup=skipped\n' >> "${work_dir}/wal_catchup.out"
    return 0
  fi

  set +e
  if compose_mode_configured; then
    if [ -z "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
      printf 'missing=TS_BACKUP_DOCKER_EXEC_CONTAINER\n' >> "${work_dir}/wal_catchup.out"
      wal_catchup_status="fail"
      set -e
      return 1
    fi
    run_with_timeout "$wal_catchup_timeout_s" "${work_dir}/wal_catchup.out" wal_catchup \
      docker exec \
        -u "${TS_BACKUP_DOCKER_EXEC_USER:-postgres}" \
        -e PGDATA="${TS_BACKUP_DOCKER_PGDATA:-/var/lib/postgresql/data}" \
        -e TS_BACKUP_ROOT=/var/backups/trading \
        -e TS_BACKUP_WAL_DIR=/var/backups/trading/wal \
        -e TS_WAL_ARCHIVE_SCRIPT=/opt/trading/ops/backup/wal_archive.sh \
        -e TS_WAL_ARCHIVE_REQUIRE_MOUNT=1 \
        "$TS_BACKUP_DOCKER_EXEC_CONTAINER" \
        /opt/trading/ops/backup/wal_archive_catchup.sh
  else
    run_with_timeout "$wal_catchup_timeout_s" "${work_dir}/wal_catchup.out" wal_catchup bash "$wal_catchup_script"
  fi
  local rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    if timeout_exit_code "$rc"; then
      wal_catchup_status="timeout"
    else
      wal_catchup_status="fail"
    fi
    return "$rc"
  fi
  wal_catchup_status="pass"
  wal_catchup_verified_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  return 0
}

verify_wal_archive() {
  local start_epoch deadline latest_file latest_epoch
  : > "${work_dir}/wal_archive.out"
  if ! require_listable_dir "$wal_dir" wal_dir "${work_dir}/wal_archive.out"; then
    wal_archive_status="missing"
    return 1
  fi
  if ! command -v psql >/dev/null 2>&1; then
    printf 'psql=missing\n' >> "${work_dir}/wal_archive.out"
    return 1
  fi
  start_epoch="$(date +%s)"
  if ! run_with_timeout "$wal_switch_timeout_s" "${work_dir}/wal_archive.out" wal_switch env PGCONNECT_TIMEOUT="$(int_seconds "$wal_switch_timeout_s")" psql -X -v ON_ERROR_STOP=1 -Atqc "SELECT pg_switch_wal();"; then
    if grep -q '^wal_switch_timeout_s=' "${work_dir}/wal_archive.out"; then
      wal_archive_status="timeout"
    fi
    return 1
  fi
  deadline="$((start_epoch + wal_wait_s))"
  while [ "$(date +%s)" -le "$deadline" ]; do
    if ! latest_file="$(latest_file "$wal_dir" '*' wal_archive_latest_file)"; then
      cat "${work_dir}/wal_archive_latest_file.find.err" >> "${work_dir}/wal_archive.out"
      wal_archive_status="fail"
      return 1
    fi
    if [ -n "$latest_file" ] && [ -s "$latest_file" ]; then
      latest_epoch="$(stat -c %Y "$latest_file")"
      if [ "$latest_epoch" -ge "$start_epoch" ]; then
        wal_observed_file="$latest_file"
        wal_verified_at="$(file_mtime_iso "$latest_file")"
        wal_archive_status="pass"
        printf 'observed_wal=%s\n' "$latest_file" >> "${work_dir}/wal_archive.out"
        return 0
      fi
    fi
    sleep 2
  done
  printf 'wal_archive_timeout_s=%s\n' "$wal_wait_s" >> "${work_dir}/wal_archive.out"
  wal_archive_status="stale"
  return 1
}

verify_wal_archiver_stats() {
  local row query_out last_archived_epoch last_failed_epoch now_epoch last_archived_age_s wal_rpo_int
  : > "${work_dir}/wal_archiver.out"
  query_out="${work_dir}/wal_archiver.query.out"
  : > "$query_out"
  if ! command -v psql >/dev/null 2>&1; then
    printf 'psql=missing\n' >> "${work_dir}/wal_archiver.out"
    return 1
  fi
  if ! run_capture_with_timeout "$wal_archiver_stats_timeout_s" "$query_out" "${work_dir}/wal_archiver.out" wal_archiver_stats \
    env PGCONNECT_TIMEOUT="$(int_seconds "$wal_archiver_stats_timeout_s")" psql -X -v ON_ERROR_STOP=1 -AtF '|' -c "
      SELECT
        current_setting('archive_mode', true),
        current_setting('archive_command', true),
        archived_count::bigint,
        COALESCE(last_archived_wal, ''),
        COALESCE(to_char(last_archived_time AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), ''),
        COALESCE(EXTRACT(EPOCH FROM last_archived_time)::bigint::text, ''),
        failed_count::bigint,
        COALESCE(last_failed_wal, ''),
        COALESCE(to_char(last_failed_time AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), ''),
        COALESCE(EXTRACT(EPOCH FROM last_failed_time)::bigint::text, ''),
        COALESCE(to_char(stats_reset AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), '')
      FROM pg_stat_archiver;
    "; then
    if grep -q '^wal_archiver_stats_timeout_s=' "${work_dir}/wal_archiver.out"; then
      wal_archiver_status="timeout"
    fi
    return 1
  fi
  row="$(head -n 1 "$query_out")"
  IFS='|' read -r \
    wal_archiver_archive_mode \
    wal_archiver_archive_command \
    wal_archiver_archived_count \
    wal_archiver_last_archived_wal \
    wal_archiver_last_archived_at \
    wal_archiver_last_archived_at_ts \
    wal_archiver_failed_count \
    wal_archiver_last_failed_wal \
    wal_archiver_last_failed_at \
    wal_archiver_last_failed_at_ts \
    wal_archiver_stats_reset_at <<< "$row"

  printf 'archive_mode=%s\n' "$wal_archiver_archive_mode" >> "${work_dir}/wal_archiver.out"
  printf 'archive_command=%s\n' "$wal_archiver_archive_command" >> "${work_dir}/wal_archiver.out"
  printf 'archived_count=%s\n' "$wal_archiver_archived_count" >> "${work_dir}/wal_archiver.out"
  printf 'last_archived_wal=%s\n' "$wal_archiver_last_archived_wal" >> "${work_dir}/wal_archiver.out"
  printf 'last_archived_at=%s\n' "$wal_archiver_last_archived_at" >> "${work_dir}/wal_archiver.out"
  printf 'failed_count=%s\n' "$wal_archiver_failed_count" >> "${work_dir}/wal_archiver.out"
  printf 'last_failed_wal=%s\n' "$wal_archiver_last_failed_wal" >> "${work_dir}/wal_archiver.out"
  printf 'last_failed_at=%s\n' "$wal_archiver_last_failed_at" >> "${work_dir}/wal_archiver.out"
  printf 'stats_reset_at=%s\n' "$wal_archiver_stats_reset_at" >> "${work_dir}/wal_archiver.out"

  case "$wal_archiver_archive_mode" in
    on|always) ;;
    *)
      printf 'archive_mode_disabled=%s\n' "$wal_archiver_archive_mode" >> "${work_dir}/wal_archiver.out"
      return 1
      ;;
  esac
  case "$wal_archiver_archive_command" in
    *wal_archive.sh*"%p"*"%f"*) ;;
    *)
      printf 'archive_command_unaudited=%s\n' "$wal_archiver_archive_command" >> "${work_dir}/wal_archiver.out"
      return 1
      ;;
  esac
  if [ "${wal_archiver_archived_count:-0}" -le 0 ] || [ -z "${wal_archiver_last_archived_at_ts:-}" ]; then
    printf 'last_archive_missing=1\n' >> "${work_dir}/wal_archiver.out"
    return 1
  fi
  last_archived_epoch="${wal_archiver_last_archived_at_ts:-0}"
  last_failed_epoch="${wal_archiver_last_failed_at_ts:-0}"
  now_epoch="$(date +%s)"
  wal_rpo_int="$(int_seconds "$wal_rpo_s")"
  last_archived_age_s="$((now_epoch - last_archived_epoch))"
  printf 'last_archived_age_s=%s\n' "$last_archived_age_s" >> "${work_dir}/wal_archiver.out"
  printf 'wal_rpo_s=%s\n' "$wal_rpo_s" >> "${work_dir}/wal_archiver.out"
  if [ "$wal_rpo_int" -gt 0 ] && [ "$last_archived_age_s" -gt "$wal_rpo_int" ]; then
    printf 'last_archive_stale=1\n' >> "${work_dir}/wal_archiver.out"
    wal_archiver_status="stale"
    return 1
  fi
  if [ -n "$last_failed_epoch" ] && [ "$last_failed_epoch" -gt "$last_archived_epoch" ]; then
    printf 'unrecovered_failure=1\n' >> "${work_dir}/wal_archiver.out"
    wal_archiver_status="fail"
    return 1
  fi
  wal_archiver_verified_at="$wal_archiver_last_archived_at"
  wal_archiver_status="pass"
  return 0
}

reuse_restore_drill_if_fresh() {
  local latest_report
  require_listable_dir "$drill_dir" restore_drill_dir "${work_dir}/restore_drill.out" || return 1
  latest_report="$(readlink -f "${drill_dir}/latest_restore_drill.txt" 2>/dev/null || true)"
  if [ -z "$latest_report" ] || [ ! -f "$latest_report" ]; then
    if ! latest_report="$(latest_file "$drill_dir" 'restore_drill_*.txt' restore_drill_latest_file)"; then
      cat "${work_dir}/restore_drill_latest_file.find.err" >> "${work_dir}/restore_drill.out"
      return 1
    fi
  fi
  [ -n "$latest_report" ] && [ -f "$latest_report" ] || return 1
  grep -q '^status=pass$' "$latest_report" || return 1
  restore_drill_report="$latest_report"
  restore_time_to_recover_s="$(report_value "$restore_drill_report" time_to_recover_s)"
  restore_drill_verified_at="$(report_value "$restore_drill_report" generated_at)"
  if [ -z "$restore_drill_verified_at" ]; then
    restore_drill_verified_at="$(file_mtime_iso "$restore_drill_report")"
  fi
  if ! fresh_enough "$latest_report" "$restore_reuse_max_s"; then
    printf 'restore_drill_stale=%s\n' "$latest_report" >> "${work_dir}/restore_drill.out"
    printf 'restore_drill_max_age_s=%s\n' "$restore_reuse_max_s" >> "${work_dir}/restore_drill.out"
    return 1
  fi
  restore_drill_status="pass"
  printf 'restore_drill_reused=%s\n' "$latest_report" > "${work_dir}/restore_drill.out"
  return 0
}

run_restore_drill_from_gate() {
  is_truthy "${TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL:-0}" || is_truthy "${TS_BACKUP_EVIDENCE_FORCE_RESTORE_DRILL:-0}"
}

run_restore_drill() {
  : > "${work_dir}/restore_drill.out"
  if ! is_truthy "${TS_BACKUP_EVIDENCE_FORCE_RESTORE_DRILL:-0}" && reuse_restore_drill_if_fresh; then
    return 0
  fi
  if ! run_restore_drill_from_gate; then
    if [ -n "$restore_drill_report" ]; then
      restore_drill_status="stale"
    else
      restore_drill_status="missing"
    fi
    printf 'restore_drill_gate_action=not_running\n' >> "${work_dir}/restore_drill.out"
    printf 'restore_drill_required_timer=trading-restore-drill.timer\n' >> "${work_dir}/restore_drill.out"
    return 1
  fi
  set +e
  run_with_timeout "$restore_drill_timeout_s" "${work_dir}/restore_drill.out" restore_drill bash "$restore_drill_script"
  local rc=$?
  set -e
  if [ "$rc" -ne 0 ] && timeout_exit_code "$rc"; then
    restore_drill_status="timeout"
  fi
  restore_drill_report="$(latest_file "$drill_dir" 'restore_drill_*.txt' restore_drill_latest_file_after_run || true)"
  if [ -n "$restore_drill_report" ] && [ -f "$restore_drill_report" ]; then
    restore_time_to_recover_s="$(report_value "$restore_drill_report" time_to_recover_s)"
    restore_drill_verified_at="$(report_value "$restore_drill_report" generated_at)"
    if [ -z "$restore_drill_verified_at" ]; then
      restore_drill_verified_at="$(file_mtime_iso "$restore_drill_report")"
    fi
  fi
  if [ "$rc" -eq 0 ] && [ -n "$restore_drill_report" ] && grep -q '^status=pass$' "$restore_drill_report"; then
    restore_drill_status="pass"
    return 0
  fi
  if [ "$rc" -eq 0 ]; then
    restore_drill_status="fail"
    return 1
  fi
  return "$rc"
}

write_reports() {
  local status="$1"
  {
    printf 'backup_restore_evidence_report_version=1\n'
    printf 'generated_at=%s\n' "$generated_at"
	    printf 'status=%s\n' "$status"
	    printf 'script_checks_status=%s\n' "$script_checks_status"
	    printf 'compose_checks_status=%s\n' "$compose_checks_status"
	    printf 'systemd_checks_status=%s\n' "$systemd_checks_status"
	    printf 'base_backup_status=%s\n' "$base_backup_status"
    printf 'base_backup_dir=%s\n' "$base_backup_dir"
    printf 'base_verify_log=%s\n' "$base_verify_log"
    printf 'base_verified_at=%s\n' "$base_verified_at"
    printf 'wal_archive_status=%s\n' "$wal_archive_status"
    printf 'wal_verified_at=%s\n' "$wal_verified_at"
    printf 'wal_observed_file=%s\n' "$wal_observed_file"
    printf 'wal_catchup_status=%s\n' "$wal_catchup_status"
    printf 'wal_catchup_verified_at=%s\n' "$wal_catchup_verified_at"
    printf 'wal_archiver_status=%s\n' "$wal_archiver_status"
    printf 'wal_archiver_verified_at=%s\n' "$wal_archiver_verified_at"
    printf 'wal_archiver_archive_mode=%s\n' "$wal_archiver_archive_mode"
    printf 'wal_archiver_archive_command=%s\n' "$wal_archiver_archive_command"
    printf 'wal_archiver_archived_count=%s\n' "$wal_archiver_archived_count"
    printf 'wal_archiver_last_archived_wal=%s\n' "$wal_archiver_last_archived_wal"
    printf 'wal_archiver_last_archived_at=%s\n' "$wal_archiver_last_archived_at"
    printf 'wal_archiver_failed_count=%s\n' "$wal_archiver_failed_count"
    printf 'wal_archiver_last_failed_wal=%s\n' "$wal_archiver_last_failed_wal"
    printf 'wal_archiver_last_failed_at=%s\n' "$wal_archiver_last_failed_at"
    printf 'wal_archiver_stats_reset_at=%s\n' "$wal_archiver_stats_reset_at"
    printf 'restore_drill_status=%s\n' "$restore_drill_status"
    printf 'restore_drill_report=%s\n' "$restore_drill_report"
    printf 'restore_drill_verified_at=%s\n' "$restore_drill_verified_at"
	    printf 'restore_time_to_recover_s=%s\n' "$restore_time_to_recover_s"
	    printf 'signature_status=%s\n' "$signature_status"
	    printf 'publish_status=%s\n' "$publish_status"
	    printf 'lock_timeout_s=%s\n' "$lock_timeout_s"
	    printf 'probe_timeout_s=%s\n' "$probe_timeout_s"
	    printf 'systemctl_timeout_s=%s\n' "$systemctl_timeout_s"
	    printf 'base_backup_timeout_s=%s\n' "$base_backup_timeout_s"
	    printf 'wal_rpo_s=%s\n' "$wal_rpo_s"
	    printf 'wal_catchup_timeout_s=%s\n' "$wal_catchup_timeout_s"
	    printf 'wal_switch_timeout_s=%s\n' "$wal_switch_timeout_s"
	    printf 'wal_archiver_stats_timeout_s=%s\n' "$wal_archiver_stats_timeout_s"
	    printf 'restore_drill_timeout_s=%s\n' "$restore_drill_timeout_s"
	    printf 'signature_timeout_s=%s\n' "$signature_timeout_s"
	    printf 'publish_timeout_s=%s\n' "$publish_timeout_s"
	    printf '\n[script_checks]\n'
	    [ -f "${work_dir}/script_checks.out" ] && cat "${work_dir}/script_checks.out"
	    printf '\n[compose_checks]\n'
	    [ -f "${work_dir}/compose_checks.out" ] && cat "${work_dir}/compose_checks.out"
	    printf '\n[systemd_checks]\n'
	    [ -f "${work_dir}/systemd_checks.out" ] && cat "${work_dir}/systemd_checks.out"
    printf '\n[base_backup]\n'
    [ -f "${work_dir}/base_backup.out" ] && tail -n 120 "${work_dir}/base_backup.out"
    printf '\n[wal_catchup]\n'
    [ -f "${work_dir}/wal_catchup.out" ] && cat "${work_dir}/wal_catchup.out"
    printf '\n[wal_archive]\n'
    [ -f "${work_dir}/wal_archive.out" ] && cat "${work_dir}/wal_archive.out"
    printf '\n[wal_archiver]\n'
    [ -f "${work_dir}/wal_archiver.out" ] && cat "${work_dir}/wal_archiver.out"
    printf '\n[restore_drill]\n'
    [ -f "${work_dir}/restore_drill.out" ] && tail -n 160 "${work_dir}/restore_drill.out"
  } > "$report_txt"
  publish_report_file "$report_txt"

  set +e
  REPORT_JSON="$report_json" \
  STATUS="$status" \
  GENERATED_AT="$generated_at" \
  REPORT_TXT="$report_txt" \
  SCRIPT_CHECKS_STATUS="$script_checks_status" \
  COMPOSE_CHECKS_STATUS="$compose_checks_status" \
  SYSTEMD_CHECKS_STATUS="$systemd_checks_status" \
  BASE_BACKUP_STATUS="$base_backup_status" \
  BASE_BACKUP_DIR="$base_backup_dir" \
  BASE_VERIFY_LOG="$base_verify_log" \
  BASE_VERIFIED_AT="$base_verified_at" \
  WAL_ARCHIVE_STATUS="$wal_archive_status" \
  WAL_VERIFIED_AT="$wal_verified_at" \
  WAL_OBSERVED_FILE="$wal_observed_file" \
  WAL_CATCHUP_STATUS="$wal_catchup_status" \
  WAL_CATCHUP_VERIFIED_AT="$wal_catchup_verified_at" \
  WAL_ARCHIVER_STATUS="$wal_archiver_status" \
  WAL_ARCHIVER_VERIFIED_AT="$wal_archiver_verified_at" \
  WAL_ARCHIVER_ARCHIVE_MODE="$wal_archiver_archive_mode" \
  WAL_ARCHIVER_ARCHIVE_COMMAND="$wal_archiver_archive_command" \
  WAL_ARCHIVER_ARCHIVED_COUNT="$wal_archiver_archived_count" \
  WAL_ARCHIVER_LAST_ARCHIVED_WAL="$wal_archiver_last_archived_wal" \
  WAL_ARCHIVER_LAST_ARCHIVED_AT="$wal_archiver_last_archived_at" \
  WAL_ARCHIVER_LAST_ARCHIVED_AT_TS="$wal_archiver_last_archived_at_ts" \
  WAL_ARCHIVER_FAILED_COUNT="$wal_archiver_failed_count" \
  WAL_ARCHIVER_LAST_FAILED_WAL="$wal_archiver_last_failed_wal" \
  WAL_ARCHIVER_LAST_FAILED_AT="$wal_archiver_last_failed_at" \
  WAL_ARCHIVER_LAST_FAILED_AT_TS="$wal_archiver_last_failed_at_ts" \
  WAL_ARCHIVER_STATS_RESET_AT="$wal_archiver_stats_reset_at" \
  RESTORE_DRILL_STATUS="$restore_drill_status" \
  RESTORE_DRILL_REPORT="$restore_drill_report" \
  RESTORE_DRILL_VERIFIED_AT="$restore_drill_verified_at" \
  RESTORE_TIME_TO_RECOVER_S="$restore_time_to_recover_s" \
  PUBLISH_STATUS="$publish_status" \
  LOCK_TIMEOUT_S="$lock_timeout_s" \
  PROBE_TIMEOUT_S="$probe_timeout_s" \
  SYSTEMCTL_TIMEOUT_S="$systemctl_timeout_s" \
  BASE_BACKUP_TIMEOUT_S="$base_backup_timeout_s" \
  WAL_RPO_S="$wal_rpo_s" \
  WAL_CATCHUP_TIMEOUT_S="$wal_catchup_timeout_s" \
  WAL_SWITCH_TIMEOUT_S="$wal_switch_timeout_s" \
  WAL_ARCHIVER_STATS_TIMEOUT_S="$wal_archiver_stats_timeout_s" \
  RESTORE_DRILL_TIMEOUT_S="$restore_drill_timeout_s" \
  SIGNATURE_TIMEOUT_S="$signature_timeout_s" \
  PUBLISH_TIMEOUT_S="$publish_timeout_s" \
  run_capture_with_timeout "$signature_timeout_s" "${work_dir}/signature_python.out" "${work_dir}/signature_python.err" signature_generation python3 - <<'PY'
import hashlib
import hmac
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def ts(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def maybe_float(value: str):
    try:
        return float(value)
    except Exception:
        return None


def maybe_int(value: str):
    try:
        return int(float(value))
    except Exception:
        return None


def truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def signing_key():
    for name in ("BACKUP_EVIDENCE_HMAC_KEY", "BACKUP_EVIDENCE_SIGNING_KEY"):
        value = os.environ.get(name) or ""
        if value.strip():
            return value.encode("utf-8"), f"env:{name}"
    for name in ("BACKUP_EVIDENCE_HMAC_KEY_FILE", "BACKUP_EVIDENCE_SIGNING_KEY_FILE"):
        raw_path = (os.environ.get(name) or "").strip()
        if not raw_path:
            continue
        try:
            value = Path(raw_path).read_text(encoding="utf-8").strip()
        except Exception:
            return None, f"unreadable:{name}"
        if value:
            return value.encode("utf-8"), f"file:{name}"
        return None, f"empty:{name}"
    return None, "missing"


def canonical_payload_bytes(payload):
    unsigned = dict(payload)
    unsigned.pop("signature", None)
    return json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def signature_input_bytes(payload_bytes, *, algorithm, key_id, signed_at, payload_sha256):
    metadata_bytes = json.dumps(
        {
            "algorithm": str(algorithm),
            "key_id": str(key_id),
            "payload_sha256": str(payload_sha256),
            "signed_at": str(signed_at),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return payload_bytes + b"\n" + metadata_bytes


generated_at = os.environ["GENERATED_AT"]
signature_required = truthy(
    os.environ.get("BACKUP_EVIDENCE_REQUIRE_SIGNATURE")
    or os.environ.get("BACKUP_EVIDENCE_SIGNATURE_REQUIRED")
    or os.environ.get("PREFLIGHT_REQUIRE_BACKUP_EVIDENCE")
    or "0"
) or (os.environ.get("ENGINE_MODE") or "").strip().lower() == "live" or (
    os.environ.get("EXECUTION_MODE") or ""
).strip().lower() == "live"
payload = {
    "schema_version": 1,
    "generated_at": generated_at,
    "generated_at_ts": ts(generated_at),
    "status": os.environ["STATUS"],
    "report": os.environ["REPORT_TXT"],
    "script_checks": {
        "status": os.environ["SCRIPT_CHECKS_STATUS"],
    },
    "compose_checks": {
        "status": os.environ["COMPOSE_CHECKS_STATUS"],
    },
    "systemd_checks": {
        "status": os.environ["SYSTEMD_CHECKS_STATUS"],
    },
    "timeouts": {
        "lock_s": maybe_float(os.environ["LOCK_TIMEOUT_S"]),
        "probe_s": maybe_float(os.environ["PROBE_TIMEOUT_S"]),
        "systemctl_s": maybe_float(os.environ["SYSTEMCTL_TIMEOUT_S"]),
        "base_backup_s": maybe_float(os.environ["BASE_BACKUP_TIMEOUT_S"]),
        "wal_rpo_s": maybe_float(os.environ["WAL_RPO_S"]),
        "wal_catchup_s": maybe_float(os.environ["WAL_CATCHUP_TIMEOUT_S"]),
        "wal_switch_s": maybe_float(os.environ["WAL_SWITCH_TIMEOUT_S"]),
        "wal_archiver_stats_s": maybe_float(os.environ["WAL_ARCHIVER_STATS_TIMEOUT_S"]),
        "restore_drill_s": maybe_float(os.environ["RESTORE_DRILL_TIMEOUT_S"]),
        "signature_s": maybe_float(os.environ["SIGNATURE_TIMEOUT_S"]),
        "publish_s": maybe_float(os.environ["PUBLISH_TIMEOUT_S"]),
    },
    "base_backup": {
        "status": os.environ["BASE_BACKUP_STATUS"],
        "backup_dir": os.environ["BASE_BACKUP_DIR"],
        "verify_log": os.environ["BASE_VERIFY_LOG"],
        "verified_at": os.environ["BASE_VERIFIED_AT"],
        "verified_at_ts": ts(os.environ["BASE_VERIFIED_AT"]),
    },
    "wal_archive": {
        "status": os.environ["WAL_ARCHIVE_STATUS"],
        "wal_file": os.environ["WAL_OBSERVED_FILE"],
        "verified_at": os.environ["WAL_VERIFIED_AT"],
        "verified_at_ts": ts(os.environ["WAL_VERIFIED_AT"]),
    },
    "wal_catchup": {
        "status": os.environ["WAL_CATCHUP_STATUS"],
        "verified_at": os.environ["WAL_CATCHUP_VERIFIED_AT"],
        "verified_at_ts": ts(os.environ["WAL_CATCHUP_VERIFIED_AT"]),
    },
    "wal_archiver": {
        "status": os.environ["WAL_ARCHIVER_STATUS"],
        "source": "pg_stat_archiver",
        "archive_mode": os.environ["WAL_ARCHIVER_ARCHIVE_MODE"],
        "archive_command": os.environ["WAL_ARCHIVER_ARCHIVE_COMMAND"],
        "archived_count": maybe_int(os.environ["WAL_ARCHIVER_ARCHIVED_COUNT"]),
        "last_archived_wal": os.environ["WAL_ARCHIVER_LAST_ARCHIVED_WAL"],
        "last_archived_at": os.environ["WAL_ARCHIVER_LAST_ARCHIVED_AT"],
        "last_archived_at_ts": maybe_float(os.environ["WAL_ARCHIVER_LAST_ARCHIVED_AT_TS"]),
        "verified_at": os.environ["WAL_ARCHIVER_VERIFIED_AT"],
        "verified_at_ts": ts(os.environ["WAL_ARCHIVER_VERIFIED_AT"]),
        "failed_count": maybe_int(os.environ["WAL_ARCHIVER_FAILED_COUNT"]),
        "last_failed_wal": os.environ["WAL_ARCHIVER_LAST_FAILED_WAL"],
        "last_failed_at": os.environ["WAL_ARCHIVER_LAST_FAILED_AT"],
        "last_failed_at_ts": maybe_float(os.environ["WAL_ARCHIVER_LAST_FAILED_AT_TS"]),
        "stats_reset": os.environ["WAL_ARCHIVER_STATS_RESET_AT"],
        "stats_reset_ts": ts(os.environ["WAL_ARCHIVER_STATS_RESET_AT"]),
    },
    "restore_drill": {
        "status": os.environ["RESTORE_DRILL_STATUS"],
        "report": os.environ["RESTORE_DRILL_REPORT"],
        "verified_at": os.environ["RESTORE_DRILL_VERIFIED_AT"] if os.environ["RESTORE_DRILL_STATUS"] == "pass" else "",
        "verified_at_ts": ts(os.environ["RESTORE_DRILL_VERIFIED_AT"]) if os.environ["RESTORE_DRILL_STATUS"] == "pass" else None,
        "time_to_recover_s": maybe_float(os.environ["RESTORE_TIME_TO_RECOVER_S"]),
    },
    "publish": {
        "status": os.environ["PUBLISH_STATUS"],
    },
}
key, key_source = signing_key()
if key:
    payload_bytes = canonical_payload_bytes(payload)
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    algorithm = "hmac-sha256"
    key_id = os.environ.get("BACKUP_EVIDENCE_KEY_ID", "backup-evidence")
    signed_at = generated_at
    payload["signature"] = {
        "status": "signed",
        "algorithm": algorithm,
        "key_id": key_id,
        "key_source": key_source,
        "signed_at": signed_at,
        "payload_sha256": payload_sha256,
        "value": hmac.new(
            key,
            signature_input_bytes(
                payload_bytes,
                algorithm=algorithm,
                key_id=key_id,
                signed_at=signed_at,
                payload_sha256=payload_sha256,
            ),
            hashlib.sha256,
        ).hexdigest(),
    }
else:
    unsigned_status = "unsigned" if not signature_required else "key_missing"
    if str(key_source).startswith("unreadable:"):
        unsigned_status = "key_unreadable"
    elif str(key_source).startswith("empty:"):
        unsigned_status = "key_empty"
    payload["signature"] = {
        "status": unsigned_status,
        "required": signature_required,
        "algorithm": "hmac-sha256",
        "key_id": os.environ.get("BACKUP_EVIDENCE_KEY_ID", "backup-evidence"),
        "key_source": key_source,
        "signed_at": "",
        "payload_sha256": "",
        "value": "",
    }
target = Path(os.environ["REPORT_JSON"])
target.parent.mkdir(parents=True, exist_ok=True)
with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(target.parent), delete=False) as fh:
    json.dump(payload, fh, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    fh.write("\n")
    tmp = fh.name
Path(tmp).replace(target)
PY
  local json_rc=$?
  set -e
  if [ "$json_rc" -ne 0 ]; then
    if timeout_exit_code "$json_rc"; then
      signature_status="timeout"
    else
      signature_status="fail"
    fi
    printf 'signature_generation_failed_rc=%s\n' "$json_rc" >> "$report_txt"
    publish_report_file "$report_txt"
    return "$json_rc"
  fi
  publish_report_file "$report_json"

  : > "${work_dir}/publish.out"
  set +e
  run_with_timeout "$publish_timeout_s" "${work_dir}/publish.out" publish_latest_txt ln -sfn "$(basename "$report_txt")" "$latest_txt"
  local publish_rc=$?
  set -e
  if [ "$publish_rc" -ne 0 ]; then
    if timeout_exit_code "$publish_rc"; then
      publish_status="timeout"
    else
      publish_status="fail"
    fi
    return "$publish_rc"
  fi
  set +e
  run_with_timeout "$publish_timeout_s" "${work_dir}/publish.out" publish_latest_json_dir mkdir -p "$(dirname "$latest_json")"
  publish_rc=$?
  set -e
  if [ "$publish_rc" -ne 0 ]; then
    if timeout_exit_code "$publish_rc"; then
      publish_status="timeout"
    else
      publish_status="fail"
    fi
    return "$publish_rc"
  fi
  set +e
  run_with_timeout "$publish_timeout_s" "${work_dir}/publish.out" publish_latest_json_cp cp "$report_json" "${latest_json}.$$"
  publish_rc=$?
  set -e
  if [ "$publish_rc" -ne 0 ]; then
    if timeout_exit_code "$publish_rc"; then
      publish_status="timeout"
    else
      publish_status="fail"
    fi
    return "$publish_rc"
  fi
  publish_report_file "${latest_json}.$$"
  set +e
  run_with_timeout "$publish_timeout_s" "${work_dir}/publish.out" publish_latest_json_mv mv -f "${latest_json}.$$" "$latest_json"
  publish_rc=$?
  set -e
  if [ "$publish_rc" -ne 0 ]; then
    if timeout_exit_code "$publish_rc"; then
      publish_status="timeout"
    else
      publish_status="fail"
    fi
    return "$publish_rc"
  fi
  publish_status="pass"
}

overall_rc=0
check_scripts || overall_rc=1
check_compose || overall_rc=1
check_systemd || overall_rc=1
run_base_backup || overall_rc=1
run_wal_archive_catchup || overall_rc=1
verify_wal_archive || overall_rc=1
verify_wal_archiver_stats || overall_rc=1
run_restore_drill || overall_rc=1
key_state="$(backup_evidence_signing_key_state)"
if [ "$key_state" = "available" ]; then
  signature_status="signed"
elif backup_evidence_signature_required; then
  case "$key_state" in
    unreadable) signature_status="key_unreadable" ;;
    empty) signature_status="key_empty" ;;
    *) signature_status="key_missing" ;;
  esac
  overall_rc=1
else
  signature_status="unsigned"
fi

if [ "$overall_rc" -eq 0 ]; then
  write_reports pass || overall_rc=1
else
  write_reports fail || overall_rc=1
fi

log info evidence_report_written "report=${report_txt} json=${report_json} latest_json=${latest_json} status=$([ "$overall_rc" -eq 0 ] && printf pass || printf fail)"
exit "$overall_rc"
