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
restore_script="${TS_RESTORE_SCRIPT:-${script_dir}/restore.sh}"
restore_drill_script="${TS_RESTORE_DRILL_SCRIPT:-${script_dir}/restore_drill.sh}"
evidence_script="${TS_BACKUP_EVIDENCE_SCRIPT:-${script_dir}/backup_restore_evidence.sh}"
base_dir="${TS_BACKUP_BASE_DIR:-/var/backups/trading/base}"
wal_dir="${TS_BACKUP_WAL_DIR:-/var/backups/trading/wal}"
drill_dir="${TS_RESTORE_DRILL_DIR:-/var/backups/trading/drills}"
systemd_unit_dir="${TS_BACKUP_SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
wal_wait_s="${TS_BACKUP_WAL_VERIFY_TIMEOUT_S:-120}"
base_reuse_max_s="${TS_BACKUP_EVIDENCE_REUSE_BASE_MAX_AGE_S:-${BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S:-93600}}"
restore_reuse_max_s="${TS_BACKUP_EVIDENCE_REUSE_RESTORE_MAX_AGE_S:-${BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S:-7776000}}"

script_checks_status="fail"
systemd_checks_status="fail"
base_backup_status="fail"
wal_archive_status="fail"
restore_drill_status="fail"
base_backup_dir=""
base_verify_log=""
base_verified_at=""
wal_verified_at=""
wal_observed_file=""
restore_drill_report=""
restore_time_to_recover_s=""

mkdir -p "$evidence_dir" "$work_dir"

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

lock_path="${TS_BACKUP_EVIDENCE_LOCK:-${evidence_dir}/backup_restore_evidence.lock}"
exec 9>"$lock_path"
if is_truthy "${TS_BACKUP_EVIDENCE_WAIT_LOCK:-0}"; then
  flock 9
elif ! flock -n 9; then
  log warn already_running "lock=${lock_path}"
  exit 0
fi

check_scripts() {
  local missing=0
  local path
  : > "${work_dir}/script_checks.out"
  for path in "$base_backup_script" "$wal_archive_script" "$restore_script" "$restore_drill_script" "$evidence_script"; do
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

check_systemd() {
  local unit rc=0
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
    if systemctl cat "$unit" >/dev/null 2>&1 || [ -f "${systemd_unit_dir}/${unit}" ]; then
      printf 'unit_present=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'unit_missing=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
      rc=1
    fi
  done
  for unit in "${timers[@]}"; do
    if systemctl is-enabled --quiet "$unit"; then
      printf 'timer_enabled=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'timer_not_enabled=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
      rc=1
    fi
    if systemctl is-active --quiet "$unit"; then
      printf 'timer_active=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'timer_not_active=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
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
  find "$dir" -maxdepth 1 -type f -name "$pattern" -printf '%T@ %p\n' 2>/dev/null \
    | sort -nr \
    | head -n 1 \
    | cut -d' ' -f2-
}

latest_dir() {
  local dir="$1"
  find "$dir" -mindepth 1 -maxdepth 1 -type d ! -name '.*' ! -name '*.in_progress' -printf '%T@ %p\n' 2>/dev/null \
    | sort -nr \
    | head -n 1 \
    | cut -d' ' -f2-
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

reuse_base_backup_if_fresh() {
  local latest verify_log
  latest="$(readlink -f "${base_dir}/latest" 2>/dev/null || true)"
  if [ -z "$latest" ] || [ ! -d "$latest" ]; then
    latest="$(latest_dir "$base_dir")"
  fi
  [ -n "$latest" ] && [ -d "$latest" ] || return 1
  verify_log="${latest}/pg_verifybackup.out"
  [ -f "${latest}/backup_manifest" ] || return 1
  [ -s "$verify_log" ] || return 1
  fresh_enough "$verify_log" "$base_reuse_max_s" || return 1
  base_backup_dir="$latest"
  base_verify_log="$verify_log"
  base_verified_at="$(date -u -d "@$(stat -c %Y "$verify_log")" +%Y-%m-%dT%H:%M:%SZ)"
  base_backup_status="pass"
  printf 'base_backup_reused=%s\n' "$latest" > "${work_dir}/base_backup.out"
  return 0
}

run_base_backup() {
  if ! is_truthy "${TS_BACKUP_EVIDENCE_FORCE_BASE_BACKUP:-0}" && reuse_base_backup_if_fresh; then
    return 0
  fi
  set +e
  bash "$base_backup_script" > "${work_dir}/base_backup.out" 2>&1
  local rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
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
  base_verified_at="$(date -u -d "@$(stat -c %Y "$base_verify_log")" +%Y-%m-%dT%H:%M:%SZ)"
  base_backup_status="pass"
  return 0
}

verify_wal_archive() {
  local start_epoch deadline latest_file latest_epoch
  : > "${work_dir}/wal_archive.out"
  if [ ! -d "$wal_dir" ]; then
    printf 'wal_dir_missing=%s\n' "$wal_dir" >> "${work_dir}/wal_archive.out"
    return 1
  fi
  if ! command -v psql >/dev/null 2>&1; then
    printf 'psql=missing\n' >> "${work_dir}/wal_archive.out"
    return 1
  fi
  start_epoch="$(date +%s)"
  if ! psql -X -v ON_ERROR_STOP=1 -Atqc "SELECT pg_switch_wal();" >> "${work_dir}/wal_archive.out" 2>&1; then
    return 1
  fi
  deadline="$((start_epoch + wal_wait_s))"
  while [ "$(date +%s)" -le "$deadline" ]; do
    latest_file="$(
      find "$wal_dir" -maxdepth 1 -type f ! -name '.*' -printf '%T@ %p\n' 2>/dev/null \
        | sort -nr \
        | head -n 1 \
        | cut -d' ' -f2-
    )"
    if [ -n "$latest_file" ] && [ -s "$latest_file" ]; then
      latest_epoch="$(stat -c %Y "$latest_file")"
      if [ "$latest_epoch" -ge "$start_epoch" ]; then
        wal_observed_file="$latest_file"
        wal_verified_at="$(date -u -d "@${latest_epoch}" +%Y-%m-%dT%H:%M:%SZ)"
        wal_archive_status="pass"
        printf 'observed_wal=%s\n' "$latest_file" >> "${work_dir}/wal_archive.out"
        return 0
      fi
    fi
    sleep 2
  done
  printf 'wal_archive_timeout_s=%s\n' "$wal_wait_s" >> "${work_dir}/wal_archive.out"
  return 1
}

reuse_restore_drill_if_fresh() {
  local latest_report
  latest_report="$(readlink -f "${drill_dir}/latest_restore_drill.txt" 2>/dev/null || true)"
  if [ -z "$latest_report" ] || [ ! -f "$latest_report" ]; then
    latest_report="$(latest_file "$drill_dir" 'restore_drill_*.txt')"
  fi
  [ -n "$latest_report" ] && [ -f "$latest_report" ] || return 1
  grep -q '^status=pass$' "$latest_report" || return 1
  fresh_enough "$latest_report" "$restore_reuse_max_s" || return 1
  restore_drill_report="$latest_report"
  restore_time_to_recover_s="$(awk -F= '$1=="time_to_recover_s" {print $2; exit}' "$restore_drill_report")"
  restore_drill_status="pass"
  printf 'restore_drill_reused=%s\n' "$latest_report" > "${work_dir}/restore_drill.out"
  return 0
}

run_restore_drill() {
  if ! is_truthy "${TS_BACKUP_EVIDENCE_FORCE_RESTORE_DRILL:-0}" && reuse_restore_drill_if_fresh; then
    return 0
  fi
  set +e
  bash "$restore_drill_script" > "${work_dir}/restore_drill.out" 2>&1
  local rc=$?
  set -e
  restore_drill_report="$(
    find "$drill_dir" -maxdepth 1 -type f -name 'restore_drill_*.txt' -printf '%T@ %p\n' 2>/dev/null \
      | sort -nr \
      | head -n 1 \
      | cut -d' ' -f2-
  )"
  if [ -n "$restore_drill_report" ] && [ -f "$restore_drill_report" ]; then
    restore_time_to_recover_s="$(awk -F= '$1=="time_to_recover_s" {print $2; exit}' "$restore_drill_report")"
  fi
  if [ "$rc" -eq 0 ] && [ -n "$restore_drill_report" ] && grep -q '^status=pass$' "$restore_drill_report"; then
    restore_drill_status="pass"
    return 0
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
    printf 'systemd_checks_status=%s\n' "$systemd_checks_status"
    printf 'base_backup_status=%s\n' "$base_backup_status"
    printf 'base_backup_dir=%s\n' "$base_backup_dir"
    printf 'base_verify_log=%s\n' "$base_verify_log"
    printf 'base_verified_at=%s\n' "$base_verified_at"
    printf 'wal_archive_status=%s\n' "$wal_archive_status"
    printf 'wal_verified_at=%s\n' "$wal_verified_at"
    printf 'wal_observed_file=%s\n' "$wal_observed_file"
    printf 'restore_drill_status=%s\n' "$restore_drill_status"
    printf 'restore_drill_report=%s\n' "$restore_drill_report"
    printf 'restore_time_to_recover_s=%s\n' "$restore_time_to_recover_s"
    printf '\n[script_checks]\n'
    [ -f "${work_dir}/script_checks.out" ] && cat "${work_dir}/script_checks.out"
    printf '\n[systemd_checks]\n'
    [ -f "${work_dir}/systemd_checks.out" ] && cat "${work_dir}/systemd_checks.out"
    printf '\n[base_backup]\n'
    [ -f "${work_dir}/base_backup.out" ] && tail -n 120 "${work_dir}/base_backup.out"
    printf '\n[wal_archive]\n'
    [ -f "${work_dir}/wal_archive.out" ] && cat "${work_dir}/wal_archive.out"
    printf '\n[restore_drill]\n'
    [ -f "${work_dir}/restore_drill.out" ] && tail -n 160 "${work_dir}/restore_drill.out"
  } > "$report_txt"

  REPORT_JSON="$report_json" \
  STATUS="$status" \
  GENERATED_AT="$generated_at" \
  REPORT_TXT="$report_txt" \
  SCRIPT_CHECKS_STATUS="$script_checks_status" \
  SYSTEMD_CHECKS_STATUS="$systemd_checks_status" \
  BASE_BACKUP_STATUS="$base_backup_status" \
  BASE_BACKUP_DIR="$base_backup_dir" \
  BASE_VERIFY_LOG="$base_verify_log" \
  BASE_VERIFIED_AT="$base_verified_at" \
  WAL_ARCHIVE_STATUS="$wal_archive_status" \
  WAL_VERIFIED_AT="$wal_verified_at" \
  WAL_OBSERVED_FILE="$wal_observed_file" \
  RESTORE_DRILL_STATUS="$restore_drill_status" \
  RESTORE_DRILL_REPORT="$restore_drill_report" \
  RESTORE_TIME_TO_RECOVER_S="$restore_time_to_recover_s" \
  python3 - <<'PY'
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


generated_at = os.environ["GENERATED_AT"]
payload = {
    "schema_version": 1,
    "generated_at": generated_at,
    "generated_at_ts": ts(generated_at),
    "status": os.environ["STATUS"],
    "report": os.environ["REPORT_TXT"],
    "script_checks": {
        "status": os.environ["SCRIPT_CHECKS_STATUS"],
    },
    "systemd_checks": {
        "status": os.environ["SYSTEMD_CHECKS_STATUS"],
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
    "restore_drill": {
        "status": os.environ["RESTORE_DRILL_STATUS"],
        "report": os.environ["RESTORE_DRILL_REPORT"],
        "verified_at": generated_at if os.environ["RESTORE_DRILL_STATUS"] == "pass" else "",
        "verified_at_ts": ts(generated_at) if os.environ["RESTORE_DRILL_STATUS"] == "pass" else None,
        "time_to_recover_s": maybe_float(os.environ["RESTORE_TIME_TO_RECOVER_S"]),
    },
}
target = Path(os.environ["REPORT_JSON"])
target.parent.mkdir(parents=True, exist_ok=True)
with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(target.parent), delete=False) as fh:
    json.dump(payload, fh, sort_keys=True, separators=(",", ":"))
    fh.write("\n")
    tmp = fh.name
Path(tmp).replace(target)
PY

  ln -sfn "$(basename "$report_txt")" "$latest_txt"
  mkdir -p "$(dirname "$latest_json")"
  cp "$report_json" "${latest_json}.$$"
  mv -f "${latest_json}.$$" "$latest_json"
}

overall_rc=0
check_scripts || overall_rc=1
check_systemd || overall_rc=1
run_base_backup || overall_rc=1
verify_wal_archive || overall_rc=1
run_restore_drill || overall_rc=1

if [ "$overall_rc" -eq 0 ]; then
  write_reports pass
else
  write_reports fail
fi

log info evidence_report_written "report=${report_txt} json=${report_json} latest_json=${latest_json} status=$([ "$overall_rc" -eq 0 ] && printf pass || printf fail)"
exit "$overall_rc"
