#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=prune %s\n' "$level" "$event" "$*"
}

backup_root="${TRADING_BACKUP_ROOT:-${TS_BACKUP_ROOT:-/var/backups/trading}}"
base_dir="${TS_BACKUP_BASE_DIR:-${backup_root}/base}"
wal_dir="${TS_BACKUP_WAL_DIR:-${backup_root}/wal}"
drill_dir="${TS_RESTORE_DRILL_DIR:-${backup_root}/drills}"
drill_work_root="${TS_RESTORE_DRILL_WORK_ROOT:-${drill_dir}/work}"
keep_daily_days="${TS_BACKUP_KEEP_DAILY_DAYS:-14}"
keep_weekly_days="${TS_BACKUP_KEEP_WEEKLY_DAYS:-365}"
wal_cushion_days="${TS_BACKUP_WAL_CUSHION_DAYS:-7}"
restore_drill_work_ttl_days="${TS_RESTORE_DRILL_WORK_TTL_DAYS:-2}"
wal_observation_days="${TS_BACKUP_WAL_OBSERVATION_DAYS:-1}"
backup_max_bytes_raw="${TS_BACKUP_MAX_BYTES:-}"
enforce_budget="${TS_BACKUP_ENFORCE_BUDGET:-0}"
capacity_preflight_enabled="${TS_BACKUP_CAPACITY_PREFLIGHT:-}"
if [ -z "$capacity_preflight_enabled" ] && [ -n "$backup_max_bytes_raw" ]; then
  capacity_preflight_enabled=1
fi
now_epoch="$(date +%s)"

declare -A keep=()
declare -A weekly_seen=()
deleted_base=0
deleted_wal=0
deleted_budget_wal=0
deleted_drill_work=0
oldest_keep_epoch=0
newest_keep_epoch=0

truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

parse_nonnegative_int() {
  local raw="$1"
  local fallback="$2"
  case "$raw" in
    ''|*[!0-9]*) printf '%s' "$fallback" ;;
    *) printf '%s' "$raw" ;;
  esac
}

parse_bytes() {
  local raw="${1:-}"
  local number unit factor
  raw="${raw//[[:space:]]/}"
  if [ -z "$raw" ]; then
    printf '0'
    return 0
  fi
  if [[ "$raw" =~ ^[0-9]+$ ]]; then
    printf '%s' "$raw"
    return 0
  fi
  if [[ "$raw" =~ ^([0-9]+)([KkMmGgTtPp])([Ii]?[Bb]?)?$ ]]; then
    number="${BASH_REMATCH[1]}"
    unit="$(printf '%s' "${BASH_REMATCH[2]}" | tr '[:lower:]' '[:upper:]')"
    case "$unit" in
      K) factor=1024 ;;
      M) factor=$((1024 * 1024)) ;;
      G) factor=$((1024 * 1024 * 1024)) ;;
      T) factor=$((1024 * 1024 * 1024 * 1024)) ;;
      P) factor=$((1024 * 1024 * 1024 * 1024 * 1024)) ;;
      *) factor=1 ;;
    esac
    printf '%s' "$((number * factor))"
    return 0
  fi
  log error invalid_byte_budget "value=${raw}"
  exit 2
}

backup_budget_bytes="$(parse_bytes "$backup_max_bytes_raw")"
wal_observation_days="$(parse_nonnegative_int "$wal_observation_days" 1)"
if [ "$wal_observation_days" -lt 1 ]; then
  wal_observation_days=1
fi
restore_drill_work_ttl_days="$(parse_nonnegative_int "$restore_drill_work_ttl_days" 2)"

dir_epoch() {
  stat -c %Y "$1"
}

dir_week_key() {
  date -u -d "@$(dir_epoch "$1")" +%G-W%V
}

du_apparent_bytes() {
  local path="$1"
  if [ ! -e "$path" ]; then
    printf '0'
    return 0
  fi
  du -sb "$path" 2>/dev/null | awk '{print $1}'
}

df_path() {
  local path="$1"
  while [ ! -e "$path" ] && [ "$path" != "/" ]; do
    path="$(dirname "$path")"
  done
  printf '%s' "$path"
}

filesystem_free_bytes() {
  local path
  if [ -n "${TS_BACKUP_CAPACITY_FREE_BYTES_OVERRIDE:-}" ]; then
    parse_bytes "$TS_BACKUP_CAPACITY_FREE_BYTES_OVERRIDE"
    return 0
  fi
  path="$(df_path "$1")"
  df -PB1 "$path" 2>/dev/null | awk 'NR==2 {print $4}'
}

is_wal_name() {
  local wal_name="$1"
  case "$wal_name" in
    [0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F]|*.backup|*.history)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

compute_wal_stats() {
  wal_total_bytes=0
  wal_recent_bytes=0
  wal_bytes_per_day=0
  wal_file_count=0
  wal_oldest_epoch=0
  wal_newest_epoch=0
  local cutoff_epoch="$((now_epoch - (wal_observation_days * 86400)))"
  local wal_file wal_epoch wal_size

  [ -d "$wal_dir" ] || return 0
  while IFS= read -r -d '' wal_file; do
    [ -f "$wal_file" ] || continue
    is_wal_name "$(basename "$wal_file")" || continue
    wal_epoch="$(stat -c %Y "$wal_file")"
    wal_size="$(stat -c %s "$wal_file")"
    wal_total_bytes="$((wal_total_bytes + wal_size))"
    wal_file_count="$((wal_file_count + 1))"
    if [ "$wal_epoch" -ge "$cutoff_epoch" ]; then
      wal_recent_bytes="$((wal_recent_bytes + wal_size))"
    fi
    if [ "$wal_oldest_epoch" -eq 0 ] || [ "$wal_epoch" -lt "$wal_oldest_epoch" ]; then
      wal_oldest_epoch="$wal_epoch"
    fi
    if [ "$wal_newest_epoch" -eq 0 ] || [ "$wal_epoch" -gt "$wal_newest_epoch" ]; then
      wal_newest_epoch="$wal_epoch"
    fi
  done < <(find "$wal_dir" -mindepth 1 -maxdepth 1 -type f -print0)

  if [ "$wal_recent_bytes" -gt 0 ]; then
    wal_bytes_per_day="$(( (wal_recent_bytes + wal_observation_days - 1) / wal_observation_days ))"
  elif [ "$wal_total_bytes" -gt 0 ] && [ "$wal_newest_epoch" -gt "$wal_oldest_epoch" ]; then
    local span_s="$((wal_newest_epoch - wal_oldest_epoch))"
    [ "$span_s" -lt 1 ] && span_s=1
    wal_bytes_per_day="$(( (wal_total_bytes * 86400 + span_s - 1) / span_s ))"
  else
    wal_bytes_per_day="$wal_total_bytes"
  fi
}

capacity_preflight() {
  truthy "$capacity_preflight_enabled" || return 0
  compute_wal_stats
  local retention_days="$((keep_daily_days + wal_cushion_days))"
  local required_free_bytes="$((wal_bytes_per_day * retention_days))"
  local free_bytes
  free_bytes="$(filesystem_free_bytes "$backup_root")"
  log info backup_capacity_preflight \
    "backup_root=${backup_root}" \
    "free_bytes=${free_bytes:-0}" \
    "observed_wal_bytes_per_day=${wal_bytes_per_day}" \
    "retention_days=${retention_days}" \
    "required_free_bytes=${required_free_bytes}"
  if [ "$wal_bytes_per_day" -gt 0 ] && [ "${free_bytes:-0}" -lt "$required_free_bytes" ]; then
    log error backup_capacity_preflight_failed \
      "backup_root=${backup_root}" \
      "free_bytes=${free_bytes:-0}" \
      "observed_wal_bytes_per_day=${wal_bytes_per_day}" \
      "retention_days=${retention_days}" \
      "required_free_bytes=${required_free_bytes}"
    return 1
  fi
  return 0
}

restore_drill_work_live() {
  local path="$1"
  local pidfile="${path}/restore_drill.pid"
  local pid
  if truthy "${TS_RESTORE_DRILL_ASSUME_NO_LIVE_PROCESS:-0}"; then
    return 1
  fi
  if [ -f "$pidfile" ]; then
    pid="$(cat "$pidfile" 2>/dev/null || true)"
    if [ -n "${pid:-}" ] && kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
  fi
  if command -v pgrep >/dev/null 2>&1 && pgrep -f "ops/backup/restore_drill.sh|restore_drill.sh" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

reap_restore_drill_work() {
  [ -d "$drill_work_root" ] || return 0
  local ttl_cutoff_epoch="$((now_epoch - (restore_drill_work_ttl_days * 86400)))"
  local work_dir work_epoch reason
  while IFS= read -r -d '' work_dir; do
    [ -d "$work_dir" ] || continue
    work_epoch="$(dir_epoch "$work_dir")"
    reason=""
    if [ "$work_epoch" -lt "$ttl_cutoff_epoch" ]; then
      reason="ttl"
    elif ! restore_drill_work_live "$work_dir"; then
      reason="orphaned"
    fi
    if [ -n "$reason" ]; then
      rm -rf -- "$work_dir"
      deleted_drill_work="$((deleted_drill_work + 1))"
      log info restore_drill_work_reaped "work_dir=${work_dir}" "reason=${reason}" "ttl_days=${restore_drill_work_ttl_days}"
    fi
  done < <(find "$drill_work_root" -mindepth 1 -maxdepth 1 -type d -name 'restore_drill_*' -print0)
}

enforce_backup_budget() {
  [ "$backup_budget_bytes" -gt 0 ] || return 0
  local apparent_bytes
  apparent_bytes="$(du_apparent_bytes "$backup_root")"
  if [ "$apparent_bytes" -le "$backup_budget_bytes" ]; then
    return 0
  fi

  log error backup_over_budget \
    "backup_root=${backup_root}" \
    "apparent_bytes=${apparent_bytes}" \
    "budget_bytes=${backup_budget_bytes}" \
    "enforce=${enforce_budget}"

  truthy "$enforce_budget" || return 0
  if [ "$newest_keep_epoch" -le 0 ] || [ ! -d "$wal_dir" ]; then
    log error backup_budget_enforcement_skipped \
      "reason=no_retained_base_or_wal_dir" \
      "newest_retained_epoch=${newest_keep_epoch}" \
      "wal_dir=${wal_dir}"
    return 0
  fi

  local cushion_cutoff_epoch="$((now_epoch - (wal_cushion_days * 86400)))"
  local budget_wal_cutoff_epoch="$newest_keep_epoch"
  if [ "$cushion_cutoff_epoch" -lt "$budget_wal_cutoff_epoch" ]; then
    budget_wal_cutoff_epoch="$cushion_cutoff_epoch"
  fi
  if [ "$budget_wal_cutoff_epoch" -le 0 ]; then
    log error backup_budget_enforcement_skipped "reason=no_safe_wal_cutoff"
    return 0
  fi

  local line wal_epoch wal_file wal_size
  while [ "$apparent_bytes" -gt "$backup_budget_bytes" ] && IFS= read -r line; do
    wal_epoch="${line%% *}"
    wal_file="${line#* }"
    [ -f "$wal_file" ] || continue
    is_wal_name "$(basename "$wal_file")" || continue
    wal_epoch="${wal_epoch%%.*}"
    [ "$wal_epoch" -lt "$budget_wal_cutoff_epoch" ] || continue
    wal_size="$(stat -c %s "$wal_file")"
    rm -f -- "$wal_file"
    deleted_wal="$((deleted_wal + 1))"
    deleted_budget_wal="$((deleted_budget_wal + 1))"
    log info wal_deleted \
      "wal=${wal_file}" \
      "reason=budget" \
      "wal_epoch=${wal_epoch}" \
      "safe_cutoff_epoch=${budget_wal_cutoff_epoch}" \
      "bytes=${wal_size}"
    apparent_bytes="$(du_apparent_bytes "$backup_root")"
  done < <(find "$wal_dir" -mindepth 1 -maxdepth 1 -type f -printf '%T@ %p\n' | sort -n)

  if [ "$apparent_bytes" -gt "$backup_budget_bytes" ]; then
    log error backup_over_budget_unresolved \
      "backup_root=${backup_root}" \
      "apparent_bytes=${apparent_bytes}" \
      "budget_bytes=${backup_budget_bytes}" \
      "deleted_budget_wal=${deleted_budget_wal}" \
      "newest_retained_epoch=${newest_keep_epoch}" \
      "wal_budget_cutoff_epoch=${budget_wal_cutoff_epoch}" \
      "wal_cushion_days=${wal_cushion_days}"
  else
    log info backup_budget_enforced \
      "backup_root=${backup_root}" \
      "apparent_bytes=${apparent_bytes}" \
      "budget_bytes=${backup_budget_bytes}" \
      "deleted_budget_wal=${deleted_budget_wal}" \
      "newest_retained_epoch=${newest_keep_epoch}" \
      "wal_budget_cutoff_epoch=${budget_wal_cutoff_epoch}"
  fi
}

reap_restore_drill_work

if [ -d "$base_dir" ]; then
  while IFS= read -r backup; do
    [ -d "$backup" ] || continue
    name="$(basename "$backup")"
    [ "$name" = "latest" ] && continue
    case "$name" in
      .*|*.in_progress) continue ;;
    esac

    epoch="$(dir_epoch "$backup")"
    age_days="$(( (now_epoch - epoch) / 86400 ))"
    if [ "$age_days" -le "$keep_daily_days" ]; then
      keep["$backup"]=1
      continue
    fi
    if [ "$age_days" -le "$keep_weekly_days" ]; then
      week="$(dir_week_key "$backup")"
      if [ -z "${weekly_seen[$week]:-}" ]; then
        weekly_seen["$week"]="$backup"
        keep["$backup"]=1
      fi
    fi
  done < <(find "$base_dir" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -rn | cut -d' ' -f2-)

  while IFS= read -r backup; do
    [ -d "$backup" ] || continue
    name="$(basename "$backup")"
    [ "$name" = "latest" ] && continue
    case "$name" in
      .*|*.in_progress) continue ;;
    esac
    if [ -n "${keep[$backup]:-}" ]; then
      epoch="$(dir_epoch "$backup")"
      if [ "$oldest_keep_epoch" -eq 0 ] || [ "$epoch" -lt "$oldest_keep_epoch" ]; then
        oldest_keep_epoch="$epoch"
      fi
      if [ "$newest_keep_epoch" -eq 0 ] || [ "$epoch" -gt "$newest_keep_epoch" ]; then
        newest_keep_epoch="$epoch"
      fi
      continue
    fi
    rm -rf -- "$backup"
    deleted_base="$((deleted_base + 1))"
    log info base_deleted "backup_dir=${backup}"
  done < <(find "$base_dir" -mindepth 1 -maxdepth 1 -type d -print)
fi

if [ "$oldest_keep_epoch" -gt 0 ] && [ -d "$wal_dir" ]; then
  wal_cutoff_epoch="$((oldest_keep_epoch - (wal_cushion_days * 86400)))"
  while IFS= read -r wal_file; do
    [ -f "$wal_file" ] || continue
    wal_name="$(basename "$wal_file")"
    is_wal_name "$wal_name" || continue
    wal_epoch="$(stat -c %Y "$wal_file")"
    if [ "$wal_epoch" -lt "$wal_cutoff_epoch" ]; then
      rm -f -- "$wal_file"
      deleted_wal="$((deleted_wal + 1))"
      log info wal_deleted "wal=${wal_file}"
    fi
  done < <(find "$wal_dir" -mindepth 1 -maxdepth 1 -type f -print)
else
  wal_cutoff_epoch=0
fi

enforce_backup_budget
capacity_preflight

log info prune_complete \
  "deleted_base=${deleted_base}" \
  "deleted_wal=${deleted_wal}" \
  "deleted_budget_wal=${deleted_budget_wal}" \
  "deleted_drill_work=${deleted_drill_work}" \
  "oldest_retained_epoch=${oldest_keep_epoch}" \
  "newest_retained_epoch=${newest_keep_epoch}" \
  "wal_cutoff_epoch=${wal_cutoff_epoch:-0}"
