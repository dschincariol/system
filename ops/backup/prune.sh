#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=prune %s\n' "$level" "$event" "$*"
}

base_dir="${TS_BACKUP_BASE_DIR:-/var/backups/trading/base}"
wal_dir="${TS_BACKUP_WAL_DIR:-/var/backups/trading/wal}"
keep_recent_count="${TS_BACKUP_KEEP_RECENT_COUNT:-2}"
keep_daily_days="${TS_BACKUP_KEEP_DAILY_DAYS:-0}"
keep_weekly_days="${TS_BACKUP_KEEP_WEEKLY_DAYS:-0}"
wal_cushion_days="${TS_BACKUP_WAL_CUSHION_DAYS:-10}"
now_epoch="$(date +%s)"

declare -A keep=()
declare -A weekly_seen=()
deleted_base=0
deleted_wal=0
oldest_keep_epoch=0

require_nonnegative_int() {
  local name="$1" value="$2"
  case "$value" in
    ''|*[!0-9]*)
      log error invalid_retention_value "name=${name} value=${value}"
      exit 1
      ;;
  esac
}

dir_epoch() {
  stat -c %Y "$1"
}

dir_week_key() {
  date -u -d "@$(dir_epoch "$1")" +%G-W%V
}

require_nonnegative_int TS_BACKUP_KEEP_RECENT_COUNT "$keep_recent_count"
require_nonnegative_int TS_BACKUP_KEEP_DAILY_DAYS "$keep_daily_days"
require_nonnegative_int TS_BACKUP_KEEP_WEEKLY_DAYS "$keep_weekly_days"
require_nonnegative_int TS_BACKUP_WAL_CUSHION_DAYS "$wal_cushion_days"

if [ -d "$base_dir" ]; then
  retained_seen=0
  while IFS= read -r backup; do
    [ -d "$backup" ] || continue
    name="$(basename "$backup")"
    [ "$name" = "latest" ] && continue
    case "$name" in
      .*|*.in_progress) continue ;;
    esac

    epoch="$(dir_epoch "$backup")"
    age_days="$(( (now_epoch - epoch) / 86400 ))"
    if [ "$retained_seen" -lt "$keep_recent_count" ]; then
      keep["$backup"]=1
      retained_seen="$((retained_seen + 1))"
      continue
    fi
    if [ "$keep_daily_days" -gt 0 ] && [ "$age_days" -le "$keep_daily_days" ]; then
      keep["$backup"]=1
      continue
    fi
    if [ "$keep_weekly_days" -gt 0 ] && [ "$age_days" -le "$keep_weekly_days" ]; then
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
    case "$wal_name" in
      [0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F]|*.backup|*.history)
        ;;
      *)
        continue
        ;;
    esac
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

log info prune_complete "deleted_base=${deleted_base} deleted_wal=${deleted_wal} oldest_retained_epoch=${oldest_keep_epoch} wal_cutoff_epoch=${wal_cutoff_epoch:-0} keep_recent_count=${keep_recent_count} keep_daily_days=${keep_daily_days} keep_weekly_days=${keep_weekly_days} wal_cushion_days=${wal_cushion_days}"
