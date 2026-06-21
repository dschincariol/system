#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=wal_archive_catchup %s\n' "$level" "$event" "$*"
}

die() {
  log error "$1" "${2:-}"
  exit 1
}

parent_dir() {
  local path="$1"
  path="${path%/}"
  if [ "${path%/*}" = "$path" ]; then
    printf '.\n'
  elif [ -z "${path%/*}" ]; then
    printf '/\n'
  else
    printf '%s\n' "${path%/*}"
  fi
}

bytes_available_for() {
  local path="$1"
  df -Pk "$path" 2>/dev/null | awk 'NR == 2 {printf "%.0f\n", $4 * 1024}'
}

pgdata="${PGDATA:-/var/lib/postgresql/data}"
pg_wal_dir="${TS_PG_WAL_DIR:-${pgdata}/pg_wal}"
status_dir="${TS_PG_ARCHIVE_STATUS_DIR:-${pg_wal_dir}/archive_status}"
archive_script="${TS_WAL_ARCHIVE_SCRIPT:-/opt/trading/ops/backup/wal_archive.sh}"
wal_dir="${TS_BACKUP_WAL_DIR:-/var/backups/trading/wal}"
wal_parent="$(parent_dir "$wal_dir")"
min_free_bytes="${TS_WAL_ARCHIVE_CATCHUP_MIN_FREE_BYTES:-2147483648}"
max_segments="${TS_WAL_ARCHIVE_CATCHUP_MAX_SEGMENTS:-100000}"
ready_list="${TMPDIR:-/tmp}/wal_archive_catchup_ready.$$"

cleanup() {
  rm -f "$ready_list"
}
trap cleanup EXIT

[ -d "$pg_wal_dir" ] || die pg_wal_dir_missing "pg_wal_dir=${pg_wal_dir}"
[ -d "$status_dir" ] || die archive_status_dir_missing "status_dir=${status_dir}"
[ -x "$archive_script" ] || die archive_script_missing "archive_script=${archive_script}"
[ -d "$wal_dir" ] || [ -d "$wal_parent" ] || die wal_archive_parent_missing "wal_dir=${wal_dir} parent=${wal_parent}"

case "$min_free_bytes" in
  ''|*[!0-9]*) die invalid_min_free_bytes "value=${min_free_bytes}" ;;
esac
case "$max_segments" in
  ''|*[!0-9]*) die invalid_max_segments "value=${max_segments}" ;;
esac

{
  for status_path in "$status_dir"/*.ready; do
    [ -e "$status_path" ] || continue
    status_name="${status_path##*/}"
    printf '%s\n' "${status_name%.ready}"
  done
} | sort > "$ready_list"

ready_count="$(wc -l < "$ready_list" | tr -d ' ')"
if [ "${ready_count:-0}" -eq 0 ]; then
  log info no_backlog "status_dir=${status_dir} wal_dir=${wal_dir}"
  exit 0
fi

total_bytes=0
while IFS= read -r wal_name; do
  [ -n "$wal_name" ] || continue
  case "$wal_name" in
    *[!A-Za-z0-9._-]*)
      die invalid_ready_wal_name "wal_name=${wal_name}"
      ;;
  esac
  src="${pg_wal_dir}/${wal_name}"
  [ -f "$src" ] || die ready_wal_source_missing "wal_name=${wal_name} source=${src}"
  bytes="$(wc -c < "$src" | tr -d ' ')"
  total_bytes="$((total_bytes + bytes))"
done < "$ready_list"

space_path="$wal_dir"
if [ ! -d "$space_path" ]; then
  space_path="$wal_parent"
fi
free_bytes="$(bytes_available_for "$space_path")"
case "$free_bytes" in
  ''|*[!0-9]*) die wal_archive_free_space_unknown "path=${space_path}" ;;
esac
if [ "$free_bytes" -lt "$((total_bytes + min_free_bytes))" ]; then
  die wal_archive_catchup_insufficient_space "wal_dir=${wal_dir} ready_count=${ready_count} backlog_bytes=${total_bytes} free_bytes=${free_bytes} min_free_bytes=${min_free_bytes}"
fi

processed=0
while IFS= read -r wal_name; do
  [ -n "$wal_name" ] || continue
  if [ "$processed" -ge "$max_segments" ]; then
    log warn segment_limit_reached "ready_count=${ready_count} processed=${processed} max_segments=${max_segments}"
    break
  fi
  src="${pg_wal_dir}/${wal_name}"
  "$archive_script" "$src" "$wal_name"
  processed="$((processed + 1))"
done < "$ready_list"

log info catchup_complete "ready_count=${ready_count} processed=${processed} backlog_bytes=${total_bytes} wal_dir=${wal_dir} postgres_archiver_will_mark_done=1"
