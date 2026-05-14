#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=wal_archive %s\n' "$level" "$event" "$*"
}

die() {
  log error "$1" "${2:-}"
  exit 1
}

fsync_path() {
  python3 - "$1" <<'PY'
import os
import sys

path = sys.argv[1]
fd = os.open(path, os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY
}

if [ "$#" -ne 2 ]; then
  die invalid_args "expected_args=2 actual_args=$#"
fi

src="$1"
wal_name="$2"
wal_dir="${TS_BACKUP_WAL_DIR:-/var/backups/trading/wal}"
tmp_dir="${wal_dir}/.tmp"
final="${wal_dir}/${wal_name}"
tmp="${tmp_dir}/${wal_name}.$$.$RANDOM.tmp"

case "$wal_name" in
  *[!A-Za-z0-9._-]*|'')
    die invalid_wal_name "wal_name=${wal_name}"
    ;;
esac

[ -f "$src" ] || die source_missing "source=${src} wal_name=${wal_name}"

mkdir -p "$tmp_dir"

cleanup() {
  rm -f "$tmp"
}
trap cleanup EXIT

offsite_copy() {
  local cmd_template cmd
  cmd_template="${TS_WAL_OFFSITE_CMD:-}"
  [ -n "$cmd_template" ] || return 0
  cmd="${cmd_template//<name>/${wal_name}}"
  TS_WAL_NAME="$wal_name" TS_WAL_PATH="$final" bash -o pipefail -c "$cmd" < "$final"
}

if [ -f "$final" ]; then
  if cmp -s "$src" "$final"; then
    offsite_copy
    log info already_archived "wal_name=${wal_name} destination=${final}"
    exit 0
  fi
  die archive_conflict "wal_name=${wal_name} destination=${final}"
fi

cp -- "$src" "$tmp"
chmod 0640 "$tmp"
fsync_path "$tmp"
mv -f -- "$tmp" "$final"
fsync_path "$wal_dir"

offsite_copy

bytes="$(wc -c < "$final" | tr -d ' ')"
log info archived "wal_name=${wal_name} destination=${final} bytes=${bytes} offsite=$([ -n "${TS_WAL_OFFSITE_CMD:-}" ] && printf true || printf false)"
