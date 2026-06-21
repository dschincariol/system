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

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
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

normalize_existing_path() {
  local path="$1"
  if command -v readlink >/dev/null 2>&1; then
    readlink -f -- "$path" 2>/dev/null && return 0
  fi
  (cd "$path" 2>/dev/null && pwd -P) || printf '%s\n' "$path"
}

mountinfo_best_for() {
  local target="$1"
  awk -v target="$target" '
    function unescape_mount(s) {
      gsub(/\\040/, " ", s)
      gsub(/\\011/, "\t", s)
      gsub(/\\012/, "\n", s)
      gsub(/\\134/, "\\", s)
      return s
    }
    {
      sep = 0
      for (i = 1; i <= NF; i++) {
        if ($i == "-") {
          sep = i
          break
        }
      }
      if (sep == 0 || sep + 2 > NF) {
        next
      }
      mount_point = unescape_mount($5)
      matched = (target == mount_point) ||
        (mount_point == "/" && substr(target, 1, 1) == "/") ||
        (mount_point != "/" && index(target, mount_point "/") == 1)
      if (matched && length(mount_point) > best_len) {
        best_len = length(mount_point)
        best = mount_point
        best_fs = $(sep + 1)
        best_source = unescape_mount($(sep + 2))
      }
    }
    END {
      if (best != "") {
        printf "%s|%s|%s\n", best, best_fs, best_source
      }
    }
  ' /proc/self/mountinfo 2>/dev/null || true
}

require_archive_root_mount() {
  local archive_root="$1"
  local root_resolved info mount_point fs_type mount_source

  [ -d "$archive_root" ] || die archive_root_missing "archive_root=${archive_root} wal_dir=${wal_dir}"
  root_resolved="$(normalize_existing_path "$archive_root")"
  info="$(mountinfo_best_for "$root_resolved")"
  mount_point="${info%%|*}"
  fs_type="${info#*|}"
  fs_type="${fs_type%%|*}"
  mount_source="${info##*|}"

  if [ -z "$mount_point" ] || [ "$mount_point" = "/" ]; then
    die archive_root_not_mounted "archive_root=${archive_root} resolved=${root_resolved} mount_point=${mount_point:-missing}"
  fi
  if ! is_truthy "${TS_WAL_ARCHIVE_ALLOW_PARENT_MOUNT:-0}" && [ "$mount_point" != "$root_resolved" ]; then
    die archive_root_mount_mismatch "archive_root=${archive_root} resolved=${root_resolved} mount_point=${mount_point} fs_type=${fs_type} mount_source=${mount_source}"
  fi
}

fsync_path() {
  local path="$1"
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$path" <<'PY'
import os
import sys

path = sys.argv[1]
fd = os.open(path, os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY
    return 0
  fi
  if command -v sync >/dev/null 2>&1; then
    if sync -f "$path" >/dev/null 2>&1; then
      return 0
    fi
    sync
    return 0
  fi
  die fsync_unavailable "path=${path}"
}

prepare_archive_dir() {
  local archive_root="$1"
  local require_mount="$2"
  if is_truthy "$require_mount"; then
    require_archive_root_mount "$archive_root"
  fi
  mkdir -p "$tmp_dir" || die archive_dir_prepare_failed "wal_dir=${wal_dir} tmp_dir=${tmp_dir}"
  [ -d "$wal_dir" ] || die archive_dir_missing "wal_dir=${wal_dir}"
  [ -w "$wal_dir" ] && [ -x "$wal_dir" ] || die archive_dir_not_writable "wal_dir=${wal_dir} uid=$(id -u) gid=$(id -g)"
  [ -w "$tmp_dir" ] && [ -x "$tmp_dir" ] || die archive_tmp_dir_not_writable "tmp_dir=${tmp_dir} uid=$(id -u) gid=$(id -g)"
}

if [ "$#" -ne 2 ]; then
  die invalid_args "expected_args=2 actual_args=$#"
fi

src="$1"
wal_name="$2"
wal_dir="${TS_BACKUP_WAL_DIR:-/var/backups/trading/wal}"
if [ -n "${TS_BACKUP_ROOT:-}" ]; then
  archive_root="$TS_BACKUP_ROOT"
elif [[ "$wal_dir" == /var/backups/trading || "$wal_dir" == /var/backups/trading/* ]]; then
  archive_root="/var/backups/trading"
else
  archive_root="$(parent_dir "$wal_dir")"
fi
tmp_dir="${wal_dir}/.tmp"
final="${wal_dir}/${wal_name}"
tmp="${tmp_dir}/${wal_name}.$$.$RANDOM.tmp"
require_mount="${TS_WAL_ARCHIVE_REQUIRE_MOUNT:-}"
if [ -z "$require_mount" ]; then
  if [[ "$wal_dir" == /var/backups/trading || "$wal_dir" == /var/backups/trading/* ]]; then
    require_mount=1
  else
    require_mount=0
  fi
fi

case "$wal_name" in
  *[!A-Za-z0-9._-]*|'')
    die invalid_wal_name "wal_name=${wal_name}"
    ;;
esac

[ -f "$src" ] || die source_missing "source=${src} wal_name=${wal_name}"

prepare_archive_dir "$archive_root" "$require_mount"

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
