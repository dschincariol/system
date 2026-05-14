#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=base_backup %s\n' "$level" "$event" "$*"
}

die() {
  log error "$1" "${2:-}"
  exit 1
}

extract_tar() {
  local archive="$1"
  local dest="$2"
  case "$archive" in
    *.tar.gz|*.tgz)
      tar -xzf "$archive" -C "$dest"
      ;;
    *.tar)
      tar -xf "$archive" -C "$dest"
      ;;
    *)
      die unsupported_archive "archive=${archive}"
      ;;
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
  local verify_log="${backup_dir}/pg_verifybackup.out"
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

if [ "${1:-}" = "--verify-only" ]; then
  [ "$#" -eq 2 ] || die invalid_args "usage=--verify-only_<backup_dir>"
  verify_backup_dir "$2"
  exit 0
fi

base_dir="${TS_BACKUP_BASE_DIR:-/var/backups/trading/base}"
stamp="${TS_BACKUP_STAMP:-$(date -u +%Y-%m-%dT%H%M%SZ)}"
backup_dir="${base_dir}/${stamp}"
work_dir="${backup_dir}.in_progress"
latest_tmp="${base_dir}/.latest.$$"
pg_basebackup_bin="${PGBASEBACKUP_BIN:-pg_basebackup}"

mkdir -p "$base_dir"
if [ -e "$backup_dir" ] || [ -e "$work_dir" ]; then
  stamp="${stamp}.$$"
  backup_dir="${base_dir}/${stamp}"
  work_dir="${backup_dir}.in_progress"
fi

cleanup_work() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    rm -f "$latest_tmp"
    if [ -d "$work_dir" ]; then
      touch "${work_dir}/FAILED"
    fi
  fi
}
trap cleanup_work EXIT

mkdir -p "$work_dir"
log info backup_started "backup_dir=${backup_dir} work_dir=${work_dir}"

"$pg_basebackup_bin" \
  -D "$work_dir" \
  -F tar \
  -z \
  -X stream \
  -P \
  -R \
  ${TS_PG_BASEBACKUP_EXTRA:-} \
  > "${work_dir}/pg_basebackup.out" 2>&1

verify_backup_dir "$work_dir"

mv "$work_dir" "$backup_dir"

if [ -n "${TS_BASE_BACKUP_OFFSITE_CMD:-}" ]; then
  cmd="${TS_BASE_BACKUP_OFFSITE_CMD//<name>/${stamp}}"
  tar -C "$base_dir" -cf - "$(basename "$backup_dir")" | TS_BASE_BACKUP_NAME="$stamp" bash -o pipefail -c "$cmd"
  log info offsite_copied "backup_name=${stamp}"
fi

ln -sfn "$stamp" "$latest_tmp"
mv -Tf "$latest_tmp" "${base_dir}/latest"

log info backup_complete "backup_dir=${backup_dir} latest=${base_dir}/latest"
