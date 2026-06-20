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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

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
  local verify_dir base_tar wal_tar rc pg_uid_gid

  [ -f "${backup_dir}/backup_manifest" ] || die manifest_missing "backup_dir=${backup_dir}"
  base_tar="$(base_tar_for "$backup_dir")" || die base_tar_missing "backup_dir=${backup_dir}"
  wal_tar="$(wal_tar_for "$backup_dir")" || die wal_tar_missing "backup_dir=${backup_dir}"

  if [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
    pg_uid_gid="$(
      docker exec "$TS_BACKUP_DOCKER_EXEC_CONTAINER" sh -lc 'printf "%s:%s\n" "$(id -u postgres)" "$(id -g postgres)"'
    )"
    if docker exec -u "${TS_BACKUP_DOCKER_EXEC_USER:-postgres}" "$TS_BACKUP_DOCKER_EXEC_CONTAINER" pg_verifybackup "$backup_dir" > "$verify_log" 2>&1; then
      log info verified "backup_dir=${backup_dir} verify_log=${verify_log} verify_mode=docker_exec"
      return 0
    fi
  elif "${PGVERIFYBACKUP_BIN:-pg_verifybackup}" "$backup_dir" > "$verify_log" 2>&1; then
    log info verified "backup_dir=${backup_dir} verify_log=${verify_log} verify_mode=direct"
    return 0
  fi
  printf '\n-- retrying after tar extraction for pg_verifybackup versions that require plain backups --\n' >> "$verify_log"

  if [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
    verify_dir="$(mktemp -d "${backup_dir}.verify.XXXXXX")"
    chown "$pg_uid_gid" "$verify_dir"
  else
    verify_dir="$(mktemp -d "${TMPDIR:-/tmp}/trading-pg-verify.XXXXXX")"
  fi
  rc=0
  {
    extract_tar "$base_tar" "$verify_dir"
    mkdir -p "${verify_dir}/pg_wal"
    extract_tar "$wal_tar" "${verify_dir}/pg_wal"
    cp "${backup_dir}/backup_manifest" "${verify_dir}/backup_manifest"
    if [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
      chown -R "$pg_uid_gid" "$verify_dir"
      docker exec -u "${TS_BACKUP_DOCKER_EXEC_USER:-postgres}" "$TS_BACKUP_DOCKER_EXEC_CONTAINER" pg_verifybackup "$verify_dir"
    else
      "${PGVERIFYBACKUP_BIN:-pg_verifybackup}" "$verify_dir"
    fi
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
pg_basebackup_log="${work_dir}.pg_basebackup.out"
latest_tmp="${base_dir}/.latest.$$"
pg_basebackup_bin="${PGBASEBACKUP_BIN:-pg_basebackup}"

if [ -n "${TS_BACKUP_DOCKER_IMAGE:-}" ] && [ "${TS_BACKUP_IN_DOCKER:-0}" != "1" ]; then
  command -v docker >/dev/null 2>&1 || die docker_missing "image=${TS_BACKUP_DOCKER_IMAGE}"
  mkdir -p "$base_dir"
  docker run --rm \
    --network "${TS_BACKUP_DOCKER_NETWORK:-host}" \
    --user "${TS_BACKUP_DOCKER_USER:-postgres}" \
    -v "${repo_root}:${repo_root}:ro" \
    -v "${base_dir}:${base_dir}" \
    -e TS_BACKUP_IN_DOCKER=1 \
    -e TS_BACKUP_BASE_DIR \
    -e TS_BACKUP_STAMP \
    -e TS_PG_BASEBACKUP_EXTRA \
    -e TS_BASE_BACKUP_OFFSITE_CMD \
    -e PGHOST \
    -e PGPORT \
    -e PGUSER \
    -e PGPASSWORD \
    -e PGDATABASE \
    "${TS_BACKUP_DOCKER_IMAGE}" \
    bash "${script_dir}/base_backup.sh"
  exit $?
fi

run_pg_basebackup() {
  if [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
    docker exec \
      -u "${TS_BACKUP_DOCKER_EXEC_USER:-postgres}" \
      -e PGHOST="${PGHOST:-127.0.0.1}" \
      -e PGPORT="${PGPORT:-5432}" \
      -e PGUSER="${PGUSER:-postgres}" \
      -e PGPASSWORD="${PGPASSWORD:-}" \
      -e PGDATABASE="${PGDATABASE:-postgres}" \
      "$TS_BACKUP_DOCKER_EXEC_CONTAINER" \
      pg_basebackup \
      -D "$work_dir" \
      -F tar \
      -z \
      -X stream \
      -P \
      -R \
      ${TS_PG_BASEBACKUP_EXTRA:-}
    return $?
  fi

  "$pg_basebackup_bin" \
    -D "$work_dir" \
    -F tar \
    -z \
    -X stream \
    -P \
    -R \
    ${TS_PG_BASEBACKUP_EXTRA:-}
}

mkdir -p "$base_dir"
if [ -e "$backup_dir" ] || [ -e "$work_dir" ]; then
  stamp="${stamp}.$$"
  backup_dir="${base_dir}/${stamp}"
  work_dir="${backup_dir}.in_progress"
  pg_basebackup_log="${work_dir}.pg_basebackup.out"
fi

cleanup_work() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    rm -f "$latest_tmp"
    if [ -f "$pg_basebackup_log" ] && [ -d "$work_dir" ]; then
      mv -f "$pg_basebackup_log" "${work_dir}/pg_basebackup.out" || true
    fi
    if [ -d "$work_dir" ]; then
      touch "${work_dir}/FAILED"
    fi
  fi
}
trap cleanup_work EXIT

mkdir -p "$work_dir"
if [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
  pg_uid_gid="$(
    docker exec "$TS_BACKUP_DOCKER_EXEC_CONTAINER" sh -lc 'printf "%s:%s\n" "$(id -u postgres)" "$(id -g postgres)"'
  )"
  if [ -n "${TS_BACKUP_READ_GROUP:-}" ]; then
    chown "${pg_uid_gid%%:*}:${TS_BACKUP_READ_GROUP}" "$work_dir"
    chmod 2750 "$work_dir"
  else
    chown "$pg_uid_gid" "$work_dir"
  fi
fi
log info backup_started "backup_dir=${backup_dir} work_dir=${work_dir}"

run_pg_basebackup > "$pg_basebackup_log" 2>&1

mv -f "$pg_basebackup_log" "${work_dir}/pg_basebackup.out"

verify_backup_dir "$work_dir"

if [ -n "${TS_BACKUP_READ_GROUP:-}" ] && [ -n "${pg_uid_gid:-}" ]; then
  chown -R "${pg_uid_gid%%:*}:${TS_BACKUP_READ_GROUP}" "$work_dir"
  find "$work_dir" -type d -exec chmod 2750 {} +
  find "$work_dir" -type f -exec chmod 0640 {} +
fi

mv "$work_dir" "$backup_dir"

if [ -n "${TS_BASE_BACKUP_OFFSITE_CMD:-}" ]; then
  cmd="${TS_BASE_BACKUP_OFFSITE_CMD//<name>/${stamp}}"
  tar -C "$base_dir" -cf - "$(basename "$backup_dir")" | TS_BASE_BACKUP_NAME="$stamp" bash -o pipefail -c "$cmd"
  log info offsite_copied "backup_name=${stamp}"
fi

ln -sfn "$stamp" "$latest_tmp"
mv -Tf "$latest_tmp" "${base_dir}/latest"

log info backup_complete "backup_dir=${backup_dir} latest=${base_dir}/latest"
