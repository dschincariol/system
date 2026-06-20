#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=backup_accounting %s\n' "$level" "$event" "$*"
}

backup_root="${TRADING_BACKUP_ROOT:-${TS_BACKUP_ROOT:-/var/backups/trading}}"
container_path="${BACKUP_ACCOUNTING_CONTAINER_PATH:-/var/backups/trading}"
runtime_container="${BACKUP_ACCOUNTING_RUNTIME_CONTAINER:-trading-runtime}"
base_dir="${TS_BACKUP_BASE_DIR:-${backup_root}/base}"
wal_dir="${TS_BACKUP_WAL_DIR:-${backup_root}/wal}"
drill_dir="${TS_RESTORE_DRILL_DIR:-${backup_root}/drills}"
keep_daily_days="${TS_BACKUP_KEEP_DAILY_DAYS:-14}"
keep_weekly_days="${TS_BACKUP_KEEP_WEEKLY_DAYS:-365}"
wal_cushion_days="${TS_BACKUP_WAL_CUSHION_DAYS:-7}"

du_bytes() {
  local mode="$1"
  local path="$2"
  if [ ! -e "$path" ]; then
    printf '0'
    return 0
  fi
  case "$mode" in
    apparent) du -sb "$path" 2>/dev/null | awk '{print $1}' ;;
    allocated) du -sB1 "$path" 2>/dev/null | awk '{print $1}' ;;
    *) printf '0' ;;
  esac
}

count_dirs() {
  local path="$1"
  [ -d "$path" ] || {
    printf '0'
    return 0
  }
  find "$path" -mindepth 1 -maxdepth 1 -type d ! -name latest ! -name '.*' ! -name '*.in_progress' 2>/dev/null | wc -l | tr -d ' '
}

count_in_progress() {
  local path="$1"
  [ -d "$path" ] || {
    printf '0'
    return 0
  }
  find "$path" -mindepth 1 -maxdepth 1 -type d -name '*.in_progress' 2>/dev/null | wc -l | tr -d ' '
}

count_files() {
  local path="$1"
  [ -d "$path" ] || {
    printf '0'
    return 0
  }
  find "$path" -mindepth 1 -maxdepth 1 -type f ! -name '.*' 2>/dev/null | wc -l | tr -d ' '
}

latest_path() {
  local path="$1"
  local kind="$2"
  [ -d "$path" ] || return 0
  find "$path" -mindepth 1 -maxdepth 1 "-type" "$kind" ! -name latest ! -name '.*' ! -name '*.in_progress' -printf '%T@ %p\n' 2>/dev/null \
    | sort -nr \
    | head -n 1 \
    | cut -d' ' -f2-
}

mount_report() {
  local path="$1"
  if command -v findmnt >/dev/null 2>&1 && [ -e "$path" ]; then
    findmnt -T "$path" -no SOURCE,TARGET,FSTYPE,OPTIONS 2>/dev/null | head -n 1 | awk '{print "mount_source="$1" mount_target="$2" fstype="$3" mount_options="$4}'
    return 0
  fi
  if [ -e "$path" ]; then
    df -P "$path" 2>/dev/null | awk 'NR==2 {print "mount_source="$1" mount_target="$6" fstype=unknown mount_options=unknown"}'
  fi
}

docker_mount_report() {
  command -v docker >/dev/null 2>&1 || return 0
  docker inspect -f '{{range .Mounts}}{{if eq .Destination "/var/backups/trading"}}docker_mount_source={{.Source}} docker_mount_destination={{.Destination}} docker_mount_mode={{.Mode}} docker_mount_rw={{.RW}}{{println}}{{end}}{{end}}' "$runtime_container" 2>/dev/null \
    | head -n 1
}

fs_bytes() {
  local path="$1"
  [ -e "$path" ] || return 0
  df -PB1 "$path" 2>/dev/null | awk 'NR==2 {print "filesystem_total_bytes="$2" filesystem_used_bytes="$3" filesystem_free_bytes="$4" filesystem_used_pct="$5}'
}

if [ ! -e "$backup_root" ]; then
  log warn backup_root_missing "host_path=${backup_root} container_path=${container_path}"
  exit 1
fi

root_apparent="$(du_bytes apparent "$backup_root")"
root_allocated="$(du_bytes allocated "$backup_root")"
base_count="$(count_dirs "$base_dir")"
base_in_progress="$(count_in_progress "$base_dir")"
wal_count="$(count_files "$wal_dir")"
latest_base="$(latest_path "$base_dir" d || true)"
latest_wal="$(latest_path "$wal_dir" f || true)"
mount_line="$(mount_report "$backup_root" || true)"
docker_mount_line="$(docker_mount_report || true)"
fs_line="$(fs_bytes "$backup_root" || true)"

log info backup_accounting \
  "host_path=${backup_root}" \
  "container_path=${container_path}" \
  "apparent_bytes=${root_apparent}" \
  "allocated_bytes=${root_allocated}" \
  "base_backup_count=${base_count}" \
  "base_in_progress_count=${base_in_progress}" \
  "wal_file_count=${wal_count}" \
  "latest_base=${latest_base:-}" \
  "latest_wal=${latest_wal:-}" \
  "keep_daily_days=${keep_daily_days}" \
  "keep_weekly_days=${keep_weekly_days}" \
  "wal_cushion_days=${wal_cushion_days}" \
  "retention_status=configured"

for entry in \
  "base:${base_dir}" \
  "wal:${wal_dir}" \
  "drills:${drill_dir}" \
  "evidence:${backup_root}/evidence" \
  "state:${backup_root}/state" \
  "artifacts:${backup_root}/artifacts"
do
  name="${entry%%:*}"
  path="${entry#*:}"
  log info backup_accounting_subdir \
    "name=${name}" \
    "path=${path}" \
    "exists=$([ -e "$path" ] && printf 1 || printf 0)" \
    "apparent_bytes=$(du_bytes apparent "$path")" \
    "allocated_bytes=$(du_bytes allocated "$path")"
done

[ -n "$mount_line" ] && log info backup_accounting_mount "$mount_line"
[ -n "$docker_mount_line" ] && log info backup_accounting_docker_mount "$docker_mount_line"
[ -n "$fs_line" ] && log info backup_accounting_filesystem "$fs_line"
