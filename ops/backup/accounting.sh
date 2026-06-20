#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=backup_accounting %s\n' "$level" "$event" "$*"
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

df_path() {
  local path="$1"
  while [ ! -e "$path" ] && [ "$path" != "/" ]; do
    path="$(dirname "$path")"
  done
  printf '%s' "$path"
}

fs_bytes() {
  local path
  path="$(df_path "$1")"
  [ -e "$path" ] || return 0
  df -PB1 "$path" 2>/dev/null | awk 'NR==2 {print "filesystem_total_bytes="$2" filesystem_used_bytes="$3" filesystem_free_bytes="$4" filesystem_used_pct="$5}'
}

fs_free_bytes() {
  local path
  path="$(df_path "$1")"
  [ -e "$path" ] || {
    printf '0'
    return 0
  }
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

days_to_full() {
  local free_bytes="$1"
  local bytes_per_day="$2"
  if [ "$bytes_per_day" -le 0 ]; then
    printf 'unbounded'
    return 0
  fi
  awk -v free="$free_bytes" -v rate="$bytes_per_day" 'BEGIN { printf "%.2f", free / rate }'
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
wal_observation_days="$(parse_nonnegative_int "${TS_BACKUP_WAL_OBSERVATION_DAYS:-1}" 1)"
if [ "$wal_observation_days" -lt 1 ]; then
  wal_observation_days=1
fi
backup_budget_bytes="$(parse_bytes "${TS_BACKUP_MAX_BYTES:-}")"
now_epoch="$(date +%s)"

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
free_bytes="$(fs_free_bytes "$backup_root")"
compute_wal_stats

budget_remaining_bytes=0
budget_over=0
if [ "$backup_budget_bytes" -gt 0 ]; then
  budget_remaining_bytes="$((backup_budget_bytes - root_apparent))"
  if [ "$budget_remaining_bytes" -lt 0 ]; then
    budget_over=1
  fi
fi
retention_days="$((keep_daily_days + wal_cushion_days))"
required_free_bytes="$((wal_bytes_per_day * retention_days))"
projected_days_to_full="$(days_to_full "${free_bytes:-0}" "$wal_bytes_per_day")"

log info backup_accounting \
  "host_path=${backup_root}" \
  "container_path=${container_path}" \
  "apparent_bytes=${root_apparent}" \
  "allocated_bytes=${root_allocated}" \
  "budget_bytes=${backup_budget_bytes}" \
  "budget_remaining_bytes=${budget_remaining_bytes}" \
  "over_budget=${budget_over}" \
  "observed_wal_bytes_per_day=${wal_bytes_per_day}" \
  "wal_observation_days=${wal_observation_days}" \
  "projected_days_to_full=${projected_days_to_full}" \
  "retention_days=${retention_days}" \
  "retention_required_free_bytes=${required_free_bytes}" \
  "filesystem_free_bytes=${free_bytes:-0}" \
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
  "drills_work:${drill_dir}/work" \
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
