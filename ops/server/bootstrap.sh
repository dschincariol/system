#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[bootstrap] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

export DEBIAN_FRONTEND="${DEBIAN_FRONTEND:-noninteractive}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TRADING_USER="${TRADING_USER:-trading}"
TRADING_GROUP="${TRADING_GROUP:-trading}"

INSTALL_ROOT="${TRADING_INSTALL_ROOT:-/opt/trading}"
APP_ROOT="${TRADING_APP_ROOT:-/opt/trading/app}"
VENV_DIR="${TRADING_VENV_DIR:-${INSTALL_ROOT}/venv}"
DEPENDENCY_PROFILE="${TRADING_DEPENDENCY_PROFILE:-cpu}"
REQUIREMENTS_FILE="${TRADING_REQUIREMENTS_FILE:-}"

DATA_ROOT="${TRADING_DATA_ROOT:-/var/lib/trading}"
DB_DIR="${TRADING_DB_DIR:-${DATA_ROOT}/db}"
POSTGRES_DATA_DIR="${TRADING_POSTGRES_DATA_DIR:-${DB_DIR}/postgresql/16/main}"
REDIS_DIR="${TRADING_REDIS_DIR:-${DATA_ROOT}/redis}"
ARTIFACT_DIR="${TRADING_ARTIFACT_DIR:-${DATA_ROOT}/artifacts}"
NLP_MODELS_DIR="${TRADING_NLP_MODELS_DIR:-${DATA_ROOT}/nlp_models}"
APP_LOG_DIR="${TRADING_APP_LOG_DIR:-${DATA_ROOT}/logs}"

BACKUP_ROOT="${TRADING_BACKUP_ROOT:-/var/backups/trading}"
BACKUP_BASE_DIR="${TRADING_BACKUP_BASE_DIR:-${BACKUP_ROOT}/base}"
BACKUP_WAL_DIR="${TRADING_BACKUP_WAL_DIR:-${BACKUP_ROOT}/wal}"
BACKUP_STATE_DIR="${TRADING_BACKUP_STATE_DIR:-${BACKUP_ROOT}/state}"
BACKUP_ARTIFACT_DIR="${TRADING_BACKUP_ARTIFACT_DIR:-${BACKUP_ROOT}/artifacts}"
BACKUP_DRILL_DIR="${TRADING_BACKUP_DRILL_DIR:-${BACKUP_ROOT}/drills}"
BACKUP_EVIDENCE_DIR="${TRADING_BACKUP_EVIDENCE_DIR:-${BACKUP_ROOT}/evidence}"

ETC_DIR="${TRADING_ETC_DIR:-/etc/trading}"
CREDSTORE_DIR="${TRADING_CREDSTORE_DIR:-/etc/credstore.encrypted}"
PROVIDER_ENV="${TRADING_PROVIDER_ENV:-${ETC_DIR}/provider.env}"
TRADING_ENV_FILE="${TRADING_ENV_FILE:-${ETC_DIR}/trading.env}"
BACKUP_EVIDENCE_HMAC_KEY_FILE="${TRADING_BACKUP_EVIDENCE_HMAC_KEY_FILE:-${ETC_DIR}/backup_evidence.hmac.key}"

POSTGRES_VERSION="${TRADING_POSTGRES_VERSION:-16}"
POSTGRES_DB="${TRADING_POSTGRES_DB:-trading}"
POSTGRES_MAX_CONNECTIONS="${TRADING_POSTGRES_MAX_CONNECTIONS:-100}"
PGBOUNCER_PORT="${TRADING_PGBOUNCER_PORT:-6432}"
POSTGRES_SOCKET_DIR="${TRADING_POSTGRES_SOCKET_DIR:-/var/run/postgresql}"
REDIS_SOCKET="${TRADING_REDIS_SOCKET:-/var/run/redis/trading.sock}"
REDIS_TCP_PORT="${TRADING_REDIS_TCP_PORT:-6379}"
OPERATOR_PORT="${TRADING_OPERATOR_PORT:-4001}"
DASHBOARD_PORT="${TRADING_DASHBOARD_PORT:-8000}"
FIREWALL_UI_PORT="${TRADING_FIREWALL_UI_PORT:-${OPERATOR_PORT}}"
NODE_MAJOR="${TRADING_NODE_MAJOR:-20}"

INSTALL_PYTHON_REQUIREMENTS="${TRADING_INSTALL_PYTHON_REQUIREMENTS:-1}"
INSTALL_NODE_DEPENDENCIES="${TRADING_INSTALL_NODE_DEPENDENCIES:-1}"
ENABLE_UFW="${TRADING_ENABLE_UFW:-1}"

POSTGRES_TEMPLATE="${SCRIPT_DIR}/config/postgres.conf.tmpl"
PGBOUNCER_TEMPLATE="${SCRIPT_DIR}/config/pgbouncer.ini.tmpl"
PGBOUNCER_USERLIST_TEMPLATE="${SCRIPT_DIR}/config/pgbouncer.userlist.txt.tmpl"
REDIS_TEMPLATE="${SCRIPT_DIR}/config/redis.conf.tmpl"
SYSTEMD_SRC_DIR="${SCRIPT_DIR}/systemd"
SYSTEMD_DST_DIR="${TRADING_SYSTEMD_DIR:-/etc/systemd/system}"
CREDSTORE_INSTALL_SCRIPT="${SCRIPT_DIR}/credstore/install.sh"
BACKUP_SCRIPT_SRC_DIR="${REPO_ROOT}/ops/backup"
BACKUP_SCRIPT_DST_DIR="${INSTALL_ROOT}/ops/backup"

APT_UPDATED=0
POSTGRES_RESTART_NEEDED=0
REDIS_RESTART_NEEDED=0
PGBOUNCER_RESTART_NEEDED=0

log() {
  printf '[bootstrap] %s\n' "$*"
}

die() {
  printf '[bootstrap] ERROR: %s\n' "$*" >&2
  exit 1
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    die "bootstrap.sh must run as root"
  fi
}

load_os_release() {
  if [ ! -r /etc/os-release ]; then
    die "/etc/os-release is missing"
  fi
  # shellcheck disable=SC1091
  . /etc/os-release
  OS_ID="${ID:-}"
  OS_VERSION_ID="${VERSION_ID:-}"
  OS_CODENAME="${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
  case "${OS_ID}:${OS_CODENAME}" in
    ubuntu:jammy|debian:bookworm) ;;
    *)
      die "unsupported OS ${OS_ID:-unknown} ${OS_VERSION_ID:-unknown} ${OS_CODENAME:-unknown}; expected Ubuntu 22.04 jammy or Debian 12 bookworm"
      ;;
  esac
}

is_container() {
  if command -v systemd-detect-virt >/dev/null 2>&1 && systemd-detect-virt --container --quiet; then
    return 0
  fi
  grep -qaE '(docker|containerd|kubepods|lxc)' /proc/1/cgroup 2>/dev/null
}

systemd_available() {
  command -v systemctl >/dev/null 2>&1 && [ "$(ps -p 1 -o comm= | tr -d ' ')" = "systemd" ]
}

apt_update() {
  if [ "${APT_UPDATED}" -eq 0 ]; then
    log "apt-get update"
    apt-get update
    APT_UPDATED=1
  fi
}

package_installed() {
  dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q 'install ok installed'
}

apt_install_packages() {
  local missing=()
  local pkg
  for pkg in "$@"; do
    if ! package_installed "$pkg"; then
      missing+=("$pkg")
    fi
  done

  if [ "${#missing[@]}" -eq 0 ]; then
    log "packages already installed: $*"
    return 0
  fi

  apt_update
  log "installing packages: ${missing[*]}"
  apt-get install -y --no-install-recommends "${missing[@]}"
}

install_if_changed_from_file() {
  local source="$1"
  local target="$2"
  local mode="$3"
  local owner="$4"
  local group="$5"

  if [ ! -d "$(dirname "$target")" ]; then
    install -d -m 0755 "$(dirname "$target")"
  fi
  if [ -f "$target" ] && cmp -s "$source" "$target"; then
    return 1
  fi

  install -m "$mode" -o "$owner" -g "$group" "$source" "$target"
  log "updated ${target}"
  return 0
}

write_if_changed() {
  local target="$1"
  local mode="$2"
  local owner="$3"
  local group="$4"
  local tmp
  tmp="$(mktemp)"
  cat > "$tmp"
  if install_if_changed_from_file "$tmp" "$target" "$mode" "$owner" "$group"; then
    rm -f "$tmp"
    return 0
  fi
  rm -f "$tmp"
  return 1
}

ensure_base_packages() {
  apt_update
  apt_install_packages \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    openssl \
    sudo \
    procps \
    rsync \
    util-linux \
    systemd \
    logrotate
}

set_kernel_tuning() {
  log "configuring kernel tuning"
  if write_if_changed /etc/sysctl.d/99-trading.conf 0644 root root <<'EOF'
vm.overcommit_memory=2
vm.swappiness=1
net.core.somaxconn=4096
fs.file-max=262144
EOF
  then
    if ! is_container; then
      sysctl --system >/dev/null
    else
      log "container detected; wrote sysctl config but skipped live sysctl apply"
    fi
  fi

  local thp_path
  for thp_path in /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag; do
    if [ -w "$thp_path" ]; then
      printf never > "$thp_path"
      log "disabled transparent hugepages at ${thp_path}"
    else
      log "transparent hugepage control not writable: ${thp_path}"
    fi
  done
}

ensure_group_membership() {
  local user="$1"
  local group="$2"
  if id -u "$user" >/dev/null 2>&1 && getent group "$group" >/dev/null 2>&1; then
    if ! id -nG "$user" | tr ' ' '\n' | grep -qx "$group"; then
      usermod -aG "$group" "$user"
      log "added ${user} to ${group}"
    fi
  fi
}

create_users_and_dirs() {
  log "creating trading user and filesystem layout"
  if ! getent group "$TRADING_GROUP" >/dev/null 2>&1; then
    groupadd --system "$TRADING_GROUP"
  fi
  if ! id -u "$TRADING_USER" >/dev/null 2>&1; then
    useradd --system --gid "$TRADING_GROUP" --home-dir "$DATA_ROOT" --shell /usr/sbin/nologin "$TRADING_USER"
  fi

  local dir
  for dir in \
    "$DATA_ROOT" \
    "$DB_DIR" \
    "$REDIS_DIR" \
    "$ARTIFACT_DIR" \
    "$NLP_MODELS_DIR" \
    "$APP_LOG_DIR" \
    "$ETC_DIR" \
    "$INSTALL_ROOT" \
    "$APP_ROOT" \
    "$APP_ROOT/data" \
    "$APP_ROOT/logs"
  do
    install -d -o "$TRADING_USER" -g "$TRADING_GROUP" -m 0750 "$dir"
  done

  for dir in \
    "$BACKUP_ROOT" \
    "$BACKUP_BASE_DIR" \
    "$BACKUP_WAL_DIR" \
    "$BACKUP_WAL_DIR/.tmp" \
    "$BACKUP_STATE_DIR" \
    "$BACKUP_ARTIFACT_DIR" \
    "$BACKUP_DRILL_DIR" \
    "$BACKUP_EVIDENCE_DIR"
  do
    install -d -o "$TRADING_USER" -g "$TRADING_GROUP" -m 0770 "$dir"
  done

  install -d -o "$TRADING_USER" -g "$TRADING_GROUP" -m 0755 /var/run/redis
  install -d -o root -g root -m 0700 "$CREDSTORE_DIR"

  if [ ! -f "$PROVIDER_ENV" ]; then
    write_if_changed "$PROVIDER_ENV" 0600 "$TRADING_USER" "$TRADING_GROUP" <<'EOF' || true
# Provider runtime settings are intentionally empty after bootstrap.
# Secrets belong in data_sources or /etc/credstore.encrypted, not in this file.
EOF
  fi
}

sync_app_source() {
  log "syncing application source"
  local source_abs target_abs
  source_abs="$(cd "$REPO_ROOT" && pwd -P)"
  target_abs="$(cd "$APP_ROOT" && pwd -P)"

  if [ "$source_abs" = "$target_abs" ]; then
    log "source already matches APP_ROOT: ${APP_ROOT}"
    chown -R "$TRADING_USER:$TRADING_GROUP" "$APP_ROOT"
    return
  fi

  case "$target_abs" in
    "$source_abs"/*)
      die "APP_ROOT must not be inside the source tree: source=${source_abs} target=${target_abs}"
      ;;
  esac

  rsync -a --delete \
    --include '.env.example' \
    --exclude '.git/' \
    --exclude '.env' \
    --exclude '.env.*' \
    --exclude '.venv/' \
    --exclude 'venv/' \
    --exclude 'env/' \
    --exclude 'ENV/' \
    --exclude 'node_modules/' \
    --exclude 'var/' \
    --exclude '__pycache__/' \
    --exclude '.pytest_cache/' \
    --exclude '.ruff_cache/' \
    --exclude '.mypy_cache/' \
    --exclude '.claude/' \
    --exclude '.vscode/' \
    --exclude 'dist/' \
    --exclude 'build/' \
    --exclude 'coverage/' \
    --exclude 'logs/' \
    --exclude 'logs-*/' \
    --exclude 'tmp/' \
    --exclude 'data-staging/' \
    --exclude 'data-isolation/' \
    --exclude 'data/operator/' \
    --exclude 'data/runtime/' \
    --exclude 'data/retraining/' \
    --exclude 'models/' \
    --exclude '*.db' \
    --exclude '*.sqlite' \
    --exclude '*.sqlite3' \
    --exclude '*.db-wal' \
    --exclude '*.db-shm' \
    --exclude '*.sqlite-wal' \
    --exclude '*.sqlite-shm' \
    --exclude '*.log' \
    --exclude '*.log.*' \
    --exclude '*.tmp' \
    --exclude '*.pid' \
    --exclude '*.seed' \
    --exclude '*.out' \
    --exclude '*.err' \
    --exclude '*.lock' \
    --exclude '*_trace.txt' \
    --exclude '*_trace.log' \
    --exclude '*_probe_trace.txt' \
    --exclude '*_probe_trace.log' \
    --exclude '*_hang.txt' \
    --exclude '*_hang_dump.txt' \
    --exclude 'pyright*.json' \
    --exclude 'pyright*.txt' \
    --exclude 'ruff_repo.json' \
    --exclude 'ingestion_runtime-pyright.json' \
    "$source_abs/" "$target_abs/"
  chown -R "$TRADING_USER:$TRADING_GROUP" "$APP_ROOT"
}

add_pgdg_repo() {
  install -d -m 0755 /usr/share/postgresql-common/pgdg
  if [ ! -f /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc ]; then
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
    chmod 0644 /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
    APT_UPDATED=0
  fi

  local arch
  arch="$(dpkg --print-architecture)"
  if write_if_changed /etc/apt/sources.list.d/pgdg.sources 0644 root root <<EOF
Types: deb
URIs: https://apt.postgresql.org/pub/repos/apt
Suites: ${OS_CODENAME}-pgdg
Architectures: ${arch}
Components: main
Signed-By: /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
EOF
  then
    APT_UPDATED=0
  fi
}

add_timescale_repo() {
  local keyring=/usr/share/keyrings/timescale-archive-keyring.gpg
  if [ ! -f "$keyring" ]; then
    curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor -o "$keyring"
    chmod 0644 "$keyring"
    APT_UPDATED=0
  fi

  if write_if_changed /etc/apt/sources.list.d/timescaledb.list 0644 root root <<EOF
deb [signed-by=${keyring}] https://packagecloud.io/timescale/timescaledb/${OS_ID}/ ${OS_CODENAME} main
EOF
  then
    APT_UPDATED=0
  fi
}

add_redis_repo() {
  local keyring=/usr/share/keyrings/redis-archive-keyring.gpg
  if [ ! -f "$keyring" ]; then
    curl -fsSL https://packages.redis.io/gpg | gpg --dearmor -o "$keyring"
    chmod 0644 "$keyring"
    APT_UPDATED=0
  fi

  if write_if_changed /etc/apt/sources.list.d/redis.list 0644 root root <<EOF
deb [signed-by=${keyring}] https://packages.redis.io/deb ${OS_CODENAME} main
EOF
  then
    APT_UPDATED=0
  fi
}

add_nodesource_repo() {
  local keyring=/usr/share/keyrings/nodesource.gpg
  if [ ! -f "$keyring" ]; then
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o "$keyring"
    chmod 0644 "$keyring"
    APT_UPDATED=0
  fi

  if write_if_changed /etc/apt/sources.list.d/nodesource.list 0644 root root <<EOF
deb [signed-by=${keyring}] https://deb.nodesource.com/node_${NODE_MAJOR}.x nodistro main
EOF
  then
    APT_UPDATED=0
  fi
}

cluster_exists() {
  pg_lsclusters --no-header 2>/dev/null | awk -v ver="$POSTGRES_VERSION" '$1 == ver && $2 == "main" {found=1} END {exit found ? 0 : 1}'
}

cluster_data_dir() {
  pg_conftool "$POSTGRES_VERSION" main show data_directory 2>/dev/null | tr -d "'"
}

directory_empty() {
  [ -d "$1" ] && [ -z "$(find "$1" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]
}

start_postgres_cluster() {
  pg_ctlcluster "$POSTGRES_VERSION" main start >/dev/null 2>&1 || true
}

stop_postgres_cluster() {
  pg_ctlcluster "$POSTGRES_VERSION" main stop >/dev/null 2>&1 || true
}

trading_database_exists() {
  runuser -u postgres -- psql -h "$POSTGRES_SOCKET_DIR" -p 5432 -Atqc \
    "SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_DB}'" 2>/dev/null | grep -qx 1
}

ensure_postgres_cluster() {
  local target_real db_real current
  target_real="$(realpath -m "$POSTGRES_DATA_DIR")"
  db_real="$(realpath -m "$DB_DIR")"
  case "$target_real" in
    "$db_real"/*) ;;
    *) die "refusing Postgres data directory outside ${DB_DIR}: ${POSTGRES_DATA_DIR}" ;;
  esac

  install -d -o postgres -g postgres -m 0750 "$(dirname "$(dirname "$POSTGRES_DATA_DIR")")"
  install -d -o postgres -g postgres -m 0750 "$(dirname "$POSTGRES_DATA_DIR")"
  install -d -o postgres -g postgres -m 0700 "$POSTGRES_DATA_DIR"

  if ! cluster_exists; then
    log "creating PostgreSQL ${POSTGRES_VERSION}/main cluster at ${POSTGRES_DATA_DIR}"
    pg_createcluster "$POSTGRES_VERSION" main --datadir "$POSTGRES_DATA_DIR" -- --data-checksums
    POSTGRES_RESTART_NEEDED=1
    start_postgres_cluster
    return
  fi

  start_postgres_cluster
  current="$(cluster_data_dir || true)"
  if [ "$current" = "$POSTGRES_DATA_DIR" ]; then
    return
  fi

  if directory_empty "$POSTGRES_DATA_DIR" && ! trading_database_exists; then
    log "moving fresh PostgreSQL cluster from ${current} to ${POSTGRES_DATA_DIR}"
    stop_postgres_cluster
    pg_dropcluster --stop "$POSTGRES_VERSION" main
    pg_createcluster "$POSTGRES_VERSION" main --datadir "$POSTGRES_DATA_DIR" -- --data-checksums
    POSTGRES_RESTART_NEEDED=1
    start_postgres_cluster
  else
    log "existing PostgreSQL cluster data_directory=${current}; leaving it in place"
  fi
}

install_postgres_timescale() {
  log "installing PostgreSQL ${POSTGRES_VERSION} and TimescaleDB"
  add_pgdg_repo
  add_timescale_repo
  apt_update

  local timescale_pkg
  if apt-cache show "timescaledb-2-postgresql-${POSTGRES_VERSION}" >/dev/null 2>&1; then
    timescale_pkg="timescaledb-2-postgresql-${POSTGRES_VERSION}"
  elif apt-cache show "postgresql-${POSTGRES_VERSION}-timescaledb" >/dev/null 2>&1; then
    timescale_pkg="postgresql-${POSTGRES_VERSION}-timescaledb"
  else
    die "could not find a TimescaleDB package for PostgreSQL ${POSTGRES_VERSION}"
  fi

  local packages=(
    "postgresql-${POSTGRES_VERSION}"
    "postgresql-client-${POSTGRES_VERSION}"
    "$timescale_pkg"
  )
  if apt-cache show "postgresql-contrib-${POSTGRES_VERSION}" 2>/dev/null | grep -q '^Package:'; then
    packages+=("postgresql-contrib-${POSTGRES_VERSION}")
  fi

  apt_install_packages "${packages[@]}"

  ensure_group_membership postgres "$TRADING_GROUP"
  ensure_postgres_cluster
}

bytes_to_mb_setting() {
  local mb="$1"
  if [ "$mb" -ge 1024 ]; then
    printf '%sGB' "$((mb / 1024))"
  else
    printf '%sMB' "$mb"
  fi
}

first_env_value() {
  local name value
  for name in "$@"; do
    value="${!name:-}"
    if [ -n "$value" ]; then
      printf '%s' "$value"
      return 0
    fi
  done
  return 1
}

size_to_mb() {
  local raw lower number unit
  raw="$(printf '%s' "$1" | tr -d '[:space:]')"
  lower="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  if ! printf '%s' "$lower" | grep -Eq '^[0-9]+([.][0-9]+)?([kmgt]i?b?|b)?$'; then
    die "invalid size setting: ${1}"
  fi
  number="$(printf '%s' "$lower" | sed -E 's/^([0-9]+([.][0-9]+)?).*/\1/')"
  unit="$(printf '%s' "$lower" | sed -E 's/^[0-9]+([.][0-9]+)?//')"
  case "$unit" in
    g|gb|gib) awk -v n="$number" 'BEGIN { printf "%d", n * 1024 }' ;;
    m|mb|mib) awk -v n="$number" 'BEGIN { printf "%d", n }' ;;
    k|kb|kib) awk -v n="$number" 'BEGIN { v = n / 1024; printf "%d", v < 1 ? 1 : v }' ;;
    b|"") awk -v n="$number" 'BEGIN { v = n / 1048576; printf "%d", v < 1 ? 1 : v }' ;;
    *) die "invalid size unit in setting: ${1}" ;;
  esac
}

size_env_or_default_mb() {
  local default_mb="$1"
  shift
  local raw
  raw="$(first_env_value "$@" || true)"
  if [ -n "$raw" ]; then
    size_to_mb "$raw"
  else
    printf '%s' "$default_mb"
  fi
}

cgroup_memory_limit_mb() {
  local path raw
  for path in /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory/memory.limit_in_bytes; do
    [ -r "$path" ] || continue
    raw="$(cat "$path" 2>/dev/null || true)"
    [ -n "$raw" ] && [ "$raw" != "max" ] || continue
    if printf '%s' "$raw" | grep -Eq '^[0-9]+$' && [ "$raw" -gt 0 ] && [ "$raw" -lt 1152921504606846976 ]; then
      printf '%s' "$((raw / 1048576))"
      return 0
    fi
  done
  return 1
}

postgres_tuning_ram_mb() {
  local raw host_mb cgroup_mb
  raw="$(first_env_value TRADING_POSTGRES_MEMORY_LIMIT TIMESCALE_MEM_LIMIT TIMESCALE_MEMORY_LIMIT || true)"
  if [ -n "$raw" ]; then
    size_to_mb "$raw"
    return 0
  fi
  host_mb="$(( $(awk '/MemTotal:/ {print $2}' /proc/meminfo) / 1024 ))"
  cgroup_mb="$(cgroup_memory_limit_mb || true)"
  if [ -n "$cgroup_mb" ] && [ "$cgroup_mb" -gt 0 ] && [ "$cgroup_mb" -lt "$host_mb" ]; then
    printf '%s' "$cgroup_mb"
  else
    printf '%s' "$host_mb"
  fi
}

tune_postgres() {
  log "rendering PostgreSQL tuning"
  local ram_mb vcpu max_connections
  local shared_buffers_mb effective_cache_mb work_mem_mb maintenance_mb autovacuum_work_mem_mb
  local max_worker_processes max_parallel_workers max_parallel_workers_per_gather
  local max_parallel_maintenance_workers timescaledb_max_background_workers autovacuum_max_workers tmp
  local wal_buffers min_wal_size max_wal_size wal_keep_size max_slot_wal_keep_size
  local archive_timeout checkpoint_timeout checkpoint_completion_target
  local random_page_cost effective_io_concurrency maintenance_io_concurrency
  local autovacuum_naptime autovacuum_vacuum_cost_limit autovacuum_vacuum_cost_delay

  ram_mb="$(postgres_tuning_ram_mb)"
  vcpu="$(nproc)"
  max_connections="${TRADING_POSTGRES_MAX_CONNECTIONS:-${TIMESCALE_MAX_CONNECTIONS:-${POSTGRES_MAX_CONNECTIONS}}}"

  shared_buffers_mb="$(size_env_or_default_mb "$((ram_mb / 4))" TRADING_POSTGRES_SHARED_BUFFERS TIMESCALE_SHARED_BUFFERS)"
  effective_cache_mb="$(size_env_or_default_mb "$(((ram_mb * 3) / 4))" TRADING_POSTGRES_EFFECTIVE_CACHE_SIZE TIMESCALE_EFFECTIVE_CACHE_SIZE)"
  work_mem_mb="$(size_env_or_default_mb "$((((ram_mb * 5) / 1000) / max_connections))" TRADING_POSTGRES_WORK_MEM TIMESCALE_WORK_MEM)"
  [ "$work_mem_mb" -lt 4 ] && work_mem_mb=4
  maintenance_mb="$(size_env_or_default_mb "$((ram_mb / 16))" TRADING_POSTGRES_MAINTENANCE_WORK_MEM TIMESCALE_MAINTENANCE_WORK_MEM)"
  [ "$maintenance_mb" -lt 64 ] && maintenance_mb=64
  [ "$maintenance_mb" -gt 2048 ] && maintenance_mb=2048
  autovacuum_work_mem_mb="$(size_env_or_default_mb "$((ram_mb / 128))" TRADING_POSTGRES_AUTOVACUUM_WORK_MEM TIMESCALE_AUTOVACUUM_WORK_MEM)"
  [ "$autovacuum_work_mem_mb" -lt 128 ] && autovacuum_work_mem_mb=128
  [ "$autovacuum_work_mem_mb" -gt 1024 ] && autovacuum_work_mem_mb=1024

  max_worker_processes="${TRADING_POSTGRES_MAX_WORKER_PROCESSES:-${TIMESCALE_MAX_WORKER_PROCESSES:-$((vcpu * 2))}}"
  max_parallel_workers="${TRADING_POSTGRES_MAX_PARALLEL_WORKERS:-${TIMESCALE_MAX_PARALLEL_WORKERS:-$vcpu}}"
  max_parallel_workers_per_gather="${TRADING_POSTGRES_MAX_PARALLEL_WORKERS_PER_GATHER:-${TIMESCALE_MAX_PARALLEL_WORKERS_PER_GATHER:-$((vcpu / 2))}}"
  [ "$max_parallel_workers_per_gather" -lt 1 ] && max_parallel_workers_per_gather=1
  max_parallel_maintenance_workers="${TRADING_POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS:-${TIMESCALE_MAX_PARALLEL_MAINTENANCE_WORKERS:-$((vcpu / 2))}}"
  [ "$max_parallel_maintenance_workers" -lt 2 ] && max_parallel_maintenance_workers=2
  timescaledb_max_background_workers="${TRADING_POSTGRES_TIMESCALEDB_MAX_BACKGROUND_WORKERS:-${TIMESCALE_TIMESCALEDB_MAX_BACKGROUND_WORKERS:-$vcpu}}"
  [ "$timescaledb_max_background_workers" -lt 4 ] && timescaledb_max_background_workers=4
  autovacuum_max_workers="${TRADING_POSTGRES_AUTOVACUUM_MAX_WORKERS:-${TIMESCALE_AUTOVACUUM_MAX_WORKERS:-$((vcpu / 2))}}"
  [ "$autovacuum_max_workers" -lt 3 ] && autovacuum_max_workers=3
  wal_buffers="${TRADING_POSTGRES_WAL_BUFFERS:-${TIMESCALE_WAL_BUFFERS:-64MB}}"
  min_wal_size="${TRADING_POSTGRES_MIN_WAL_SIZE:-${TIMESCALE_MIN_WAL_SIZE:-4GB}}"
  max_wal_size="${TRADING_POSTGRES_MAX_WAL_SIZE:-${TIMESCALE_MAX_WAL_SIZE:-16GB}}"
  wal_keep_size="${TRADING_POSTGRES_WAL_KEEP_SIZE:-${TIMESCALE_WAL_KEEP_SIZE:-1GB}}"
  max_slot_wal_keep_size="${TRADING_POSTGRES_MAX_SLOT_WAL_KEEP_SIZE:-${TIMESCALE_MAX_SLOT_WAL_KEEP_SIZE:-8GB}}"
  archive_timeout="${TRADING_POSTGRES_ARCHIVE_TIMEOUT:-${TIMESCALE_ARCHIVE_TIMEOUT:-60s}}"
  checkpoint_timeout="${TRADING_POSTGRES_CHECKPOINT_TIMEOUT:-${TIMESCALE_CHECKPOINT_TIMEOUT:-15min}}"
  checkpoint_completion_target="${TRADING_POSTGRES_CHECKPOINT_COMPLETION_TARGET:-${TIMESCALE_CHECKPOINT_COMPLETION_TARGET:-0.9}}"
  random_page_cost="${TRADING_POSTGRES_RANDOM_PAGE_COST:-${TIMESCALE_RANDOM_PAGE_COST:-1.1}}"
  effective_io_concurrency="${TRADING_POSTGRES_EFFECTIVE_IO_CONCURRENCY:-${TIMESCALE_EFFECTIVE_IO_CONCURRENCY:-200}}"
  maintenance_io_concurrency="${TRADING_POSTGRES_MAINTENANCE_IO_CONCURRENCY:-${TIMESCALE_MAINTENANCE_IO_CONCURRENCY:-200}}"
  autovacuum_naptime="${TRADING_POSTGRES_AUTOVACUUM_NAPTIME:-${TIMESCALE_AUTOVACUUM_NAPTIME:-10s}}"
  autovacuum_vacuum_cost_limit="${TRADING_POSTGRES_AUTOVACUUM_VACUUM_COST_LIMIT:-${TIMESCALE_AUTOVACUUM_VACUUM_COST_LIMIT:-4000}}"
  autovacuum_vacuum_cost_delay="${TRADING_POSTGRES_AUTOVACUUM_VACUUM_COST_DELAY:-${TIMESCALE_AUTOVACUUM_VACUUM_COST_DELAY:-2ms}}"

  install -d -o postgres -g postgres -m 0755 "/etc/postgresql/${POSTGRES_VERSION}/main/conf.d"
  if ! grep -Eq "^[[:space:]]*include_dir[[:space:]]*=[[:space:]]*'conf.d'" "/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf"; then
    printf "\ninclude_dir = 'conf.d'\n" >> "/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf"
    POSTGRES_RESTART_NEEDED=1
  fi

  tmp="$(mktemp)"
  sed \
    -e "s|{{ max_connections }}|${max_connections}|g" \
    -e "s|{{ shared_buffers }}|$(bytes_to_mb_setting "$shared_buffers_mb")|g" \
    -e "s|{{ effective_cache_size }}|$(bytes_to_mb_setting "$effective_cache_mb")|g" \
    -e "s|{{ work_mem }}|$(bytes_to_mb_setting "$work_mem_mb")|g" \
    -e "s|{{ maintenance_work_mem }}|$(bytes_to_mb_setting "$maintenance_mb")|g" \
    -e "s|{{ autovacuum_work_mem }}|$(bytes_to_mb_setting "$autovacuum_work_mem_mb")|g" \
    -e "s|{{ wal_buffers }}|${wal_buffers}|g" \
    -e "s|{{ min_wal_size }}|${min_wal_size}|g" \
    -e "s|{{ max_wal_size }}|${max_wal_size}|g" \
    -e "s|{{ wal_keep_size }}|${wal_keep_size}|g" \
    -e "s|{{ max_slot_wal_keep_size }}|${max_slot_wal_keep_size}|g" \
    -e "s|{{ archive_timeout }}|${archive_timeout}|g" \
    -e "s|{{ checkpoint_timeout }}|${checkpoint_timeout}|g" \
    -e "s|{{ checkpoint_completion_target }}|${checkpoint_completion_target}|g" \
    -e "s|{{ random_page_cost }}|${random_page_cost}|g" \
    -e "s|{{ effective_io_concurrency }}|${effective_io_concurrency}|g" \
    -e "s|{{ maintenance_io_concurrency }}|${maintenance_io_concurrency}|g" \
    -e "s|{{ postgres_socket_dir }}|${POSTGRES_SOCKET_DIR}|g" \
    -e "s|{{ max_worker_processes }}|${max_worker_processes}|g" \
    -e "s|{{ max_parallel_workers }}|${max_parallel_workers}|g" \
    -e "s|{{ max_parallel_workers_per_gather }}|${max_parallel_workers_per_gather}|g" \
    -e "s|{{ max_parallel_maintenance_workers }}|${max_parallel_maintenance_workers}|g" \
    -e "s|{{ timescaledb_max_background_workers }}|${timescaledb_max_background_workers}|g" \
    -e "s|{{ autovacuum_max_workers }}|${autovacuum_max_workers}|g" \
    -e "s|{{ autovacuum_naptime }}|${autovacuum_naptime}|g" \
    -e "s|{{ autovacuum_vacuum_cost_limit }}|${autovacuum_vacuum_cost_limit}|g" \
    -e "s|{{ autovacuum_vacuum_cost_delay }}|${autovacuum_vacuum_cost_delay}|g" \
    "$POSTGRES_TEMPLATE" > "$tmp"

  if install_if_changed_from_file "$tmp" "/etc/postgresql/${POSTGRES_VERSION}/main/conf.d/trading.conf" 0644 postgres postgres; then
    POSTGRES_RESTART_NEEDED=1
  fi
  rm -f "$tmp"
}

configure_postgres_hba() {
  local hba="/etc/postgresql/${POSTGRES_VERSION}/main/pg_hba.conf"
  local stripped tmp
  stripped="$(mktemp)"
  tmp="$(mktemp)"

  awk '
    /^# BEGIN trading bootstrap$/ {skip=1; next}
    /^# END trading bootstrap$/ {skip=0; next}
    skip != 1 {print}
  ' "$hba" > "$stripped"

  {
    cat <<EOF
# BEGIN trading bootstrap
local   ${POSTGRES_DB}   ts_app,ts_ingest,ts_reader   scram-sha-256
host    ${POSTGRES_DB}   ts_app,ts_ingest,ts_reader   127.0.0.1/32   scram-sha-256
host    ${POSTGRES_DB}   ts_app,ts_ingest,ts_reader   ::1/128        scram-sha-256
# END trading bootstrap
EOF
    cat "$stripped"
  } > "$tmp"

  if install_if_changed_from_file "$tmp" "$hba" 0640 postgres postgres; then
    POSTGRES_RESTART_NEEDED=1
  fi
  rm -f "$stripped" "$tmp"
}

restart_postgres_if_needed() {
  if [ "$POSTGRES_RESTART_NEEDED" -eq 1 ]; then
    log "restarting PostgreSQL"
    pg_ctlcluster "$POSTGRES_VERSION" main restart
  else
    start_postgres_cluster
  fi
}

psql_postgres() {
  runuser -u postgres -- psql -v ON_ERROR_STOP=1 -h "$POSTGRES_SOCKET_DIR" -p 5432 "$@"
}

role_exists() {
  local role="$1"
  psql_postgres -Atqc "SELECT 1 FROM pg_roles WHERE rolname = '${role}'" | grep -qx 1
}

create_role_if_missing() {
  local role="$1"
  local password="$2"
  if role_exists "$role"; then
    log "PostgreSQL role already exists: ${role}"
    return
  fi
  psql_postgres -v role="$role" -v pwd="$password" -d postgres <<'SQL'
CREATE ROLE :"role" LOGIN PASSWORD :'pwd';
SQL
}

create_database_if_missing() {
  if psql_postgres -Atqc "SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_DB}'" | grep -qx 1; then
    log "PostgreSQL database already exists: ${POSTGRES_DB}"
    return
  fi
  runuser -u postgres -- createdb -h "$POSTGRES_SOCKET_DIR" -p 5432 -O ts_app "$POSTGRES_DB"
}

install_credstore() {
  log "installing encrypted systemd credentials"
  if [ ! -r "$CREDSTORE_INSTALL_SCRIPT" ]; then
    die "missing credential installer: ${CREDSTORE_INSTALL_SCRIPT}"
  fi
  TRADING_CREDSTORE_DIR="$CREDSTORE_DIR" bash "$CREDSTORE_INSTALL_SCRIPT"
}

read_credstore_secret() {
  local name="$1" path
  path="${CREDSTORE_DIR}/${name}.cred"
  [ -r "$path" ] || die "missing encrypted credential ${path}"
  systemd-creds decrypt --name="$name" "$path" -
}

ensure_backup_evidence_hmac_key() {
  local key_dir tmp
  key_dir="$(dirname "$BACKUP_EVIDENCE_HMAC_KEY_FILE")"
  install -d -o root -g "$TRADING_GROUP" -m 0750 "$key_dir"
  if [ -f "$BACKUP_EVIDENCE_HMAC_KEY_FILE" ]; then
    [ -s "$BACKUP_EVIDENCE_HMAC_KEY_FILE" ] || die "backup evidence HMAC key file is empty: ${BACKUP_EVIDENCE_HMAC_KEY_FILE}"
    chown root:"$TRADING_GROUP" "$BACKUP_EVIDENCE_HMAC_KEY_FILE"
    chmod 0640 "$BACKUP_EVIDENCE_HMAC_KEY_FILE"
    return 0
  fi

  tmp="$(mktemp "${key_dir}/backup_evidence.hmac.key.tmp.XXXXXX")"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32 > "$tmp"
  else
    python3 - <<'PY' > "$tmp"
import secrets

print(secrets.token_hex(32))
PY
  fi
  install -m 0640 -o root -g "$TRADING_GROUP" "$tmp" "$BACKUP_EVIDENCE_HMAC_KEY_FILE"
  rm -f "$tmp"
  log "created backup evidence HMAC key: ${BACKUP_EVIDENCE_HMAC_KEY_FILE}"
}

write_runtime_env_files() {
  ensure_backup_evidence_hmac_key
  write_if_changed "$TRADING_ENV_FILE" 0640 "$TRADING_USER" "$TRADING_GROUP" <<EOF || true
TS_ENV=production
TRADING_ENV=production
TS_SECRETS_PROVIDER=systemd-creds
APP_ROOT=${APP_ROOT}
PYTHONPATH=${APP_ROOT}
DATA_DIR=${DATA_ROOT}
LOG_DIR=${APP_LOG_DIR}
DB_PATH=${DATA_ROOT}
SQLITE_LIVENESS_DB_PATH=${DB_DIR}/trading.liveness.sqlite
DASHBOARD_HOST=127.0.0.1
DASHBOARD_PORT=${DASHBOARD_PORT}
OPERATOR_BIND_HOST=127.0.0.1
OPERATOR_PORT=${OPERATOR_PORT}
OPERATOR_ALLOWED_ORIGIN=http://127.0.0.1:${DASHBOARD_PORT}
REDIS_SOCKET=${REDIS_SOCKET}
PGBOUNCER_PORT=${PGBOUNCER_PORT}
PGBASEBACKUP_BIN=/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_basebackup
PGVERIFYBACKUP_BIN=/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_verifybackup
PGCTL_BIN=/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_ctl
PGCONTROLDATA_BIN=/usr/lib/postgresql/${POSTGRES_VERSION}/bin/pg_controldata
AUTO_BOOT_DAEMONS=0
START_INGESTION_WITH_SERVER=0
OPEN_DASHBOARD_BROWSER_ON_START=0
TRADING_STARTUP_HEALTH_FAIL_OPEN=0
TRADING_STARTUP_HEALTH_ASYNC_BIND=1
TRADING_IMPORT_SMOKE_IMPORT_JOBS=0
TRADING_IMPORT_SMOKE_TIMEOUT_S=12.0
BACKUP_EVIDENCE_PATH=${BACKUP_ROOT}/evidence/latest_backup_restore_evidence.json
BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S=93600
BACKUP_EVIDENCE_RPO_S=120
BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S=7776000
BACKUP_EVIDENCE_RTO_S=1800
BACKUP_EVIDENCE_SIGNATURE_MAX_AGE_S=120
BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1
BACKUP_EVIDENCE_HMAC_KEY_FILE=${BACKUP_EVIDENCE_HMAC_KEY_FILE}
TIMESCALE_ENABLED=1
TIMESCALE_PRICES_ENABLED=1
EOF
}

init_databases() {
  log "initializing PostgreSQL roles, database, and extensions"
  install_credstore

  create_role_if_missing ts_ingest "$(read_credstore_secret pg_password_ingest)"
  create_role_if_missing ts_app "$(read_credstore_secret pg_password_app)"
  create_role_if_missing ts_reader "$(read_credstore_secret pg_password_reader)"
  create_database_if_missing

  psql_postgres -v db="$POSTGRES_DB" -d "$POSTGRES_DB" <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

GRANT CONNECT ON DATABASE :"db" TO ts_ingest, ts_app, ts_reader;
GRANT USAGE ON SCHEMA public TO ts_ingest, ts_app, ts_reader;
GRANT CREATE ON SCHEMA public TO ts_ingest, ts_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ts_reader;
SQL

  write_runtime_env_files
}

validate_scram_hash() {
  local role="$1"
  local secret="$2"
  case "$secret" in
    SCRAM-SHA-256\$*) ;;
    *) die "PostgreSQL role ${role} does not have a SCRAM-SHA-256 password verifier" ;;
  esac
}

refresh_pgbouncer_userlist() {
  local tmp role secret ts_ingest_scram ts_app_scram ts_reader_scram
  if [ ! -r "$PGBOUNCER_USERLIST_TEMPLATE" ]; then
    die "missing PgBouncer userlist template: ${PGBOUNCER_USERLIST_TEMPLATE}"
  fi

  ts_ingest_scram=""
  ts_app_scram=""
  ts_reader_scram=""

  while IFS=$'\t' read -r role secret; do
    [ -n "$role" ] || continue
    validate_scram_hash "$role" "$secret"
    case "$role" in
      ts_ingest) ts_ingest_scram="$secret" ;;
      ts_app) ts_app_scram="$secret" ;;
      ts_reader) ts_reader_scram="$secret" ;;
    esac
  done < <(
    runuser -u postgres -- psql -h "$POSTGRES_SOCKET_DIR" -p 5432 -d postgres -AtF $'\t' \
      -c "SELECT rolname, rolpassword FROM pg_authid WHERE rolname IN ('ts_ingest','ts_app','ts_reader') ORDER BY rolname"
  )

  validate_scram_hash ts_ingest "$ts_ingest_scram"
  validate_scram_hash ts_app "$ts_app_scram"
  validate_scram_hash ts_reader "$ts_reader_scram"

  tmp="$(mktemp)"
  awk \
    -v ts_ingest_scram="$ts_ingest_scram" \
    -v ts_app_scram="$ts_app_scram" \
    -v ts_reader_scram="$ts_reader_scram" \
    '{
      gsub(/\{\{ ts_ingest_scram \}\}/, ts_ingest_scram);
      gsub(/\{\{ ts_app_scram \}\}/, ts_app_scram);
      gsub(/\{\{ ts_reader_scram \}\}/, ts_reader_scram);
      print;
    }' "$PGBOUNCER_USERLIST_TEMPLATE" > "$tmp"

  if install_if_changed_from_file "$tmp" /etc/pgbouncer/userlist.txt 0640 postgres postgres; then
    PGBOUNCER_RESTART_NEEDED=1
  fi
  rm -f "$tmp"
}

install_pgbouncer() {
  log "installing PgBouncer"
  apt_install_packages pgbouncer
  install -d -o postgres -g postgres -m 0755 /etc/pgbouncer /var/log/postgresql
  install -d -m 0755 "$SYSTEMD_DST_DIR"

  if [ -f "${SYSTEMD_SRC_DIR}/pgbouncer.service" ]; then
    if install_if_changed_from_file "${SYSTEMD_SRC_DIR}/pgbouncer.service" "${SYSTEMD_DST_DIR}/pgbouncer.service" 0644 root root; then
      PGBOUNCER_RESTART_NEEDED=1
      if systemd_available; then
        systemctl daemon-reload
      fi
    fi
  fi

  local tmp
  tmp="$(mktemp)"
  sed \
    -e "s|{{ postgres_db }}|${POSTGRES_DB}|g" \
    -e "s|{{ postgres_socket_dir }}|${POSTGRES_SOCKET_DIR}|g" \
    -e "s|{{ pgbouncer_port }}|${PGBOUNCER_PORT}|g" \
    -e "s|{{ trading_group }}|${TRADING_GROUP}|g" \
    "$PGBOUNCER_TEMPLATE" > "$tmp"
  if install_if_changed_from_file "$tmp" /etc/pgbouncer/pgbouncer.ini 0640 postgres postgres; then
    PGBOUNCER_RESTART_NEEDED=1
  fi
  rm -f "$tmp"

  if write_if_changed /etc/default/pgbouncer 0644 root root <<'EOF'
START=1
EOF
  then
    PGBOUNCER_RESTART_NEEDED=1
  fi

  refresh_pgbouncer_userlist

  if systemd_available; then
    systemctl enable pgbouncer >/dev/null
    if [ "$PGBOUNCER_RESTART_NEEDED" -eq 1 ]; then
      systemctl restart pgbouncer
    else
      systemctl start pgbouncer >/dev/null 2>&1 || systemctl restart pgbouncer
    fi
  else
    if [ ! -S "${POSTGRES_SOCKET_DIR}/.s.PGSQL.${PGBOUNCER_PORT}" ]; then
      log "starting PgBouncer without systemd"
      runuser -u postgres -- pgbouncer -d /etc/pgbouncer/pgbouncer.ini
    fi
  fi
}

install_redis() {
  log "installing Redis"
  add_redis_repo
  apt_install_packages redis-server redis-tools
  chown root:"$TRADING_GROUP" /etc/redis
  chmod 0750 /etc/redis

  local mem_kb ram_bytes redis_maxmemory redis_supervised tmp
  mem_kb="$(awk '/MemTotal:/ {print $2}' /proc/meminfo)"
  ram_bytes="$((mem_kb * 1024))"
  redis_maxmemory="$((ram_bytes / 4))"
  redis_supervised="no"
  if systemd_available; then
    redis_supervised="systemd"
  fi

  tmp="$(mktemp)"
  sed \
    -e "s|{{ redis_maxmemory }}|${redis_maxmemory}|g" \
    -e "s|{{ redis_supervised }}|${redis_supervised}|g" \
    -e "s|{{ redis_tcp_port }}|${REDIS_TCP_PORT}|g" \
    -e "s|{{ redis_dir }}|${REDIS_DIR}|g" \
    -e "s|{{ redis_socket }}|${REDIS_SOCKET}|g" \
    "$REDIS_TEMPLATE" > "$tmp"

  if install_if_changed_from_file "$tmp" /etc/redis/redis.conf 0640 "$TRADING_USER" "$TRADING_GROUP"; then
    REDIS_RESTART_NEEDED=1
  fi
  rm -f "$tmp"

  if systemd_available; then
    install -d -m 0755 /etc/systemd/system/redis-server.service.d
    if write_if_changed /etc/systemd/system/redis-server.service.d/10-trading.conf 0644 root root <<EOF
[Service]
User=${TRADING_USER}
Group=${TRADING_GROUP}
ReadWritePaths=${REDIS_DIR} /var/run/redis
EOF
    then
      systemctl daemon-reload
      REDIS_RESTART_NEEDED=1
    fi
    systemctl enable redis-server >/dev/null
    if [ "$REDIS_RESTART_NEEDED" -eq 1 ]; then
      systemctl restart redis-server
    else
      systemctl start redis-server >/dev/null 2>&1 || systemctl restart redis-server
    fi
  else
    if ! redis-cli -s "$REDIS_SOCKET" PING >/dev/null 2>&1; then
      log "starting Redis without systemd"
      runuser -u "$TRADING_USER" -- redis-server /etc/redis/redis.conf --daemonize yes --supervised no
    fi
  fi
}

install_python_runtime() {
  log "installing Python runtime"
  if ! command -v python3.11 >/dev/null 2>&1; then
    if [ "$OS_ID" = "ubuntu" ]; then
      apt_install_packages software-properties-common
      if ! grep -Rqs "deadsnakes" /etc/apt/sources.list /etc/apt/sources.list.d 2>/dev/null; then
        add-apt-repository -y ppa:deadsnakes/ppa
        APT_UPDATED=0
      fi
    fi
  fi

  apt_install_packages python3.11 python3.11-venv python3.11-dev build-essential pkg-config

  if [ ! -x "${VENV_DIR}/bin/python" ]; then
    install -d -o "$TRADING_USER" -g "$TRADING_GROUP" -m 0750 "$VENV_DIR"
    python3.11 -m venv "$VENV_DIR"
    chown -R "$TRADING_USER:$TRADING_GROUP" "$VENV_DIR"
  fi

  if [ "$INSTALL_PYTHON_REQUIREMENTS" != "1" ]; then
    log "skipping Python requirements because TRADING_INSTALL_PYTHON_REQUIREMENTS=${INSTALL_PYTHON_REQUIREMENTS}"
    return
  fi

  local resolved_requirements
  if [ -n "$REQUIREMENTS_FILE" ]; then
    if [[ "$REQUIREMENTS_FILE" = /* ]]; then
      resolved_requirements="$REQUIREMENTS_FILE"
    else
      resolved_requirements="${REPO_ROOT}/${REQUIREMENTS_FILE}"
    fi
  else
    resolved_requirements="$(
      TRADING_DEPENDENCY_PROFILE="$DEPENDENCY_PROFILE" \
        bash "${REPO_ROOT}/deploy/bin/resolve_python_requirements.sh" "$REPO_ROOT"
    )"
  fi

  if [ ! -f "$resolved_requirements" ]; then
    log "requirements file not found, skipping pip install: ${resolved_requirements}"
    return
  fi

  local req_hash marker
  req_hash="$(
    {
      sha256sum "$resolved_requirements"
      find "$REPO_ROOT" -maxdepth 1 -name 'requirements*.txt' -type f -print0 | sort -z | xargs -0 sha256sum
    } | sha256sum | awk '{print $1}'
  )"
  marker="${VENV_DIR}/.requirements.${DEPENDENCY_PROFILE}.sha256"
  if [ -f "$marker" ] && [ "$(cat "$marker")" = "$req_hash" ]; then
    log "Python requirements already installed for ${req_hash}"
    return
  fi

  PIP_DISABLE_PIP_VERSION_CHECK=1 "$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
  PIP_DISABLE_PIP_VERSION_CHECK=1 "$VENV_DIR/bin/python" -m pip install -r "$resolved_requirements"
  printf '%s' "$req_hash" > "$marker"
  chown -R "$TRADING_USER:$TRADING_GROUP" "$VENV_DIR"
}

install_node_runtime() {
  local major=0
  if command -v node >/dev/null 2>&1; then
    major="$(node -p 'Number(process.versions.node.split(".")[0])' 2>/dev/null || printf 0)"
  fi
  if [ "$major" -ge 18 ]; then
    log "Node.js already satisfies runtime requirement: $(node --version)"
    return
  fi

  log "installing Node.js ${NODE_MAJOR}.x"
  add_nodesource_repo
  apt_install_packages nodejs
}

install_node_dependencies() {
  log "installing Node.js dependencies"
  if [ "$INSTALL_NODE_DEPENDENCIES" != "1" ]; then
    log "skipping Node dependencies because TRADING_INSTALL_NODE_DEPENDENCIES=${INSTALL_NODE_DEPENDENCIES}"
    return
  fi
  if [ ! -f "${APP_ROOT}/package.json" ]; then
    log "package.json not found, skipping npm install: ${APP_ROOT}/package.json"
    return
  fi

  local marker desired_hash
  marker="${APP_ROOT}/node_modules/.package-lock.sha256"
  if [ -f "${APP_ROOT}/package-lock.json" ]; then
    desired_hash="$(sha256sum "${APP_ROOT}/package-lock.json" | awk '{print $1}')"
    if [ -f "$marker" ] && [ "$(cat "$marker")" = "$desired_hash" ]; then
      log "Node dependencies already installed for ${desired_hash}"
      return
    fi
    runuser -u "$TRADING_USER" -- bash -lc "cd '$APP_ROOT' && npm ci --omit=dev"
  else
    desired_hash="$(sha256sum "${APP_ROOT}/package.json" | awk '{print $1}')"
    if [ -f "$marker" ] && [ "$(cat "$marker")" = "$desired_hash" ]; then
      log "Node dependencies already installed for ${desired_hash}"
      return
    fi
    runuser -u "$TRADING_USER" -- bash -lc "cd '$APP_ROOT' && npm install --omit=dev"
  fi
  printf '%s' "$desired_hash" > "$marker"
  chown -R "$TRADING_USER:$TRADING_GROUP" "${APP_ROOT}/node_modules"
}

install_backup_scripts() {
  log "installing backup scripts"
  install -d -o root -g "$TRADING_GROUP" -m 0750 "$BACKUP_SCRIPT_DST_DIR"
  install -d -o root -g "$TRADING_GROUP" -m 0750 "${INSTALL_ROOT}/tools"
  local script
  for script in \
    wal_archive.sh \
    base_backup.sh \
    state_snapshot.sh \
    artifact_snapshot.sh \
    prune.sh \
    restore.sh \
    restore_drill.sh \
    backup_restore_evidence.sh
  do
    if install_if_changed_from_file "${BACKUP_SCRIPT_SRC_DIR}/${script}" "${BACKUP_SCRIPT_DST_DIR}/${script}" 0755 root "$TRADING_GROUP"; then
      :
    fi
  done
  install_if_changed_from_file "${REPO_ROOT}/tools/restore_sanity.sql" "${INSTALL_ROOT}/tools/restore_sanity.sql" 0644 root "$TRADING_GROUP" >/dev/null || true
}

install_systemd_units() {
  log "installing trading systemd units"
  install -d -m 0755 "$SYSTEMD_DST_DIR"
  local unit changed=0
  for unit in \
    trading-prod-preflight.service \
    trading-api.service \
    trading-jobs.service \
    trading-stream-prices.service \
    trading-ingest.service \
    trading-base-backup.service \
    trading-base-backup.timer \
    trading-backup-evidence.service \
    trading-backup-evidence.timer \
    trading-state-snapshot.service \
    trading-state-snapshot.timer \
    trading-artifact-snapshot.service \
    trading-artifact-snapshot.timer \
    trading-backup-prune.service \
    trading-backup-prune.timer \
    trading-restore-drill.service \
    trading-restore-drill.timer \
    trading.target
  do
    if install_if_changed_from_file "${SYSTEMD_SRC_DIR}/${unit}" "${SYSTEMD_DST_DIR}/${unit}" 0644 root root; then
      changed=1
    fi
  done

  if systemd_available; then
    if [ "$changed" -eq 1 ]; then
      systemctl daemon-reload
    fi
    if ! systemctl is-enabled trading.target >/dev/null 2>&1; then
      systemctl enable trading.target >/dev/null
    fi
    for unit in \
      trading-base-backup.timer \
      trading-backup-evidence.timer \
      trading-state-snapshot.timer \
      trading-artifact-snapshot.timer \
      trading-backup-prune.timer \
      trading-restore-drill.timer
    do
      systemctl enable --now "$unit" >/dev/null
    done
  else
    log "systemd is not PID 1; units copied but not enabled"
  fi
}

setup_logrotate() {
  log "configuring logrotate"
  write_if_changed /etc/logrotate.d/trading 0644 root root <<EOF || true
${APP_LOG_DIR}/*.log ${APP_LOG_DIR}/*.out ${APP_LOG_DIR}/*.err ${APP_LOG_DIR}/*.jsonl ${APP_ROOT}/boot/*.log ${APP_ROOT}/data/ai_operator_log.jsonl /var/log/postgresql/*.log {
    daily
    maxsize 50M
    rotate 10
    maxage 21
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    create 0640 ${TRADING_USER} ${TRADING_GROUP}
}
EOF
}

setup_firewall() {
  log "configuring firewall"
  apt_install_packages ufw

  if [ "$ENABLE_UFW" != "1" ]; then
    log "skipping ufw enable because TRADING_ENABLE_UFW=${ENABLE_UFW}"
    return
  fi
  if is_container; then
    log "container detected; skipping live ufw enable"
    return
  fi

  ufw default deny incoming
  ufw default allow outgoing
  ufw allow 22/tcp comment 'SSH'
  ufw allow "${FIREWALL_UI_PORT}/tcp" comment 'Trading UI'
  ufw --force enable
}

main() {
  require_root
  load_os_release
  ensure_base_packages
  set_kernel_tuning
  create_users_and_dirs
  sync_app_source
  install_postgres_timescale
  tune_postgres
  configure_postgres_hba
  restart_postgres_if_needed
  init_databases
  install_redis
  install_pgbouncer
  install_python_runtime
  install_node_runtime
  install_node_dependencies
  install_backup_scripts
  install_systemd_units
  setup_logrotate
  setup_firewall
  log "bootstrap complete; run ${SCRIPT_DIR}/verify.sh before deploying the app"
}

main "$@"
