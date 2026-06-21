#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[memory-pressure] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$(realpath "${BASH_SOURCE[0]}")"

ROOT_DIR="/"
APPLY_RUNTIME=1
DRY_RUN=0

TRADING_SWAPPINESS="${TRADING_SWAPPINESS:-10}"
TRADING_ZRAM_SIZE_GIB="${TRADING_ZRAM_SIZE_GIB:-32}"
TRADING_ZRAM_PRIORITY="${TRADING_ZRAM_PRIORITY:-100}"
TRADING_ZRAM_ALGORITHM="${TRADING_ZRAM_ALGORITHM:-zstd}"
TRADING_SWAPFILE_SIZE_GIB="${TRADING_SWAPFILE_SIZE_GIB:-16}"
TRADING_SWAPFILE_PRIORITY="${TRADING_SWAPFILE_PRIORITY:-10}"
TRADING_SWAPFILE_PATH="${TRADING_SWAPFILE_PATH:-/swapfile-trading}"
TRADING_ZFS_ARC_MAX_GIB="${TRADING_ZFS_ARC_MAX_GIB:-48}"

MANAGED_SCRIPT="/usr/local/sbin/trading-memory-pressure"
SYSCTL_CONF="/etc/sysctl.d/zz-trading-memory-pressure.conf"
ZFS_ARC_CONF="/etc/modprobe.d/trading-zfs-arc.conf"
ZRAM_UNIT="/etc/systemd/system/trading-zram-swap.service"
SWAPFILE_UNIT="/etc/systemd/system/trading-swapfile.service"

log() {
  printf '[memory-pressure] %s\n' "$*"
}

die() {
  printf '[memory-pressure] ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  ops/server/memory_pressure_hardening.sh install [--root DIR] [--no-apply] [--dry-run]
  ops/server/memory_pressure_hardening.sh verify [--root DIR]
  ops/server/memory_pressure_hardening.sh remove [--root DIR] [--no-apply] [--dry-run]

Internal systemd helper actions:
  zram-start | zram-stop | swapfile-start | swapfile-stop

Defaults for bart-class hosts:
  TRADING_SWAPPINESS=10
  TRADING_ZRAM_SIZE_GIB=32
  TRADING_SWAPFILE_SIZE_GIB=16
  TRADING_ZFS_ARC_MAX_GIB=48

The install action persists vm.swappiness through sysctl.d, creates zram and
swapfile systemd units, persists zfs_arc_max through modprobe.d, applies the
runtime values when run on the live root, and enables the swap units.
EOF
}

positive_int() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "${name} must be a positive integer, got ${value}"
  [ "$value" -gt 0 ] || die "${name} must be greater than zero"
}

gib_to_bytes() {
  local gib="$1"
  positive_int "GiB value" "$gib"
  printf '%s\n' "$((gib * 1024 * 1024 * 1024))"
}

ZRAM_SIZE_BYTES="$(gib_to_bytes "$TRADING_ZRAM_SIZE_GIB")"
SWAPFILE_SIZE_BYTES="$(gib_to_bytes "$TRADING_SWAPFILE_SIZE_GIB")"
ZFS_ARC_MAX_BYTES="$(gib_to_bytes "$TRADING_ZFS_ARC_MAX_GIB")"

validate_config() {
  positive_int "TRADING_SWAPPINESS" "$TRADING_SWAPPINESS"
  [ "$TRADING_SWAPPINESS" -le 200 ] || die "TRADING_SWAPPINESS must be <= 200"
  positive_int "TRADING_ZRAM_PRIORITY" "$TRADING_ZRAM_PRIORITY"
  positive_int "TRADING_SWAPFILE_PRIORITY" "$TRADING_SWAPFILE_PRIORITY"
  case "$TRADING_SWAPFILE_PATH" in
    /*) ;;
    *) die "TRADING_SWAPFILE_PATH must be absolute" ;;
  esac
}

root_path() {
  local path="${1#/}"
  if [ "$ROOT_DIR" = "/" ]; then
    printf '/%s\n' "$path"
  else
    printf '%s/%s\n' "${ROOT_DIR%/}" "$path"
  fi
}

runtime_root() {
  [ "$ROOT_DIR" = "/" ] && [ "$APPLY_RUNTIME" -eq 1 ] && [ "$DRY_RUN" -eq 0 ]
}

require_root_for_runtime() {
  if runtime_root && [ "$(id -u)" -ne 0 ]; then
    die "runtime apply/remove must run as root; use --no-apply to render files only"
  fi
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

systemd_available() {
  command -v systemctl >/dev/null 2>&1 && [ "$(ps -p 1 -o comm= | tr -d ' ')" = "systemd" ]
}

is_container() {
  command -v systemd-detect-virt >/dev/null 2>&1 && systemd-detect-virt --container --quiet
}

write_file() {
  local target="$1" mode="$2"
  local full tmp
  full="$(root_path "$target")"
  if [ "$DRY_RUN" -eq 1 ]; then
    log "[dry-run] would write ${full}"
    cat >/dev/null
    return 0
  fi
  install -d -m 0755 "$(dirname "$full")"
  tmp="$(mktemp)"
  cat > "$tmp"
  if [ -f "$full" ] && cmp -s "$tmp" "$full"; then
    rm -f "$tmp"
    log "unchanged ${target}"
    return 1
  fi
  install -m "$mode" "$tmp" "$full"
  rm -f "$tmp"
  log "updated ${target}"
  return 0
}

install_self_copy() {
  local target
  target="$(root_path "$MANAGED_SCRIPT")"
  if [ "$DRY_RUN" -eq 1 ]; then
    log "[dry-run] would install ${MANAGED_SCRIPT}"
    return 0
  fi
  install -d -m 0755 "$(dirname "$target")"
  if [ -f "$target" ] && cmp -s "$SCRIPT_PATH" "$target"; then
    chmod 0755 "$target"
    log "unchanged ${MANAGED_SCRIPT}"
    return 0
  fi
  install -m 0755 "$SCRIPT_PATH" "$target"
  log "updated ${MANAGED_SCRIPT}"
}

write_sysctl_config() {
  write_file "$SYSCTL_CONF" 0644 <<EOF
# Managed by ${SCRIPT_DIR}/memory_pressure_hardening.sh.
# Lower swap aggressiveness for a Postgres + ML + ZFS host while preserving an
# emergency swap floor through zram and a managed disk swapfile.
vm.swappiness = ${TRADING_SWAPPINESS}
EOF
}

write_zfs_arc_config() {
  write_file "$ZFS_ARC_CONF" 0644 <<EOF
# Managed by ${SCRIPT_DIR}/memory_pressure_hardening.sh.
# bart has 128 GiB RAM. Cap ARC at ${TRADING_ZFS_ARC_MAX_GIB} GiB so roughly
# 80 GiB remains for Postgres, ML allocations, Docker, tests, and the kernel.
options zfs zfs_arc_max=${ZFS_ARC_MAX_BYTES}
EOF
}

write_zram_unit() {
  write_file "$ZRAM_UNIT" 0644 <<EOF
[Unit]
Description=Trading managed zram swap
Documentation=file:/opt/trading/app/docs/MEMORY_PRESSURE_RUNBOOK.md
DefaultDependencies=no
After=systemd-modules-load.service local-fs.target
Before=swap.target
ConditionVirtualization=!container

[Service]
Type=oneshot
RemainAfterExit=yes
Environment=TRADING_ZRAM_SIZE_GIB=${TRADING_ZRAM_SIZE_GIB}
Environment=TRADING_ZRAM_PRIORITY=${TRADING_ZRAM_PRIORITY}
Environment=TRADING_ZRAM_ALGORITHM=${TRADING_ZRAM_ALGORITHM}
ExecStart=${MANAGED_SCRIPT} zram-start
ExecStop=${MANAGED_SCRIPT} zram-stop
TimeoutStartSec=2min
TimeoutStopSec=1min
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=swap.target
EOF
}

write_swapfile_unit() {
  write_file "$SWAPFILE_UNIT" 0644 <<EOF
[Unit]
Description=Trading managed disk swapfile
Documentation=file:/opt/trading/app/docs/MEMORY_PRESSURE_RUNBOOK.md
After=local-fs.target
Before=swap.target
ConditionVirtualization=!container

[Service]
Type=oneshot
RemainAfterExit=yes
Environment=TRADING_SWAPFILE_PATH=${TRADING_SWAPFILE_PATH}
Environment=TRADING_SWAPFILE_SIZE_GIB=${TRADING_SWAPFILE_SIZE_GIB}
Environment=TRADING_SWAPFILE_PRIORITY=${TRADING_SWAPFILE_PRIORITY}
ExecStart=${MANAGED_SCRIPT} swapfile-start
ExecStop=${MANAGED_SCRIPT} swapfile-stop
TimeoutStartSec=10min
TimeoutStopSec=1min
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=swap.target
EOF
}

install_persistent_files() {
  install_self_copy
  write_sysctl_config || true
  write_zfs_arc_config || true
  write_zram_unit || true
  write_swapfile_unit || true
}

apply_swappiness() {
  require_command sysctl
  log "applying vm.swappiness=${TRADING_SWAPPINESS}"
  sysctl -w "vm.swappiness=${TRADING_SWAPPINESS}" >/dev/null
}

apply_zfs_arc() {
  local param="/sys/module/zfs/parameters/zfs_arc_max"
  if [ ! -e "$param" ]; then
    log "ZFS module parameter not present; persisted ${ZFS_ARC_CONF} for next module load"
    return 0
  fi
  if [ ! -w "$param" ]; then
    die "${param} is not writable; cannot apply active ARC cap"
  fi
  log "applying zfs_arc_max=${ZFS_ARC_MAX_BYTES}"
  printf '%s\n' "$ZFS_ARC_MAX_BYTES" > "$param"
}

zram_start() {
  require_command mkswap
  require_command swapon
  if is_container; then
    log "container detected; skipping zram activation"
    return 0
  fi
  if swapon --noheadings --show=NAME 2>/dev/null | awk '{print $1}' | grep -Eq '^/dev/zram[0-9]+$'; then
    log "zram swap already active"
    return 0
  fi
  if command -v modprobe >/dev/null 2>&1; then
    modprobe zram num_devices=1 2>/dev/null || modprobe zram
  fi
  [ -e /sys/block/zram0/disksize ] || die "/sys/block/zram0/disksize not found after loading zram"
  if [ -w /sys/block/zram0/reset ]; then
    printf '1\n' > /sys/block/zram0/reset 2>/dev/null || true
  fi
  if [ -w /sys/block/zram0/comp_algorithm ] && grep -qw "$TRADING_ZRAM_ALGORITHM" /sys/block/zram0/comp_algorithm; then
    printf '%s\n' "$TRADING_ZRAM_ALGORITHM" > /sys/block/zram0/comp_algorithm
  fi
  printf '%s\n' "$ZRAM_SIZE_BYTES" > /sys/block/zram0/disksize
  mkswap -U clear /dev/zram0 >/dev/null
  swapon -p "$TRADING_ZRAM_PRIORITY" /dev/zram0
  log "activated /dev/zram0 size=${TRADING_ZRAM_SIZE_GIB}GiB priority=${TRADING_ZRAM_PRIORITY}"
}

zram_stop() {
  if swapon --noheadings --show=NAME 2>/dev/null | awk '{print $1}' | grep -qx /dev/zram0; then
    swapoff /dev/zram0
    log "deactivated /dev/zram0"
  fi
  if [ -w /sys/block/zram0/reset ]; then
    printf '1\n' > /sys/block/zram0/reset 2>/dev/null || true
  fi
}

swapfile_is_active() {
  swapon --noheadings --show=NAME 2>/dev/null | awk '{print $1}' | grep -qx "$TRADING_SWAPFILE_PATH"
}

swapfile_start() {
  require_command mkswap
  require_command swapon
  if is_container; then
    log "container detected; skipping swapfile activation"
    return 0
  fi

  local path="$TRADING_SWAPFILE_PATH"
  local current_size=0
  if [ -e "$path" ]; then
    current_size="$(stat -c '%s' "$path")"
  fi
  if swapfile_is_active && [ "$current_size" -ge "$SWAPFILE_SIZE_BYTES" ]; then
    log "${path} already active"
    return 0
  fi
  if swapfile_is_active; then
    swapoff "$path"
  fi

  if [ "$current_size" -lt "$SWAPFILE_SIZE_BYTES" ]; then
    local parent available
    parent="$(dirname "$path")"
    install -d -m 0755 "$parent"
    available="$(df -PB1 "$parent" | awk 'NR==2 {print $4}')"
    [ "${available:-0}" -gt "$SWAPFILE_SIZE_BYTES" ] || die "not enough free space under ${parent} for ${TRADING_SWAPFILE_SIZE_GIB}GiB swapfile"
    log "creating ${path} size=${TRADING_SWAPFILE_SIZE_GIB}GiB"
    rm -f "$path"
    if command -v fallocate >/dev/null 2>&1; then
      fallocate -l "$SWAPFILE_SIZE_BYTES" "$path" || dd if=/dev/zero of="$path" bs=1M count="$((SWAPFILE_SIZE_BYTES / 1048576))" status=progress
    else
      dd if=/dev/zero of="$path" bs=1M count="$((SWAPFILE_SIZE_BYTES / 1048576))" status=progress
    fi
  fi

  chmod 0600 "$path"
  mkswap -f "$path" >/dev/null
  swapon -p "$TRADING_SWAPFILE_PRIORITY" "$path"
  log "activated ${path} size=${TRADING_SWAPFILE_SIZE_GIB}GiB priority=${TRADING_SWAPFILE_PRIORITY}"
}

swapfile_stop() {
  if swapfile_is_active; then
    swapoff "$TRADING_SWAPFILE_PATH"
    log "deactivated ${TRADING_SWAPFILE_PATH}"
  fi
}

enable_runtime() {
  require_root_for_runtime
  apply_swappiness
  apply_zfs_arc
  if systemd_available; then
    systemctl daemon-reload
    systemctl enable --now trading-zram-swap.service trading-swapfile.service
  else
    log "systemd is not PID 1; applying swap devices directly"
    zram_start
    swapfile_start
  fi
}

remove_persistent_files() {
  local target
  for target in "$SYSCTL_CONF" "$ZFS_ARC_CONF" "$ZRAM_UNIT" "$SWAPFILE_UNIT" "$MANAGED_SCRIPT"; do
    if [ "$DRY_RUN" -eq 1 ]; then
      log "[dry-run] would remove ${target}"
      continue
    fi
    rm -f "$(root_path "$target")"
    log "removed ${target}"
  done
}

remove_runtime() {
  require_root_for_runtime
  if systemd_available; then
    systemctl disable --now trading-zram-swap.service trading-swapfile.service 2>/dev/null || true
    systemctl daemon-reload
  else
    zram_stop || true
    swapfile_stop || true
  fi
  if [ -e "$TRADING_SWAPFILE_PATH" ]; then
    rm -f "$TRADING_SWAPFILE_PATH"
    log "removed ${TRADING_SWAPFILE_PATH}"
  fi
}

verify_file_contains() {
  local target="$1" pattern="$2" label="$3"
  local full
  full="$(root_path "$target")"
  [ -f "$full" ] || die "missing ${target}"
  grep -Eq "$pattern" "$full" || die "${target} missing ${label}"
}

verify_persisted() {
  verify_file_contains "$SYSCTL_CONF" "^vm\\.swappiness[[:space:]]*=[[:space:]]*${TRADING_SWAPPINESS}$" "vm.swappiness=${TRADING_SWAPPINESS}"
  verify_file_contains "$ZFS_ARC_CONF" "zfs_arc_max=${ZFS_ARC_MAX_BYTES}" "zfs_arc_max=${ZFS_ARC_MAX_BYTES}"
  verify_file_contains "$ZRAM_UNIT" "TRADING_ZRAM_SIZE_GIB=${TRADING_ZRAM_SIZE_GIB}" "zram size"
  verify_file_contains "$SWAPFILE_UNIT" "TRADING_SWAPFILE_SIZE_GIB=${TRADING_SWAPFILE_SIZE_GIB}" "swapfile size"
  [ -x "$(root_path "$MANAGED_SCRIPT")" ] || die "missing executable ${MANAGED_SCRIPT}"
  log "persisted memory-pressure config verified"
}

verify_active() {
  local actual arc_param="/sys/module/zfs/parameters/zfs_arc_max"
  require_command sysctl
  require_command swapon

  actual="$(sysctl -n vm.swappiness)"
  [ "$actual" = "$TRADING_SWAPPINESS" ] || die "vm.swappiness active=${actual}, expected ${TRADING_SWAPPINESS}"

  [ -r "$arc_param" ] || die "${arc_param} is not readable; ZFS ARC cap is not active"
  actual="$(cat "$arc_param")"
  [ "$actual" = "$ZFS_ARC_MAX_BYTES" ] || die "zfs_arc_max active=${actual}, expected ${ZFS_ARC_MAX_BYTES}"

  swapon --noheadings --bytes --show=NAME,SIZE,PRIO | awk -v min="$ZRAM_SIZE_BYTES" -v prio="$TRADING_ZRAM_PRIORITY" '
    $1 ~ /^\/dev\/zram[0-9]+$/ && $2 >= min && $3 >= prio { found=1 }
    END { exit(found ? 0 : 1) }
  ' || die "active zram swap >= ${TRADING_ZRAM_SIZE_GIB}GiB with priority >= ${TRADING_ZRAM_PRIORITY} not found"

  swapon --noheadings --bytes --show=NAME,SIZE,PRIO | awk -v path="$TRADING_SWAPFILE_PATH" -v min="$SWAPFILE_SIZE_BYTES" -v prio="$TRADING_SWAPFILE_PRIORITY" '
    $1 == path && $2 >= min && $3 >= prio { found=1 }
    END { exit(found ? 0 : 1) }
  ' || die "active ${TRADING_SWAPFILE_PATH} >= ${TRADING_SWAPFILE_SIZE_GIB}GiB with priority >= ${TRADING_SWAPFILE_PRIORITY} not found"

  log "active swappiness, zram, swapfile, and ZFS ARC values verified"
}

install_action() {
  validate_config
  install_persistent_files
  if runtime_root; then
    enable_runtime
  else
    log "rendered persistent config only; runtime apply skipped"
  fi
}

verify_action() {
  validate_config
  verify_persisted
  if [ "$ROOT_DIR" = "/" ]; then
    verify_active
  else
    log "alternate root verification only; active host checks skipped"
  fi
}

remove_action() {
  validate_config
  if runtime_root; then
    remove_runtime
  else
    log "runtime removal skipped"
  fi
  remove_persistent_files
}

ACTION="${1:-install}"
if [ "$#" -gt 0 ] && [[ "$1" != --* ]]; then
  shift
else
  ACTION="install"
fi

while [ "$#" -gt 0 ]; do
  case "$1" in
    --root)
      ROOT_DIR="$2"
      shift 2
      ;;
    --no-apply|--render-only)
      APPLY_RUNTIME=0
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      APPLY_RUNTIME=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

if [ "$ROOT_DIR" != "/" ]; then
  APPLY_RUNTIME=0
fi

case "$ACTION" in
  install) install_action ;;
  verify) verify_action ;;
  remove|uninstall) remove_action ;;
  zram-start) validate_config; zram_start ;;
  zram-stop) validate_config; zram_stop ;;
  swapfile-start) validate_config; swapfile_start ;;
  swapfile-stop) validate_config; swapfile_stop ;;
  -h|--help|help) usage ;;
  *) die "unknown action: ${ACTION}" ;;
esac
