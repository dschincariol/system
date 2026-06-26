#!/usr/bin/env bash
set -euo pipefail

if [ "${EUID}" -ne 0 ]; then
  echo "apply_host_hardening.sh must be run as root; use: sudo bash deploy/bin/apply_host_hardening.sh" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

STEP_NAMES=()
STEP_RESULTS=()
STEP_DETAILS=()

record_step() {
  local name="$1" result="$2" detail="${3:-}"
  STEP_NAMES+=("$name")
  STEP_RESULTS+=("$result")
  STEP_DETAILS+=("$detail")
}

print_step_result() {
  local name="$1" result="$2" detail="${3:-}"
  if [ -n "$detail" ]; then
    printf '[host-hardening] %s %s %s\n' "$name" "$result" "$detail"
  else
    printf '[host-hardening] %s %s\n' "$name" "$result"
  fi
}

run_step() {
  local name="$1"
  shift
  local rc
  printf '[host-hardening] %s START\n' "$name"
  set +e
  "$@"
  rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    record_step "$name" "PASS"
    print_step_result "$name" "PASS"
  else
    record_step "$name" "FAIL" "exit=${rc}"
    print_step_result "$name" "FAIL" "exit=${rc}"
  fi
}

apply_memory_hardening() {
  export TRADING_ZRAM_SIZE_GIB="${TRADING_ZRAM_SIZE_GIB:-32}"
  export TRADING_SWAPFILE_SIZE_GIB="${TRADING_SWAPFILE_SIZE_GIB:-16}"
  export TRADING_ZFS_ARC_MAX_GIB="${TRADING_ZFS_ARC_MAX_GIB:-48}"

  bash "${REPO_DIR}/ops/server/memory_pressure_hardening.sh" install || return $?
  bash "${REPO_DIR}/ops/server/memory_pressure_hardening.sh" verify || return $?
}

copy_unit_if_changed() {
  local unit="$1"
  local source="${REPO_DIR}/deploy/systemd/${unit}"
  local target="/etc/systemd/system/${unit}"

  if [ ! -f "$source" ]; then
    printf '[host-hardening] missing unit source unit=%s\n' "$unit" >&2
    return 1
  fi

  if [ -f "$target" ] && cmp -s "$source" "$target"; then
    printf '[host-hardening] systemd unit unchanged unit=%s\n' "$unit"
    return 0
  fi

  install -D -m 0644 "$source" "$target"
  printf '[host-hardening] systemd unit installed unit=%s\n' "$unit"
}

install_systemd_units() {
  command -v systemctl >/dev/null 2>&1 || {
    echo "[host-hardening] systemctl not found" >&2
    return 1
  }

  local unit
  for unit in \
    trading-engine.service \
    trading-operator.service \
    trading-backup.service \
    trading-backup.timer \
    trading-restore-drill.service \
    trading-restore-drill.timer
  do
    copy_unit_if_changed "$unit" || return $?
  done

  systemctl daemon-reload || return $?
  systemctl enable --now trading-engine.service trading-operator.service || return $?
  systemctl enable --now trading-backup.timer trading-restore-drill.timer || return $?
}

harden_backup_evidence_key() {
  local key_path="${BACKUP_EVIDENCE_HMAC_KEY_FILE:-/etc/trading/backup_evidence.hmac.key}"
  if [ ! -e "$key_path" ]; then
    printf '[host-hardening] backup evidence HMAC key missing path=%s; provision it before rerunning\n' "$key_path" >&2
    return 1
  fi
  if [ ! -f "$key_path" ]; then
    printf '[host-hardening] backup evidence HMAC key is not a regular file path=%s\n' "$key_path" >&2
    return 1
  fi

  chown root:root "$key_path" || return $?
  chmod 0600 "$key_path" || return $?

  local mode owner
  mode="$(stat -c '%a' "$key_path")" || return $?
  owner="$(stat -c '%U:%G' "$key_path")" || return $?
  if [ "$mode" != "600" ] || [ "$owner" != "root:root" ]; then
    printf '[host-hardening] backup evidence HMAC key hardening verification failed mode=%s owner=%s\n' "$mode" "$owner" >&2
    return 1
  fi
  printf '[host-hardening] backup evidence HMAC key hardened mode=%s owner=%s\n' "$mode" "$owner"
}

print_summary_and_exit() {
  local failures=0
  local i

  printf '\n[host-hardening] summary\n'
  for i in "${!STEP_NAMES[@]}"; do
    if [ "${STEP_RESULTS[$i]}" != "PASS" ]; then
      failures=$((failures + 1))
    fi
    if [ -n "${STEP_DETAILS[$i]}" ]; then
      printf '[host-hardening] summary %-28s %s %s\n' "${STEP_NAMES[$i]}" "${STEP_RESULTS[$i]}" "${STEP_DETAILS[$i]}"
    else
      printf '[host-hardening] summary %-28s %s\n' "${STEP_NAMES[$i]}" "${STEP_RESULTS[$i]}"
    fi
  done

  if [ "$failures" -gt 0 ]; then
    printf '[host-hardening] result FAIL failed_steps=%s\n' "$failures" >&2
    exit 1
  fi
  printf '[host-hardening] result PASS\n'
}

run_step "memory-hardening" apply_memory_hardening
run_step "systemd-units" install_systemd_units
run_step "backup-evidence-key" harden_backup_evidence_key
print_summary_and_exit
