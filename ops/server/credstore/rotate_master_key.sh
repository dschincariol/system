#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[rotate-master-key] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

CREDSTORE_DIR="${TRADING_CREDSTORE_DIR:-/etc/credstore.encrypted}"
APP_ROOT="${TRADING_APP_ROOT:-/opt/trading/app}"
PYTHON_BIN="${TRADING_PYTHON_BIN:-/opt/trading/venv/bin/python}"
SYSTEMD_SERVICES="${TRADING_MASTER_KEY_SERVICES:-trading-jobs.service trading-ingest.service}"
ARCHIVE_DIR="${TRADING_MASTER_KEY_ARCHIVE_DIR:-${CREDSTORE_DIR}/keys/archive}"
ARCHIVE_RETENTION_HOURS="${TRADING_MASTER_KEY_ARCHIVE_RETENTION_HOURS:-0}"

log() {
  printf '[rotate-master-key] %s\n' "$*"
}

die() {
  printf '[rotate-master-key] ERROR: %s\n' "$*" >&2
  exit 1
}

panic() {
  printf '[rotate-master-key] PANIC: %s\n' "$*" >&2
  exit 3
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    die "rotate_master_key.sh must run as root"
  fi
}

validate_retention() {
  case "$ARCHIVE_RETENTION_HOURS" in
    ''|*[!0-9]*) die "TRADING_MASTER_KEY_ARCHIVE_RETENTION_HOURS must be a non-negative integer" ;;
  esac
}

encrypt_args() {
  if [ -n "${SYSTEMD_CREDS_ENCRYPT_ARGS:-}" ]; then
    # shellcheck disable=SC2086
    printf '%s\n' ${SYSTEMD_CREDS_ENCRYPT_ARGS}
    return
  fi
  if [ -e /sys/class/tpm/tpm0 ]; then
    printf '%s\n' '--tpm2-pcrs=7'
  fi
}

install_next_key() {
  # shellcheck disable=SC2046
  openssl rand -base64 32 | systemd-creds encrypt --name=master_key.next $(encrypt_args) - "${CREDSTORE_DIR}/master_key.next.cred"
  chown root:root "${CREDSTORE_DIR}/master_key.next.cred"
  chmod 0400 "${CREDSTORE_DIR}/master_key.next.cred"
  log "installed ${CREDSTORE_DIR}/master_key.next.cred"
}

phase_1_reencrypt() {
  local rc
  log "phase_1_reencrypt: re-encrypting data-source credentials with master_key.next"
  systemd-run --wait --collect --pipe \
    --property="LoadCredentialEncrypted=master_key:${CREDSTORE_DIR}/master_key.cred" \
    --property="LoadCredentialEncrypted=master_key.next:${CREDSTORE_DIR}/master_key.next.cred" \
    --setenv=TS_SECRETS_PROVIDER=systemd-creds \
    --setenv=PYTHONPATH="${APP_ROOT}" \
    --working-directory="${APP_ROOT}" \
    "$PYTHON_BIN" -c 'from services.secrets.rotation import re_encrypt_data_sources; print(re_encrypt_data_sources(final_key_version="master_key"))'
  rc=$?
  if [ "$rc" -ne 0 ]; then
    return "$rc"
  fi
  log "phase_1_reencrypt: complete"
}

phase_2_verify() {
  local rc
  log "phase_2_verify: verifying rotated rows with master_key.next before credential swap"
  systemd-run --wait --collect --pipe \
    --property="LoadCredentialEncrypted=master_key.next:${CREDSTORE_DIR}/master_key.next.cred" \
    --setenv=TS_SECRETS_PROVIDER=systemd-creds \
    --setenv=PYTHONPATH="${APP_ROOT}" \
    --working-directory="${APP_ROOT}" \
    "$PYTHON_BIN" -c 'from services.secrets.rotation import verify_data_sources_key; print({"verified": verify_data_sources_key(new_key_name="master_key", decrypt_key_name="master_key.next")})'
  rc=$?
  if [ "$rc" -ne 0 ]; then
    return "$rc"
  fi
  log "phase_2_verify: complete"
}

verify_live_master_key() {
  systemd-run --wait --collect --pipe \
    --property="LoadCredentialEncrypted=master_key:${CREDSTORE_DIR}/master_key.cred" \
    --setenv=TS_SECRETS_PROVIDER=systemd-creds \
    --setenv=PYTHONPATH="${APP_ROOT}" \
    --working-directory="${APP_ROOT}" \
    "$PYTHON_BIN" -c 'from services.secrets.rotation import verify_data_sources_key; print({"verified": verify_data_sources_key(new_key_name="master_key")})'
}

archive_enabled() {
  [ "$ARCHIVE_RETENTION_HOURS" -gt 0 ]
}

stash_old_master_key() {
  local timestamp stash
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)" || panic "failed to create archive timestamp"
  stash="${ARCHIVE_DIR}/master_key.${timestamp}.cred.pending"
  install -d -o root -g root -m 0700 "$ARCHIVE_DIR" || panic "failed to create ${ARCHIVE_DIR}"
  cp -p "${CREDSTORE_DIR}/master_key.cred" "$stash" || panic "failed to stash old master_key.cred"
  chown root:root "$stash" || panic "failed to set owner on ${stash}"
  chmod 0400 "$stash" || panic "failed to set mode on ${stash}"
  printf '%s\n' "$stash"
}

finalize_old_master_key() {
  local stash="$1" final
  if archive_enabled; then
    final="${stash%.pending}"
    mv -f "$stash" "$final" || panic "failed to archive old master key at ${final}"
    chown root:root "$final" || panic "failed to set owner on ${final}"
    chmod 0400 "$final" || panic "failed to set mode on ${final}"
    log "phase_3_swap_and_cleanup: archived old master key at ${final}"
    return
  fi
  rm -f "$stash" || panic "failed to purge stashed old master key"
  log "phase_3_swap_and_cleanup: purged old master key credential"
}

prune_archive() {
  local minutes
  archive_enabled || return 0
  [ -d "$ARCHIVE_DIR" ] || return 0
  minutes="$((ARCHIVE_RETENTION_HOURS * 60))"
  find "$ARCHIVE_DIR" -type f -name 'master_key.*.cred' -mmin +"$minutes" -delete
}

phase_3_swap_and_cleanup() {
  local stash service
  log "phase_3_swap_and_cleanup: swapping master_key.next into master_key"
  [ -f "${CREDSTORE_DIR}/master_key.next.cred" ] || panic "missing ${CREDSTORE_DIR}/master_key.next.cred before swap"
  [ -f "${CREDSTORE_DIR}/master_key.cred" ] || panic "missing ${CREDSTORE_DIR}/master_key.cred before swap"

  stash="$(stash_old_master_key)" || panic "failed to preserve old master key before swap"
  mv -f "${CREDSTORE_DIR}/master_key.next.cred" "${CREDSTORE_DIR}/master_key.cred" || panic "failed to swap master_key.next into master_key"
  chown root:root "${CREDSTORE_DIR}/master_key.cred" || panic "failed to set owner on master_key.cred"
  chmod 0400 "${CREDSTORE_DIR}/master_key.cred" || panic "failed to set mode on master_key.cred"
  log "phase_3_swap_and_cleanup: swapped master_key.next into master_key"

  verify_live_master_key || panic "new master_key.cred failed live decrypt verification; old key stash remains at ${stash}"
  log "phase_3_swap_and_cleanup: verified live master_key.cred"

  finalize_old_master_key "$stash"
  prune_archive || panic "failed to prune archived master keys"

  for service in $SYSTEMD_SERVICES; do
    systemctl restart "$service" || panic "failed to restart ${service} after master key swap"
  done
  log "phase_3_swap_and_cleanup: services restarted"
}

main() {
  require_root
  validate_retention
  command -v systemd-creds >/dev/null 2>&1 || die "systemd-creds is required"
  command -v systemd-run >/dev/null 2>&1 || die "systemd-run is required"
  command -v openssl >/dev/null 2>&1 || die "openssl is required"
  install -d -o root -g root -m 0700 "$CREDSTORE_DIR"
  [ -f "${CREDSTORE_DIR}/master_key.cred" ] || die "missing ${CREDSTORE_DIR}/master_key.cred"

  install_next_key
  if ! phase_1_reencrypt; then
    log "phase_1_reencrypt: failed; leaving master_key.cred and master_key.next.cred intact"
    exit 1
  fi
  if ! phase_2_verify; then
    log "phase_2_verify: failed; leaving master_key.cred and master_key.next.cred intact"
    exit 2
  fi
  phase_3_swap_and_cleanup
  log "master key rotation complete"
}

main "$@"
