#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[credstore-install] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

CREDSTORE_DIR="${TRADING_CREDSTORE_DIR:-/etc/credstore.encrypted}"
LEGACY_SECRET_DIR="${TRADING_LEGACY_SECRET_DIR:-/etc/trading/secrets}"
SECRET_NAMES="${TRADING_CREDSTORE_SECRET_NAMES:-master_key pg_password_app pg_password_ingest pg_password_reader redis_password object_store_secret_key dashboard_api_token operator_api_token backup_evidence_hmac_key}"

log() {
  printf '[credstore-install] %s\n' "$*"
}

die() {
  printf '[credstore-install] ERROR: %s\n' "$*" >&2
  exit 1
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    die "install.sh must run as root"
  fi
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

env_name_for_secret() {
  printf 'TS_SECRET_%s' "$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]' | tr '.-' '__')"
}

read_secret_value() {
  local name="$1" env_name value confirm
  env_name="$(env_name_for_secret "$name")"
  value="${!env_name:-}"
  if [ -z "$value" ] && [ "$name" = "master_key" ] && [ "${TRADING_GENERATE_MASTER_KEY:-1}" = "1" ]; then
    value="$(openssl rand -base64 32)"
  fi
  if [ -z "$value" ] && [ "$name" = "backup_evidence_hmac_key" ] && [ "${TRADING_GENERATE_BACKUP_EVIDENCE_HMAC_KEY:-1}" = "1" ]; then
    value="$(openssl rand -hex 32)"
  fi
  if [ -z "$value" ]; then
    read -r -s -p "Enter value for ${name}: " value
    printf '\n'
    read -r -s -p "Confirm value for ${name}: " confirm
    printf '\n'
    if [ "$value" != "$confirm" ]; then
      die "values did not match for ${name}"
    fi
  fi
  if [ -z "$value" ]; then
    die "empty secret is not allowed: ${name}"
  fi
  printf '%s' "$value"
}

install_secret() {
  local name="$1" target value
  target="${CREDSTORE_DIR}/${name}.cred"
  if [ -f "$target" ] && [ "${TRADING_CREDSTORE_FORCE:-0}" != "1" ]; then
    chown root:root "$target"
    chmod 0400 "$target"
    log "kept existing ${target}"
    return
  fi
  value="$(read_secret_value "$name")"
  # shellcheck disable=SC2046
  printf '%s' "$value" | systemd-creds encrypt --name="$name" $(encrypt_args) - "$target"
  unset value
  chown root:root "$target"
  chmod 0400 "$target"
  log "installed ${target}"
}

remove_legacy_plaintext() {
  if [ ! -d "$LEGACY_SECRET_DIR" ]; then
    return
  fi
  find "$LEGACY_SECRET_DIR" -type f -name '*password' -delete
  find "$LEGACY_SECRET_DIR" -type f -name '*.env' -delete
  rmdir "$LEGACY_SECRET_DIR" 2>/dev/null || true
  log "removed deprecated plaintext secret files from ${LEGACY_SECRET_DIR}"
}

main() {
  require_root
  command -v systemd-creds >/dev/null 2>&1 || die "systemd-creds is required"
  command -v openssl >/dev/null 2>&1 || die "openssl is required"
  install -d -o root -g root -m 0700 "$CREDSTORE_DIR"
  local name
  for name in $SECRET_NAMES; do
    install_secret "$name"
  done
  remove_legacy_plaintext
  log "credential store installation complete"
}

main "$@"
