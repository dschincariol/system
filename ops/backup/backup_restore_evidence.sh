#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=backup_restore_evidence %s\n' "$level" "$event" "$*"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

stamp="${TS_BACKUP_EVIDENCE_STAMP:-$(date -u +%Y-%m-%dT%H%M%SZ)}"
generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
evidence_dir="${TS_BACKUP_EVIDENCE_DIR:-/var/backups/trading/evidence}"
backup_root="${TS_BACKUP_ROOT:-$(dirname "$evidence_dir")}"
work_dir="${TS_BACKUP_EVIDENCE_WORK_DIR:-${evidence_dir}/work/${stamp}}"
report_txt="${evidence_dir}/backup_restore_evidence_${stamp}.txt"
report_json="${evidence_dir}/backup_restore_evidence_${stamp}.json"
latest_txt="${evidence_dir}/latest_backup_restore_evidence.txt"
latest_json="${BACKUP_EVIDENCE_PATH:-${evidence_dir}/latest_backup_restore_evidence.json}"

base_backup_script="${TS_BASE_BACKUP_SCRIPT:-${script_dir}/base_backup.sh}"
wal_archive_script="${TS_WAL_ARCHIVE_SCRIPT:-${script_dir}/wal_archive.sh}"
wal_catchup_script="${TS_WAL_ARCHIVE_CATCHUP_SCRIPT:-${script_dir}/wal_archive_catchup.sh}"
restore_script="${TS_RESTORE_SCRIPT:-${script_dir}/restore.sh}"
restore_drill_script="${TS_RESTORE_DRILL_SCRIPT:-${script_dir}/restore_drill.sh}"
evidence_script="${TS_BACKUP_EVIDENCE_SCRIPT:-${script_dir}/backup_restore_evidence.sh}"
base_dir="${TS_BACKUP_BASE_DIR:-/var/backups/trading/base}"
wal_dir="${TS_BACKUP_WAL_DIR:-/var/backups/trading/wal}"
drill_dir="${TS_RESTORE_DRILL_DIR:-/var/backups/trading/drills}"
systemd_unit_dir="${TS_BACKUP_SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
wal_wait_s="${TS_BACKUP_WAL_VERIFY_TIMEOUT_S:-120}"
wal_rpo_s="${BACKUP_EVIDENCE_WAL_RPO_S:-${BACKUP_EVIDENCE_RPO_S:-${BACKUP_RPO_S:-120}}}"
base_reuse_max_s="${TS_BACKUP_EVIDENCE_REUSE_BASE_MAX_AGE_S:-${BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S:-93600}}"
restore_reuse_max_s="${TS_BACKUP_EVIDENCE_REUSE_RESTORE_MAX_AGE_S:-${BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S:-1209600}}"
lock_timeout_s="${TS_BACKUP_EVIDENCE_LOCK_TIMEOUT_S:-30}"
probe_timeout_s="${TS_BACKUP_EVIDENCE_PROBE_TIMEOUT_S:-5}"
systemctl_timeout_s="${TS_BACKUP_EVIDENCE_SYSTEMCTL_TIMEOUT_S:-5}"
base_backup_timeout_s="${TS_BACKUP_EVIDENCE_BASE_BACKUP_TIMEOUT_S:-${TS_BASE_BACKUP_TIMEOUT_S:-7200}}"
wal_switch_timeout_s="${TS_BACKUP_EVIDENCE_WAL_SWITCH_TIMEOUT_S:-30}"
wal_archiver_stats_timeout_s="${TS_BACKUP_EVIDENCE_WAL_ARCHIVER_STATS_TIMEOUT_S:-30}"
wal_catchup_timeout_s="${TS_BACKUP_EVIDENCE_WAL_CATCHUP_TIMEOUT_S:-300}"
restore_drill_timeout_s="${TS_BACKUP_EVIDENCE_RESTORE_DRILL_TIMEOUT_S:-${TS_RESTORE_DRILL_TIMEOUT_S:-3600}}"
signature_timeout_s="${TS_BACKUP_EVIDENCE_SIGNATURE_TIMEOUT_S:-30}"
publish_timeout_s="${TS_BACKUP_EVIDENCE_PUBLISH_TIMEOUT_S:-30}"

script_checks_status="fail"
compose_checks_status="not_applicable"
systemd_checks_status="fail"
base_backup_status="fail"
wal_archive_status="fail"
wal_catchup_status="not_applicable"
wal_archiver_status="fail"
restore_drill_status="fail"
publish_status="not_started"
base_backup_dir=""
base_verify_log=""
base_verified_at=""
wal_verified_at=""
wal_observed_file=""
wal_catchup_verified_at=""
wal_archiver_verified_at=""
wal_archiver_archive_mode=""
wal_archiver_archive_command=""
wal_archiver_archived_count=""
wal_archiver_last_archived_wal=""
wal_archiver_last_archived_at=""
wal_archiver_last_archived_at_ts=""
wal_archiver_failed_count=""
wal_archiver_last_failed_wal=""
wal_archiver_last_failed_at=""
wal_archiver_last_failed_at_ts=""
wal_archiver_stats_reset_at=""
wal_target_status="fail"
wal_target_verified_at=""
wal_target_root=""
wal_target_dir=""
wal_target_tmp_dir=""
wal_target_expected_owner_uid=""
wal_target_expected_group=""
wal_target_expected_group_gid=""
wal_target_expected_dir_mode="${TS_BACKUP_WAL_TARGET_DIR_MODE:-2750}"
wal_target_repaired="false"
wal_target_issue_count="0"
wal_target_diagnosis_source=""
wal_target_diagnosis_probe_status=""
wal_target_diagnosis_probe_wal_name=""
wal_target_diagnosis_probe_output=""
wal_target_diagnosis_signature=""
wal_target_diagnosis_exit_code=""
wal_target_diagnosis_fix=""
restore_drill_report=""
restore_drill_verified_at=""
restore_time_to_recover_s=""
signature_status="not_required"
evidence_read_group="${TS_BACKUP_EVIDENCE_READ_GROUP:-${TS_BACKUP_READ_GROUP:-}}"
evidence_read_users="${TS_BACKUP_EVIDENCE_READ_USERS:-${TS_BACKUP_EVIDENCE_OPERATOR_USER:-}}"

mkdir -p "$evidence_dir" "$work_dir"

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

timeout_exit_code() {
  case "$1" in
    124|137) return 0 ;;
    *) return 1 ;;
  esac
}

run_with_timeout() {
  local max_s="$1"
  local output_file="$2"
  local label="$3"
  shift 3
  if ! command -v timeout >/dev/null 2>&1; then
    printf '%s_timeout_command_missing=1\n' "$label" >> "$output_file"
    return 127
  fi
  set +e
  timeout "$max_s" "$@" >> "$output_file" 2>&1
  local rc=$?
  set -e
  if timeout_exit_code "$rc"; then
    printf '%s_timeout_s=%s\n' "$label" "$max_s" >> "$output_file"
  elif [ "$rc" -ne 0 ]; then
    printf '%s_exit_code=%s\n' "$label" "$rc" >> "$output_file"
  fi
  return "$rc"
}

run_capture_with_timeout() {
  local max_s="$1"
  local stdout_file="$2"
  local stderr_file="$3"
  local label="$4"
  shift 4
  if ! command -v timeout >/dev/null 2>&1; then
    printf '%s_timeout_command_missing=1\n' "$label" >> "$stderr_file"
    return 127
  fi
  set +e
  timeout "$max_s" "$@" > "$stdout_file" 2>> "$stderr_file"
  local rc=$?
  set -e
  if timeout_exit_code "$rc"; then
    printf '%s_timeout_s=%s\n' "$label" "$max_s" >> "$stderr_file"
  elif [ "$rc" -ne 0 ]; then
    printf '%s_exit_code=%s\n' "$label" "$rc" >> "$stderr_file"
  fi
  return "$rc"
}

backup_evidence_signature_required() {
  is_truthy "${BACKUP_EVIDENCE_REQUIRE_SIGNATURE:-0}" \
    || is_truthy "${BACKUP_EVIDENCE_SIGNATURE_REQUIRED:-0}" \
    || is_truthy "${PREFLIGHT_REQUIRE_BACKUP_EVIDENCE:-0}" \
    || [ "${ENGINE_MODE:-}" = "live" ] \
    || [ "${EXECUTION_MODE:-}" = "live" ]
}

backup_evidence_signing_key_available() {
  [ "$(backup_evidence_signing_key_state)" = "available" ]
}

backup_evidence_signing_key_state() {
  if [ -n "${BACKUP_EVIDENCE_HMAC_KEY:-}" ] || [ -n "${BACKUP_EVIDENCE_SIGNING_KEY:-}" ]; then
    printf 'available\n'
    return 0
  fi
  local name path secret_name cred_dir provider
  for name in BACKUP_EVIDENCE_HMAC_KEY_FILE BACKUP_EVIDENCE_SIGNING_KEY_FILE; do
    path="${!name:-}"
    [ -n "$path" ] || continue
    if [ ! -e "$path" ]; then
      printf 'missing_file\n'
      return 0
    fi
    if [ ! -f "$path" ]; then
      printf 'unreadable\n'
      return 0
    fi
    if [ ! -r "$path" ]; then
      printf 'unreadable\n'
      return 0
    fi
    if [ ! -s "$path" ]; then
      printf 'empty\n'
      return 0
    fi
    printf 'available\n'
    return 0
  done
  cred_dir="${CREDENTIALS_DIRECTORY:-}"
  for name in BACKUP_EVIDENCE_HMAC_KEY_SECRET BACKUP_EVIDENCE_SIGNING_KEY_SECRET; do
    secret_name="${!name:-}"
    [ -n "$secret_name" ] || continue
    if [ -z "$cred_dir" ]; then
      printf 'missing_file\n'
      return 0
    fi
    path="${cred_dir}/${secret_name}"
    if [ ! -e "$path" ]; then
      printf 'missing_file\n'
      return 0
    fi
    if [ ! -f "$path" ] || [ ! -r "$path" ]; then
      printf 'unreadable\n'
      return 0
    fi
    if [ ! -s "$path" ]; then
      printf 'empty\n'
      return 0
    fi
    printf 'available\n'
    return 0
  done
  provider="${TS_SECRETS_PROVIDER:-}"
  if [ -n "$cred_dir" ] || [ -n "$provider" ]; then
    for secret_name in backup_evidence_hmac_key BACKUP_EVIDENCE_HMAC_KEY backup_evidence_signing_key; do
      [ -n "$cred_dir" ] || break
      path="${cred_dir}/${secret_name}"
      [ -e "$path" ] || continue
      if [ ! -f "$path" ] || [ ! -r "$path" ]; then
        printf 'unreadable\n'
        return 0
      fi
      if [ ! -s "$path" ]; then
        printf 'empty\n'
        return 0
      fi
      printf 'available\n'
      return 0
    done
  fi
  printf 'missing\n'
}

file_mtime_iso() {
  date -u -d "@$(stat -c %Y "$1")" +%Y-%m-%dT%H:%M:%SZ
}

report_value() {
  local report="$1"
  local key="$2"
  awk -F= -v key="$key" '$1==key {print $2; exit}' "$report"
}

int_seconds() {
  local value="${1:-0}"
  value="${value%%.*}"
  case "$value" in
    ''|*[!0-9]*) printf '0\n' ;;
    *) printf '%s\n' "$value" ;;
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

wal_archive_root_for_dir() {
  if [ -n "${TS_BACKUP_ROOT:-}" ]; then
    printf '%s\n' "$TS_BACKUP_ROOT"
  elif [[ "$wal_dir" == /var/backups/trading || "$wal_dir" == /var/backups/trading/* ]]; then
    printf '/var/backups/trading\n'
  else
    parent_dir "$wal_dir"
  fi
}

group_gid_for() {
  local group="$1"
  case "$group" in
    ''|*[!0-9]*)
      getent group "$group" | awk -F: 'NR == 1 {print $3}'
      ;;
    *)
      printf '%s\n' "$group"
      ;;
  esac
}

mode_matches() {
  local actual="$1"
  local expected="$2"
  [ "$actual" = "$expected" ] || [ "$actual" = "${expected#0}" ]
}

mode_with_other_execute() {
  local mode="$1"
  local prefix last_digit
  mode="${mode#0}"
  prefix="${mode%?}"
  last_digit="${mode#"${prefix}"}"
  case "$last_digit" in
    0) last_digit=1 ;;
    2) last_digit=3 ;;
    4) last_digit=5 ;;
    6) last_digit=7 ;;
  esac
  printf '%s%s\n' "$prefix" "$last_digit"
}

mode_matches_wal_target_path() {
  local path="$1"
  local actual="$2"
  local expected="$3"
  mode_matches "$actual" "$expected" && return 0
  if [ "$path" = "$backup_root" ] && [ -n "$evidence_read_users" ]; then
    mode_matches "$actual" "$(mode_with_other_execute "$expected")"
    return $?
  fi
  return 1
}

append_wal_target_state() {
  local phase="$1"
  local path="$2"
  if [ ! -e "$path" ]; then
    printf '%s_path_missing=%s\n' "$phase" "$path" >> "${work_dir}/wal_archive_target.out"
    return 1
  fi
  printf '%s_path=%s uid=%s gid=%s mode=%s\n' \
    "$phase" \
    "$path" \
    "$(stat -c %u "$path")" \
    "$(stat -c %g "$path")" \
    "$(stat -c %a "$path")" \
    >> "${work_dir}/wal_archive_target.out"
}

archive_command_event_from_output() {
  awk '
    {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^event=/) {
          sub(/^event=/, "", $i)
          print $i
          exit
        }
      }
    }
  ' "$@" 2>/dev/null || true
}

one_line_tail() {
  tail -n 8 "$@" 2>/dev/null \
    | tr '\n' ' ' \
    | awk '{$1=$1; print substr($0, 1, 700)}' 2>/dev/null || true
}

run_wal_archive_target_diagnosis_probe() {
  local uid="$1"
  local gid="$2"
  local current_uid probe_src probe_stdout probe_stderr probe_rc event probe_cmd_rc
  local probe_name

  current_uid="$(id -u)"
  probe_name="0000000100000000000000FD.diagnosis.${stamp//[^A-Za-z0-9._-]/_}"
  probe_src="${work_dir}/${probe_name}.src"
  probe_stdout="${work_dir}/wal_archive_target_probe.out"
  probe_stderr="${work_dir}/wal_archive_target_probe.err"
  wal_target_diagnosis_source="wal_archive_probe"
  wal_target_diagnosis_probe_wal_name="$probe_name"
  printf 'wal archive target diagnosis probe\n' > "$probe_src"
  if [ "$(id -u)" -eq 0 ]; then
    chown "${uid}:${gid}" "$probe_src" 2>/dev/null || true
  else
    chgrp "$gid" "$probe_src" 2>/dev/null || true
  fi
  chmod 0640 "$probe_src" 2>/dev/null || true

  : > "$probe_stdout"
  : > "$probe_stderr"
  printf 'diagnosis_probe_wal_name=%s\n' "$probe_name" >> "${work_dir}/wal_archive_target.out"
  printf 'diagnosis_probe_uid=%s\n' "$uid" >> "${work_dir}/wal_archive_target.out"
  printf 'diagnosis_probe_gid=%s\n' "$gid" >> "${work_dir}/wal_archive_target.out"

  set +e
  if [ "$current_uid" -eq 0 ]; then
    if command -v setpriv >/dev/null 2>&1; then
      setpriv --reuid "$uid" --regid "$gid" --clear-groups \
        env \
          TS_BACKUP_ROOT="$wal_target_root" \
          TS_BACKUP_WAL_DIR="$wal_target_dir" \
          TS_WAL_OFFSITE_CMD= \
          bash "$wal_archive_script" "$probe_src" "$probe_name" \
        > "$probe_stdout" 2> "$probe_stderr"
      probe_cmd_rc=$?
    else
      printf 'setpriv_missing=1\n' >> "$probe_stderr"
      probe_cmd_rc=126
    fi
  elif [ "$current_uid" = "$uid" ]; then
    env \
      TS_BACKUP_ROOT="$wal_target_root" \
      TS_BACKUP_WAL_DIR="$wal_target_dir" \
      TS_WAL_OFFSITE_CMD= \
      bash "$wal_archive_script" "$probe_src" "$probe_name" \
      > "$probe_stdout" 2> "$probe_stderr"
    probe_cmd_rc=$?
  else
    printf 'current_uid_mismatch current_uid=%s expected_uid=%s\n' "$current_uid" "$uid" >> "$probe_stderr"
    probe_cmd_rc=126
  fi
  set -e

  probe_rc="$probe_cmd_rc"
  wal_target_diagnosis_exit_code="$probe_rc"
  event="$(archive_command_event_from_output "$probe_stdout" "$probe_stderr")"
  if [ -z "$event" ]; then
    case "$probe_rc" in
      0) event="" ;;
      126) event="archive_command_probe_unavailable" ;;
      *) event="archive_command_probe_failed" ;;
    esac
  fi
  wal_target_diagnosis_signature="$event"
  wal_target_diagnosis_probe_output="$(one_line_tail "$probe_stdout" "$probe_stderr")"

  if [ "$probe_rc" -eq 0 ]; then
    wal_target_diagnosis_probe_status="unexpected_success"
    rm -f "${wal_target_dir}/${probe_name}" "${wal_target_tmp_dir}/${probe_name}"* 2>/dev/null || true
  elif [ "$probe_rc" -eq 126 ] && [ "$event" = "archive_command_probe_unavailable" ]; then
    wal_target_diagnosis_probe_status="unavailable"
  else
    wal_target_diagnosis_probe_status="observed_failure"
  fi

  printf 'diagnosis_probe_status=%s\n' "$wal_target_diagnosis_probe_status" >> "${work_dir}/wal_archive_target.out"
  printf 'diagnosis_probe_exit_code=%s\n' "$wal_target_diagnosis_exit_code" >> "${work_dir}/wal_archive_target.out"
  printf 'diagnosis_probe_failure_signature=%s\n' "$wal_target_diagnosis_signature" >> "${work_dir}/wal_archive_target.out"
  printf 'diagnosis_probe_output=%s\n' "$wal_target_diagnosis_probe_output" >> "${work_dir}/wal_archive_target.out"

  [ "$wal_target_diagnosis_probe_status" != "unavailable" ]
}

repair_wal_archive_target() {
  local rc=0
  local uid group gid mode current_uid path actual_uid actual_gid actual_mode
  local paths=()

  : > "${work_dir}/wal_archive_target.out"
  wal_target_root="$(wal_archive_root_for_dir)"
  wal_target_dir="$wal_dir"
  wal_target_tmp_dir="${wal_dir}/.tmp"
  uid="${TS_BACKUP_WAL_TARGET_OWNER_UID:-}"
  group="${TS_BACKUP_WAL_TARGET_GROUP:-${evidence_read_group:-}}"
  current_uid="$(id -u)"

  if [ -z "$uid" ] && [ "$current_uid" -ne 0 ]; then
    uid="$current_uid"
  fi
  if [ -z "$group" ] && [ "$current_uid" -ne 0 ]; then
    group="$(id -gn)"
  fi
  mode="$wal_target_expected_dir_mode"

  printf 'wal_archive_target_root=%s\n' "$wal_target_root" >> "${work_dir}/wal_archive_target.out"
  printf 'wal_archive_target_dir=%s\n' "$wal_target_dir" >> "${work_dir}/wal_archive_target.out"
  printf 'wal_archive_target_tmp_dir=%s\n' "$wal_target_tmp_dir" >> "${work_dir}/wal_archive_target.out"
  printf 'expected_owner_uid=%s\n' "$uid" >> "${work_dir}/wal_archive_target.out"
  printf 'expected_group=%s\n' "$group" >> "${work_dir}/wal_archive_target.out"
  printf 'expected_dir_mode=%s\n' "$mode" >> "${work_dir}/wal_archive_target.out"

  case "$uid" in
    ''|*[!0-9]*)
      printf 'wal_archive_target_owner_uid_invalid=%s\n' "$uid" >> "${work_dir}/wal_archive_target.out"
      wal_target_status="fail"
      return 1
      ;;
  esac
  case "$mode" in
    ''|*[!0-7]*)
      printf 'wal_archive_target_dir_mode_invalid=%s\n' "$mode" >> "${work_dir}/wal_archive_target.out"
      wal_target_status="fail"
      return 1
      ;;
  esac
  gid="$(group_gid_for "$group")"
  case "$gid" in
    ''|*[!0-9]*)
      printf 'wal_archive_target_group_invalid=%s\n' "$group" >> "${work_dir}/wal_archive_target.out"
      wal_target_status="fail"
      return 1
      ;;
  esac

  wal_target_expected_owner_uid="$uid"
  wal_target_expected_group="$group"
  wal_target_expected_group_gid="$gid"
  wal_target_expected_dir_mode="$mode"

  if [ ! -d "$wal_target_root" ]; then
    printf 'wal_archive_target_root_missing=%s\n' "$wal_target_root" >> "${work_dir}/wal_archive_target.out"
    wal_target_status="fail"
    return 1
  fi
  if [ ! -d "$wal_target_dir" ]; then
    printf 'wal_archive_target_dir_missing=%s\n' "$wal_target_dir" >> "${work_dir}/wal_archive_target.out"
    wal_target_status="fail"
    return 1
  fi
  if ! mkdir -p "$wal_target_tmp_dir"; then
    printf 'wal_archive_target_tmp_prepare_failed=%s\n' "$wal_target_tmp_dir" >> "${work_dir}/wal_archive_target.out"
    wal_target_status="fail"
    return 1
  fi

  paths=("$wal_target_root" "$wal_target_dir" "$wal_target_tmp_dir")
  for path in "${paths[@]}"; do
    append_wal_target_state before "$path" || rc=1
    [ -d "$path" ] || rc=1
    actual_uid="$(stat -c %u "$path")"
    actual_gid="$(stat -c %g "$path")"
    actual_mode="$(stat -c %a "$path")"
    if [ "$actual_uid" != "$uid" ] || [ "$actual_gid" != "$gid" ] || ! mode_matches_wal_target_path "$path" "$actual_mode" "$mode"; then
      wal_target_issue_count="$((wal_target_issue_count + 1))"
      printf 'wal_archive_target_mismatch=%s actual_uid=%s actual_gid=%s actual_mode=%s expected_uid=%s expected_gid=%s expected_mode=%s\n' \
        "$path" "$actual_uid" "$actual_gid" "$actual_mode" "$uid" "$gid" "$mode" >> "${work_dir}/wal_archive_target.out"
    fi
  done

  if [ "$wal_target_issue_count" -gt 0 ]; then
    wal_target_repaired="true"
    run_wal_archive_target_diagnosis_probe "$uid" "$gid" || rc=1
    wal_target_diagnosis_fix="chown ${uid}:${group} ${wal_target_root} ${wal_target_dir} ${wal_target_tmp_dir}; chmod ${mode} ${wal_target_root} ${wal_target_dir} ${wal_target_tmp_dir}"
    printf 'diagnosis_source=%s\n' "$wal_target_diagnosis_source" >> "${work_dir}/wal_archive_target.out"
    printf 'diagnosis_probe_status=%s\n' "$wal_target_diagnosis_probe_status" >> "${work_dir}/wal_archive_target.out"
    printf 'diagnosis_original_archive_command_failure_signature=%s\n' "$wal_target_diagnosis_signature" >> "${work_dir}/wal_archive_target.out"
    printf 'diagnosis_original_archive_command_exit_code=%s\n' "$wal_target_diagnosis_exit_code" >> "${work_dir}/wal_archive_target.out"
    printf 'diagnosis_failure_signature=%s\n' "$wal_target_diagnosis_signature" >> "${work_dir}/wal_archive_target.out"
    printf 'diagnosis_archive_command_exit_code=%s\n' "$wal_target_diagnosis_exit_code" >> "${work_dir}/wal_archive_target.out"
    printf 'diagnosis_fix=%s\n' "$wal_target_diagnosis_fix" >> "${work_dir}/wal_archive_target.out"
    for path in "${paths[@]}"; do
      if [ "$current_uid" -eq 0 ]; then
        chown "${uid}:${gid}" "$path" || rc=1
      fi
      chmod "$mode" "$path" || rc=1
    done
  fi

  for path in "${paths[@]}"; do
    append_wal_target_state after "$path" || rc=1
    actual_uid="$(stat -c %u "$path")"
    actual_gid="$(stat -c %g "$path")"
    actual_mode="$(stat -c %a "$path")"
    if [ "$actual_uid" != "$uid" ] || [ "$actual_gid" != "$gid" ] || ! mode_matches_wal_target_path "$path" "$actual_mode" "$mode"; then
      rc=1
    fi
  done

  grant_evidence_operator_access
  wal_target_verified_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [ "$rc" -eq 0 ]; then
    wal_target_status="pass"
    printf 'wal_archive_target_status=pass\n' >> "${work_dir}/wal_archive_target.out"
    printf 'wal_archive_target_verified_at=%s\n' "$wal_target_verified_at" >> "${work_dir}/wal_archive_target.out"
    return 0
  fi
  wal_target_status="fail"
  printf 'wal_archive_target_status=fail\n' >> "${work_dir}/wal_archive_target.out"
  printf 'wal_archive_target_verified_at=%s\n' "$wal_target_verified_at" >> "${work_dir}/wal_archive_target.out"
  return 1
}

grant_evidence_file_read_access() {
  local path="$1"
  local user
  [ -n "$evidence_read_users" ] || return 0
  IFS=','
  for user in $evidence_read_users; do
    user="$(printf '%s' "$user" | tr -d '[:space:]')"
    [ -n "$user" ] || continue
    id -u "$user" >/dev/null 2>&1 || continue
    if command -v setfacl >/dev/null 2>&1; then
      setfacl -m "u:${user}:r--" "$path" 2>/dev/null || chmod o+r "$path" 2>/dev/null || true
    else
      chmod o+r "$path" 2>/dev/null || true
    fi
  done
  unset IFS
}

grant_evidence_operator_access() {
  local user acl_ok
  [ -n "$evidence_read_users" ] || return 0
  acl_ok=1
  IFS=','
  for user in $evidence_read_users; do
    user="$(printf '%s' "$user" | tr -d '[:space:]')"
    [ -n "$user" ] || continue
    id -u "$user" >/dev/null 2>&1 || continue
    if command -v setfacl >/dev/null 2>&1; then
      setfacl -m "u:${user}:--x" "$backup_root" 2>/dev/null || acl_ok=0
      setfacl -m "u:${user}:r-x" "$evidence_dir" 2>/dev/null || acl_ok=0
      setfacl -d -m "u:${user}:r-X" "$evidence_dir" 2>/dev/null || acl_ok=0
    else
      acl_ok=0
    fi
  done
  unset IFS
  if [ "$acl_ok" -eq 0 ]; then
    chmod o+x "$backup_root" 2>/dev/null || true
    chmod o+rx "$evidence_dir" 2>/dev/null || true
  fi
}

require_listable_dir() {
  local dir="$1"
  local label="$2"
  local output_file="$3"
  if [ ! -d "$dir" ]; then
    printf '%s_missing=%s\n' "$label" "$dir" >> "$output_file"
    return 1
  fi
  if [ ! -r "$dir" ] || [ ! -x "$dir" ]; then
    printf '%s_not_listable=%s\n' "$label" "$dir" >> "$output_file"
    printf '%s_user=%s group=%s\n' "$label" "$(id -un 2>/dev/null || id -u)" "$(id -gn 2>/dev/null || id -g)" >> "$output_file"
    return 1
  fi
  return 0
}

lock_path="${TS_BACKUP_EVIDENCE_LOCK:-${evidence_dir}/backup_restore_evidence.lock}"
exec 9>"$lock_path"
if is_truthy "${TS_BACKUP_EVIDENCE_WAIT_LOCK:-0}"; then
  if ! flock -w "$lock_timeout_s" 9; then
    log error lock_timeout "lock=${lock_path} timeout_s=${lock_timeout_s}"
    exit 75
  fi
elif ! flock -n 9; then
  log error already_running "lock=${lock_path}"
  exit 75
fi

check_scripts() {
  local missing=0
  local path
  : > "${work_dir}/script_checks.out"
  for path in "$base_backup_script" "$wal_archive_script" "$wal_catchup_script" "$restore_script" "$restore_drill_script" "$evidence_script"; do
    if [ ! -f "$path" ] || [ ! -r "$path" ]; then
      printf 'missing_or_unreadable=%s\n' "$path" >> "${work_dir}/script_checks.out"
      missing=1
    else
      printf 'ok=%s\n' "$path" >> "${work_dir}/script_checks.out"
    fi
  done
  if [ "$missing" -eq 0 ]; then
    script_checks_status="pass"
    return 0
  fi
  return 1
}

compose_mode_configured() {
  [ "${TS_BACKUP_EVIDENCE_DEPLOYMENT_MODE:-}" = "compose" ] \
    || [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ] \
    || [ -n "${TS_RESTORE_DOCKER_IMAGE:-}" ]
}

check_compose() {
  local rc=0
  : > "${work_dir}/compose_checks.out"
  if ! compose_mode_configured; then
    printf 'compose_check=not_applicable\n' >> "${work_dir}/compose_checks.out"
    compose_checks_status="not_applicable"
    return 0
  fi
  compose_checks_status="fail"
  printf 'compose_check=enabled\n' >> "${work_dir}/compose_checks.out"
  if [ -z "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
    printf 'missing=TS_BACKUP_DOCKER_EXEC_CONTAINER\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -z "${TS_BACKUP_DOCKER_EXEC_USER:-}" ]; then
    printf 'missing=TS_BACKUP_DOCKER_EXEC_USER\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -z "${TS_RESTORE_DOCKER_IMAGE:-}" ]; then
    printf 'missing=TS_RESTORE_DOCKER_IMAGE\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -z "${TS_RESTORE_DB:-}" ]; then
    printf 'missing=TS_RESTORE_DB\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -z "${TS_RESTORE_USER:-}" ]; then
    printf 'missing=TS_RESTORE_USER\n' >> "${work_dir}/compose_checks.out"
    rc=1
  fi
  if [ -n "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
    if ! command -v docker >/dev/null 2>&1; then
      printf 'docker=missing\n' >> "${work_dir}/compose_checks.out"
      rc=1
    elif is_truthy "${TS_BACKUP_EVIDENCE_CHECK_COMPOSE_CONTAINER:-1}"; then
      if run_with_timeout 10 "${work_dir}/compose_checks.out" compose_container docker inspect -f '{{.State.Status}}' "$TS_BACKUP_DOCKER_EXEC_CONTAINER"; then
        printf 'container_present=%s\n' "$TS_BACKUP_DOCKER_EXEC_CONTAINER" >> "${work_dir}/compose_checks.out"
      else
        printf 'container_missing_or_unavailable=%s\n' "$TS_BACKUP_DOCKER_EXEC_CONTAINER" >> "${work_dir}/compose_checks.out"
        rc=1
      fi
    fi
  fi
  if [ "$rc" -eq 0 ]; then
    compose_checks_status="pass"
  fi
  return "$rc"
}

check_systemd() {
  local unit rc=0
  local stdout_file stderr_file
  local units=(
    trading-base-backup.service
    trading-base-backup.timer
    trading-backup-evidence.service
    trading-backup-evidence.timer
    trading-backup-prune.service
    trading-backup-prune.timer
    trading-restore-drill.service
    trading-restore-drill.timer
  )
  local timers=(
    trading-base-backup.timer
    trading-backup-evidence.timer
    trading-backup-prune.timer
    trading-restore-drill.timer
  )
  : > "${work_dir}/systemd_checks.out"
  if is_truthy "${TS_BACKUP_EVIDENCE_SKIP_SYSTEMD:-0}"; then
    printf 'systemd_check=skipped\n' >> "${work_dir}/systemd_checks.out"
    systemd_checks_status="pass"
    return 0
  fi
  if ! command -v systemctl >/dev/null 2>&1; then
    printf 'systemctl=missing\n' >> "${work_dir}/systemd_checks.out"
    return 1
  fi
  for unit in "${units[@]}"; do
    stdout_file="${work_dir}/systemd_${unit}.cat.out"
    stderr_file="${work_dir}/systemd_${unit}.cat.err"
    if run_capture_with_timeout "$systemctl_timeout_s" "$stdout_file" "$stderr_file" "systemctl_cat_${unit//[^A-Za-z0-9_]/_}" systemctl cat "$unit" || [ -f "${systemd_unit_dir}/${unit}" ]; then
      printf 'unit_present=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'unit_missing=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
      cat "$stderr_file" >> "${work_dir}/systemd_checks.out"
      rc=1
    fi
  done
  for unit in "${timers[@]}"; do
    stdout_file="${work_dir}/systemd_${unit}.enabled.out"
    stderr_file="${work_dir}/systemd_${unit}.enabled.err"
    if run_capture_with_timeout "$systemctl_timeout_s" "$stdout_file" "$stderr_file" "systemctl_enabled_${unit//[^A-Za-z0-9_]/_}" systemctl is-enabled --quiet "$unit"; then
      printf 'timer_enabled=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'timer_not_enabled=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
      cat "$stderr_file" >> "${work_dir}/systemd_checks.out"
      rc=1
    fi
    stdout_file="${work_dir}/systemd_${unit}.active.out"
    stderr_file="${work_dir}/systemd_${unit}.active.err"
    if run_capture_with_timeout "$systemctl_timeout_s" "$stdout_file" "$stderr_file" "systemctl_active_${unit//[^A-Za-z0-9_]/_}" systemctl is-active --quiet "$unit"; then
      printf 'timer_active=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
    else
      printf 'timer_not_active=%s\n' "$unit" >> "${work_dir}/systemd_checks.out"
      cat "$stderr_file" >> "${work_dir}/systemd_checks.out"
      rc=1
    fi
  done
  if [ "$rc" -eq 0 ]; then
    systemd_checks_status="pass"
  fi
  return "$rc"
}

latest_file() {
  local dir="$1"
  local pattern="$2"
  local label="${3:-latest_file}"
  local stdout_file="${work_dir}/${label}.find.out"
  local stderr_file="${work_dir}/${label}.find.err"
  : > "$stdout_file"
  : > "$stderr_file"
  if ! run_capture_with_timeout "$probe_timeout_s" "$stdout_file" "$stderr_file" "$label" \
    find "$dir" -maxdepth 1 -type f -name "$pattern" -printf '%T@ %p\n'; then
    return 1
  fi
  awk '
        $1 > best {
          best = $1
          $1 = ""
          sub(/^ /, "")
          path = $0
        }
        END {
          if (path != "") {
            print path
          }
        }
      ' "$stdout_file"
}

latest_dir() {
  local dir="$1"
  local label="${2:-latest_dir}"
  local stdout_file="${work_dir}/${label}.find.out"
  local stderr_file="${work_dir}/${label}.find.err"
  : > "$stdout_file"
  : > "$stderr_file"
  if ! run_capture_with_timeout "$probe_timeout_s" "$stdout_file" "$stderr_file" "$label" \
    find "$dir" -mindepth 1 -maxdepth 1 -type d ! -name '.*' ! -name '*.in_progress' -printf '%T@ %p\n'; then
    return 1
  fi
  awk '
        $1 > best {
          best = $1
          $1 = ""
          sub(/^ /, "")
          path = $0
        }
        END {
          if (path != "") {
            print path
          }
        }
      ' "$stdout_file"
}

fresh_enough() {
  local path="$1"
  local max_age_s="$2"
  local mtime age
  [ -e "$path" ] || return 1
  max_age_s="${max_age_s%%.*}"
  [ -n "$max_age_s" ] || return 1
  mtime="$(stat -c %Y "$path")"
  age="$(($(date +%s) - mtime))"
  [ "$age" -le "$max_age_s" ]
}

publish_report_file() {
  local path="$1"
  [ -e "$path" ] || return 0
  grant_evidence_operator_access
  chmod 0640 "$path"
  if [ -n "$evidence_read_group" ]; then
    chgrp "$evidence_read_group" "$path"
  fi
  grant_evidence_file_read_access "$path"
}

reuse_base_backup_if_fresh() {
  local latest verify_log
  require_listable_dir "$base_dir" base_dir "${work_dir}/base_backup.out" || return 1
  latest="$(readlink -f "${base_dir}/latest" 2>/dev/null || true)"
  if [ -z "$latest" ] || [ ! -d "$latest" ]; then
    if ! latest="$(latest_dir "$base_dir" base_latest_dir)"; then
      cat "${work_dir}/base_latest_dir.find.err" >> "${work_dir}/base_backup.out"
      return 1
    fi
  fi
  [ -n "$latest" ] && [ -d "$latest" ] || return 1
  verify_log="${latest}/pg_verifybackup.out"
  [ -f "${latest}/backup_manifest" ] || return 1
  [ -s "$verify_log" ] || return 1
  base_backup_dir="$latest"
  base_verify_log="$verify_log"
  base_verified_at="$(file_mtime_iso "$verify_log")"
  if ! fresh_enough "$verify_log" "$base_reuse_max_s"; then
    printf 'base_backup_stale=%s\n' "$latest" >> "${work_dir}/base_backup.out"
    printf 'base_backup_max_age_s=%s\n' "$base_reuse_max_s" >> "${work_dir}/base_backup.out"
    return 1
  fi
  base_backup_status="pass"
  printf 'base_backup_reused=%s\n' "$latest" > "${work_dir}/base_backup.out"
  return 0
}

run_base_backup_from_gate() {
  is_truthy "${TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP:-0}" || is_truthy "${TS_BACKUP_EVIDENCE_FORCE_BASE_BACKUP:-0}"
}

run_base_backup() {
  : > "${work_dir}/base_backup.out"
  if ! is_truthy "${TS_BACKUP_EVIDENCE_FORCE_BASE_BACKUP:-0}" && reuse_base_backup_if_fresh; then
    return 0
  fi
  if ! run_base_backup_from_gate; then
    if [ -n "$base_verify_log" ]; then
      base_backup_status="stale"
    else
      base_backup_status="missing"
    fi
    printf 'base_backup_gate_action=not_running\n' >> "${work_dir}/base_backup.out"
    printf 'base_backup_required_timer=trading-base-backup.timer\n' >> "${work_dir}/base_backup.out"
    return 1
  fi
  set +e
  run_with_timeout "$base_backup_timeout_s" "${work_dir}/base_backup.out" base_backup bash "$base_backup_script"
  local rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    if timeout_exit_code "$rc"; then
      base_backup_status="timeout"
    fi
    return "$rc"
  fi
  base_backup_dir="$(readlink -f "${base_dir}/latest" 2>/dev/null || true)"
  if [ -z "$base_backup_dir" ] || [ ! -d "$base_backup_dir" ]; then
    return 1
  fi
  base_verify_log="${base_backup_dir}/pg_verifybackup.out"
  if [ ! -s "$base_verify_log" ]; then
    return 1
  fi
  base_verified_at="$(file_mtime_iso "$base_verify_log")"
  base_backup_status="pass"
  return 0
}

wal_catchup_enabled() {
  is_truthy "${TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP:-0}" || is_truthy "${TS_BACKUP_EVIDENCE_FORCE_WAL_CATCHUP:-0}"
}

run_wal_archive_catchup() {
  : > "${work_dir}/wal_catchup.out"
  if ! wal_catchup_enabled; then
    wal_catchup_status="skipped"
    printf 'wal_catchup=skipped\n' >> "${work_dir}/wal_catchup.out"
    return 0
  fi

  set +e
  if compose_mode_configured; then
    if [ -z "${TS_BACKUP_DOCKER_EXEC_CONTAINER:-}" ]; then
      printf 'missing=TS_BACKUP_DOCKER_EXEC_CONTAINER\n' >> "${work_dir}/wal_catchup.out"
      wal_catchup_status="fail"
      set -e
      return 1
    fi
    run_with_timeout "$wal_catchup_timeout_s" "${work_dir}/wal_catchup.out" wal_catchup \
      docker exec \
        -u "${TS_BACKUP_DOCKER_EXEC_USER:-postgres}" \
        -e PGDATA="${TS_BACKUP_DOCKER_PGDATA:-/var/lib/postgresql/data}" \
        -e TS_BACKUP_ROOT=/var/backups/trading \
        -e TS_BACKUP_WAL_DIR=/var/backups/trading/wal \
        -e TS_WAL_ARCHIVE_SCRIPT=/opt/trading/ops/backup/wal_archive.sh \
        -e TS_WAL_ARCHIVE_REQUIRE_MOUNT=1 \
        "$TS_BACKUP_DOCKER_EXEC_CONTAINER" \
        /opt/trading/ops/backup/wal_archive_catchup.sh
  else
    run_with_timeout "$wal_catchup_timeout_s" "${work_dir}/wal_catchup.out" wal_catchup bash "$wal_catchup_script"
  fi
  local rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    if timeout_exit_code "$rc"; then
      wal_catchup_status="timeout"
    else
      wal_catchup_status="fail"
    fi
    return "$rc"
  fi
  wal_catchup_status="pass"
  wal_catchup_verified_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  return 0
}

verify_wal_archive() {
  local start_epoch deadline latest_file latest_epoch
  : > "${work_dir}/wal_archive.out"
  if ! require_listable_dir "$wal_dir" wal_dir "${work_dir}/wal_archive.out"; then
    wal_archive_status="missing"
    return 1
  fi
  if ! command -v psql >/dev/null 2>&1; then
    printf 'psql=missing\n' >> "${work_dir}/wal_archive.out"
    return 1
  fi
  start_epoch="$(date +%s)"
  if ! run_with_timeout "$wal_switch_timeout_s" "${work_dir}/wal_archive.out" wal_switch env PGCONNECT_TIMEOUT="$(int_seconds "$wal_switch_timeout_s")" psql -X -v ON_ERROR_STOP=1 -Atqc "SELECT pg_switch_wal();"; then
    if grep -q '^wal_switch_timeout_s=' "${work_dir}/wal_archive.out"; then
      wal_archive_status="timeout"
    fi
    return 1
  fi
  deadline="$((start_epoch + wal_wait_s))"
  while [ "$(date +%s)" -le "$deadline" ]; do
    if ! latest_file="$(latest_file "$wal_dir" '*' wal_archive_latest_file)"; then
      cat "${work_dir}/wal_archive_latest_file.find.err" >> "${work_dir}/wal_archive.out"
      wal_archive_status="fail"
      return 1
    fi
    if [ -n "$latest_file" ] && [ -s "$latest_file" ]; then
      latest_epoch="$(stat -c %Y "$latest_file")"
      if [ "$latest_epoch" -ge "$start_epoch" ]; then
        wal_observed_file="$latest_file"
        wal_verified_at="$(file_mtime_iso "$latest_file")"
        wal_archive_status="pass"
        printf 'observed_wal=%s\n' "$latest_file" >> "${work_dir}/wal_archive.out"
        return 0
      fi
    fi
    sleep 2
  done
  printf 'wal_archive_timeout_s=%s\n' "$wal_wait_s" >> "${work_dir}/wal_archive.out"
  wal_archive_status="stale"
  return 1
}

verify_wal_archiver_stats() {
  local row query_out last_archived_epoch last_failed_epoch now_epoch last_archived_age_s wal_rpo_int
  : > "${work_dir}/wal_archiver.out"
  query_out="${work_dir}/wal_archiver.query.out"
  : > "$query_out"
  if ! command -v psql >/dev/null 2>&1; then
    printf 'psql=missing\n' >> "${work_dir}/wal_archiver.out"
    return 1
  fi
  if ! run_capture_with_timeout "$wal_archiver_stats_timeout_s" "$query_out" "${work_dir}/wal_archiver.out" wal_archiver_stats \
    env PGCONNECT_TIMEOUT="$(int_seconds "$wal_archiver_stats_timeout_s")" psql -X -v ON_ERROR_STOP=1 -AtF '|' -c "
      SELECT
        current_setting('archive_mode', true),
        current_setting('archive_command', true),
        archived_count::bigint,
        COALESCE(last_archived_wal, ''),
        COALESCE(to_char(last_archived_time AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), ''),
        COALESCE(EXTRACT(EPOCH FROM last_archived_time)::bigint::text, ''),
        failed_count::bigint,
        COALESCE(last_failed_wal, ''),
        COALESCE(to_char(last_failed_time AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), ''),
        COALESCE(EXTRACT(EPOCH FROM last_failed_time)::bigint::text, ''),
        COALESCE(to_char(stats_reset AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), '')
      FROM pg_stat_archiver;
    "; then
    if grep -q '^wal_archiver_stats_timeout_s=' "${work_dir}/wal_archiver.out"; then
      wal_archiver_status="timeout"
    fi
    return 1
  fi
  row="$(head -n 1 "$query_out")"
  IFS='|' read -r \
    wal_archiver_archive_mode \
    wal_archiver_archive_command \
    wal_archiver_archived_count \
    wal_archiver_last_archived_wal \
    wal_archiver_last_archived_at \
    wal_archiver_last_archived_at_ts \
    wal_archiver_failed_count \
    wal_archiver_last_failed_wal \
    wal_archiver_last_failed_at \
    wal_archiver_last_failed_at_ts \
    wal_archiver_stats_reset_at <<< "$row"

  printf 'archive_mode=%s\n' "$wal_archiver_archive_mode" >> "${work_dir}/wal_archiver.out"
  printf 'archive_command=%s\n' "$wal_archiver_archive_command" >> "${work_dir}/wal_archiver.out"
  printf 'archived_count=%s\n' "$wal_archiver_archived_count" >> "${work_dir}/wal_archiver.out"
  printf 'last_archived_wal=%s\n' "$wal_archiver_last_archived_wal" >> "${work_dir}/wal_archiver.out"
  printf 'last_archived_at=%s\n' "$wal_archiver_last_archived_at" >> "${work_dir}/wal_archiver.out"
  printf 'failed_count=%s\n' "$wal_archiver_failed_count" >> "${work_dir}/wal_archiver.out"
  printf 'last_failed_wal=%s\n' "$wal_archiver_last_failed_wal" >> "${work_dir}/wal_archiver.out"
  printf 'last_failed_at=%s\n' "$wal_archiver_last_failed_at" >> "${work_dir}/wal_archiver.out"
  printf 'stats_reset_at=%s\n' "$wal_archiver_stats_reset_at" >> "${work_dir}/wal_archiver.out"

  case "$wal_archiver_archive_mode" in
    on|always) ;;
    *)
      printf 'archive_mode_disabled=%s\n' "$wal_archiver_archive_mode" >> "${work_dir}/wal_archiver.out"
      return 1
      ;;
  esac
  case "$wal_archiver_archive_command" in
    *wal_archive.sh*"%p"*"%f"*) ;;
    *)
      printf 'archive_command_unaudited=%s\n' "$wal_archiver_archive_command" >> "${work_dir}/wal_archiver.out"
      return 1
      ;;
  esac
  if [ "${wal_archiver_archived_count:-0}" -le 0 ] || [ -z "${wal_archiver_last_archived_at_ts:-}" ]; then
    printf 'last_archive_missing=1\n' >> "${work_dir}/wal_archiver.out"
    return 1
  fi
  last_archived_epoch="${wal_archiver_last_archived_at_ts:-0}"
  last_failed_epoch="${wal_archiver_last_failed_at_ts:-0}"
  now_epoch="$(date +%s)"
  wal_rpo_int="$(int_seconds "$wal_rpo_s")"
  last_archived_age_s="$((now_epoch - last_archived_epoch))"
  printf 'last_archived_age_s=%s\n' "$last_archived_age_s" >> "${work_dir}/wal_archiver.out"
  printf 'wal_rpo_s=%s\n' "$wal_rpo_s" >> "${work_dir}/wal_archiver.out"
  if [ "$wal_rpo_int" -gt 0 ] && [ "$last_archived_age_s" -gt "$wal_rpo_int" ]; then
    printf 'last_archive_stale=1\n' >> "${work_dir}/wal_archiver.out"
    wal_archiver_status="stale"
    return 1
  fi
  if [ -n "$last_failed_epoch" ] && [ "$last_failed_epoch" -gt "$last_archived_epoch" ]; then
    printf 'unrecovered_failure=1\n' >> "${work_dir}/wal_archiver.out"
    wal_archiver_status="fail"
    return 1
  fi
  wal_archiver_verified_at="$wal_archiver_last_archived_at"
  wal_archiver_status="pass"
  return 0
}

reuse_restore_drill_if_fresh() {
  local latest_report
  require_listable_dir "$drill_dir" restore_drill_dir "${work_dir}/restore_drill.out" || return 1
  latest_report="$(readlink -f "${drill_dir}/latest_restore_drill.txt" 2>/dev/null || true)"
  if [ -z "$latest_report" ] || [ ! -f "$latest_report" ]; then
    if ! latest_report="$(latest_file "$drill_dir" 'restore_drill_*.txt' restore_drill_latest_file)"; then
      cat "${work_dir}/restore_drill_latest_file.find.err" >> "${work_dir}/restore_drill.out"
      return 1
    fi
  fi
  [ -n "$latest_report" ] && [ -f "$latest_report" ] || return 1
  grep -q '^status=pass$' "$latest_report" || return 1
  restore_drill_report="$latest_report"
  restore_time_to_recover_s="$(report_value "$restore_drill_report" time_to_recover_s)"
  restore_drill_verified_at="$(report_value "$restore_drill_report" generated_at)"
  if [ -z "$restore_drill_verified_at" ]; then
    restore_drill_verified_at="$(file_mtime_iso "$restore_drill_report")"
  fi
  if ! fresh_enough "$latest_report" "$restore_reuse_max_s"; then
    printf 'restore_drill_stale=%s\n' "$latest_report" >> "${work_dir}/restore_drill.out"
    printf 'restore_drill_max_age_s=%s\n' "$restore_reuse_max_s" >> "${work_dir}/restore_drill.out"
    return 1
  fi
  restore_drill_status="pass"
  printf 'restore_drill_reused=%s\n' "$latest_report" > "${work_dir}/restore_drill.out"
  return 0
}

run_restore_drill_from_gate() {
  is_truthy "${TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL:-0}" || is_truthy "${TS_BACKUP_EVIDENCE_FORCE_RESTORE_DRILL:-0}"
}

run_restore_drill() {
  : > "${work_dir}/restore_drill.out"
  if ! is_truthy "${TS_BACKUP_EVIDENCE_FORCE_RESTORE_DRILL:-0}" && reuse_restore_drill_if_fresh; then
    return 0
  fi
  if ! run_restore_drill_from_gate; then
    if [ -n "$restore_drill_report" ]; then
      restore_drill_status="stale"
    else
      restore_drill_status="missing"
    fi
    printf 'restore_drill_gate_action=not_running\n' >> "${work_dir}/restore_drill.out"
    printf 'restore_drill_required_timer=trading-restore-drill.timer\n' >> "${work_dir}/restore_drill.out"
    return 1
  fi
  set +e
  run_with_timeout "$restore_drill_timeout_s" "${work_dir}/restore_drill.out" restore_drill bash "$restore_drill_script"
  local rc=$?
  set -e
  if [ "$rc" -ne 0 ] && timeout_exit_code "$rc"; then
    restore_drill_status="timeout"
  fi
  restore_drill_report="$(latest_file "$drill_dir" 'restore_drill_*.txt' restore_drill_latest_file_after_run || true)"
  if [ -n "$restore_drill_report" ] && [ -f "$restore_drill_report" ]; then
    restore_time_to_recover_s="$(report_value "$restore_drill_report" time_to_recover_s)"
    restore_drill_verified_at="$(report_value "$restore_drill_report" generated_at)"
    if [ -z "$restore_drill_verified_at" ]; then
      restore_drill_verified_at="$(file_mtime_iso "$restore_drill_report")"
    fi
  fi
  if [ "$rc" -eq 0 ] && [ -n "$restore_drill_report" ] && grep -q '^status=pass$' "$restore_drill_report"; then
    restore_drill_status="pass"
    return 0
  fi
  if [ "$rc" -eq 0 ]; then
    restore_drill_status="fail"
    return 1
  fi
  return "$rc"
}

write_reports() {
  local status="$1"
  {
    printf 'backup_restore_evidence_report_version=1\n'
    printf 'generated_at=%s\n' "$generated_at"
	    printf 'status=%s\n' "$status"
	    printf 'script_checks_status=%s\n' "$script_checks_status"
	    printf 'compose_checks_status=%s\n' "$compose_checks_status"
	    printf 'systemd_checks_status=%s\n' "$systemd_checks_status"
	    printf 'base_backup_status=%s\n' "$base_backup_status"
    printf 'base_backup_dir=%s\n' "$base_backup_dir"
    printf 'base_verify_log=%s\n' "$base_verify_log"
    printf 'base_verified_at=%s\n' "$base_verified_at"
    printf 'wal_archive_status=%s\n' "$wal_archive_status"
    printf 'wal_verified_at=%s\n' "$wal_verified_at"
    printf 'wal_observed_file=%s\n' "$wal_observed_file"
    printf 'wal_catchup_status=%s\n' "$wal_catchup_status"
    printf 'wal_catchup_verified_at=%s\n' "$wal_catchup_verified_at"
    printf 'wal_archiver_status=%s\n' "$wal_archiver_status"
    printf 'wal_archiver_verified_at=%s\n' "$wal_archiver_verified_at"
    printf 'wal_archiver_archive_mode=%s\n' "$wal_archiver_archive_mode"
    printf 'wal_archiver_archive_command=%s\n' "$wal_archiver_archive_command"
    printf 'wal_archiver_archived_count=%s\n' "$wal_archiver_archived_count"
    printf 'wal_archiver_last_archived_wal=%s\n' "$wal_archiver_last_archived_wal"
    printf 'wal_archiver_last_archived_at=%s\n' "$wal_archiver_last_archived_at"
	    printf 'wal_archiver_failed_count=%s\n' "$wal_archiver_failed_count"
	    printf 'wal_archiver_last_failed_wal=%s\n' "$wal_archiver_last_failed_wal"
	    printf 'wal_archiver_last_failed_at=%s\n' "$wal_archiver_last_failed_at"
	    printf 'wal_archiver_stats_reset_at=%s\n' "$wal_archiver_stats_reset_at"
	    printf 'wal_archive_target_status=%s\n' "$wal_target_status"
	    printf 'wal_archive_target_verified_at=%s\n' "$wal_target_verified_at"
	    printf 'wal_archive_target_root=%s\n' "$wal_target_root"
	    printf 'wal_archive_target_dir=%s\n' "$wal_target_dir"
	    printf 'wal_archive_target_tmp_dir=%s\n' "$wal_target_tmp_dir"
	    printf 'wal_archive_target_expected_owner_uid=%s\n' "$wal_target_expected_owner_uid"
	    printf 'wal_archive_target_expected_group=%s\n' "$wal_target_expected_group"
	    printf 'wal_archive_target_expected_group_gid=%s\n' "$wal_target_expected_group_gid"
	    printf 'wal_archive_target_expected_dir_mode=%s\n' "$wal_target_expected_dir_mode"
	    printf 'wal_archive_target_repaired=%s\n' "$wal_target_repaired"
	    printf 'wal_archive_target_issue_count=%s\n' "$wal_target_issue_count"
	    printf 'wal_archive_target_diagnosis_source=%s\n' "$wal_target_diagnosis_source"
	    printf 'wal_archive_target_diagnosis_probe_status=%s\n' "$wal_target_diagnosis_probe_status"
	    printf 'wal_archive_target_diagnosis_probe_wal_name=%s\n' "$wal_target_diagnosis_probe_wal_name"
	    printf 'wal_archive_target_diagnosis_signature=%s\n' "$wal_target_diagnosis_signature"
	    printf 'wal_archive_target_diagnosis_exit_code=%s\n' "$wal_target_diagnosis_exit_code"
	    printf 'wal_archive_target_diagnosis_fix=%s\n' "$wal_target_diagnosis_fix"
	    printf 'wal_archive_target_diagnosis_probe_output=%s\n' "$wal_target_diagnosis_probe_output"
	    printf 'restore_drill_status=%s\n' "$restore_drill_status"
	    printf 'restore_drill_report=%s\n' "$restore_drill_report"
	    printf 'restore_drill_verified_at=%s\n' "$restore_drill_verified_at"
	    printf 'restore_time_to_recover_s=%s\n' "$restore_time_to_recover_s"
	    printf 'signature_status=%s\n' "$signature_status"
	    printf 'publish_status=%s\n' "$publish_status"
	    printf 'lock_timeout_s=%s\n' "$lock_timeout_s"
	    printf 'probe_timeout_s=%s\n' "$probe_timeout_s"
	    printf 'systemctl_timeout_s=%s\n' "$systemctl_timeout_s"
	    printf 'base_backup_timeout_s=%s\n' "$base_backup_timeout_s"
	    printf 'wal_rpo_s=%s\n' "$wal_rpo_s"
	    printf 'wal_catchup_timeout_s=%s\n' "$wal_catchup_timeout_s"
	    printf 'wal_switch_timeout_s=%s\n' "$wal_switch_timeout_s"
	    printf 'wal_archiver_stats_timeout_s=%s\n' "$wal_archiver_stats_timeout_s"
	    printf 'restore_drill_timeout_s=%s\n' "$restore_drill_timeout_s"
	    printf 'signature_timeout_s=%s\n' "$signature_timeout_s"
	    printf 'publish_timeout_s=%s\n' "$publish_timeout_s"
	    printf '\n[script_checks]\n'
	    [ -f "${work_dir}/script_checks.out" ] && cat "${work_dir}/script_checks.out"
	    printf '\n[compose_checks]\n'
	    [ -f "${work_dir}/compose_checks.out" ] && cat "${work_dir}/compose_checks.out"
	    printf '\n[systemd_checks]\n'
	    [ -f "${work_dir}/systemd_checks.out" ] && cat "${work_dir}/systemd_checks.out"
    printf '\n[base_backup]\n'
    [ -f "${work_dir}/base_backup.out" ] && tail -n 120 "${work_dir}/base_backup.out"
    printf '\n[wal_catchup]\n'
    [ -f "${work_dir}/wal_catchup.out" ] && cat "${work_dir}/wal_catchup.out"
    printf '\n[wal_archive]\n'
    [ -f "${work_dir}/wal_archive.out" ] && cat "${work_dir}/wal_archive.out"
	    printf '\n[wal_archiver]\n'
	    [ -f "${work_dir}/wal_archiver.out" ] && cat "${work_dir}/wal_archiver.out"
	    printf '\n[wal_archive_target]\n'
	    [ -f "${work_dir}/wal_archive_target.out" ] && cat "${work_dir}/wal_archive_target.out"
	    printf '\n[restore_drill]\n'
	    [ -f "${work_dir}/restore_drill.out" ] && tail -n 160 "${work_dir}/restore_drill.out"
	  } > "$report_txt"
  publish_report_file "$report_txt"

  set +e
  REPORT_JSON="$report_json" \
  STATUS="$status" \
  GENERATED_AT="$generated_at" \
  REPORT_TXT="$report_txt" \
  SCRIPT_CHECKS_STATUS="$script_checks_status" \
  COMPOSE_CHECKS_STATUS="$compose_checks_status" \
  SYSTEMD_CHECKS_STATUS="$systemd_checks_status" \
  BASE_BACKUP_STATUS="$base_backup_status" \
  BASE_BACKUP_DIR="$base_backup_dir" \
  BASE_VERIFY_LOG="$base_verify_log" \
  BASE_VERIFIED_AT="$base_verified_at" \
  WAL_ARCHIVE_STATUS="$wal_archive_status" \
  WAL_VERIFIED_AT="$wal_verified_at" \
  WAL_OBSERVED_FILE="$wal_observed_file" \
  WAL_CATCHUP_STATUS="$wal_catchup_status" \
  WAL_CATCHUP_VERIFIED_AT="$wal_catchup_verified_at" \
  WAL_ARCHIVER_STATUS="$wal_archiver_status" \
  WAL_ARCHIVER_VERIFIED_AT="$wal_archiver_verified_at" \
  WAL_ARCHIVER_ARCHIVE_MODE="$wal_archiver_archive_mode" \
  WAL_ARCHIVER_ARCHIVE_COMMAND="$wal_archiver_archive_command" \
  WAL_ARCHIVER_ARCHIVED_COUNT="$wal_archiver_archived_count" \
  WAL_ARCHIVER_LAST_ARCHIVED_WAL="$wal_archiver_last_archived_wal" \
  WAL_ARCHIVER_LAST_ARCHIVED_AT="$wal_archiver_last_archived_at" \
  WAL_ARCHIVER_LAST_ARCHIVED_AT_TS="$wal_archiver_last_archived_at_ts" \
  WAL_ARCHIVER_FAILED_COUNT="$wal_archiver_failed_count" \
	  WAL_ARCHIVER_LAST_FAILED_WAL="$wal_archiver_last_failed_wal" \
	  WAL_ARCHIVER_LAST_FAILED_AT="$wal_archiver_last_failed_at" \
	  WAL_ARCHIVER_LAST_FAILED_AT_TS="$wal_archiver_last_failed_at_ts" \
	  WAL_ARCHIVER_STATS_RESET_AT="$wal_archiver_stats_reset_at" \
	  WAL_TARGET_STATUS="$wal_target_status" \
	  WAL_TARGET_VERIFIED_AT="$wal_target_verified_at" \
	  WAL_TARGET_ROOT="$wal_target_root" \
	  WAL_TARGET_DIR="$wal_target_dir" \
	  WAL_TARGET_TMP_DIR="$wal_target_tmp_dir" \
	  WAL_TARGET_EXPECTED_OWNER_UID="$wal_target_expected_owner_uid" \
	  WAL_TARGET_EXPECTED_GROUP="$wal_target_expected_group" \
	  WAL_TARGET_EXPECTED_GROUP_GID="$wal_target_expected_group_gid" \
	  WAL_TARGET_EXPECTED_DIR_MODE="$wal_target_expected_dir_mode" \
	  WAL_TARGET_REPAIRED="$wal_target_repaired" \
	  WAL_TARGET_ISSUE_COUNT="$wal_target_issue_count" \
	  WAL_TARGET_DIAGNOSIS_SOURCE="$wal_target_diagnosis_source" \
	  WAL_TARGET_DIAGNOSIS_PROBE_STATUS="$wal_target_diagnosis_probe_status" \
	  WAL_TARGET_DIAGNOSIS_PROBE_WAL_NAME="$wal_target_diagnosis_probe_wal_name" \
	  WAL_TARGET_DIAGNOSIS_SIGNATURE="$wal_target_diagnosis_signature" \
	  WAL_TARGET_DIAGNOSIS_EXIT_CODE="$wal_target_diagnosis_exit_code" \
	  WAL_TARGET_DIAGNOSIS_FIX="$wal_target_diagnosis_fix" \
	  WAL_TARGET_DIAGNOSIS_PROBE_OUTPUT="$wal_target_diagnosis_probe_output" \
	  RESTORE_DRILL_STATUS="$restore_drill_status" \
	  RESTORE_DRILL_REPORT="$restore_drill_report" \
	  RESTORE_DRILL_VERIFIED_AT="$restore_drill_verified_at" \
  RESTORE_TIME_TO_RECOVER_S="$restore_time_to_recover_s" \
  PUBLISH_STATUS="$publish_status" \
  LOCK_TIMEOUT_S="$lock_timeout_s" \
  PROBE_TIMEOUT_S="$probe_timeout_s" \
  SYSTEMCTL_TIMEOUT_S="$systemctl_timeout_s" \
  BASE_BACKUP_TIMEOUT_S="$base_backup_timeout_s" \
  WAL_RPO_S="$wal_rpo_s" \
  WAL_CATCHUP_TIMEOUT_S="$wal_catchup_timeout_s" \
  WAL_SWITCH_TIMEOUT_S="$wal_switch_timeout_s" \
  WAL_ARCHIVER_STATS_TIMEOUT_S="$wal_archiver_stats_timeout_s" \
  RESTORE_DRILL_TIMEOUT_S="$restore_drill_timeout_s" \
  SIGNATURE_TIMEOUT_S="$signature_timeout_s" \
  PUBLISH_TIMEOUT_S="$publish_timeout_s" \
  run_capture_with_timeout "$signature_timeout_s" "${work_dir}/signature_python.out" "${work_dir}/signature_python.err" signature_generation python3 - <<'PY'
import hashlib
import hmac
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def ts(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def maybe_float(value: str):
    try:
        return float(value)
    except Exception:
        return None


def maybe_int(value: str):
    try:
        return int(float(value))
    except Exception:
        return None


def truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def signing_key():
    for name in ("BACKUP_EVIDENCE_HMAC_KEY", "BACKUP_EVIDENCE_SIGNING_KEY"):
        value = os.environ.get(name) or ""
        if value.strip():
            return value.encode("utf-8"), f"env:{name}"
    for name in ("BACKUP_EVIDENCE_HMAC_KEY_FILE", "BACKUP_EVIDENCE_SIGNING_KEY_FILE"):
        raw_path = (os.environ.get(name) or "").strip()
        if not raw_path:
            continue
        try:
            value = Path(raw_path).read_text(encoding="utf-8").strip()
        except Exception:
            return None, f"unreadable:{name}"
        if value:
            return value.encode("utf-8"), f"file:{name}"
        return None, f"empty:{name}"

    def credential_value(secret_name):
        cred_dir = (os.environ.get("CREDENTIALS_DIRECTORY") or "").strip()
        if not cred_dir or not secret_name:
            return None, "missing"
        path = Path(cred_dir) / secret_name
        try:
            value = path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return None, "missing"
        except Exception:
            return None, "unreadable"
        if value:
            return value.encode("utf-8"), "available"
        return None, "empty"

    for name in ("BACKUP_EVIDENCE_HMAC_KEY_SECRET", "BACKUP_EVIDENCE_SIGNING_KEY_SECRET"):
        secret_name = (os.environ.get(name) or "").strip()
        if not secret_name:
            continue
        value, state = credential_value(secret_name)
        if value:
            return value, f"secret:{name}"
        if state == "unreadable":
            return None, f"unreadable:{name}"
        if state == "empty":
            return None, f"empty:{name}"
        return None, f"missing:{name}"

    if (os.environ.get("CREDENTIALS_DIRECTORY") or os.environ.get("TS_SECRETS_PROVIDER") or "").strip():
        for secret_name in ("backup_evidence_hmac_key", "BACKUP_EVIDENCE_HMAC_KEY", "backup_evidence_signing_key"):
            value, state = credential_value(secret_name)
            if value:
                return value, "provider:backup_evidence_hmac_key"
            if state in {"unreadable", "empty"}:
                return None, f"{state}:backup_evidence_hmac_key"
    return None, "missing"


def canonical_payload_bytes(payload):
    unsigned = dict(payload)
    unsigned.pop("signature", None)
    return json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def signature_input_bytes(payload_bytes, *, algorithm, key_id, signed_at, payload_sha256):
    metadata_bytes = json.dumps(
        {
            "algorithm": str(algorithm),
            "key_id": str(key_id),
            "payload_sha256": str(payload_sha256),
            "signed_at": str(signed_at),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return payload_bytes + b"\n" + metadata_bytes


generated_at = os.environ["GENERATED_AT"]
signature_required = truthy(
    os.environ.get("BACKUP_EVIDENCE_REQUIRE_SIGNATURE")
    or os.environ.get("BACKUP_EVIDENCE_SIGNATURE_REQUIRED")
    or os.environ.get("PREFLIGHT_REQUIRE_BACKUP_EVIDENCE")
    or "0"
) or (os.environ.get("ENGINE_MODE") or "").strip().lower() == "live" or (
    os.environ.get("EXECUTION_MODE") or ""
).strip().lower() == "live"
payload = {
    "schema_version": 1,
    "generated_at": generated_at,
    "generated_at_ts": ts(generated_at),
    "status": os.environ["STATUS"],
    "report": os.environ["REPORT_TXT"],
    "script_checks": {
        "status": os.environ["SCRIPT_CHECKS_STATUS"],
    },
    "compose_checks": {
        "status": os.environ["COMPOSE_CHECKS_STATUS"],
    },
    "systemd_checks": {
        "status": os.environ["SYSTEMD_CHECKS_STATUS"],
    },
    "timeouts": {
        "lock_s": maybe_float(os.environ["LOCK_TIMEOUT_S"]),
        "probe_s": maybe_float(os.environ["PROBE_TIMEOUT_S"]),
        "systemctl_s": maybe_float(os.environ["SYSTEMCTL_TIMEOUT_S"]),
        "base_backup_s": maybe_float(os.environ["BASE_BACKUP_TIMEOUT_S"]),
        "wal_rpo_s": maybe_float(os.environ["WAL_RPO_S"]),
        "wal_catchup_s": maybe_float(os.environ["WAL_CATCHUP_TIMEOUT_S"]),
        "wal_switch_s": maybe_float(os.environ["WAL_SWITCH_TIMEOUT_S"]),
        "wal_archiver_stats_s": maybe_float(os.environ["WAL_ARCHIVER_STATS_TIMEOUT_S"]),
        "restore_drill_s": maybe_float(os.environ["RESTORE_DRILL_TIMEOUT_S"]),
        "signature_s": maybe_float(os.environ["SIGNATURE_TIMEOUT_S"]),
        "publish_s": maybe_float(os.environ["PUBLISH_TIMEOUT_S"]),
    },
    "base_backup": {
        "status": os.environ["BASE_BACKUP_STATUS"],
        "backup_dir": os.environ["BASE_BACKUP_DIR"],
        "verify_log": os.environ["BASE_VERIFY_LOG"],
        "verified_at": os.environ["BASE_VERIFIED_AT"],
        "verified_at_ts": ts(os.environ["BASE_VERIFIED_AT"]),
    },
    "wal_archive": {
        "status": os.environ["WAL_ARCHIVE_STATUS"],
        "wal_file": os.environ["WAL_OBSERVED_FILE"],
        "verified_at": os.environ["WAL_VERIFIED_AT"],
        "verified_at_ts": ts(os.environ["WAL_VERIFIED_AT"]),
    },
    "wal_catchup": {
        "status": os.environ["WAL_CATCHUP_STATUS"],
        "verified_at": os.environ["WAL_CATCHUP_VERIFIED_AT"],
        "verified_at_ts": ts(os.environ["WAL_CATCHUP_VERIFIED_AT"]),
    },
	    "wal_archiver": {
	        "status": os.environ["WAL_ARCHIVER_STATUS"],
	        "source": "pg_stat_archiver",
        "archive_mode": os.environ["WAL_ARCHIVER_ARCHIVE_MODE"],
        "archive_command": os.environ["WAL_ARCHIVER_ARCHIVE_COMMAND"],
        "archived_count": maybe_int(os.environ["WAL_ARCHIVER_ARCHIVED_COUNT"]),
        "last_archived_wal": os.environ["WAL_ARCHIVER_LAST_ARCHIVED_WAL"],
        "last_archived_at": os.environ["WAL_ARCHIVER_LAST_ARCHIVED_AT"],
        "last_archived_at_ts": maybe_float(os.environ["WAL_ARCHIVER_LAST_ARCHIVED_AT_TS"]),
        "verified_at": os.environ["WAL_ARCHIVER_VERIFIED_AT"],
        "verified_at_ts": ts(os.environ["WAL_ARCHIVER_VERIFIED_AT"]),
        "failed_count": maybe_int(os.environ["WAL_ARCHIVER_FAILED_COUNT"]),
        "last_failed_wal": os.environ["WAL_ARCHIVER_LAST_FAILED_WAL"],
        "last_failed_at": os.environ["WAL_ARCHIVER_LAST_FAILED_AT"],
        "last_failed_at_ts": maybe_float(os.environ["WAL_ARCHIVER_LAST_FAILED_AT_TS"]),
	        "stats_reset": os.environ["WAL_ARCHIVER_STATS_RESET_AT"],
	        "stats_reset_ts": ts(os.environ["WAL_ARCHIVER_STATS_RESET_AT"]),
	    },
	    "wal_archive_target": {
	        "status": os.environ["WAL_TARGET_STATUS"],
	        "source": "filesystem_repair",
	        "root": os.environ["WAL_TARGET_ROOT"],
	        "wal_dir": os.environ["WAL_TARGET_DIR"],
	        "tmp_dir": os.environ["WAL_TARGET_TMP_DIR"],
	        "expected_owner_uid": maybe_int(os.environ["WAL_TARGET_EXPECTED_OWNER_UID"]),
	        "expected_group": os.environ["WAL_TARGET_EXPECTED_GROUP"],
	        "expected_group_gid": maybe_int(os.environ["WAL_TARGET_EXPECTED_GROUP_GID"]),
	        "expected_dir_mode": os.environ["WAL_TARGET_EXPECTED_DIR_MODE"],
	        "repaired": truthy(os.environ["WAL_TARGET_REPAIRED"]),
	        "issue_count": maybe_int(os.environ["WAL_TARGET_ISSUE_COUNT"]),
	        "verified_at": os.environ["WAL_TARGET_VERIFIED_AT"],
	        "verified_at_ts": ts(os.environ["WAL_TARGET_VERIFIED_AT"]),
	        "diagnosis": {
	            "source": os.environ["WAL_TARGET_DIAGNOSIS_SOURCE"],
	            "archive_command_probe_status": os.environ["WAL_TARGET_DIAGNOSIS_PROBE_STATUS"],
	            "archive_command_probe_wal_name": os.environ["WAL_TARGET_DIAGNOSIS_PROBE_WAL_NAME"],
	            "observed_pg_stat_archiver_failed_count": maybe_int(os.environ["WAL_ARCHIVER_FAILED_COUNT"]),
	            "observed_pg_stat_archiver_last_failed_wal": os.environ["WAL_ARCHIVER_LAST_FAILED_WAL"],
	            "observed_pg_stat_archiver_last_failed_at": os.environ["WAL_ARCHIVER_LAST_FAILED_AT"],
	            "observed_pg_stat_archiver_last_failed_at_ts": maybe_float(os.environ["WAL_ARCHIVER_LAST_FAILED_AT_TS"]),
	            "original_archive_command_failure_signature": os.environ["WAL_TARGET_DIAGNOSIS_SIGNATURE"],
	            "original_archive_command_exit_code": maybe_int(os.environ["WAL_TARGET_DIAGNOSIS_EXIT_CODE"]),
	            "archive_command_failure_signature": os.environ["WAL_TARGET_DIAGNOSIS_SIGNATURE"],
	            "archive_command_exit_code": maybe_int(os.environ["WAL_TARGET_DIAGNOSIS_EXIT_CODE"]),
	            "archive_command_probe_output": os.environ["WAL_TARGET_DIAGNOSIS_PROBE_OUTPUT"],
	            "fix": os.environ["WAL_TARGET_DIAGNOSIS_FIX"],
	        },
	    },
	    "restore_drill": {
	        "status": os.environ["RESTORE_DRILL_STATUS"],
	        "report": os.environ["RESTORE_DRILL_REPORT"],
        "verified_at": os.environ["RESTORE_DRILL_VERIFIED_AT"] if os.environ["RESTORE_DRILL_STATUS"] == "pass" else "",
        "verified_at_ts": ts(os.environ["RESTORE_DRILL_VERIFIED_AT"]) if os.environ["RESTORE_DRILL_STATUS"] == "pass" else None,
        "time_to_recover_s": maybe_float(os.environ["RESTORE_TIME_TO_RECOVER_S"]),
    },
    "publish": {
        "status": os.environ["PUBLISH_STATUS"],
    },
}
key, key_source = signing_key()
if key:
    payload_bytes = canonical_payload_bytes(payload)
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    algorithm = "hmac-sha256"
    key_id = os.environ.get("BACKUP_EVIDENCE_KEY_ID", "backup-evidence")
    signed_at = generated_at
    payload["signature"] = {
        "status": "signed",
        "algorithm": algorithm,
        "key_id": key_id,
        "key_source": key_source,
        "signed_at": signed_at,
        "payload_sha256": payload_sha256,
        "value": hmac.new(
            key,
            signature_input_bytes(
                payload_bytes,
                algorithm=algorithm,
                key_id=key_id,
                signed_at=signed_at,
                payload_sha256=payload_sha256,
            ),
            hashlib.sha256,
        ).hexdigest(),
    }
else:
    unsigned_status = "unsigned" if not signature_required else "key_missing"
    if str(key_source).startswith("unreadable:"):
        unsigned_status = "key_unreadable"
    elif str(key_source).startswith("empty:"):
        unsigned_status = "key_empty"
    payload["signature"] = {
        "status": unsigned_status,
        "required": signature_required,
        "algorithm": "hmac-sha256",
        "key_id": os.environ.get("BACKUP_EVIDENCE_KEY_ID", "backup-evidence"),
        "key_source": key_source,
        "signed_at": "",
        "payload_sha256": "",
        "value": "",
    }
target = Path(os.environ["REPORT_JSON"])
target.parent.mkdir(parents=True, exist_ok=True)
with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(target.parent), delete=False) as fh:
    json.dump(payload, fh, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    fh.write("\n")
    tmp = fh.name
Path(tmp).replace(target)
PY
  local json_rc=$?
  set -e
  if [ "$json_rc" -ne 0 ]; then
    if timeout_exit_code "$json_rc"; then
      signature_status="timeout"
    else
      signature_status="fail"
    fi
    printf 'signature_generation_failed_rc=%s\n' "$json_rc" >> "$report_txt"
    publish_report_file "$report_txt"
    return "$json_rc"
  fi
  publish_report_file "$report_json"

  : > "${work_dir}/publish.out"
  set +e
  run_with_timeout "$publish_timeout_s" "${work_dir}/publish.out" publish_latest_txt ln -sfn "$(basename "$report_txt")" "$latest_txt"
  local publish_rc=$?
  set -e
  if [ "$publish_rc" -ne 0 ]; then
    if timeout_exit_code "$publish_rc"; then
      publish_status="timeout"
    else
      publish_status="fail"
    fi
    return "$publish_rc"
  fi
  set +e
  run_with_timeout "$publish_timeout_s" "${work_dir}/publish.out" publish_latest_json_dir mkdir -p "$(dirname "$latest_json")"
  publish_rc=$?
  set -e
  if [ "$publish_rc" -ne 0 ]; then
    if timeout_exit_code "$publish_rc"; then
      publish_status="timeout"
    else
      publish_status="fail"
    fi
    return "$publish_rc"
  fi
  set +e
  run_with_timeout "$publish_timeout_s" "${work_dir}/publish.out" publish_latest_json_cp cp "$report_json" "${latest_json}.$$"
  publish_rc=$?
  set -e
  if [ "$publish_rc" -ne 0 ]; then
    if timeout_exit_code "$publish_rc"; then
      publish_status="timeout"
    else
      publish_status="fail"
    fi
    return "$publish_rc"
  fi
  publish_report_file "${latest_json}.$$"
  set +e
  run_with_timeout "$publish_timeout_s" "${work_dir}/publish.out" publish_latest_json_mv mv -f "${latest_json}.$$" "$latest_json"
  publish_rc=$?
  set -e
  if [ "$publish_rc" -ne 0 ]; then
    if timeout_exit_code "$publish_rc"; then
      publish_status="timeout"
    else
      publish_status="fail"
    fi
    return "$publish_rc"
  fi
  publish_status="pass"
}

overall_rc=0
check_scripts || overall_rc=1
check_compose || overall_rc=1
check_systemd || overall_rc=1
repair_wal_archive_target || overall_rc=1
run_base_backup || overall_rc=1
run_wal_archive_catchup || overall_rc=1
verify_wal_archive || overall_rc=1
verify_wal_archiver_stats || overall_rc=1
run_restore_drill || overall_rc=1
key_state="$(backup_evidence_signing_key_state)"
if [ "$key_state" = "available" ]; then
  signature_status="signed"
elif backup_evidence_signature_required; then
  case "$key_state" in
    unreadable) signature_status="key_unreadable" ;;
    empty) signature_status="key_empty" ;;
    *) signature_status="key_missing" ;;
  esac
  overall_rc=1
else
  signature_status="unsigned"
fi

if [ "$overall_rc" -eq 0 ]; then
  write_reports pass || overall_rc=1
else
  write_reports fail || overall_rc=1
fi

log info evidence_report_written "report=${report_txt} json=${report_json} latest_json=${latest_json} status=$([ "$overall_rc" -eq 0 ] && printf pass || printf fail)"
exit "$overall_rc"
