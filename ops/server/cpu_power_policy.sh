#!/usr/bin/env bash
set -euo pipefail

PROFILE="${TRADING_CPU_POWER_PROFILE:-performance}"
EPP="${TRADING_CPU_EPP:-performance}"
GOVERNOR_FALLBACK="${TRADING_CPU_GOVERNOR_FALLBACK:-performance}"
CPU_SYSFS_ROOT="${TRADING_CPU_SYSFS_ROOT:-/sys/devices/system/cpu}"
POWERPROFILESCTL="${TRADING_POWERPROFILESCTL:-powerprofilesctl}"

log() {
  printf '[cpu-power-policy] %s\n' "$*"
}

fail() {
  printf '[cpu-power-policy] ERROR: %s\n' "$*" >&2
  exit 1
}

command_available() {
  [ -n "$1" ] && command -v "$1" >/dev/null 2>&1
}

cpu_cpufreq_files() {
  local leaf="$1"
  local path
  for path in "${CPU_SYSFS_ROOT}"/cpu[0-9]*/cpufreq/"${leaf}"; do
    [ -e "$path" ] || continue
    printf '%s\n' "$path"
  done | sort -V
}

count_files() {
  local leaf="$1"
  cpu_cpufreq_files "$leaf" | wc -l | tr -d ' '
}

available_contains() {
  local available_file="$1"
  local expected="$2"
  [ ! -r "$available_file" ] && return 0
  grep -qw -- "$expected" "$available_file"
}

profile_list() {
  command_available "$POWERPROFILESCTL" || return 1
  "$POWERPROFILESCTL" list 2>/dev/null
}

profile_available() {
  profile_list | awk -v profile="$PROFILE" '
    {
      line=$0
      sub(/^[[:space:]]*\*[[:space:]]*/, "", line)
      sub(/^[[:space:]]+/, "", line)
      if (line == profile ":") found=1
    }
    END { exit found ? 0 : 1 }
  '
}

profile_degraded() {
  profile_list | awk -v profile="$PROFILE" '
    {
      line=$0
      sub(/^[[:space:]]*\*[[:space:]]*/, "", line)
      sub(/^[[:space:]]+/, "", line)
    }
    line == profile ":" { in_profile=1; next }
    line ~ /^[A-Za-z0-9_-]+:$/ { in_profile=0 }
    in_profile && $1 == "Degraded:" {
      value=$2
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      print tolower(value)
      found=1
      exit
    }
    END { if (!found) print "unknown" }
  '
}

current_profile() {
  command_available "$POWERPROFILESCTL" || return 1
  "$POWERPROFILESCTL" get 2>/dev/null
}

apply_powerprofiles_profile() {
  command_available "$POWERPROFILESCTL" || {
    log "powerprofilesctl not available; trying sysfs policy"
    return 1
  }
  profile_available || {
    log "power-profiles-daemon profile ${PROFILE} not available; trying sysfs policy"
    return 1
  }

  local degraded current
  degraded="$(profile_degraded || true)"
  if [ "$degraded" = "yes" ]; then
    fail "power-profiles-daemon reports ${PROFILE} degraded"
  fi

  current="$(current_profile || true)"
  if [ "$current" = "$PROFILE" ]; then
    log "power-profiles-daemon already set to ${PROFILE}"
  else
    "$POWERPROFILESCTL" set "$PROFILE"
    log "set power-profiles-daemon profile to ${PROFILE}"
  fi
  return 0
}

apply_epp() {
  local files=()
  local file available_file current changed=0
  mapfile -t files < <(cpu_cpufreq_files energy_performance_preference)
  if [ "${#files[@]}" -eq 0 ]; then
    log "no energy_performance_preference files found"
    return 1
  fi

  for file in "${files[@]}"; do
    available_file="$(dirname "$file")/energy_performance_available_preferences"
    available_contains "$available_file" "$EPP" || fail "${file} does not support EPP ${EPP}"
    [ -w "$file" ] || fail "${file} is not writable"
    current="$(tr -d '[:space:]' < "$file")"
    if [ "$current" != "$EPP" ]; then
      printf '%s\n' "$EPP" > "$file"
      changed=1
    fi
  done

  if [ "$changed" -eq 1 ]; then
    log "set EPP ${EPP} on ${#files[@]} CPU policy file(s)"
  else
    log "EPP already ${EPP} on ${#files[@]} CPU policy file(s)"
  fi
  return 0
}

apply_governor_fallback() {
  local files=()
  local file available_file current changed=0
  mapfile -t files < <(cpu_cpufreq_files scaling_governor)
  if [ "${#files[@]}" -eq 0 ]; then
    log "no scaling_governor files found"
    return 1
  fi

  for file in "${files[@]}"; do
    available_file="$(dirname "$file")/scaling_available_governors"
    available_contains "$available_file" "$GOVERNOR_FALLBACK" || fail "${file} does not support governor ${GOVERNOR_FALLBACK}"
    [ -w "$file" ] || fail "${file} is not writable"
    current="$(tr -d '[:space:]' < "$file")"
    if [ "$current" != "$GOVERNOR_FALLBACK" ]; then
      printf '%s\n' "$GOVERNOR_FALLBACK" > "$file"
      changed=1
    fi
  done

  if [ "$changed" -eq 1 ]; then
    log "set governor ${GOVERNOR_FALLBACK} on ${#files[@]} CPU policy file(s)"
  else
    log "governor already ${GOVERNOR_FALLBACK} on ${#files[@]} CPU policy file(s)"
  fi
  return 0
}

summarize_cpufreq_leaf() {
  local leaf="$1"
  local files=()
  local file value total matches
  mapfile -t files < <(cpu_cpufreq_files "$leaf")
  total="${#files[@]}"
  if [ "$total" -eq 0 ]; then
    printf '%s=missing\n' "$leaf"
    return 0
  fi

  while IFS= read -r value; do
    [ -n "$value" ] || continue
    matches=0
    for file in "${files[@]}"; do
      if [ "$(tr -d '[:space:]' < "$file")" = "$value" ]; then
        matches=$((matches + 1))
      fi
    done
    printf '%s=%s (%s/%s)\n' "$leaf" "$value" "$matches" "$total"
  done < <(
    for file in "${files[@]}"; do
      tr -d '[:space:]' < "$file"
      printf '\n'
    done | sort -u
  )
}

all_cpufreq_values_match() {
  local leaf="$1"
  local expected="$2"
  local file count=0
  while IFS= read -r file; do
    count=$((count + 1))
    [ "$(tr -d '[:space:]' < "$file")" = "$expected" ] || return 1
  done < <(cpu_cpufreq_files "$leaf")
  [ "$count" -gt 0 ]
}

print_status() {
  local profile degraded amd_status
  if command_available "$POWERPROFILESCTL"; then
    profile="$(current_profile || true)"
    [ -n "$profile" ] || profile="unavailable"
    degraded="$(profile_degraded || true)"
    [ -n "$degraded" ] || degraded="unknown"
  else
    profile="unavailable"
    degraded="unknown"
  fi

  amd_status="missing"
  if [ -r "${CPU_SYSFS_ROOT}/amd_pstate/status" ]; then
    amd_status="$(tr -d '[:space:]' < "${CPU_SYSFS_ROOT}/amd_pstate/status")"
  fi

  printf 'power_profile=%s\n' "$profile"
  printf 'power_profile_degraded=%s\n' "$degraded"
  printf 'amd_pstate_status=%s\n' "$amd_status"
  summarize_cpufreq_leaf scaling_driver
  summarize_cpufreq_leaf scaling_governor
  summarize_cpufreq_leaf energy_performance_preference
}

verify_policy() {
  local profile degraded ok=1
  print_status

  profile="$(current_profile || true)"
  degraded="$(profile_degraded || true)"
  if [ "$profile" = "$PROFILE" ] && [ "$degraded" != "yes" ]; then
    ok=0
  elif all_cpufreq_values_match energy_performance_preference "$EPP"; then
    ok=0
  elif all_cpufreq_values_match scaling_governor "$GOVERNOR_FALLBACK"; then
    ok=0
  fi

  if [ "$ok" -eq 0 ]; then
    printf 'intended_state=PASS\n'
    return 0
  fi
  printf 'intended_state=FAIL expected profile=%s or epp=%s or governor=%s\n' "$PROFILE" "$EPP" "$GOVERNOR_FALLBACK"
  return 1
}

apply_policy() {
  local profile_ok=1 epp_ok=1
  apply_powerprofiles_profile && profile_ok=0 || profile_ok=1
  apply_epp && epp_ok=0 || epp_ok=1

  if [ "$profile_ok" -ne 0 ] && [ "$epp_ok" -ne 0 ]; then
    apply_governor_fallback || fail "could not apply power policy through power-profiles-daemon, EPP, or governor"
  fi
  verify_policy >/dev/null
}

usage() {
  cat <<'EOF'
Usage: cpu_power_policy.sh apply|verify|status

Environment:
  TRADING_CPU_POWER_PROFILE       power-profiles-daemon profile, default performance
  TRADING_CPU_EPP                 energy_performance_preference value, default performance
  TRADING_CPU_GOVERNOR_FALLBACK   governor fallback when profile/EPP are unavailable, default performance
  TRADING_CPU_SYSFS_ROOT          CPU sysfs root for tests, default /sys/devices/system/cpu
  TRADING_POWERPROFILESCTL        powerprofilesctl command path, default powerprofilesctl
EOF
}

case "${1:-}" in
  apply)
    apply_policy
    ;;
  verify)
    verify_policy
    ;;
  status)
    print_status
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
