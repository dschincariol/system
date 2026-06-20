#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${1:-$(pwd)}"
PROFILE_RAW="${TRADING_DEPENDENCY_PROFILE:-cpu}"
OVERRIDE_RAW="${TRADING_REQUIREMENTS_FILE:-}"

normalize_profile() {
  printf '%s' "${1:-cpu}" | tr '[:upper:]_' '[:lower:]-'
}

resolve_path() {
  local candidate="$1"
  if [[ "$candidate" = /* ]]; then
    printf '%s\n' "$candidate"
  else
    printf '%s\n' "$REPO_DIR/$candidate"
  fi
}

if [[ -n "$OVERRIDE_RAW" ]]; then
  resolved="$(resolve_path "$OVERRIDE_RAW")"
  if [[ ! -f "$resolved" ]]; then
    echo "requirements_override_not_found:$resolved" >&2
    exit 66
  fi
  printf '%s\n' "$resolved"
  exit 0
fi

profile="$(normalize_profile "$PROFILE_RAW")"
case "$profile" in
  ""|cpu|default|runtime|cpu-runtime)
    requirements_file="requirements.txt"
    ;;
  nvidia|cuda|nvidia-cuda)
    requirements_file="requirements-nvidia-cuda.txt"
    ;;
  amd|rocm|amd-rocm)
    echo "amd_rocm_dependency_profile_not_validated:profile=$PROFILE_RAW; set TRADING_REQUIREMENTS_FILE to a reviewed host-specific ROCm requirements file after validating ROCm, container device permissions, and PyTorch HIP support" >&2
    exit 64
    ;;
  *)
    echo "unsupported_dependency_profile:$PROFILE_RAW" >&2
    exit 64
    ;;
esac

resolved="$(resolve_path "$requirements_file")"
if [[ ! -f "$resolved" ]]; then
  echo "requirements_profile_file_not_found:profile=$PROFILE_RAW:path=$resolved" >&2
  exit 66
fi

printf '%s\n' "$resolved"
