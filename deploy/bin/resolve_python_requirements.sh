#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${1:-$(pwd)}"
PROFILE_RAW="${TRADING_DEPENDENCY_PROFILE:-cpu}"
OVERRIDE_RAW="${TRADING_REQUIREMENTS_FILE:-}"
PYTHON_MARKER_BIN="${PYTHON_BIN:-${TRADING_PYTHON_BIN:-${PYTHON:-python}}}"

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

validate_amd_rocm_profile_file() {
  local path="$1"
  if ! grep -Eq 'gfx1151|Strix Halo' "$path"; then
    echo "amd_rocm_profile_not_validated:path=$path:missing=gfx1151_marker" >&2
    exit 65
  fi
  if ! grep -Eq 'repo\.radeon\.com/rocm/.+rocm-rel-7\.2\.4' "$path"; then
    echo "amd_rocm_profile_not_validated:path=$path:missing=rocm_7_2_4_wheel_source" >&2
    exit 65
  fi
  if ! grep -Eq '^torch @ .+rocm7\.2\.4' "$path"; then
    echo "amd_rocm_profile_not_validated:path=$path:missing=torch_rocm_wheel_pin" >&2
    exit 65
  fi
}

validate_amd_rocm_python_runtime() {
  local python_bin="$PYTHON_MARKER_BIN"
  if ! command -v "$python_bin" >/dev/null 2>&1; then
    echo "amd_rocm_python_runtime_unsupported:python_bin=$python_bin:reason=python_not_found" >&2
    exit 65
  fi
  if ! "$python_bin" - <<'PY'
import platform
import sys

version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
system = platform.system()
if system != "Linux":
    raise SystemExit(
        "amd_rocm_python_runtime_unsupported:"
        f"platform={system}:required_platform=Linux:"
        "reason=rocm_7_2_4_wheels_are_linux_cp312"
    )
if sys.version_info[:2] < (3, 12):
    raise SystemExit(
        "amd_rocm_python_runtime_unsupported:"
        f"python={version}:required_python=>=3.12:"
        "reason=rocm_7_2_4_wheels_are_cp312"
    )
PY
  then
    exit 65
  fi
}

is_amd_rocm_profile() {
  case "$(normalize_profile "$1")" in
    amd|rocm|amd-rocm) return 0 ;;
    *) return 1 ;;
  esac
}

is_amd_rocm_requirements_file() {
  case "$(basename "$1")" in
    requirements-amd-rocm.txt|requirements-amd-rocm-full.txt) return 0 ;;
    *) return 1 ;;
  esac
}

if [[ -n "$OVERRIDE_RAW" ]]; then
  resolved="$(resolve_path "$OVERRIDE_RAW")"
  if [[ ! -f "$resolved" ]]; then
    echo "requirements_override_not_found:$resolved" >&2
    exit 66
  fi
  if is_amd_rocm_profile "$PROFILE_RAW" || is_amd_rocm_requirements_file "$resolved"; then
    validate_amd_rocm_profile_file "$resolved"
    validate_amd_rocm_python_runtime
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
    requirements_file="requirements-amd-rocm.txt"
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
if is_amd_rocm_profile "$profile"; then
  validate_amd_rocm_profile_file "$resolved"
  validate_amd_rocm_python_runtime
fi

printf '%s\n' "$resolved"
