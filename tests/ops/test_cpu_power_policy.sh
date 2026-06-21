#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
POLICY="${REPO_ROOT}/ops/server/cpu_power_policy.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

cpu_root="${tmp_dir}/sys/devices/system/cpu"
fake_bin="${tmp_dir}/bin"
mkdir -p "${fake_bin}" "${cpu_root}/amd_pstate" "${cpu_root}/cpu0/cpufreq" "${cpu_root}/cpu1/cpufreq"
printf 'active\n' > "${cpu_root}/amd_pstate/status"

for cpu in cpu0 cpu1; do
  cpufreq="${cpu_root}/${cpu}/cpufreq"
  printf 'amd-pstate-epp\n' > "${cpufreq}/scaling_driver"
  printf 'powersave\n' > "${cpufreq}/scaling_governor"
  printf 'performance powersave\n' > "${cpufreq}/scaling_available_governors"
  printf 'balance_performance\n' > "${cpufreq}/energy_performance_preference"
  printf 'default performance balance_performance balance_power power\n' > "${cpufreq}/energy_performance_available_preferences"
done

cat > "${fake_bin}/powerprofilesctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
state_file="${POWERPROFILE_STATE_FILE:?POWERPROFILE_STATE_FILE is required}"
case "${1:-}" in
  get)
    cat "${state_file}"
    ;;
  set)
    printf '%s\n' "${2:?profile is required}" > "${state_file}"
    ;;
  list)
    current="$(cat "${state_file}")"
    for profile in performance balanced power-saver; do
      marker=" "
      [ "$profile" = "$current" ] && marker="*"
      printf '%s %s:\n' "$marker" "$profile"
      printf '    Driver: test\n'
      if [ "$profile" = "performance" ]; then
        printf '    Degraded: no\n'
      fi
    done
    ;;
  *)
    exit 2
    ;;
esac
EOF
chmod +x "${fake_bin}/powerprofilesctl"
printf 'balanced\n' > "${tmp_dir}/powerprofile-state"

env_common=(
  "TRADING_CPU_SYSFS_ROOT=${cpu_root}"
  "TRADING_POWERPROFILESCTL=${fake_bin}/powerprofilesctl"
  "POWERPROFILE_STATE_FILE=${tmp_dir}/powerprofile-state"
)

env "${env_common[@]}" bash "${POLICY}" apply
env "${env_common[@]}" bash "${POLICY}" apply

[ "$(cat "${tmp_dir}/powerprofile-state")" = "performance" ]
for cpu in cpu0 cpu1; do
  [ "$(cat "${cpu_root}/${cpu}/cpufreq/energy_performance_preference")" = "performance" ]
  [ "$(cat "${cpu_root}/${cpu}/cpufreq/scaling_governor")" = "powersave" ]
done

verify_output="$(env "${env_common[@]}" bash "${POLICY}" verify)"
printf '%s\n' "$verify_output"
grep -q '^power_profile=performance$' <<<"$verify_output"
grep -q '^power_profile_degraded=no$' <<<"$verify_output"
grep -q '^amd_pstate_status=active$' <<<"$verify_output"
grep -q '^energy_performance_preference=performance (2/2)$' <<<"$verify_output"
grep -q '^scaling_governor=powersave (2/2)$' <<<"$verify_output"
grep -q '^intended_state=PASS$' <<<"$verify_output"

printf 'balanced\n' > "${tmp_dir}/powerprofile-state"
for cpu in cpu0 cpu1; do
  printf 'balance_performance\n' > "${cpu_root}/${cpu}/cpufreq/energy_performance_preference"
done
if env "${env_common[@]}" bash "${POLICY}" verify >/tmp/cpu-power-policy-unexpected-pass 2>&1; then
  cat /tmp/cpu-power-policy-unexpected-pass >&2
  echo "verify unexpectedly passed for balanced profile and EPP" >&2
  exit 1
fi

printf 'performance\n' > "${cpu_root}/cpu0/cpufreq/energy_performance_preference"
printf 'performance\n' > "${cpu_root}/cpu1/cpufreq/energy_performance_preference"
verify_without_ppd="$(env TRADING_CPU_SYSFS_ROOT="${cpu_root}" TRADING_POWERPROFILESCTL="${tmp_dir}/missing-powerprofilesctl" bash "${POLICY}" verify)"
printf '%s\n' "$verify_without_ppd"
grep -q '^power_profile=unavailable$' <<<"$verify_without_ppd"
grep -q '^energy_performance_preference=performance (2/2)$' <<<"$verify_without_ppd"
grep -q '^intended_state=PASS$' <<<"$verify_without_ppd"
