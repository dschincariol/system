# CPU Power Policy

Host `bart` is a plugged-in workstation for the always-on trading runtime. The
production policy is to prefer sustained CPU clocks and lower scheduling latency
over minimum watts.

The boot-enforced setting is:

```bash
TRADING_CPU_POWER_PROFILE=performance
TRADING_CPU_EPP=performance
```

`ops/server/bootstrap.sh` installs and enables
`trading-cpu-power-policy.service`. The service runs
`/opt/trading/app/ops/server/cpu_power_policy.sh apply` before
`trading.target`. The script is idempotent:

- use `powerprofilesctl set performance` when power-profiles-daemon exposes a
  non-degraded `performance` profile;
- set each CPU `energy_performance_preference` sysfs file to `performance` when
  AMD EPP files are present;
- fall back to the `performance` scaling governor only when neither
  power-profiles-daemon nor EPP can apply the policy.

The trading target requires the policy service. A normal
`systemctl start trading.target` therefore fails instead of silently starting the
runtime under an unintended CPU power policy.

CPU power policy is separate from host memory-pressure hardening. A host can be
in the correct performance profile and still be NO-GO for live trading if
`/api/health.memory_pressure` or `python -m engine.runtime.memory_pressure
--json --required` reports undersized swap, missing zram, wrong swappiness, or
ZFS ARC drift. See [MEMORY_PRESSURE_RUNBOOK.md](MEMORY_PRESSURE_RUNBOOK.md).

## Verify

The verifier is read-only and does not require sudo:

```bash
bash ops/server/cpu_power_policy.sh verify
```

Expected output on `bart` should include:

```text
power_profile=performance
power_profile_degraded=no
amd_pstate_status=active
energy_performance_preference=performance (.../...)
intended_state=PASS
```

The verifier also reports the active scaling driver and governor. With
`amd_pstate` active, the governor may still be `powersave`; the EPP value and
power-profiles-daemon profile are the deliberate performance controls for this
host.

## Drift Detection

Production preflight invokes the same read-only verifier through
`engine.runtime.cpu_power_policy.verify_cpu_power_policy()`. The preflight gate
does not re-apply the boot policy; it only detects the current state.

The advisory-vs-blocking rule is:

- `cpu_power_policy_drift` is blocking. If the verifier can see the host CPU
  controls and reports profile/EPP/governor drift away from the performance
  target, `prod_preflight.py` returns non-zero with a `cpu_power_policy` JSON
  section showing the parsed profile, EPP/governor state, `status=drift`, and
  `reason=cpu_power_policy_drift`.
- `cpu_power_policy_unavailable` is advisory when the policy is not required.
  Generic CI runners and development containers often expose no
  power-profiles-daemon or cpufreq sysfs state, so preflight reports a warning
  instead of blocking.
- Any non-drift failure is blocking when `PREFLIGHT_REQUIRE_CPU_POWER_POLICY=1`
  or any runtime mode is `live`; the systemd production preflight unit and
  bootstrap-generated `/etc/trading/trading.env` set the required flag for
  `bart`.

Staging harnesses are advisory by default because generic CI runners do not
expose the target host CPU policy. Set `PREFLIGHT_REQUIRE_CPU_POWER_POLICY=1`
in the staging env file only when the staging target is the real host class and
its cpufreq/power-profiles state is visible.

The recurring `observability_snapshot` job also runs the verifier without
repairing state. It records component health under `cpu_power_policy` every
snapshot interval; drift reports `ok=false`, `status=drift`, and emits the
existing `component_health_ok` metric with `observed_component=cpu_power_policy`.
If the check is not required and no host CPU controls are visible, the
component is marked `skipped` rather than failing generic development health.

## Thread-Pool Caps

CPU performance mode does not permit each supervised Python process to consume a
full host-sized BLAS or torch pool. Runtime startup applies
`TRADING_CPU_THREAD_POLICY=auto` through `engine.runtime.thread_policy` before
supervised child processes import NumPy/BLAS/NumExpr/torch. The policy sets
`OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `OPENBLAS_NUM_THREADS`,
`NUMEXPR_NUM_THREADS`, `TORCH_CPU_THREADS`, and `TORCH_INTEROP_THREADS` from the
process role and total supervised process count:

- `runtime` and `ingestion` parents receive small pools for orchestration work;
- `ingestion_child` feed processes are capped at one CPU thread by default;
- `inference` jobs get a small inference pool when capacity is available;
- `training` and `offline` roles can use larger pools, still bounded by the
  per-process budget.

Operators can set `TRADING_CPU_THREADS_PER_PROCESS` and
`TRADING_TORCH_INTEROP_THREADS_PER_PROCESS` for an explicit uniform cap. Set
`TRADING_CPU_THREAD_POLICY=manual` only when fixed thread variables are
intentional overrides and the operator has checked aggregate process
oversubscription. `/api/health` exposes the effective policy under
`runtime_hardware.thread_policy`.

## Trade-Off

The performance profile and EPP `performance` bias the CPU toward faster
frequency response and higher sustained clocks. That is appropriate for an
always-on, latency-sensitive trading runtime where scheduler wake-up latency and
burst processing matter.

The cost is higher idle and average power draw, more heat, potentially louder
fans, and less thermal headroom for simultaneous CPU-heavy backfills or GPU
workloads. Operators should watch package temperature, fan behavior, and
throttling evidence during soak. If thermal throttling appears, fix cooling or
capacity first rather than masking it with ad hoc profile changes.

## ROCm/GPU Composition

This policy only touches CPU power-profiles-daemon state and CPU cpufreq sysfs
files under `/sys/devices/system/cpu`. It does not set GPU clocks, GPU power
limits, ROCm SMI settings, container device access, or model accelerator
profiles.

If T1.1 enables a ROCm GPU workload, keep the GPU thermal/power controls in the
ROCm-specific deployment layer. CPU performance mode may increase total chassis
heat, so the combined CPU+GPU soak must verify that GPU limits, fan curves, and
runtime throttling remain within the reviewed envelope. Do not add a GPU power
cap to `trading-cpu-power-policy.service`; keep that ownership separate so CPU
latency policy and GPU safety policy do not fight each other.

## Revert

To revert `bart` to the distro balanced policy:

```bash
sudo systemctl disable --now trading-cpu-power-policy.service
sudo powerprofilesctl set balanced
for f in /sys/devices/system/cpu/cpu*/cpufreq/energy_performance_preference; do
  [ -e "$f" ] && printf '%s\n' balance_performance | sudo tee "$f" >/dev/null
done
```

Then remove the ordering requirement before restarting the trading target, or
deploy a reviewed change that sets `TRADING_CPU_POWER_PROFILE=balanced` and
`TRADING_CPU_EPP=balance_performance` in the policy service environment.
