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
