# Dependency Profiles

The default runtime dependency profile is CPU-only:

```bash
TRADING_DEPENDENCY_PROFILE=cpu
RUNTIME_HARDWARE_PROFILE=cpu
TORCH_DEVICE=cpu
EMBED_DEVICE=cpu
NLP_DEVICE=cpu
FINBERT_DEVICE=cpu
TS_FOUNDATION_DEVICE=cpu
NVIDIA_TELEMETRY_ENABLED=0
GPU_THROTTLE_ENABLE=0
PINNED_ENABLE=0
PINNED_PREFETCH=0
TORCH_ALLOW_TF32=0
CUDNN_ALLOW_TF32=0
CUDNN_BENCHMARK=0
python -m pip install -r requirements.txt
```

`requirements.txt` installs `requirements-base.txt` plus the PyTorch CPU wheel.
It does not include `pynvml`, `nvidia-ml-py`, any `nvidia-*` package, or the
CUDA PyTorch wheel. The CPU base profile uses `xgboost-cpu` so a normal Linux
install does not pull `nvidia-nccl-cu12` through the default XGBoost package.
Postgres access is standardized on psycopg 3.x (`psycopg[binary,pool]` plus
`psycopg-pool`); do not add `psycopg2` or `psycopg2-binary` to any dependency
profile.

## Profiles

| Profile | Requirements file | Purpose |
| --- | --- | --- |
| `cpu` | `requirements.txt` | Default live/runtime install. Uses PyTorch CPU wheels and keeps NVIDIA telemetry disabled. |
| `nvidia-cuda` | `requirements-nvidia-cuda.txt` | Explicit NVIDIA deployment profile. Installs CUDA PyTorch plus `pynvml` and `nvidia-ml-py` for NVIDIA diagnostics. |
| `amd-rocm` | `requirements-amd-rocm.txt` | Opt-in AMD ROCm deployment profile validated for the Strix Halo/gfx1151 host path on `bart`. Defaults remain CPU unless the dependency and runtime acceleration profiles are explicitly selected. |

Installers select a profile through `deploy/bin/resolve_python_requirements.sh`.
Docker, `deploy/bin/install_python_env.sh`, `deploy/bin/upgrade_trading_system.sh`,
`deploy/install_trading_system.sh`, `ops/server/bootstrap.sh`, and
`tools/bootstrap_local_toolchain.sh` all use that resolver.
`start_system.py`, `start_ingestion.py`, health snapshots, and production
preflight log/report the selected dependency profile, hardware profile,
resolved devices, disabled-accelerator reason, and NVIDIA telemetry state.

## NVIDIA CUDA

Use NVIDIA acceleration only when the host has working NVIDIA drivers, a CUDA
runtime compatible with the selected PyTorch wheel, and container/device access
for the runtime process.

```bash
export TRADING_DEPENDENCY_PROFILE=nvidia-cuda
export RUNTIME_HARDWARE_PROFILE=nvidia
export TORCH_DEVICE=auto
export NVIDIA_TELEMETRY_ENABLED=1
python -m pip install -r requirements-nvidia-cuda.txt
python - <<'PY'
from engine.runtime.hardware import runtime_hardware_snapshot
print(runtime_hardware_snapshot())
PY
python engine/runtime/prod_preflight.py --json
```

CUDA is enabled only when both the runtime hardware profile and dependency
profile are NVIDIA-specific and `torch.cuda.is_available()` succeeds. NVIDIA
telemetry imports stay cold unless the same NVIDIA profile pair is selected and
`NVIDIA_TELEMETRY_ENABLED=1`. CUDA backend fast paths (`GPU_THROTTLE_ENABLE`,
`PINNED_ENABLE`, `PINNED_PREFETCH`, `TORCH_ALLOW_TF32`, `CUDNN_ALLOW_TF32`, and
`CUDNN_BENCHMARK`) default to `0`; enable them only in an accelerator-specific
runtime config that has passed preflight.

## AMD GPU/NPU

Use AMD ROCm acceleration only on hosts that match the validated ROCm wheel,
driver, device-permission, and Python runtime assumptions in
[ROCM_ACCELERATION.md](ROCM_ACCELERATION.md). The checked-in profile is opt-in:

```bash
export TRADING_DEPENDENCY_PROFILE=amd-rocm
export RUNTIME_HARDWARE_PROFILE=amd-rocm
export TRADING_ACCELERATION_PROFILE=amd-rocm
python -m pip install -r requirements-amd-rocm.txt
python tools/validate_rocm_acceleration.py --json
python engine/runtime/prod_preflight.py --json
```

PyTorch exposes ROCm devices through its `torch.cuda` API, so the runtime
acceleration resolver uses CUDA/HIP availability checks for the `amd-rocm`,
`rocm`, and `hip` profile aliases. The AMD NPU is not part of this profile.
Rollback is the same CPU reset shown below: set the dependency and runtime
profiles back to `cpu`, reinstall `requirements.txt`, and rerun preflight.

## Verification

Use these commands after any dependency profile change:

```bash
python tools/validate_dependency_lock.py
python -m pytest tests/test_dependency_profiles.py tests/test_runtime_hardware.py -q
python - <<'PY'
from engine.runtime.hardware import runtime_hardware_snapshot
print(runtime_hardware_snapshot())
PY
python engine/runtime/prod_preflight.py --json
```

For Docker builds, set the same profile before building:

```bash
TRADING_DEPENDENCY_PROFILE=cpu docker compose -f deploy/compose/docker-compose.stack.yml build runtime
TRADING_DEPENDENCY_PROFILE=amd-rocm docker compose -f deploy/compose/docker-compose.stack.yml -f deploy/compose/docker-compose.amd-rocm.yml build runtime
```

## Rollback To CPU

Reset both dependency and device profiles, rebuild or reinstall dependencies,
and rerun preflight:

```bash
export TRADING_DEPENDENCY_PROFILE=cpu
export RUNTIME_HARDWARE_PROFILE=cpu
export TORCH_DEVICE=cpu
export EMBED_DEVICE=cpu
export NLP_DEVICE=cpu
export FINBERT_DEVICE=cpu
export TS_FOUNDATION_DEVICE=cpu
export NVIDIA_TELEMETRY_ENABLED=0
export GPU_THROTTLE_ENABLE=0
export PINNED_ENABLE=0
export PINNED_PREFETCH=0
export TORCH_ALLOW_TF32=0
export CUDNN_ALLOW_TF32=0
export CUDNN_BENCHMARK=0
python -m pip install -r requirements.txt
python engine/runtime/prod_preflight.py --json
```
