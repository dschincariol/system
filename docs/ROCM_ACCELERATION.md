# ROCm Acceleration Profile

The default runtime remains CPU-only. ROCm is an opt-in inference/training
backend for AMD Strix Halo / `gfx1151` hosts such as bart.

This profile does not change broker routing, execution gates, kill-switch
behavior, order submission, or live arming. It only changes the torch device
selected by model code that already uses torch.

## Package Profile

Use [requirements-amd-rocm.txt](../requirements-amd-rocm.txt) when building or
installing the ROCm profile on the Python 3.12 ROCm runtime baseline:

```bash
python3.12 -m venv .venv-rocm
.venv-rocm/bin/python -m pip install --upgrade pip setuptools wheel
.venv-rocm/bin/python -m pip install -r requirements-amd-rocm-full.txt
```

The profile pins:

- `torch==2.9.1+rocm7.2.4.lw.git39497456`
- `torchaudio==2.9.0+rocm7.2.4.gite3c6ee2b`
- `triton==3.5.1+rocm7.2.4.gita272dfa8`
- `numpy==2.4.6`
- `scikit-learn==1.9.0`
- `lightgbm==4.6.0`
- `xgboost==2.1.4`

LightGBM uses its OpenCL-capable wheel when the host OpenCL runtime is present.
Upstream XGBoost GPU acceleration is CUDA-only, so the AMD profile deliberately
keeps the current CPU-safe XGBoost pin instead of claiming ROCm support that is
not available in the official Python package.

The checked-in profile follows AMD's Python 3.12 ROCm container stream. On bart,
`rocm/pytorch:rocm7.2.4_ubuntu24.04_py3.12_pytorch_release_2.9.1` successfully
ran a FP16 torch matmul on `Radeon 8060S Graphics`. The earlier CPython 3.11 /
ROCm 6.4 PyTorch wheel detected the device but failed the first kernel launch
with HIP `invalid device function`, so it is not considered host-validated for
`gfx1151`.

## Compose Runtime

The base compose stack does not mount GPU devices. Use the ROCm overlay only
when the host has working `/dev/kfd`, `/dev/dri`, and render/video group access:

```bash
export TRADING_REQUIREMENTS_FILE=requirements-amd-rocm.txt
export TRADING_ACCELERATION_PROFILE=amd-rocm
export TRADING_RENDER_GID="$(getent group render | cut -d: -f3)"
export TRADING_VIDEO_GID="$(getent group video | cut -d: -f3)"

docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  -f deploy/compose/docker-compose.amd-rocm.yml \
  up -d --build
```

The overlay maps `/dev/dri` and `/dev/kfd` into the runtime container and adds
the host render/video GIDs. It also switches only the runtime build base image to
`rocm/pytorch:rocm7.2.4_ubuntu24.04_py3.12_pytorch_release_2.9.1`. The operator
container receives no GPU devices.

## Runtime Detection

Startup calls `engine.runtime.acceleration.probe_torch_acceleration()` and logs
`runtime_acceleration_probe` with:

- `torch_cuda_is_available`
- `hip_version`
- `torch_cuda_device_count`
- `torch_cuda_devices`
- `effective_device`
- `fallback_reason`

PyTorch exposes ROCm devices through the `torch.cuda` API. The runtime chooses
GPU only when `TRADING_ACCELERATION_PROFILE=amd-rocm` or an explicit device
request is set. If torch is CPU-only, HIP is missing, `/dev/kfd` is inaccessible,
or no HIP device is visible, the effective device is `cpu` and startup
continues.

FinBERT and PatchTST both use this runtime resolver, so the fallback is enforced
in production model code rather than only in tests or documentation.

## Validation Harness

Run the standalone harness on the host or inside the ROCm runtime container:

```bash
python tools/validate_rocm_acceleration.py --require-gpu
python tools/validate_rocm_acceleration.py --require-gpu --json
```

It checks:

- `/dev/dri`, `/dev/dri/renderD128`, `/dev/kfd`, and `/dev/accel/accel0`
- membership in the `render` and `video` groups
- torch HIP availability and device count
- CPU and GPU matmul timing
- CPU and GPU PatchTST forward-pass timing

Use `--allow-missing-gpu` for CI or CPU-only developer machines. That mode still
prints the CPU benchmark and the reason GPU work was skipped.

## Host Caveats

On bart, the Radeon 8060S iGPU uses unified memory but still reports a small BIOS
UMA/VRAM carve-out. The current host posture is a 2 GB iGPU carve-out with about
66 GB GTT. Treat that as a practical memory-mapping limit for ROCm workloads,
not as proof that the entire 128 GB system memory pool is freely available to a
single torch process.

`gfx1151` ROCm support is still a moving target. The ROCm 7.2.4 container stream
is the host-validated path for bart; older CPython 3.11 / ROCm 6.4 wheels are
known to fail kernels on this device. Keep this profile opt-in, validate after
driver or wheel upgrades, and expect occasional regressions around device
discovery, memory accounting, and unsupported libraries.

The XDNA2 NPU (`/dev/accel/accel0`) is not used by this PyTorch ROCm profile.
The harness reports the device node for operator visibility only.
