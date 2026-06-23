# ROCm Acceleration Profile

The default runtime remains CPU-only. ROCm is an opt-in inference/training
backend for AMD Strix Halo / `gfx1151` hosts such as bart.

This profile does not change broker routing, execution gates, kill-switch
behavior, order submission, or live arming. It only changes the torch device
selected by model code that already uses torch.

## Supported Matrix

| Runtime | Python | Dependency profile | Support status |
| --- | --- | --- | --- |
| Standard host venv (`.venv`, install scripts, local toolchain) | 3.11.x | `cpu` | Supported CPU-only path. |
| Standard host venv (`.venv`, install scripts, local toolchain) | 3.11.x | `amd-rocm` | Not supported. The ROCm wheels are CPython 3.12 wheels, so the resolver exits with `amd_rocm_python_runtime_unsupported` before pip can silently skip them. |
| ROCm runtime container | 3.12.x | `amd-rocm` | Supported opt-in path when the container uses `rocm/pytorch:rocm7.2.4_ubuntu24.04_py3.12_pytorch_release_2.9.1`, imports HIP PyTorch, and sees at least one HIP device through the `torch.cuda` API. |

No CPython 3.11 ROCm wheel set is supported for this repository. The decision is
to standardize ROCm on AMD's Python 3.12 ROCm 7.2.4 container stream and keep the
Python 3.11 host venv CPU-only by design.

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
export TRADING_DEPENDENCY_PROFILE=amd-rocm
export RUNTIME_HARDWARE_PROFILE=amd-rocm
export TRADING_ACCELERATION_PROFILE=amd-rocm
export TORCH_DEVICE=auto
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
`rocm/pytorch:rocm7.2.4_ubuntu24.04_py3.12_pytorch_release_2.9.1`, builds the
runtime with the validated ROCm requirements profile, and sets
`TRADING_DEPENDENCY_PROFILE=amd-rocm`, `RUNTIME_HARDWARE_PROFILE=amd-rocm`,
`TRADING_ACCELERATION_PROFILE=amd-rocm`, and `TORCH_DEVICE=auto` for the runtime
container. The operator container receives no GPU devices. The dependency
resolver validates that any ROCm requirements file is the real Strix
Halo/gfx1151 ROCm 7.2.4 profile, and the runtime Dockerfile expands the
lightweight `requirements-amd-rocm.txt` selection to
`requirements-amd-rocm-full.txt` during installation.

## Runtime Detection

Startup calls `engine.runtime.acceleration.probe_torch_acceleration()` and logs
`runtime_acceleration_probe` with:

- `torch_cuda_is_available`
- `hip_version`
- `torch_cuda_device_count`
- `torch_cuda_devices`
- `effective_device`
- `fallback_reason`

PyTorch exposes ROCm devices through the `torch.cuda` API. Production model code
selects the ROCm GPU only when the dependency and runtime hardware profiles are
both `amd-rocm` and PyTorch reports a HIP build, `torch.cuda.is_available()`, and
a nonzero CUDA/HIP device count. If the `amd-rocm` dependency profile is selected
on Python 3.11, torch cannot be imported, torch is CPU-only, HIP is missing,
`/dev/kfd` is inaccessible, or no HIP device is visible, startup/preflight raises
`AccelerationProfileError` instead of falling back to CPU.

FinBERT, PatchTST, event embeddings, iTransformer, temporal predictors, and the
time-series foundation encoder use this runtime resolver, so the hard error is
enforced in production model code rather than only in tests or documentation.

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
prints the CPU benchmark and the reason GPU work was skipped; it is diagnostic
only and is not the production `amd-rocm` runtime contract. Use `--require-gpu`
inside the Python 3.12 ROCm container for a strict proof.

## Training / Batch Acceleration Profile

The live trading loop stays CPU-first by design (see the top of this document).
The place where the iGPU can actually pay off is *offline* work — training the
torch model families and large batch inference — which is throughput-bound
rather than latency-bound. That work runs under a separate, opt-in profile so it
is never coupled to the live runtime.

There are two ways to run the training/batch profile:

1. **Host venv (Python 3.12 ROCm container baseline).** Export the same
   acceleration env the runtime overlay uses, scoped to your batch/training
   shell, and run training or the benchmark directly:

   ```bash
   export TRADING_DEPENDENCY_PROFILE=amd-rocm
   export RUNTIME_HARDWARE_PROFILE=amd-rocm
   export TRADING_ACCELERATION_PROFILE=amd-rocm
   export TORCH_DEVICE=auto
   python tools/benchmark_model_acceleration.py --require-gpu --json
   ```

2. **One-shot Docker batch container.** Use the training overlay
   [`docker-compose.amd-rocm-training.yml`](../deploy/compose/docker-compose.amd-rocm-training.yml).
   It defines a non-arming `trainer` service (`profiles: [training]`,
   `ENGINE_SUPERVISED=0`, no broker, no secrets mounted) on the ROCm base image
   with `/dev/dri` + `/dev/kfd` mapped and the render/video GIDs added:

   ```bash
   export TRADING_RENDER_GID="$(getent group render | cut -d: -f3)"
   export TRADING_VIDEO_GID="$(getent group video | cut -d: -f3)"
   docker compose \
     --env-file deploy/compose/.env \
     -f deploy/compose/docker-compose.stack.yml \
     -f deploy/compose/docker-compose.amd-rocm-training.yml \
     --profile training run --rm trainer
   ```

   The default command runs the benchmark below. Override the command to run a
   real training job instead.

## Real Training Driver

[`tools/train_torch_models_gpu.py`](../tools/train_torch_models_gpu.py) exercises
the *production* train/serve wrappers (`PatchTSTRegressor`,
`ITransformerRegressor`) through their real `.fit()` path on whatever device the
runtime resolver selects. It is the "real training" command for the `trainer`
service and proves the production training code runs end-to-end on the iGPU —
not just a raw module forward pass:

```bash
# CPU host: trains on CPU, prints loss convergence + timing.
python tools/train_torch_models_gpu.py --allow-missing-gpu --epochs 30

# ROCm container: fail unless training actually ran on the cuda/HIP device.
python tools/train_torch_models_gpu.py --require-gpu --epochs 30 --json
```

It is offline/batch only: no supervised runtime, no broker, no order paths, no
real secrets. It trains on locally generated synthetic data shaped to each
model's real resolved feature schema (the registry serving columns); `.fit()`
performs no DB writes and the optional `--save-dir` persists a local artifact
directory only. A falling `loss_initial → loss_final` confirms the wired path,
and `--require-gpu` additionally asserts `ran_on_cuda` so a silent CPU fallback
fails the run.

## Benchmark Harness (CPU vs ROCm)

Before moving any workload to the GPU, get hard numbers with
[`tools/benchmark_model_acceleration.py`](../tools/benchmark_model_acceleration.py).
It builds the real `PatchTST` and `ITransformer` networks and the real FinBERT
pipeline, then times each on the CPU and (when a HIP device is visible) on the
GPU through the same `probe_torch_acceleration` gate the runtime uses:

```bash
# CPU-only machine / CI: prints the CPU baseline and why GPU was skipped.
python tools/benchmark_model_acceleration.py --allow-missing-gpu

# ROCm 7.2.4 container on bart: strict CPU-vs-GPU proof, machine readable.
python tools/benchmark_model_acceleration.py --require-gpu --json
```

It reports, per workload, per-iteration **inference** and **train**
(forward+backward+step) timings for PatchTST/iTransformer and **inference**
timing for FinBERT (inference-only in the runtime), plus the CPU/GPU speedup
ratio and peak GPU memory. A `speedup` above `1.0` means the GPU beats the CPU
for that specific workload and shape; decide per workload rather than wholesale.
Tune the workload to your real training shapes with `--batch`, `--seq-len`,
`--features`, `--d-model`, `--layers`, and `--repeat`. Skip the FinBERT download
on disconnected hosts with `--skip finbert` (or pin a local model with
`--finbert-model <path> --finbert-local-only`).

Recommended decision rule: only move a model family's training or batch
inference onto the iGPU when the benchmark shows a stable, repeatable speedup
**and** the ROCm kernels for the relevant ops do not fall back or error on
`gfx1151`. Small live-serving batches (a handful of symbols) usually do **not**
clear that bar once host↔device transfer overhead is counted — keep them on CPU.

Both tools are covered by
[`tests/ops/test_model_acceleration_benchmark.py`](../tests/ops/test_model_acceleration_benchmark.py).
The default lane runs them in `--allow-missing-gpu` mode (CPU baseline, clean
JSON, GPU scope skipped not faked); the `requires_rocm`-marked lane runs the
strict `--require-gpu` path and asserts a real speedup / `ran_on_cuda` only when
a ROCm GPU is visible, matching the existing
`tools/validate_rocm_acceleration.py` CI gate.

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
