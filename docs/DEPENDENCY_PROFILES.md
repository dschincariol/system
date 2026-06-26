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
TSFM_BENCHMARK_DEVICE=cpu
NVIDIA_TELEMETRY_ENABLED=0
GPU_THROTTLE_ENABLE=0
PINNED_ENABLE=0
PINNED_PREFETCH=0
TORCH_ALLOW_TF32=0
CUDNN_ALLOW_TF32=0
CUDNN_BENCHMARK=0
python -m pip install --require-hashes -r requirements.txt
```

`requirements.txt` is the CPU runtime install entrypoint. It applies
`requirements.lock.txt` as a transitive constraint file and installs the direct
runtime roots from `requirements.in`, which includes `requirements-base.txt` plus
the PyTorch CPU wheel. The checked-in lock carries SHA-256 hashes, including the
PyTorch CPU index wheel hashes, and CPU installs must use pip's
`--require-hashes` mode. It does not include `pytest`, `pytest-timeout`,
`pytest-cov`, `coverage`, `ruff`, `pyright`, `pynvml`, `nvidia-ml-py`, any
`nvidia-*` package, or the CUDA PyTorch wheel. The CPU base profile uses
`xgboost-cpu` so a normal Linux install does not pull `nvidia-nccl-cu12` through
the default XGBoost package. Postgres access is standardized on psycopg 3.x
(`psycopg[binary,pool]` plus `psycopg-pool`); do not add `psycopg2` or
`psycopg2-binary` to any dependency profile.

Production preflight also checks the running interpreter for direct runtime
dependency drift. `engine/runtime/prod_preflight.py` parses direct CPU roots
from `requirements.in`, reads expected pins from `requirements.lock.txt`, and
compares those names with `importlib.metadata.version()` in the live venv.
Safe/non-live runs report missing or mismatched direct packages as warnings;
live mode fails closed. Set `PROD_PREFLIGHT_REQUIRE_DEPENDENCY_LOCK_MATCH=1`
when a production host must enforce the same hard blocker before `ENGINE_MODE`
is switched to `live`. The diagnostic-only escape hatch is
`PROD_PREFLIGHT_SKIP_DEPENDENCY_DRIFT=1`; using it avoids the hard failure but
still records a warning and must not be used as go-live evidence.

Development and CI installs use `requirements-dev.txt`. That file applies
`requirements-dev.lock.txt` and installs direct dev/test roots from
`requirements-dev.in`, including pinned `pytest`, `pytest-timeout`,
`pytest-cov`, `coverage`, `ruff`, and `pyright`. The dev lock is also hashed;
CI installs it with `python -m pip install --require-hashes -r requirements-dev.txt`.

Optional exploratory tabular-foundation challengers are not part of any default
runtime profile. `tabm` and `tabpfn` live only in the
`trading-system[tabular-foundation]` pyproject extra, require an explicit license
review acknowledgement before real backends can train, and are blocked by
`tools/validate_dependency_lock.py --strict` if they appear in
`requirements-base.txt`, `requirements.in`, `requirements.txt`, or accelerator
runtime manifests. Keep `USE_TABULAR_CHALLENGERS=0` unless an offline shadow
experiment is intended.

## Profiles

| Profile | Requirements file | Purpose |
| --- | --- | --- |
| `cpu` | `requirements.txt` | Default live/runtime install. Uses PyTorch CPU wheels and keeps NVIDIA telemetry disabled. |
| `nvidia-cuda` | `requirements-nvidia-cuda.txt` | Explicit NVIDIA deployment profile. Installs CUDA PyTorch plus `pynvml` and `nvidia-ml-py` for NVIDIA diagnostics, constrained by `requirements-nvidia-cuda.lock.txt`. |
| `amd-rocm` | `requirements-amd-rocm.txt` | Opt-in AMD ROCm deployment profile validated only in the Python 3.12 ROCm container for the Strix Halo/gfx1151 host path on `bart`; Docker expands it to hash-constrained `requirements-amd-rocm-full.txt`. The Python 3.11 host venv is CPU-only and rejects this profile before install. |

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
python -m pip install --require-hashes -r requirements-nvidia-cuda.txt
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

## Lock Update Procedure

The lock update commands require `uv`; lock-backed runtime and CI installs still
use `python -m pip install --require-hashes -r ...` against the checked-in
manifests. Hash verification applies to the CPU runtime, dev/test install,
NVIDIA CUDA profile, and AMD ROCm full profile.

Use the direct input files for human edits:

- CPU runtime direct roots: `requirements.in` and `requirements-base.txt`.
- CI/dev/test direct roots: `requirements-dev.in`.
- Accelerator profile roots: `requirements-nvidia-cuda.txt`,
  `requirements-amd-rocm.txt`, and the standalone ROCm host file
  `requirements-amd-rocm-full.txt`.

After changing CPU runtime roots, regenerate the runtime lock:

```bash
uv pip compile requirements.in \
  --python-version 3.11 \
  --python-platform x86_64-manylinux_2_36 \
  --torch-backend cpu \
  --index-strategy unsafe-best-match \
  --emit-index-url \
  --generate-hashes \
  --output-file requirements.lock.txt
```

After changing runtime roots or dev/test tools, regenerate the dev/test lock:

```bash
uv pip compile requirements-dev.in \
  --python-version 3.11 \
  --python-platform x86_64-manylinux_2_36 \
  --torch-backend cpu \
  --index-strategy unsafe-best-match \
  --emit-index-url \
  --generate-hashes \
  --output-file requirements-dev.lock.txt
```

After changing the NVIDIA CUDA profile roots, regenerate the CUDA profile lock:

```bash
uv pip compile requirements-nvidia-cuda.txt \
  --python-version 3.11 \
  --python-platform x86_64-manylinux_2_36 \
  --index-strategy unsafe-best-match \
  --emit-index-url \
  --generate-hashes \
  --output-file requirements-nvidia-cuda.lock.txt
```

After changing the AMD ROCm full profile roots, regenerate the ROCm profile lock
from the Python 3.12 ROCm baseline manifest:

```bash
uv pip compile requirements-amd-rocm-full.txt \
  --python-version 3.12 \
  --python-platform x86_64-manylinux_2_36 \
  --index-strategy unsafe-best-match \
  --emit-index-url \
  --generate-hashes \
  --output-file requirements-amd-rocm.lock.txt
```

The `unsafe-best-match` resolver mode is intentional here because the pip
install path uses PyTorch's CPU/CUDA wheel indexes as extra indexes where
applicable, and the ROCm lock hashes AMD's direct ROCm wheel URLs. Review
dependency name changes carefully before regenerating locks. Do not hand-edit
lock files except to resolve a reviewed merge conflict; regenerate them with
`uv pip compile` afterwards.

Verify dependency changes before opening a PR:

```bash
python tools/validate_dependency_lock.py --strict
python -m pip install --require-hashes -r requirements-dev.txt
python -m pytest tests/test_dependency_lock_contract.py tests/test_dependency_profiles.py -q
```

CI runs `python tools/validate_dependency_lock.py --strict` before installing
Python packages, then installs `requirements-dev.txt` with `--require-hashes`.
The install fails if a direct requirement no longer matches the checked-in dev
lock or any locked artifact hash is missing. The full `python tools/validate_repo.py`
gate also runs the strict dependency-lock check.

The dependency-lock validator also compares shared scientific/runtime pins across
`requirements-base.txt`, the CPU runtime manifest and lock, the NVIDIA CUDA
manifest and lock, and the AMD ROCm full manifest and lock. The compared set
includes `numpy`, `pandas`, `scipy`, `scikit-learn`, `statsmodels`, `numba`,
`llvmlite`, `pyarrow`, `pydantic`, `pydantic-core`, `SQLAlchemy`, `lightgbm`,
`sentence-transformers`, `transformers`, `huggingface-hub`, `tokenizers`,
`safetensors`, and `joblib`. `xgboost` is excluded because the CPU profile pins
the separate `xgboost-cpu` distribution.

Any scientific-stack divergence must be listed in `SHARED_PIN_ALLOWLIST` with
package, profile, expected version, and reason. The validator only downgrades a
divergence to `cross_profile_pin_allowlisted` when the actual profile version
matches that expected version; stale or widened divergences fail as
`cross_profile_pin_mismatch`. Regenerate every affected profile lock after
changing shared pins, then rerun `python tools/validate_dependency_lock.py
--strict`. Allowlisted ROCm divergences are warnings, not errors, and model
artifacts must be re-validated on that profile before promotion or production
use.

## AMD GPU/NPU

Use AMD ROCm acceleration only on hosts that match the validated ROCm wheel,
driver, device-permission, and Python runtime assumptions in
[ROCM_ACCELERATION.md](ROCM_ACCELERATION.md). The supported matrix is explicit:
the standard Python 3.11 host venv is CPU-only, and the `amd-rocm` profile is
supported only inside the Python 3.12 ROCm 7.2.4 container. The checked-in
profile is opt-in:

```bash
export TRADING_DEPENDENCY_PROFILE=amd-rocm
export RUNTIME_HARDWARE_PROFILE=amd-rocm
export TRADING_ACCELERATION_PROFILE=amd-rocm
export TORCH_DEVICE=auto
python3.12 -m pip install --require-hashes -r requirements-amd-rocm-full.txt
python tools/validate_rocm_acceleration.py --json
python engine/runtime/prod_preflight.py --json
```

PyTorch exposes ROCm devices through its `torch.cuda` API, so the runtime
acceleration resolver uses HIP version, `torch.cuda.is_available()`, and device
count checks for the `amd-rocm`, `rocm`, and `hip` profile aliases. The AMD NPU
is not part of this profile. The resolver rejects placeholder ROCm requirement
files and also rejects `amd-rocm` when the active installer Python cannot satisfy
the ROCm wheel markers (`Linux` and Python `>=3.12`). Docker builds may select
`requirements-amd-rocm.txt`; `deploy/compose/Dockerfile.runtime` expands that
overlay to hash-verified `requirements-amd-rocm-full.txt` for the actual
install. At runtime,
`engine.runtime.acceleration` and `engine.runtime.hardware` raise
`AccelerationProfileError` if `amd-rocm` is selected without an importable HIP
torch build and a visible HIP device, preventing a silent CPU fallback. Rollback
is the same CPU reset shown below: set the dependency and runtime profiles back
to `cpu`, reinstall `requirements.txt`, and rerun preflight.

## Verification

Use these commands after any dependency profile change:

```bash
python tools/validate_dependency_lock.py --strict
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

## NLP Text Embeddings

The financial text embedding upgrade does not add a new required dependency.
`NEWS_EMBED_BACKEND=financial_sentence_transformer` / `NLP_EMBED_BACKEND=financial_sentence_transformer`
reuse the existing `sentence-transformers` package and expect the selected
finance-domain model to be available in the local model cache when
`NEWS_EMBED_LOCAL_FILES_ONLY=1`. Operator model-card/license review is enforced
with `NLP_FINANCIAL_EMBED_LICENSE_REVIEW_ACK=1`; otherwise the backend degrades
according to `NEWS_EMBED_FALLBACK_POLICY` without mixing cache namespaces.

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
export TSFM_BENCHMARK_DEVICE=cpu
export NVIDIA_TELEMETRY_ENABLED=0
export GPU_THROTTLE_ENABLE=0
export PINNED_ENABLE=0
export PINNED_PREFETCH=0
export TORCH_ALLOW_TF32=0
export CUDNN_ALLOW_TF32=0
export CUDNN_BENCHMARK=0
python -m pip install --require-hashes -r requirements.txt
python engine/runtime/prod_preflight.py --json
```
