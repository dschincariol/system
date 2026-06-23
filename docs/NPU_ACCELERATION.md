# XDNA2 NPU (Ryzen AI) Acceleration Profile

The Minisforum MS-S1 (AMD Ryzen AI Max+ 395 / Strix Halo) carries an XDNA2 NPU
in addition to the Radeon 8060S iGPU. This document covers enabling and using the
NPU. For the GPU, see [ROCM_ACCELERATION.md](ROCM_ACCELERATION.md).

The NPU is an **INT8 inference accelerator**. It is reached through a completely
different stack than the ROCm GPU and is **not a torch device**:

```text
amdxdna kernel driver + firmware  ->  XRT / xdna userspace  ->
ONNX Runtime + VitisAI execution provider  ->  INT8-quantized ONNX model
```

Like the ROCm profile, the NPU does not change broker routing, execution gates,
kill-switch behavior, order submission, or live arming. It only changes which
inference backend an NLP model selects, and it is strictly opt-in and
fail-closed.

## Why the NPU (and what to run where)

This machine has three compute tiers; the optimal split assigns each the work it
is actually good at:

| Tier | Workload | Precision |
| --- | --- | --- |
| **CPU** | The live decision / execution loop (latency- and I/O-bound). Unchanged. | FP32 |
| **GPU (ROCm)** | Training (PatchTST, iTransformer, FinBERT fine-tune), large-batch inference, backtest / Optuna / Monte-Carlo sweeps. | FP32 / FP16 |
| **NPU (XDNA2)** | Steady-state INT8 inference of the constant NLP load — FinBERT over the news / social / filings stream. | INT8 |

FinBERT is the prime NPU candidate: a small transformer that runs continuously,
is latency-tolerant, and quantizes cleanly to INT8 — so offloading it to the NPU
frees CPU/GPU at very low power. Training stays on the GPU (the NPU is
inference-only and INT8-only); live trading stays on the CPU.

## Current host state (bart, verified 2026-06-23)

Run `python tools/validate_npu_acceleration.py`. After this session's enablement:

- `kernel_ready = True` — `amdxdna` driver loaded, firmware present at
  `/lib/firmware/amdnpu/17f0_11`, `/dev/accel/accel0` present, `render`/`video`
  group access OK.
- `userspace_ready = True` — XRT installed from Ubuntu apt; `xrt-smi examine`
  reports the NPU as `[0000:f7:00.1] RyzenAI-npu5`, NPU firmware `1.1.2.65`.
- `inference_ready = False` — only the **VitisAI execution provider** remains,
  and it is gated behind AMD's Ryzen AI Linux release (see step 3).

So both the kernel and the XRT userspace are done; the single remaining piece is
AMD's VitisAI EP, which is not in any public package repo.

## Enablement Runbook

### 1. XRT userspace (done on bart — Ubuntu 26.04 packages it)

Ubuntu 26.04 ships XRT with NPU support in apt, so no source build is needed
(skip `xrt-xocl-dkms` — that is the Alveo PCIe driver, not the in-kernel NPU):

```bash
sudo apt-get update
sudo apt-get install -y libxrt2 libxrt-npu2 libxrt-utils libxrt-utils-npu python3-xrt
xrt-smi examine        # lists [0000:f7:00.1] RyzenAI-npu5
```

(If a future host lacks the apt packages, build from `github.com/amd/xdna-driver`
instead, which provides `/opt/xilinx/xrt`. The validator detects either layout.)

### 2. Locked-memory limit (REQUIRED)

The NPU pins memory for the device; the default 8 MB `RLIMIT_MEMLOCK` makes
`xrt-smi`/onnxruntime mmap fail with `EAGAIN (-11)`. Lift it for the runtime
user (done on bart via `/etc/security/limits.d/90-amd-npu.conf`):

```bash
sudo tee /etc/security/limits.d/90-amd-npu.conf >/dev/null <<'LIM'
david    soft memlock unlimited
david    hard memlock unlimited
@trading soft memlock unlimited
@trading hard memlock unlimited
LIM
# Re-login (or `sudo -i -u david`) so PAM applies it; verify with `ulimit -l`.
```

For containers, the NPU compose overlay sets `ulimits: memlock: -1`; for systemd
services add `LimitMEMLOCK=infinity`.

### 3. ONNX Runtime with the VitisAI execution provider (AMD-gated)

The export/quantize pieces and a CPU-EP fallback come from the repo profile:

```bash
pip install -r requirements-amd-npu.txt   # onnx, onnxruntime, optimum + optimum-onnx
```

The **VitisAI EP itself is not on PyPI or apt** — the public onnxruntime exposes
only `['AzureExecutionProvider','CPUExecutionProvider']`. The EP ships with AMD's
Ryzen AI Software for Linux (early access; `onnxruntime-vitisai` / `voe`,
registration required). Install that wheel, then:

```bash
python -c "import onnxruntime as o; print('VitisAIExecutionProvider' in o.get_available_providers())"
```

Until then the FinBERT ONNX backend runs correctly on the CPU EP and will switch
to the NPU automatically once the EP is present (the resolver is fail-closed).

### 4. Export + quantize FinBERT for the NPU

```bash
# Dynamic INT8 (default): accurate, weights-only — this is what the CPU-EP
# fallback serves today (verified: correct FinBERT sentiment).
python tools/export_finbert_onnx.py --model ProsusAI/finbert --output-dir artifacts/finbert_onnx
# -> writes model.onnx (FP32) and model.int8.onnx (dynamic INT8)
```

`--quant` selects the path:

- `dynamic` **(default)** — weights-only, accurate, what runs on the CPU EP today.
- `static` — the **NPU-target format** (quantizes weights AND activations from a
  calibration set, which the NPU needs). It prefers AMD's `vai_q_onnx`/Quark when
  the Ryzen AI stack is installed (transformer-aware → accurate). **Caveat
  (measured):** without `vai_q_onnx`, it falls back to ONNX Runtime's generic
  `quantize_static`, which **noticeably degrades FinBERT accuracy** (BERT-class
  activation outliers) — the tool prints an `accuracy_warning` in that case. So
  only run `--quant static` once `vai_q_onnx` is present, or verify accuracy.
- `none` — FP32 only.

Static mode uses a built-in financial-text calibration set; pass
`--calibration-file <one-text-per-line>` with representative production text for
best accuracy. Export needs torch>=2.6 (the ROCm container qualifies); static
quantization itself does not, so `--from-onnx <existing fp32 model>` (re)quantizes
without re-exporting.

### 5. Verify and switch on

```bash
export FINBERT_ONNX_PATH="$PWD/artifacts/finbert_onnx/model.int8.onnx"
export FINBERT_BACKEND=onnx-vitisai
python tools/validate_npu_acceleration.py --require-ready   # exit 0 when fully ready
```

## Repo integration (already in place)

The repo-side wiring is built and inert until the userspace lands:

- **Runtime probe + resolver** — [`engine/runtime/npu.py`](../engine/runtime/npu.py).
  `probe_npu_stack()` reports the layered readiness; `resolve_nlp_backend()` is
  **fail-closed** — it returns the NPU backend only when it is both opted-in and
  fully installed, otherwise the CPU torch path with a reason. The default
  runtime never reaches the NPU branch.
- **ONNX/VitisAI FinBERT backend** —
  [`engine/data/finbert_onnx_backend.py`](../engine/data/finbert_onnx_backend.py).
  Builds an ORT session with `VitisAIExecutionProvider` first and
  `CPUExecutionProvider` as the fail-closed floor.
- **Production seam** — `engine/data/finbert_sentiment.py` consults
  `resolve_nlp_backend()` at the top of `_probabilities_for_texts`; on any NPU
  error it logs nonfatally and falls back to torch. Enforced in runtime code, not
  just docs.
- **Dependency profile** —
  [`requirements-amd-npu.txt`](../requirements-amd-npu.txt) (additive to CPU/ROCm).
- **Export tool** — [`tools/export_finbert_onnx.py`](../tools/export_finbert_onnx.py).
- **Validator** — [`tools/validate_npu_acceleration.py`](../tools/validate_npu_acceleration.py).
- **Compose overlay** —
  [`docker-compose.amd-npu.yml`](../deploy/compose/docker-compose.amd-npu.yml)
  (non-arming `npu-inference` batch service, `profiles: [npu]`, maps
  `/dev/accel/accel0`).

## Opt-in switches

| Variable | Effect |
| --- | --- |
| `TRADING_NPU_INFERENCE=1` or `FINBERT_BACKEND=onnx-vitisai` | Opt into NPU inference (still fail-closed on readiness). |
| `FINBERT_ONNX_PATH=<path>` | Quantized ONNX model to serve. |
| `VITISAI_CONFIG=<path>` | Optional VitisAI EP config file (`vaip_config.json`). |

## Caveats

- The NPU is inference-only and INT8-only; training and FP paths belong on the
  GPU. Never put the NPU on the live order path.
- Linux NPU support is newer than the GPU path: AMD's Ryzen AI is Windows-first,
  and the Linux EP (`onnxruntime-vitisai`) is preview-grade. Validate after any
  XRT/driver/EP upgrade and expect occasional regressions.
- Static INT8 calibration matters for NPU op coverage; ops the VitisAI compiler
  cannot place fall back to CPU within ONNX Runtime, eroding the NPU win. Profile
  with `xrt-smi` after enabling.
