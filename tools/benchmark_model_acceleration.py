from __future__ import annotations

"""CPU vs ROCm/CUDA benchmark for the torch model families used by the runtime.

This harness exists to answer one question with hard numbers: does moving the
torch model families (PatchTST, iTransformer, FinBERT) onto the AMD iGPU via the
opt-in ROCm profile actually beat the CPU-first default for *training* and
*batch inference*?

It is diagnostic tooling only. It does not change broker routing, execution
gates, kill-switch behavior, order submission, or live arming. It builds the
real model modules (the same ``PatchTST`` / ``ITransformer`` networks the
runtime trains, and the real FinBERT pipeline) and times them on the CPU and,
when a HIP/ROCm device is visible, on the GPU.

PyTorch exposes ROCm devices through the ``torch.cuda`` API, so the GPU scope
uses the ``cuda`` device string even on AMD hosts. Device availability is probed
through ``engine.runtime.acceleration.probe_torch_acceleration`` so this tool
uses the exact same gating contract as the production runtime.

Examples
--------
    # CPU-only developer machine / CI: prints the CPU baseline and explains why
    # the GPU scope was skipped. Exit code 0.
    python tools/benchmark_model_acceleration.py --allow-missing-gpu

    # Inside the ROCm 7.2.4 container on bart: strict proof, machine readable.
    python tools/benchmark_model_acceleration.py --require-gpu --json
"""

import argparse
import json
import os
import sys
import time
import warnings
from typing import Any, Callable, Dict, List, Optional

# torch emits a benign nested-tensor notice for norm_first encoders; it is noise here.
warnings.filterwarnings("ignore", message="enable_nested_tensor is True")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engine.runtime.acceleration import (  # noqa: E402
    AccelerationProfileError,
    probe_torch_acceleration,
)

# Representative financial headlines so the FinBERT tokenizer produces realistic
# token counts instead of trivial single-token batches.
_FINBERT_SAMPLE_TEXTS = [
    "Quarterly earnings beat analyst expectations as margins expanded sharply.",
    "The central bank signaled a pause, citing cooling inflation and soft demand.",
    "Shares slumped after the company cut full-year guidance on weak orders.",
    "Regulators opened an antitrust probe into the proposed acquisition.",
    "Crude prices rallied on supply disruption fears and a weaker dollar.",
    "The firm announced a buyback and raised its dividend for the eighth year.",
    "Default risk rose as the issuer missed a coupon payment on senior notes.",
    "Strong jobs report pushed yields higher and pressured rate-sensitive sectors.",
]


def _sync(torch: Any, device: str) -> None:
    """Best-effort device sync so GPU timings are not measured before kernels run."""
    if str(device).startswith("cuda"):
        cuda = getattr(torch, "cuda", None)
        if cuda is not None:
            try:
                cuda.synchronize()
            except Exception:
                pass  # no-op-guard: allow - synchronize is best-effort timing cleanup only.


def _time_ms(torch: Any, device: str, fn: Callable[[], Any], *, repeat: int, warmup: int) -> Dict[str, Any]:
    for _ in range(max(0, int(warmup))):
        fn()
    _sync(torch, device)
    started = time.perf_counter()
    for _ in range(max(1, int(repeat))):
        fn()
    _sync(torch, device)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    iters = max(1, int(repeat))
    return {
        "repeat": int(iters),
        "elapsed_ms": round(float(elapsed_ms), 3),
        "per_iter_ms": round(float(elapsed_ms) / float(iters), 3),
    }


def _peak_gpu_mb(torch: Any, device: str) -> Optional[float]:
    if not str(device).startswith("cuda"):
        return None
    cuda = getattr(torch, "cuda", None)
    try:
        return round(float(cuda.max_memory_allocated()) / 1.0e6, 1)
    except Exception:
        return None


def _reset_gpu_mem(torch: Any, device: str) -> None:
    if not str(device).startswith("cuda"):
        return
    cuda = getattr(torch, "cuda", None)
    try:
        cuda.reset_peak_memory_stats()
    except Exception:
        pass  # no-op-guard: allow - peak-memory accounting is best-effort diagnostics only.


# --------------------------------------------------------------------------- #
# Workloads. Each returns {inference: {...}, train: {...}} timing dicts.
# --------------------------------------------------------------------------- #


def _bench_timeseries(
    torch: Any,
    device: str,
    *,
    builder: Callable[[], Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    import torch.nn as nn

    _reset_gpu_mem(torch, device)
    model = builder().to(device)
    x = torch.randn(
        (int(args.batch), int(args.seq_len), int(args.features)),
        dtype=torch.float32,
        device=device,
    )
    y = torch.randn((int(args.batch), int(args.horizons)), dtype=torch.float32, device=device)

    model.eval()

    def infer() -> Any:
        with torch.no_grad():
            return model(x)

    inference = _time_ms(torch, device, infer, repeat=int(args.repeat), warmup=int(args.warmup))

    model.train()
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)
    loss_fn = nn.MSELoss()

    def train_step() -> Any:
        optimizer.zero_grad(set_to_none=True)
        out = model(x)
        loss = loss_fn(out, y)
        loss.backward()
        optimizer.step()
        return loss

    train = _time_ms(torch, device, train_step, repeat=int(args.repeat), warmup=int(args.warmup))

    return {
        "skipped": False,
        "shape": {
            "batch": int(args.batch),
            "seq_len": int(args.seq_len),
            "features": int(args.features),
            "horizons": int(args.horizons),
        },
        "inference": inference,
        "train": train,
        "peak_gpu_mb": _peak_gpu_mb(torch, device),
    }


def _build_patchtst(args: argparse.Namespace) -> Callable[[], Any]:
    from engine.strategy.patchtst_core import PatchTST

    def builder() -> Any:
        seq_len = int(args.seq_len)
        return PatchTST(
            seq_len=seq_len,
            n_features=int(args.features),
            n_horizons=int(args.horizons),
            patch_len=16 if seq_len >= 32 else 8,
            stride=8 if seq_len >= 32 else 4,
            d_model=int(args.d_model),
            n_layers=int(args.layers),
            n_heads=int(args.heads),
            dropout=0.1,
        )

    return builder


def _build_itransformer(args: argparse.Namespace) -> Callable[[], Any]:
    from engine.strategy.models.itransformer import ITransformer

    def builder() -> Any:
        return ITransformer(
            seq_len=int(args.seq_len),
            n_features=int(args.features),
            n_horizons=int(args.horizons),
            d_model=int(args.d_model),
            n_layers=int(args.layers),
            n_heads=int(args.heads),
            dropout=0.1,
        )

    return builder


def _bench_finbert(torch: Any, device: str, *, args: argparse.Namespace) -> Dict[str, Any]:
    """Benchmark real FinBERT inference (the production workload is inference-only)."""
    from engine.data.finbert_sentiment import load_finbert_model

    _reset_gpu_mem(torch, device)
    bundle = load_finbert_model(
        model_name=str(args.finbert_model) if args.finbert_model else None,
        device=device,
        local_files_only=bool(args.finbert_local_only),
    )
    model = bundle["model"]
    tokenizer = bundle["tokenizer"]
    resolved_device = str(bundle["device"])

    texts: List[str] = [
        _FINBERT_SAMPLE_TEXTS[i % len(_FINBERT_SAMPLE_TEXTS)] for i in range(int(args.finbert_batch))
    ]
    encoded = tokenizer(
        texts,
        return_tensors="pt",
        padding="max_length",
        truncation=True,
        max_length=int(args.finbert_seq),
    )
    encoded = {key: value.to(resolved_device) if hasattr(value, "to") else value for key, value in encoded.items()}

    model.eval()

    def infer() -> Any:
        with torch.no_grad():
            return model(**encoded)

    inference = _time_ms(torch, resolved_device, infer, repeat=int(args.repeat), warmup=int(args.warmup))

    return {
        "skipped": False,
        "shape": {"batch": int(args.finbert_batch), "seq_len": int(args.finbert_seq)},
        "inference": inference,
        "train": {"skipped": True, "reason": "finbert_is_inference_only_in_runtime"},
        "model_name": str(bundle.get("model_name")),
        "peak_gpu_mb": _peak_gpu_mb(torch, resolved_device),
    }


_WORKLOADS = ("patchtst", "itransformer", "finbert")


def _run_workload(torch: Any, name: str, device: str, *, args: argparse.Namespace) -> Dict[str, Any]:
    try:
        if name == "patchtst":
            return _bench_timeseries(torch, device, builder=_build_patchtst(args), args=args)
        if name == "itransformer":
            return _bench_timeseries(torch, device, builder=_build_itransformer(args), args=args)
        if name == "finbert":
            return _bench_finbert(torch, device, args=args)
    except Exception as exc:  # surface, never crash the whole benchmark
        return {"skipped": True, "reason": f"{type(exc).__name__}: {exc}"}
    return {"skipped": True, "reason": f"unknown_workload:{name}"}


def _speedup(cpu: Dict[str, Any], gpu: Dict[str, Any], key: str) -> Optional[float]:
    try:
        cpu_ms = float(cpu[key]["per_iter_ms"])
        gpu_ms = float(gpu[key]["per_iter_ms"])
        if gpu_ms <= 0:
            return None
        return round(cpu_ms / gpu_ms, 2)
    except Exception:
        return None


def run_benchmark(args: argparse.Namespace) -> Dict[str, Any]:
    import torch

    if args.threads and int(args.threads) > 0:
        try:
            torch.set_num_threads(int(args.threads))
        except Exception:
            pass  # no-op-guard: allow - thread count is a best-effort tuning hint.

    torch_status = probe_torch_acceleration(
        torch_module=torch,
        persist_env=False,
        emit_log=False,
        profile=str(args.profile),
        strict_profile=bool(args.require_gpu),
    )
    gpu_available = bool(torch_status.get("rocm_available")) or str(torch_status.get("effective_device")) == "cuda"

    workloads = [w for w in _WORKLOADS if w not in set(args.skip or [])]

    results: Dict[str, Any] = {}
    for name in workloads:
        entry: Dict[str, Any] = {"cpu": _run_workload(torch, name, "cpu", args=args)}
        if gpu_available:
            entry["gpu"] = _run_workload(torch, name, "cuda", args=args)
            entry["speedup_inference"] = _speedup(entry["cpu"], entry.get("gpu", {}), "inference")
            if not entry["cpu"].get("train", {}).get("skipped") and not entry.get("gpu", {}).get("train", {}).get("skipped"):
                entry["speedup_train"] = _speedup(entry["cpu"], entry["gpu"], "train")
        else:
            entry["gpu"] = {"skipped": True, "reason": str(torch_status.get("fallback_reason") or "rocm_unavailable")}
        results[name] = entry

    require_gpu = bool(args.require_gpu)
    ok = bool((not require_gpu) or gpu_available)
    return {
        "ok": ok,
        "require_gpu": require_gpu,
        "gpu_available": gpu_available,
        "torch": torch_status,
        "config": {
            "batch": int(args.batch),
            "seq_len": int(args.seq_len),
            "features": int(args.features),
            "horizons": int(args.horizons),
            "d_model": int(args.d_model),
            "layers": int(args.layers),
            "heads": int(args.heads),
            "repeat": int(args.repeat),
            "warmup": int(args.warmup),
            "finbert_batch": int(args.finbert_batch),
            "finbert_seq": int(args.finbert_seq),
        },
        "results": results,
    }


def _one_line(value: Any, *, limit: int = 60) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _fmt_ms(block: Dict[str, Any], key: str) -> str:
    sub = block.get(key) or {}
    if sub.get("skipped"):
        return f"skipped ({_one_line(sub.get('reason'))})"
    val = sub.get("per_iter_ms")
    return f"{val} ms/iter" if val is not None else "n/a"


def _emit_text(payload: Dict[str, Any]) -> None:
    torch_status = dict(payload.get("torch") or {})
    print("Model acceleration benchmark:", "OK" if payload.get("ok") else "FAILED")
    print(f"torch={torch_status.get('torch_version') or '<missing>'} hip={torch_status.get('hip_version') or '<none>'}")
    print(
        "gpu_available="
        f"{payload.get('gpu_available')} effective_device={torch_status.get('effective_device')}"
        f" fallback_reason={torch_status.get('fallback_reason') or '-'}"
    )
    cfg = dict(payload.get("config") or {})
    print(
        f"config: batch={cfg.get('batch')} seq_len={cfg.get('seq_len')} features={cfg.get('features')} "
        f"d_model={cfg.get('d_model')} layers={cfg.get('layers')} repeat={cfg.get('repeat')}"
    )
    print("")
    print(f"{'workload':<14}{'mode':<11}{'cpu':<22}{'gpu':<34}{'speedup':<9}")
    for name, entry in dict(payload.get("results") or {}).items():
        cpu = dict(entry.get("cpu") or {})
        gpu = dict(entry.get("gpu") or {})
        cpu_skipped = bool(cpu.get("skipped"))
        gpu_skipped = bool(gpu.get("skipped"))
        for mode in ("inference", "train"):
            cpu_txt = f"skipped ({_one_line(cpu.get('reason'))})" if cpu_skipped else _fmt_ms(cpu, mode)
            gpu_txt = f"skipped ({_one_line(gpu.get('reason'))})" if gpu_skipped else _fmt_ms(gpu, mode)
            speed = entry.get(f"speedup_{mode}")
            speed_txt = f"{speed}x" if speed is not None else "-"
            print(f"{name:<14}{mode:<11}{cpu_txt:<22}{gpu_txt:<34}{speed_txt:<9}")
    print("")
    print("speedup > 1.0 means GPU is faster than CPU for that workload. Decide per workload.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark PatchTST / iTransformer / FinBERT on CPU vs ROCm with hard numbers.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--require-gpu", action="store_true", help="Exit nonzero when ROCm GPU is unavailable.")
    parser.add_argument("--allow-missing-gpu", action="store_true", help="Compatibility alias for the default.")
    parser.add_argument(
        "--profile",
        default="amd-rocm",
        help="Acceleration profile to probe; defaults to amd-rocm for standalone runs.",
    )
    parser.add_argument(
        "--skip",
        action="append",
        choices=list(_WORKLOADS),
        default=[],
        help="Workload to skip (repeatable), e.g. --skip finbert.",
    )
    # Shared time-series shape (PatchTST + iTransformer). Defaults are training-realistic.
    parser.add_argument("--batch", type=int, default=64)
    parser.add_argument("--seq-len", type=int, default=128)
    parser.add_argument("--features", type=int, default=32)
    parser.add_argument("--horizons", type=int, default=4)
    parser.add_argument("--d-model", type=int, default=64)
    parser.add_argument("--layers", type=int, default=3)
    parser.add_argument("--heads", type=int, default=4)
    parser.add_argument("--repeat", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--threads", type=int, default=0, help="torch.set_num_threads override (0 = leave default).")
    # FinBERT.
    parser.add_argument("--finbert-batch", type=int, default=32)
    parser.add_argument("--finbert-seq", type=int, default=64)
    parser.add_argument("--finbert-model", default="", help="Override FinBERT model name / local path.")
    parser.add_argument(
        "--finbert-local-only",
        action="store_true",
        help="Require a locally cached FinBERT (no network download).",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload = run_benchmark(args)
    except AccelerationProfileError as exc:
        # --require-gpu with a misconfigured/absent ROCm runtime: report and fail.
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        else:
            print(f"Model acceleration benchmark: FAILED\nacceleration_profile_error={exc}")
        return 1
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _emit_text(payload)
    return 0 if bool(payload.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
