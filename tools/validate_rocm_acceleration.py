from __future__ import annotations

"""Validate opt-in ROCm torch acceleration on AMD hosts."""

import argparse
import grp
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime.acceleration import probe_torch_acceleration  # noqa: E402


def _device_path_status(path: str) -> Dict[str, Any]:
    exists = os.path.exists(path)
    return {
        "path": path,
        "exists": bool(exists),
        "readable": bool(exists and os.access(path, os.R_OK)),
        "writable": bool(exists and os.access(path, os.W_OK)),
    }


def _group_status(name: str) -> Dict[str, Any]:
    try:
        group = grp.getgrnam(name)
    except KeyError:
        return {"name": name, "exists": False, "gid": None, "member": False}
    gids = set(os.getgroups())
    return {
        "name": name,
        "exists": True,
        "gid": int(group.gr_gid),
        "member": bool(group.gr_gid in gids),
    }


def _sync(torch: Any, device: str) -> None:
    if str(device).startswith("cuda"):
        cuda = getattr(torch, "cuda", None)
        if cuda is not None:
            try:
                cuda.synchronize()
            except Exception:
                pass  # no-op-guard: allow - synchronize is best-effort timing cleanup.


def _time_ms(torch: Any, device: str, fn: Callable[[], Any], *, repeat: int) -> Dict[str, Any]:
    for _ in range(2):
        fn()
    _sync(torch, device)
    started = time.perf_counter()
    for _ in range(max(1, int(repeat))):
        fn()
    _sync(torch, device)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return {
        "repeat": int(max(1, int(repeat))),
        "elapsed_ms": round(float(elapsed_ms), 3),
        "per_iter_ms": round(float(elapsed_ms) / float(max(1, int(repeat))), 3),
    }


def _matmul_benchmark(torch: Any, device: str, *, size: int, repeat: int) -> Dict[str, Any]:
    a = torch.randn((int(size), int(size)), dtype=torch.float32, device=device)
    b = torch.randn((int(size), int(size)), dtype=torch.float32, device=device)

    def run() -> Any:
        return a @ b

    out = _time_ms(torch, device, run, repeat=repeat)
    out["device"] = str(device)
    out["size"] = int(size)
    return out


def _patchtst_benchmark(
    torch: Any,
    device: str,
    *,
    batch: int,
    seq_len: int,
    n_features: int,
    n_horizons: int,
    repeat: int,
) -> Dict[str, Any]:
    from engine.strategy.patchtst_core import PatchTST

    model = PatchTST(
        seq_len=int(seq_len),
        n_features=int(n_features),
        n_horizons=int(n_horizons),
        patch_len=8 if int(seq_len) >= 16 else 4,
        stride=4 if int(seq_len) >= 16 else 2,
        d_model=32,
        n_layers=1,
        n_heads=2,
        dropout=0.0,
    ).to(device)
    model.eval()
    x = torch.randn((int(batch), int(seq_len), int(n_features)), dtype=torch.float32, device=device)

    def run() -> Any:
        with torch.no_grad():
            return model(x)

    out = _time_ms(torch, device, run, repeat=repeat)
    out["device"] = str(device)
    out["batch"] = int(batch)
    out["seq_len"] = int(seq_len)
    out["n_features"] = int(n_features)
    out["n_horizons"] = int(n_horizons)
    return out


def run_validation(args: argparse.Namespace) -> Dict[str, Any]:
    device_access = {
        "paths": [
            _device_path_status("/dev/dri"),
            _device_path_status("/dev/dri/renderD128"),
            _device_path_status("/dev/kfd"),
            _device_path_status("/dev/accel/accel0"),
        ],
        "groups": [_group_status("render"), _group_status("video")],
    }

    import torch

    torch_status = probe_torch_acceleration(
        torch_module=torch,
        persist_env=False,
        emit_log=False,
        profile=str(args.profile or "amd-rocm"),
    )
    gpu_available = bool(torch_status.get("rocm_available"))
    benchmarks: Dict[str, Any] = {
        "cpu": {
            "matmul": _matmul_benchmark(torch, "cpu", size=int(args.matmul_size), repeat=int(args.repeat)),
            "patchtst_forward": _patchtst_benchmark(
                torch,
                "cpu",
                batch=int(args.model_batch),
                seq_len=int(args.model_seq_len),
                n_features=int(args.model_features),
                n_horizons=int(args.model_horizons),
                repeat=int(args.repeat),
            ),
        },
        "gpu": {"skipped": True, "reason": "rocm_unavailable"},
    }
    if gpu_available:
        benchmarks["gpu"] = {
            "skipped": False,
            "matmul": _matmul_benchmark(torch, "cuda", size=int(args.matmul_size), repeat=int(args.repeat)),
            "patchtst_forward": _patchtst_benchmark(
                torch,
                "cuda",
                batch=int(args.model_batch),
                seq_len=int(args.model_seq_len),
                n_features=int(args.model_features),
                n_horizons=int(args.model_horizons),
                repeat=int(args.repeat),
            ),
        }

    require_gpu = bool(args.require_gpu)
    ok = bool((not require_gpu) or gpu_available)
    return {
        "ok": bool(ok),
        "require_gpu": bool(require_gpu),
        "device_access": device_access,
        "torch": torch_status,
        "benchmarks": benchmarks,
    }


def _emit_text(payload: Dict[str, Any]) -> None:
    torch_status = dict(payload.get("torch") or {})
    print("ROCm acceleration validation:", "OK" if payload.get("ok") else "FAILED")
    print(f"torch={torch_status.get('torch_version') or '<missing>'}")
    print(f"hip={torch_status.get('hip_version') or '<none>'}")
    print(f"torch.cuda.is_available={torch_status.get('torch_cuda_is_available')}")
    print(f"torch.cuda.device_count={torch_status.get('torch_cuda_device_count')}")
    print(f"effective_device={torch_status.get('effective_device')}")
    if torch_status.get("fallback_reason"):
        print(f"fallback_reason={torch_status.get('fallback_reason')}")
    for item in payload["device_access"]["paths"]:
        print(
            "device_path "
            f"{item['path']} exists={item['exists']} readable={item['readable']} writable={item['writable']}"
        )
    for item in payload["device_access"]["groups"]:
        print(f"group {item['name']} gid={item.get('gid')} member={item.get('member')}")
    for scope, bench in dict(payload.get("benchmarks") or {}).items():
        if bench.get("skipped"):
            print(f"{scope}: skipped reason={bench.get('reason')}")
            continue
        for name, result in dict(bench).items():
            if isinstance(result, dict):
                print(f"{scope}.{name}: {result.get('per_iter_ms')} ms/iter")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate ROCm torch acceleration with CPU fallback evidence.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--require-gpu", action="store_true", help="Exit nonzero when ROCm GPU is unavailable.")
    parser.add_argument("--allow-missing-gpu", action="store_true", help="Compatibility alias for the default.")
    parser.add_argument(
        "--profile",
        default="amd-rocm",
        help="Acceleration profile to probe; defaults to amd-rocm for standalone validation.",
    )
    parser.add_argument("--matmul-size", type=int, default=512)
    parser.add_argument("--model-batch", type=int, default=8)
    parser.add_argument("--model-seq-len", type=int, default=64)
    parser.add_argument("--model-features", type=int, default=8)
    parser.add_argument("--model-horizons", type=int, default=4)
    parser.add_argument("--repeat", type=int, default=5)
    args = parser.parse_args(argv)

    payload = run_validation(args)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _emit_text(payload)
    return 0 if bool(payload.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
