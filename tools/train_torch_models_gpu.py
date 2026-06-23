from __future__ import annotations

"""Real PatchTST / iTransformer training driver for the opt-in ROCm batch profile.

Unlike ``tools/benchmark_model_acceleration.py`` (which times raw modules), this
driver exercises the *production* train/serve wrappers — ``PatchTSTRegressor``
and ``ITransformerRegressor`` — through their real ``.fit()`` path on whatever
device the runtime hardware resolver selects. It is the default "real training"
command for the ``trainer`` service in
``deploy/compose/docker-compose.amd-rocm-training.yml``.

Scope / safety:
- Offline batch only. It does NOT start the supervised runtime, route to a
  broker, place/cancel/replace/flatten orders, or arm live trading.
- It uses no real secrets and trains on locally generated synthetic data shaped
  to the model's real resolved feature schema (the registry serving columns).
  ``.fit()`` is pure in-memory training with no DB writes; the optional
  ``--save-dir`` persists a local artifact directory only.

The point is to prove the production training code path runs end-to-end on the
AMD iGPU (ROCm exposed via the ``torch.cuda`` API) and to capture device, loss
convergence, and wall-clock time for the keep-CPU-vs-move-to-GPU decision.

Examples
--------
    # CPU-only host: trains on CPU, prints convergence + timing. Exit 0.
    python tools/train_torch_models_gpu.py --allow-missing-gpu --epochs 20

    # ROCm container on bart: require GPU, fail if training did not run on cuda.
    python tools/train_torch_models_gpu.py --require-gpu --epochs 30 --json
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _quiet_engine_logs() -> None:
    """Keep stdout clean: the engine emits structured INFO/WARNING records that
    would otherwise interleave with this tool's text/JSON output. Diagnostic CLI
    output is the product here, so silence engine logs below ERROR."""
    for name in ("engine", "model_registry", "trading-engine"):
        logging.getLogger(name).setLevel(logging.ERROR)

from engine.runtime.acceleration import (  # noqa: E402
    AccelerationProfileError,
    probe_torch_acceleration,
)

_MODELS = ("patchtst", "itransformer")


def _build_regressor(name: str, *, device: str, seq_len: int, horizons: int) -> Any:
    if name == "patchtst":
        from engine.strategy.models.patchtst import PatchTSTRegressor

        return PatchTSTRegressor(device=device, seq_len=int(seq_len), n_horizons=int(horizons))
    from engine.strategy.models.itransformer import ITransformerRegressor

    return ITransformerRegressor(device=device, seq_len=int(seq_len), n_horizons=int(horizons))


def _synthetic_dataset(
    *,
    n_samples: int,
    seq_len: int,
    n_features: int,
    n_horizons: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Registry-valid synthetic sequences with a learnable signal.

    Targets are a deterministic linear function of the last-step feature means
    plus light noise, so a correctly wired training loop visibly reduces loss.
    """
    rng = np.random.RandomState(int(seed))
    X = rng.standard_normal((int(n_samples), int(seq_len), int(n_features))).astype(np.float32)
    last_step = X[:, -1, :]
    base = last_step.mean(axis=1, keepdims=True)
    weights = np.linspace(0.5, 1.5, num=int(n_horizons), dtype=np.float32).reshape(1, -1)
    noise = 0.01 * rng.standard_normal((int(n_samples), int(n_horizons))).astype(np.float32)
    y = (base * weights + noise).astype(np.float32)
    return X, y


def _train_one(
    name: str,
    *,
    device: str,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    try:
        reg = _build_regressor(name, device=device, seq_len=int(args.seq_len), horizons=int(args.horizons))
        n_features = int(len(reg.feature_ids))
        if n_features <= 0:
            return {"skipped": True, "reason": "model_resolved_zero_features"}
        X, y = _synthetic_dataset(
            n_samples=int(args.samples),
            seq_len=int(args.seq_len),
            n_features=n_features,
            n_horizons=int(args.horizons),
            seed=int(args.seed),
        )
        started = time.perf_counter()
        losses = reg.fit(X, y, epochs=int(args.epochs), return_losses=True)
        elapsed_s = time.perf_counter() - started
        resolved_device = str(getattr(reg, "device", device))
        ran_on_cuda = resolved_device.lower().startswith("cuda")

        artifact_path: Optional[str] = None
        if args.save_dir:
            target = os.path.join(str(args.save_dir), name)
            os.makedirs(target, exist_ok=True)
            artifact_path = str(reg.save(target))

        metrics = dict(getattr(reg, "training_metrics", {}) or {})
        loss_list = [float(v) for v in (losses or [])]
        rmse_value = metrics.get("rmse")
        return {
            "skipped": False,
            "device": resolved_device,
            "ran_on_cuda": bool(ran_on_cuda),
            "n_features": n_features,
            "n_samples": int(args.samples),
            "seq_len": int(args.seq_len),
            "n_horizons": int(args.horizons),
            "epochs": int(args.epochs),
            "loss_initial": round(loss_list[0], 6) if loss_list else None,
            "loss_final": round(loss_list[-1], 6) if loss_list else None,
            "rmse": round(float(rmse_value), 6) if rmse_value is not None else None,
            "elapsed_s": round(float(elapsed_s), 3),
            "s_per_epoch": round(float(elapsed_s) / float(max(1, int(args.epochs))), 4),
            "artifact_path": artifact_path,
        }
    except Exception as exc:
        return {"skipped": True, "reason": f"{type(exc).__name__}: {exc}"}


def run(args: argparse.Namespace) -> Dict[str, Any]:
    import torch  # noqa: F401  (import validates the runtime/profile before training)

    torch_status = probe_torch_acceleration(
        torch_module=torch,
        persist_env=False,
        emit_log=False,
        profile=str(args.profile),
        strict_profile=bool(args.require_gpu),
    )
    gpu_available = bool(torch_status.get("rocm_available")) or str(torch_status.get("effective_device")) == "cuda"

    if str(args.device).strip():
        device = str(args.device).strip()
    elif gpu_available:
        device = "cuda"
    else:
        device = "cpu"

    models = [m for m in _MODELS if m not in set(args.skip or [])]
    results: Dict[str, Any] = {name: _train_one(name, device=device, args=args) for name in models}

    require_gpu = bool(args.require_gpu)
    trained = [r for r in results.values() if not r.get("skipped")]
    all_on_cuda = bool(trained) and all(r.get("ran_on_cuda") for r in trained)
    # Strict mode must both have a GPU AND have actually trained on it.
    ok = bool(((not require_gpu) or (gpu_available and all_on_cuda)) and not any(r.get("skipped") for r in results.values()))
    return {
        "ok": ok,
        "require_gpu": require_gpu,
        "gpu_available": gpu_available,
        "requested_device": device,
        "torch": torch_status,
        "results": results,
    }


def _emit_text(payload: Dict[str, Any]) -> None:
    torch_status = dict(payload.get("torch") or {})
    print("Torch model training:", "OK" if payload.get("ok") else "FAILED")
    print(
        f"requested_device={payload.get('requested_device')} gpu_available={payload.get('gpu_available')} "
        f"hip={torch_status.get('hip_version') or '<none>'} fallback_reason={torch_status.get('fallback_reason') or '-'}"
    )
    print("")
    print(f"{'model':<14}{'device':<8}{'epochs':<8}{'loss_init→final':<22}{'rmse':<10}{'sec':<8}")
    for name, r in dict(payload.get("results") or {}).items():
        if r.get("skipped"):
            print(f"{name:<14}skipped: {r.get('reason')}")
            continue
        loss_txt = f"{r.get('loss_initial')}→{r.get('loss_final')}"
        print(
            f"{name:<14}{str(r.get('device')):<8}{str(r.get('epochs')):<8}{loss_txt:<22}"
            f"{str(r.get('rmse')):<10}{str(r.get('elapsed_s')):<8}"
        )
    print("")
    print("Falling loss_init→final confirms the production training path ran end-to-end on the device.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train real PatchTST/iTransformer regressors on the resolved device (opt-in ROCm batch profile).",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--require-gpu", action="store_true", help="Fail unless training actually runs on a ROCm/CUDA device.")
    parser.add_argument("--allow-missing-gpu", action="store_true", help="Compatibility alias for the default (CPU fallback).")
    parser.add_argument("--profile", default="amd-rocm", help="Acceleration profile to probe; defaults to amd-rocm.")
    parser.add_argument(
        "--device",
        default="",
        help="Force a device (cpu / cuda). Default: runtime-resolved (cuda when the ROCm profile is active and available).",
    )
    parser.add_argument("--skip", action="append", choices=list(_MODELS), default=[], help="Model to skip (repeatable).")
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--samples", type=int, default=512)
    parser.add_argument("--seq-len", type=int, default=64)
    parser.add_argument("--horizons", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--save-dir", default="", help="Optional directory to persist trained artifacts (no DB writes).")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    _quiet_engine_logs()
    try:
        payload = run(args)
    except AccelerationProfileError as exc:
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        else:
            print(f"Torch model training: FAILED\nacceleration_profile_error={exc}")
        return 1
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _emit_text(payload)
    return 0 if bool(payload.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
