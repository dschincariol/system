"""Runtime torch acceleration detection and device selection."""

from __future__ import annotations

import importlib
import logging
import os
import time
from typing import Any, Dict, Optional

from engine.runtime.logging import get_logger, log_event

ROCM_PROFILES = {"amd-rocm", "rocm", "hip"}
CUDA_PROFILES = {"cuda", "nvidia-cuda"}
CPU_PROFILES = {"", "cpu", "none", "off", "disabled"}
TRUTHY = {"1", "true", "yes", "on"}


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def _env_truthy(name: str) -> bool:
    return _normalize(os.environ.get(name)) in TRUTHY


def acceleration_profile() -> str:
    raw = (
        os.environ.get("TRADING_ACCELERATION_PROFILE")
        or os.environ.get("TRADING_ACCELERATOR_PROFILE")
        or os.environ.get("TRADING_INFERENCE_ACCELERATOR")
        or "cpu"
    )
    profile = _normalize(raw)
    return profile or "cpu"


def _safe_call(default: Any, fn, *args: Any) -> Any:
    try:
        return fn(*args)
    except Exception:
        return default


def _torch_cuda_status(torch_module: Any) -> Dict[str, Any]:
    cuda = getattr(torch_module, "cuda", None)
    version = getattr(torch_module, "version", None)
    hip_version = str(getattr(version, "hip", "") or "")
    cuda_version = str(getattr(version, "cuda", "") or "")
    cuda_available = bool(cuda) and bool(_safe_call(False, cuda.is_available))
    device_count = int(_safe_call(0, cuda.device_count)) if cuda else 0
    devices = []
    if cuda and device_count > 0:
        get_device_name = getattr(cuda, "get_device_name", None)
        for idx in range(device_count):
            name = str(_safe_call("", get_device_name, idx) or "") if callable(get_device_name) else ""
            devices.append({"index": int(idx), "name": name})
    return {
        "torch_version": str(getattr(torch_module, "__version__", "") or ""),
        "hip_version": hip_version,
        "cuda_version": cuda_version,
        "torch_cuda_is_available": bool(cuda_available),
        "torch_cuda_device_count": int(device_count),
        "torch_cuda_devices": devices,
        "torch_is_rocm_build": bool(hip_version),
    }


def resolve_torch_device(
    torch_module: Any,
    *,
    requested: Any = None,
    profile: Optional[str] = None,
    legacy_cuda_enabled: bool = False,
) -> Dict[str, Any]:
    """Resolve the effective torch device without raising on missing GPU support.

    PyTorch exposes ROCm devices through the ``cuda`` API. This helper keeps GPU
    use opt-in for the trading runtime while still preserving explicit CUDA
    requests and older model-specific flags.
    """

    status = _torch_cuda_status(torch_module)
    selected_profile = _normalize(profile if profile is not None else acceleration_profile())
    requested_device = _normalize(requested)
    cuda_available = bool(status["torch_cuda_is_available"])
    device_count = int(status.get("torch_cuda_device_count") or 0)
    rocm_build = bool(status["torch_is_rocm_build"])

    if requested_device in {"", "auto"}:
        if legacy_cuda_enabled:
            requested_device = "cuda"
        elif selected_profile in ROCM_PROFILES:
            requested_device = "rocm"
        elif selected_profile in CUDA_PROFILES:
            requested_device = "cuda"
        else:
            return {
                **status,
                "requested_device": requested_device or "auto",
                "requested_profile": selected_profile or "cpu",
                "effective_device": "cpu",
                "fallback_reason": "cpu_profile",
            }

    if requested_device in {"cpu", "none", "off"}:
        return {
            **status,
            "requested_device": requested_device,
            "requested_profile": selected_profile or "cpu",
            "effective_device": "cpu",
            "fallback_reason": "",
        }

    wants_rocm = requested_device in {"rocm", "hip"} or selected_profile in ROCM_PROFILES
    wants_cuda = requested_device.startswith("cuda") or requested_device in {"rocm", "hip"}
    if wants_cuda:
        if not cuda_available:
            return {
                **status,
                "requested_device": requested_device,
                "requested_profile": selected_profile or "cpu",
                "effective_device": "cpu",
                "fallback_reason": "torch_cuda_unavailable",
            }
        if device_count <= 0:
            return {
                **status,
                "requested_device": requested_device,
                "requested_profile": selected_profile or "cpu",
                "effective_device": "cpu",
                "fallback_reason": "torch_cuda_device_count_zero",
            }
        if wants_rocm and not rocm_build:
            return {
                **status,
                "requested_device": requested_device,
                "requested_profile": selected_profile or "cpu",
                "effective_device": "cpu",
                "fallback_reason": "torch_not_rocm_build",
            }
        return {
            **status,
            "requested_device": requested_device,
            "requested_profile": selected_profile or "cpu",
            "effective_device": "cuda",
            "fallback_reason": "",
        }

    return {
        **status,
        "requested_device": requested_device,
        "requested_profile": selected_profile or "cpu",
        "effective_device": "cpu",
        "fallback_reason": f"unsupported_requested_device:{requested_device}",
    }


def probe_torch_acceleration(
    *,
    logger: Optional[logging.Logger] = None,
    torch_module: Any = None,
    persist_env: bool = True,
    emit_log: bool = True,
    requested_device: Any = None,
    profile: Optional[str] = None,
    legacy_cuda_enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return and log a non-fatal torch acceleration capability snapshot."""

    started = time.perf_counter()
    imported = True
    import_error = ""
    if torch_module is None:
        try:
            torch_module = importlib.import_module("torch")
        except Exception as exc:
            imported = False
            import_error = f"{type(exc).__name__}: {exc}"
            torch_module = None

    if torch_module is None:
        snapshot: Dict[str, Any] = {
            "ok": False,
            "torch_imported": False,
            "torch_import_error": import_error,
            "torch_version": "",
            "hip_version": "",
            "cuda_version": "",
            "torch_cuda_is_available": False,
            "torch_cuda_device_count": 0,
            "torch_cuda_devices": [],
            "torch_is_rocm_build": False,
            "requested_profile": acceleration_profile(),
            "requested_device": "auto",
            "effective_device": "cpu",
            "fallback_reason": "torch_import_failed",
        }
    else:
        snapshot = resolve_torch_device(
            torch_module,
            requested=requested_device if requested_device is not None else (os.environ.get("TORCH_DEVICE") or "auto"),
            profile=profile if profile is not None else acceleration_profile(),
            legacy_cuda_enabled=_env_truthy("PATCHTST_USE_CUDA")
            if legacy_cuda_enabled is None
            else bool(legacy_cuda_enabled),
        )
        snapshot["ok"] = True
        snapshot["torch_imported"] = bool(imported)
        snapshot["torch_import_error"] = str(import_error)

    snapshot["rocm_available"] = bool(
        snapshot.get("torch_is_rocm_build")
        and snapshot.get("torch_cuda_is_available")
        and int(snapshot.get("torch_cuda_device_count") or 0) > 0
    )
    snapshot["acceleration_available"] = bool(str(snapshot.get("effective_device") or "") == "cuda")
    snapshot["probe_elapsed_ms"] = int(round((time.perf_counter() - started) * 1000))
    snapshot["ts_ms"] = int(time.time() * 1000)

    if persist_env:
        os.environ["TRADING_ACCELERATION_EFFECTIVE_DEVICE"] = str(snapshot.get("effective_device") or "cpu")
        os.environ["TRADING_ACCELERATION_AVAILABLE"] = "1" if snapshot["acceleration_available"] else "0"
        os.environ["TRADING_ROCM_AVAILABLE"] = "1" if snapshot["rocm_available"] else "0"

    if emit_log:
        target_logger = logger or get_logger("runtime.acceleration")
        level = logging.INFO if snapshot.get("acceleration_available") else logging.WARNING
        log_event(
            target_logger,
            level,
            "runtime_acceleration_probe",
            component="engine.runtime.acceleration",
            extra=snapshot,
        )
    return snapshot
