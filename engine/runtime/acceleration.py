"""Runtime torch acceleration detection and device selection."""

from __future__ import annotations

import importlib
import logging
import os
import platform
import sys
import time
from typing import Any, Dict, Optional

from engine.runtime.logging import get_logger, log_event

ROCM_PROFILES = {"amd", "amd-rocm", "rocm", "hip"}
CUDA_PROFILES = {"cuda", "nvidia-cuda"}
CPU_PROFILES = {"", "cpu", "none", "off", "disabled"}
TRUTHY = {"1", "true", "yes", "on"}


class AccelerationProfileError(RuntimeError):
    """Raised when an explicit accelerator profile cannot run as configured."""


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "-")


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


def dependency_profile() -> str:
    raw = (
        os.environ.get("TRADING_DEPENDENCY_PROFILE")
        or os.environ.get("DEPENDENCY_PROFILE")
        or os.environ.get("RUNTIME_DEPENDENCY_PROFILE")
        or "cpu"
    )
    profile = _normalize(raw)
    return profile or "cpu"


def rocm_dependency_profile_selected() -> bool:
    return dependency_profile() in ROCM_PROFILES


def rocm_acceleration_profile_selected() -> bool:
    return acceleration_profile() in ROCM_PROFILES


def _python_version_text(version_info: Any = None) -> str:
    info = sys.version_info if version_info is None else version_info
    major = int(getattr(info, "major", info[0]))
    minor = int(getattr(info, "minor", info[1]))
    micro = int(getattr(info, "micro", info[2] if len(info) > 2 else 0))
    return f"{major}.{minor}.{micro}"


def amd_rocm_python_marker_error(
    *,
    version_info: Any = None,
    platform_system: str | None = None,
) -> str:
    info = sys.version_info if version_info is None else version_info
    system = str(platform.system() if platform_system is None else platform_system)
    major = int(getattr(info, "major", info[0]))
    minor = int(getattr(info, "minor", info[1]))
    if system != "Linux":
        return (
            "amd_rocm_python_runtime_unsupported:"
            f"platform={system}:required_platform=Linux:"
            "reason=rocm_7_2_4_wheels_are_linux_cp312"
        )
    if (major, minor) < (3, 12):
        return (
            "amd_rocm_python_runtime_unsupported:"
            f"python={_python_version_text(info)}:required_python=>=3.12:"
            "reason=rocm_7_2_4_wheels_are_cp312"
        )
    return ""


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


def _rocm_profile_error_from_snapshot(snapshot: Dict[str, Any]) -> str:
    if not bool(snapshot.get("torch_imported", True)):
        return (
            "amd_rocm_torch_import_failed:"
            f"error={snapshot.get('torch_import_error') or 'unknown'}"
        )
    if not bool(snapshot.get("torch_is_rocm_build")):
        return (
            "amd_rocm_torch_not_hip_build:"
            f"torch={snapshot.get('torch_version') or '<missing>'}:hip=<none>"
        )
    if not bool(snapshot.get("torch_cuda_is_available")):
        return (
            "amd_rocm_torch_cuda_unavailable:"
            f"hip={snapshot.get('hip_version') or '<none>'}"
        )
    if int(snapshot.get("torch_cuda_device_count") or 0) <= 0:
        return (
            "amd_rocm_torch_device_count_zero:"
            f"hip={snapshot.get('hip_version') or '<none>'}"
        )
    if str(snapshot.get("effective_device") or "").strip().lower() != "cuda":
        return (
            "amd_rocm_torch_not_selected:"
            f"fallback_reason={snapshot.get('fallback_reason') or 'unknown'}"
        )
    return ""


def validate_amd_rocm_torch_snapshot(snapshot: Dict[str, Any], *, profile: str = "amd-rocm") -> None:
    marker_error = amd_rocm_python_marker_error()
    if marker_error:
        raise AccelerationProfileError(f"{marker_error}:profile={_normalize(profile) or 'amd-rocm'}")
    rocm_error = _rocm_profile_error_from_snapshot(snapshot)
    if rocm_error:
        raise AccelerationProfileError(f"{rocm_error}:profile={_normalize(profile) or 'amd-rocm'}")


def assert_amd_rocm_runtime_ready(
    *,
    torch_module: Any = None,
    profile: str = "amd-rocm",
) -> Dict[str, Any]:
    marker_error = amd_rocm_python_marker_error()
    if marker_error:
        raise AccelerationProfileError(f"{marker_error}:profile={_normalize(profile) or 'amd-rocm'}")

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
            "requested_profile": _normalize(profile) or "amd-rocm",
            "requested_device": "auto",
            "effective_device": "cpu",
            "fallback_reason": "torch_import_failed",
        }
    else:
        snapshot = resolve_torch_device(
            torch_module,
            requested="auto",
            profile=_normalize(profile) or "amd-rocm",
            legacy_cuda_enabled=False,
        )
        snapshot["ok"] = True
        snapshot["torch_imported"] = bool(imported)
        snapshot["torch_import_error"] = str(import_error)

    snapshot["rocm_available"] = bool(
        snapshot.get("torch_is_rocm_build")
        and snapshot.get("torch_cuda_is_available")
        and int(snapshot.get("torch_cuda_device_count") or 0) > 0
    )
    validate_amd_rocm_torch_snapshot(snapshot, profile=profile)
    return snapshot


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
    strict_profile: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return and log a non-fatal torch acceleration capability snapshot."""

    started = time.perf_counter()
    selected_profile = _normalize(profile if profile is not None else acceleration_profile())
    strict_rocm = (
        rocm_dependency_profile_selected() or rocm_acceleration_profile_selected()
        if strict_profile is None
        else bool(strict_profile)
    )
    rocm_profile_required = selected_profile in ROCM_PROFILES or rocm_dependency_profile_selected()
    strict_profile_name = dependency_profile() if rocm_dependency_profile_selected() else selected_profile
    if strict_rocm and rocm_profile_required:
        marker_error = amd_rocm_python_marker_error()
        if marker_error:
            raise AccelerationProfileError(f"{marker_error}:profile={strict_profile_name or 'amd-rocm'}")

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
            "requested_profile": selected_profile or "cpu",
            "requested_device": "auto",
            "effective_device": "cpu",
            "fallback_reason": "torch_import_failed",
        }
    else:
        snapshot = resolve_torch_device(
            torch_module,
            requested=requested_device if requested_device is not None else (os.environ.get("TORCH_DEVICE") or "auto"),
            profile=selected_profile,
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

    if strict_rocm and rocm_profile_required:
        validate_amd_rocm_torch_snapshot(snapshot, profile=strict_profile_name or "amd-rocm")

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
