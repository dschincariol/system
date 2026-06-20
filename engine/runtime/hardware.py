"""CPU-first runtime hardware and accelerator resolution helpers."""

from __future__ import annotations

import importlib
import logging
import os
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping


CPU_FIRST_DEVICE_ENV_KEYS = (
    "TORCH_DEVICE",
    "EMBED_DEVICE",
    "NLP_DEVICE",
    "FINBERT_DEVICE",
    "TS_FOUNDATION_DEVICE",
)
CPU_THREAD_ENV_KEYS = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)
CUDA_FEATURE_ENV_DEFAULTS = {
    "NVIDIA_TELEMETRY_ENABLED": "0",
    "GPU_THROTTLE_ENABLE": "0",
    "PINNED_ENABLE": "0",
    "PINNED_PREFETCH": "0",
    "TORCH_ALLOW_TF32": "0",
    "CUDNN_ALLOW_TF32": "0",
    "CUDNN_BENCHMARK": "0",
}
DEFAULT_CPU_THREADS = 8
DEFAULT_INTEROP_THREADS = 4
DEFAULT_HARDWARE_PROFILE = "cpu"
DEFAULT_DEPENDENCY_PROFILE = "cpu"
_NVIDIA_PROFILES = {"cuda", "nvidia", "nvidia-cuda", "nvidia_cuda"}
_NVIDIA_DEPENDENCY_PROFILES = {"cuda", "nvidia", "nvidia-cuda", "nvidia_cuda"}
_AMD_PROFILES = {"amd", "rocm", "amd-rocm", "amd_rocm"}
_WARNED_LOG_KEYS: set[str] = set()


@dataclass(frozen=True)
class DeviceResolution:
    requested: str
    resolved: str
    source: str
    profile: str
    cuda_available: bool
    accelerator_enabled: bool
    disabled_accelerator_reason: str


def _truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_text(name: str, default: str = "") -> str:
    raw = os.environ.get(str(name))
    return str(default if raw is None else raw).strip()


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(str(name))
    try:
        return int(str(default if raw is None or str(raw).strip() == "" else raw).strip())
    except Exception:
        return int(default)


def runtime_hardware_profile() -> str:
    raw = (
        _env_text("RUNTIME_HARDWARE_PROFILE")
        or _env_text("TRADING_HARDWARE_PROFILE")
        or _env_text("ACCELERATOR_PROFILE")
        or _env_text("HARDWARE_PROFILE")
        or DEFAULT_HARDWARE_PROFILE
    )
    return str(raw or DEFAULT_HARDWARE_PROFILE).strip().lower()


def runtime_dependency_profile() -> str:
    raw = (
        _env_text("TRADING_DEPENDENCY_PROFILE")
        or _env_text("DEPENDENCY_PROFILE")
        or _env_text("RUNTIME_DEPENDENCY_PROFILE")
        or DEFAULT_DEPENDENCY_PROFILE
    )
    return str(raw or DEFAULT_DEPENDENCY_PROFILE).strip().lower()


def nvidia_profile_enabled() -> bool:
    return runtime_hardware_profile() in _NVIDIA_PROFILES


def nvidia_dependency_profile_enabled() -> bool:
    return runtime_dependency_profile() in _NVIDIA_DEPENDENCY_PROFILES


def amd_profile_selected() -> bool:
    return runtime_hardware_profile() in _AMD_PROFILES or runtime_dependency_profile() in _AMD_PROFILES


def accelerator_profile_error() -> str:
    hardware_profile = runtime_hardware_profile()
    dependency_profile = runtime_dependency_profile()
    if hardware_profile in _AMD_PROFILES or dependency_profile in _AMD_PROFILES:
        return "amd_rocm_profile_not_validated"
    if hardware_profile in _NVIDIA_PROFILES and dependency_profile not in _NVIDIA_DEPENDENCY_PROFILES:
        return "nvidia_runtime_requires_nvidia_dependency_profile"
    return ""


def nvidia_acceleration_profile_enabled() -> bool:
    return nvidia_profile_enabled() and nvidia_dependency_profile_enabled()


def torch_cuda_available(torch_module: Any) -> bool:
    cuda = getattr(torch_module, "cuda", None)
    is_available = getattr(cuda, "is_available", None)
    try:
        return bool(callable(is_available) and is_available())
    except Exception:
        return False


def _selected_request(
    *,
    requested: Any = None,
    env_var: str = "TORCH_DEVICE",
    fallback_envs: Iterable[str] = (),
    default: str = "cpu",
    legacy_cuda_flag: str | None = None,
) -> tuple[str, str]:
    if requested is not None and str(requested).strip():
        return str(requested).strip(), "argument"
    for key in (str(env_var), *[str(item) for item in fallback_envs]):
        value = _env_text(key)
        if value:
            return value, key
    if legacy_cuda_flag and _truthy(os.environ.get(str(legacy_cuda_flag)), default=False):
        return "cuda", legacy_cuda_flag
    return str(default or "cpu").strip() or "cpu", "default"


def resolve_torch_device(
    torch_module: Any,
    *,
    requested: Any = None,
    env_var: str = "TORCH_DEVICE",
    fallback_envs: Iterable[str] = (),
    default: str = "cpu",
    legacy_cuda_flag: str | None = None,
) -> DeviceResolution:
    """Resolve a torch device without implicitly selecting CUDA."""

    raw, source = _selected_request(
        requested=requested,
        env_var=env_var,
        fallback_envs=fallback_envs,
        default=default,
        legacy_cuda_flag=legacy_cuda_flag,
    )
    device = str(raw or "cpu").strip().lower()
    profile = runtime_hardware_profile()
    cuda_available = torch_cuda_available(torch_module)

    if device in {"", "default"}:
        device = "cpu"
    if device == "auto":
        if nvidia_acceleration_profile_enabled() and cuda_available:
            return DeviceResolution(
                requested=str(raw or "auto"),
                resolved="cuda",
                source=source,
                profile=profile,
                cuda_available=True,
                accelerator_enabled=True,
                disabled_accelerator_reason="",
            )
        if not nvidia_profile_enabled():
            reason = "accelerator_profile_not_enabled"
        elif not nvidia_dependency_profile_enabled():
            reason = "dependency_profile_not_enabled"
        else:
            reason = "cuda_unavailable"
        return DeviceResolution(
            requested=str(raw or "auto"),
            resolved="cpu",
            source=source,
            profile=profile,
            cuda_available=bool(cuda_available),
            accelerator_enabled=False,
            disabled_accelerator_reason=reason,
        )
    if device.startswith("cuda"):
        if not nvidia_profile_enabled():
            return DeviceResolution(
                requested=str(raw or device),
                resolved="cpu",
                source=source,
                profile=profile,
                cuda_available=bool(cuda_available),
                accelerator_enabled=False,
                disabled_accelerator_reason="accelerator_profile_not_enabled",
            )
        if not nvidia_dependency_profile_enabled():
            return DeviceResolution(
                requested=str(raw or device),
                resolved="cpu",
                source=source,
                profile=profile,
                cuda_available=bool(cuda_available),
                accelerator_enabled=False,
                disabled_accelerator_reason="dependency_profile_not_enabled",
            )
        if cuda_available:
            return DeviceResolution(
                requested=str(raw or device),
                resolved=device,
                source=source,
                profile=profile,
                cuda_available=True,
                accelerator_enabled=True,
                disabled_accelerator_reason="",
            )
        return DeviceResolution(
            requested=str(raw or device),
            resolved="cpu",
            source=source,
            profile=profile,
            cuda_available=False,
            accelerator_enabled=False,
            disabled_accelerator_reason="cuda_unavailable",
        )
    if device == "cpu":
        reason = "cpu_requested" if source != "default" else "cpu_first_default"
        return DeviceResolution(
            requested=str(raw or "cpu"),
            resolved="cpu",
            source=source,
            profile=profile,
            cuda_available=bool(cuda_available),
            accelerator_enabled=False,
            disabled_accelerator_reason=reason,
        )
    return DeviceResolution(
        requested=str(raw or device),
        resolved="cpu",
        source=source,
        profile=profile,
        cuda_available=bool(cuda_available),
        accelerator_enabled=False,
        disabled_accelerator_reason=f"unsupported_device:{device}",
    )


def torch_device_is_cuda(torch_module: Any, resolution: DeviceResolution | str) -> bool:
    device = resolution.resolved if isinstance(resolution, DeviceResolution) else str(resolution)
    return str(device).strip().lower().startswith("cuda") and torch_cuda_available(torch_module)


def nvidia_telemetry_enabled(torch_module: Any | None = None) -> bool:
    if not nvidia_acceleration_profile_enabled():
        return False
    if not _truthy(os.environ.get("NVIDIA_TELEMETRY_ENABLED"), default=False):
        return False
    if torch_module is None:
        try:
            torch_module = importlib.import_module("torch")
        except Exception:
            return False
    return torch_cuda_available(torch_module)


def apply_cpu_first_runtime_defaults() -> None:
    os.environ.setdefault("TRADING_DEPENDENCY_PROFILE", DEFAULT_DEPENDENCY_PROFILE)
    os.environ.setdefault("RUNTIME_HARDWARE_PROFILE", DEFAULT_HARDWARE_PROFILE)
    for key in CPU_FIRST_DEVICE_ENV_KEYS:
        os.environ.setdefault(key, "cpu")
    os.environ.setdefault("TORCH_CPU_THREADS", str(DEFAULT_CPU_THREADS))
    os.environ.setdefault("TORCH_INTEROP_THREADS", str(DEFAULT_INTEROP_THREADS))
    for key in CPU_THREAD_ENV_KEYS:
        os.environ.setdefault(key, str(DEFAULT_CPU_THREADS))
    for key, value in CUDA_FEATURE_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def runtime_hardware_snapshot(torch_module: Any | None = None) -> dict[str, Any]:
    if torch_module is None:
        try:
            torch_module = importlib.import_module("torch")
        except Exception as exc:
            return {
                "ok": False,
                "profile": runtime_hardware_profile(),
                "dependency_profile": runtime_dependency_profile(),
                "accelerator_profile_error": accelerator_profile_error(),
                "cuda_available": False,
                "nvidia_profile_enabled": nvidia_profile_enabled(),
                "nvidia_dependency_profile_enabled": nvidia_dependency_profile_enabled(),
                "nvidia_telemetry_enabled": False,
                "amd_profile_selected": amd_profile_selected(),
                "disabled_accelerator_reason": "torch_unavailable",
                "error": f"{type(exc).__name__}: {exc}",
                "devices": {},
                "threads": {
                    "cpu_threads": max(1, _env_int("TORCH_CPU_THREADS", DEFAULT_CPU_THREADS)),
                    "interop_threads": max(1, _env_int("TORCH_INTEROP_THREADS", DEFAULT_INTEROP_THREADS)),
                },
            }

    devices: dict[str, Mapping[str, Any]] = {}
    for key in CPU_FIRST_DEVICE_ENV_KEYS:
        resolution = resolve_torch_device(
            torch_module,
            env_var=key,
            fallback_envs=() if key == "TORCH_DEVICE" else ("TORCH_DEVICE",),
        )
        devices[key] = asdict(resolution)

    actual_cpu_threads = None
    actual_interop_threads = None
    try:
        getter = getattr(torch_module, "get_num_threads", None)
        if callable(getter):
            actual_cpu_threads = int(getter())
    except Exception:
        actual_cpu_threads = None
    try:
        getter = getattr(torch_module, "get_num_interop_threads", None)
        if callable(getter):
            actual_interop_threads = int(getter())
    except Exception:
        actual_interop_threads = None

    reasons = sorted(
        {
            str(item.get("disabled_accelerator_reason") or "")
            for item in devices.values()
            if str(item.get("disabled_accelerator_reason") or "")
        }
    )
    return {
        "ok": True,
        "profile": runtime_hardware_profile(),
        "dependency_profile": runtime_dependency_profile(),
        "accelerator_profile_error": accelerator_profile_error(),
        "cuda_available": torch_cuda_available(torch_module),
        "nvidia_profile_enabled": nvidia_profile_enabled(),
        "nvidia_dependency_profile_enabled": nvidia_dependency_profile_enabled(),
        "nvidia_telemetry_enabled": nvidia_telemetry_enabled(torch_module),
        "amd_profile_selected": amd_profile_selected(),
        "disabled_accelerator_reason": ",".join(reasons),
        "devices": devices,
        "threads": {
            "cpu_threads": max(1, _env_int("TORCH_CPU_THREADS", DEFAULT_CPU_THREADS)),
            "interop_threads": max(1, _env_int("TORCH_INTEROP_THREADS", DEFAULT_INTEROP_THREADS)),
            "actual_cpu_threads": actual_cpu_threads,
            "actual_interop_threads": actual_interop_threads,
            "omp_num_threads": max(1, _env_int("OMP_NUM_THREADS", DEFAULT_CPU_THREADS)),
            "mkl_num_threads": max(1, _env_int("MKL_NUM_THREADS", DEFAULT_CPU_THREADS)),
            "openblas_num_threads": max(1, _env_int("OPENBLAS_NUM_THREADS", DEFAULT_CPU_THREADS)),
            "numexpr_num_threads": max(1, _env_int("NUMEXPR_NUM_THREADS", DEFAULT_CPU_THREADS)),
        },
    }


def log_runtime_hardware_diagnostics(
    logger: logging.Logger,
    *,
    torch_module: Any | None = None,
    component: str = "runtime",
) -> dict[str, Any]:
    snapshot = runtime_hardware_snapshot(torch_module)
    key = (
        f"{component}:{snapshot.get('profile')}:{snapshot.get('dependency_profile')}:"
        f"{snapshot.get('disabled_accelerator_reason')}:{snapshot.get('accelerator_profile_error')}"
    )
    if key not in _WARNED_LOG_KEYS:
        _WARNED_LOG_KEYS.add(key)
        devices = dict(snapshot.get("devices") or {})
        threads = dict(snapshot.get("threads") or {})
        logger.info(
            "runtime_hardware component=%s profile=%s dependency_profile=%s torch_device=%s embed_device=%s nlp_device=%s "
            "finbert_device=%s ts_foundation_device=%s cpu_threads=%s interop_threads=%s "
            "nvidia_telemetry=%s disabled_accelerator_reason=%s accelerator_profile_error=%s",
            component,
            snapshot.get("profile"),
            snapshot.get("dependency_profile"),
            dict(devices.get("TORCH_DEVICE") or {}).get("resolved"),
            dict(devices.get("EMBED_DEVICE") or {}).get("resolved"),
            dict(devices.get("NLP_DEVICE") or {}).get("resolved"),
            dict(devices.get("FINBERT_DEVICE") or {}).get("resolved"),
            dict(devices.get("TS_FOUNDATION_DEVICE") or {}).get("resolved"),
            threads.get("cpu_threads"),
            threads.get("interop_threads"),
            int(bool(snapshot.get("nvidia_telemetry_enabled"))),
            snapshot.get("disabled_accelerator_reason") or "",
            snapshot.get("accelerator_profile_error") or "",
        )
    return snapshot
