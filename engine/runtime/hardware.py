"""CPU-first runtime hardware and accelerator resolution helpers."""

from __future__ import annotations

import importlib
import logging
import os
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

from engine.runtime import acceleration


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
_AMD_DEPENDENCY_PROFILES = {"amd", "rocm", "amd-rocm", "amd_rocm"}
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
    hip_version: str = ""
    rocm_available: bool = False
    torch_cuda_device_count: int = 0


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
    return runtime_hardware_profile() in _AMD_PROFILES or runtime_dependency_profile() in _AMD_DEPENDENCY_PROFILES


def amd_dependency_profile_enabled() -> bool:
    return runtime_dependency_profile() in _AMD_DEPENDENCY_PROFILES


def amd_rocm_acceleration_profile_enabled() -> bool:
    return runtime_hardware_profile() in _AMD_PROFILES and amd_dependency_profile_enabled()


def accelerator_profile_error() -> str:
    hardware_profile = runtime_hardware_profile()
    dependency_profile = runtime_dependency_profile()
    if hardware_profile in _AMD_PROFILES and dependency_profile not in _AMD_DEPENDENCY_PROFILES:
        return "amd_rocm_runtime_requires_amd_rocm_dependency_profile"
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


def torch_cuda_device_count(torch_module: Any) -> int:
    cuda = getattr(torch_module, "cuda", None)
    device_count = getattr(cuda, "device_count", None)
    try:
        return max(0, int(device_count())) if callable(device_count) else 0
    except Exception:
        return 0


def torch_hip_version(torch_module: Any) -> str:
    version = getattr(torch_module, "version", None)
    try:
        return str(getattr(version, "hip", "") or "")
    except Exception:
        return ""


def torch_rocm_available(torch_module: Any) -> bool:
    return bool(torch_hip_version(torch_module) and torch_cuda_available(torch_module) and torch_cuda_device_count(torch_module) > 0)


def _legacy_cuda_enabled(legacy_cuda_flag: str | None) -> bool:
    return bool(legacy_cuda_flag and _truthy(os.environ.get(str(legacy_cuda_flag)), default=False))


def _base_resolution(
    *,
    raw: Any,
    source: str,
    profile: str,
    torch_module: Any,
    resolved: str,
    accelerator_enabled: bool,
    disabled_accelerator_reason: str,
) -> DeviceResolution:
    return DeviceResolution(
        requested=str(raw or resolved or "cpu"),
        resolved=str(resolved or "cpu"),
        source=source,
        profile=profile,
        cuda_available=torch_cuda_available(torch_module),
        accelerator_enabled=bool(accelerator_enabled),
        disabled_accelerator_reason=str(disabled_accelerator_reason or ""),
        hip_version=torch_hip_version(torch_module),
        rocm_available=torch_rocm_available(torch_module),
        torch_cuda_device_count=torch_cuda_device_count(torch_module),
    )


def _rocm_profile_block_reason() -> str:
    if runtime_hardware_profile() not in _AMD_PROFILES:
        return "accelerator_profile_not_enabled"
    if not amd_dependency_profile_enabled():
        return "dependency_profile_not_enabled"
    return ""


def _resolve_rocm_torch_device(
    torch_module: Any,
    *,
    raw: Any,
    source: str,
    requested_device: str,
    legacy_cuda_flag: str | None,
) -> DeviceResolution:
    profile = runtime_hardware_profile()
    block_reason = _rocm_profile_block_reason()
    if block_reason:
        return _base_resolution(
            raw=raw,
            source=source,
            profile=profile,
            torch_module=torch_module,
            resolved="cpu",
            accelerator_enabled=False,
            disabled_accelerator_reason=block_reason,
        )

    snapshot = acceleration.resolve_torch_device(
        torch_module,
        requested=requested_device or "auto",
        profile="amd-rocm",
        legacy_cuda_enabled=_legacy_cuda_enabled(legacy_cuda_flag),
    )
    effective = str(snapshot.get("effective_device") or "cpu").strip().lower() or "cpu"
    selected = effective == "cuda"
    resolved = str(requested_device or "cuda").strip().lower() if selected and str(requested_device).startswith("cuda") else effective
    return DeviceResolution(
        requested=str(raw or requested_device or "auto"),
        resolved=resolved,
        source=source,
        profile=profile,
        cuda_available=bool(snapshot.get("torch_cuda_is_available")),
        accelerator_enabled=bool(selected),
        disabled_accelerator_reason=str(snapshot.get("fallback_reason") or ""),
        hip_version=str(snapshot.get("hip_version") or ""),
        rocm_available=bool(snapshot.get("rocm_available"))
        or bool(
            snapshot.get("torch_is_rocm_build")
            and snapshot.get("torch_cuda_is_available")
            and int(snapshot.get("torch_cuda_device_count") or 0) > 0
        ),
        torch_cuda_device_count=int(snapshot.get("torch_cuda_device_count") or 0),
    )


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
    rocm_device_requested = device in {"amd", "rocm", "hip", "amd-rocm", "amd_rocm"}

    if device in {"", "default"}:
        device = "cpu"
    if device == "auto":
        if runtime_hardware_profile() in _AMD_PROFILES or amd_dependency_profile_enabled():
            return _resolve_rocm_torch_device(
                torch_module,
                raw=raw or "auto",
                source=source,
                requested_device="auto",
                legacy_cuda_flag=legacy_cuda_flag,
            )
        if nvidia_acceleration_profile_enabled() and cuda_available:
            return DeviceResolution(
                requested=str(raw or "auto"),
                resolved="cuda",
                source=source,
                profile=profile,
                cuda_available=True,
                accelerator_enabled=True,
                disabled_accelerator_reason="",
                hip_version=torch_hip_version(torch_module),
                rocm_available=torch_rocm_available(torch_module),
                torch_cuda_device_count=torch_cuda_device_count(torch_module),
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
            hip_version=torch_hip_version(torch_module),
            rocm_available=torch_rocm_available(torch_module),
            torch_cuda_device_count=torch_cuda_device_count(torch_module),
        )
    if rocm_device_requested:
        return _resolve_rocm_torch_device(
            torch_module,
            raw=raw or device,
            source=source,
            requested_device="rocm",
            legacy_cuda_flag=legacy_cuda_flag,
        )
    if device.startswith("cuda"):
        if runtime_hardware_profile() in _AMD_PROFILES or amd_dependency_profile_enabled():
            return _resolve_rocm_torch_device(
                torch_module,
                raw=raw or device,
                source=source,
                requested_device=device,
                legacy_cuda_flag=legacy_cuda_flag,
            )
        if not nvidia_profile_enabled():
            return DeviceResolution(
                requested=str(raw or device),
                resolved="cpu",
                source=source,
                profile=profile,
                cuda_available=bool(cuda_available),
                accelerator_enabled=False,
                disabled_accelerator_reason="accelerator_profile_not_enabled",
                hip_version=torch_hip_version(torch_module),
                rocm_available=torch_rocm_available(torch_module),
                torch_cuda_device_count=torch_cuda_device_count(torch_module),
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
                hip_version=torch_hip_version(torch_module),
                rocm_available=torch_rocm_available(torch_module),
                torch_cuda_device_count=torch_cuda_device_count(torch_module),
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
                hip_version=torch_hip_version(torch_module),
                rocm_available=torch_rocm_available(torch_module),
                torch_cuda_device_count=torch_cuda_device_count(torch_module),
            )
        return DeviceResolution(
            requested=str(raw or device),
            resolved="cpu",
            source=source,
            profile=profile,
            cuda_available=False,
            accelerator_enabled=False,
            disabled_accelerator_reason="cuda_unavailable",
            hip_version=torch_hip_version(torch_module),
            rocm_available=torch_rocm_available(torch_module),
            torch_cuda_device_count=torch_cuda_device_count(torch_module),
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
            hip_version=torch_hip_version(torch_module),
            rocm_available=torch_rocm_available(torch_module),
            torch_cuda_device_count=torch_cuda_device_count(torch_module),
        )
    return DeviceResolution(
        requested=str(raw or device),
        resolved="cpu",
        source=source,
        profile=profile,
        cuda_available=bool(cuda_available),
        accelerator_enabled=False,
        disabled_accelerator_reason=f"unsupported_device:{device}",
        hip_version=torch_hip_version(torch_module),
        rocm_available=torch_rocm_available(torch_module),
        torch_cuda_device_count=torch_cuda_device_count(torch_module),
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
                "hip_version": "",
                "rocm_available": False,
                "torch_cuda_device_count": 0,
                "nvidia_profile_enabled": nvidia_profile_enabled(),
                "nvidia_dependency_profile_enabled": nvidia_dependency_profile_enabled(),
                "amd_dependency_profile_enabled": amd_dependency_profile_enabled(),
                "amd_rocm_acceleration_profile_enabled": amd_rocm_acceleration_profile_enabled(),
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
        "hip_version": torch_hip_version(torch_module),
        "rocm_available": torch_rocm_available(torch_module),
        "torch_cuda_device_count": torch_cuda_device_count(torch_module),
        "nvidia_profile_enabled": nvidia_profile_enabled(),
        "nvidia_dependency_profile_enabled": nvidia_dependency_profile_enabled(),
        "amd_dependency_profile_enabled": amd_dependency_profile_enabled(),
        "amd_rocm_acceleration_profile_enabled": amd_rocm_acceleration_profile_enabled(),
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
