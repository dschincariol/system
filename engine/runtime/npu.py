"""AMD XDNA2 NPU (Ryzen AI) readiness probe and fail-closed backend resolver.

The XDNA2 NPU is an INT8 inference accelerator reached through a different stack
than the ROCm GPU and is NOT a torch device:

    amdxdna kernel driver + firmware  ->  XRT / xdna userspace  ->
    ONNX Runtime + VitisAI execution provider  ->  INT8-quantized model

This module keeps NPU use strictly opt-in and fail-closed: ``resolve_nlp_backend``
only returns the NPU/ONNX path when the operator has explicitly enabled it AND
the full stack is actually present; otherwise it returns the CPU torch path so
the runtime never depends on a half-installed accelerator. Like the ROCm
profile, the NPU never touches the live order path — it only changes which
inference backend an NLP model selects.

Nothing here imports torch or onnxruntime at module load; probes are guarded so
this is safe to import from any process.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import Any, Dict, List

ACCEL_DEVICE = "/dev/accel/accel0"
FIRMWARE_DIR = "/lib/firmware/amdnpu"
TRUTHY = {"1", "true", "yes", "on"}
NPU_BACKEND_NAME = "onnx-vitisai"
CPU_BACKEND_NAME = "torch-cpu"


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _env_truthy(name: str) -> bool:
    return _normalize(os.environ.get(name)) in TRUTHY


def npu_inference_enabled() -> bool:
    """True when the operator has explicitly opted into NPU inference.

    Either a dedicated toggle or selecting the onnx-vitisai NLP backend counts.
    """
    if _env_truthy("TRADING_NPU_INFERENCE"):
        return True
    if _normalize(os.environ.get("TRADING_ACCELERATION_PROFILE")) in {"amd-npu", "npu", "xdna"}:
        return True
    return _normalize(os.environ.get("FINBERT_BACKEND")) == NPU_BACKEND_NAME or _normalize(
        os.environ.get("NLP_BACKEND")
    ) == NPU_BACKEND_NAME


def _module_loaded(name: str) -> bool:
    try:
        with open("/proc/modules", "r", encoding="utf-8") as handle:
            return any(line.split(" ", 1)[0] == name for line in handle)
    except OSError:
        return False


def _device_status(path: str = ACCEL_DEVICE) -> Dict[str, Any]:
    exists = os.path.exists(path)
    return {
        "path": path,
        "exists": bool(exists),
        "readable": bool(exists and os.access(path, os.R_OK)),
        "writable": bool(exists and os.access(path, os.W_OK)),
    }


def _firmware_status() -> Dict[str, Any]:
    try:
        revisions = sorted(os.listdir(FIRMWARE_DIR))
    except OSError:
        return {"dir": FIRMWARE_DIR, "exists": False, "revisions": []}
    return {"dir": FIRMWARE_DIR, "exists": True, "revisions": revisions}


def _group_member(name: str) -> bool:
    try:
        import grp

        return grp.getgrnam(name).gr_gid in set(os.getgroups())
    except Exception:
        return False


def _xrt_status() -> Dict[str, Any]:
    return {
        "xrt_smi": shutil.which("xrt-smi"),
        "xbutil": shutil.which("xbutil"),
        "opt_xilinx_xrt": os.path.isdir("/opt/xilinx/xrt"),
    }


def _onnxruntime_status() -> Dict[str, Any]:
    try:
        import onnxruntime as ort  # type: ignore
    except Exception as exc:
        return {"installed": False, "error": f"{type(exc).__name__}: {exc}", "providers": [], "vitisai": False}
    try:
        providers = list(ort.get_available_providers())
    except Exception as exc:
        return {"installed": True, "error": f"{type(exc).__name__}: {exc}", "providers": [], "vitisai": False}
    return {
        "installed": True,
        "version": str(getattr(ort, "__version__", "")),
        "providers": providers,
        "vitisai": bool("VitisAIExecutionProvider" in providers),
    }


@dataclass(frozen=True)
class NpuReadiness:
    kernel_ready: bool
    userspace_ready: bool
    inference_ready: bool
    next_step: str


def probe_npu_stack() -> Dict[str, Any]:
    """Return a layered readiness snapshot for the NPU enablement stack."""
    device = _device_status()
    firmware = _firmware_status()
    groups = {"render": _group_member("render"), "video": _group_member("video")}
    xrt = _xrt_status()
    ort = _onnxruntime_status()

    kernel_ready = bool(
        _module_loaded("amdxdna")
        and device["exists"]
        and device["readable"]
        and firmware["exists"]
        and (groups["render"] or groups["video"])
    )
    userspace_ready = bool(xrt["opt_xilinx_xrt"] or xrt["xrt_smi"])
    inference_ready = bool(ort["installed"] and ort["vitisai"])

    if not kernel_ready:
        next_step = "install_or_fix_kernel_driver_firmware_or_group_access"
    elif not userspace_ready:
        next_step = "install_xrt_xdna_userspace"
    elif not inference_ready:
        next_step = "install_onnxruntime_vitisai_execution_provider"
    else:
        next_step = "ready_export_quantize_and_run_int8_model"

    return {
        "kernel_ready": kernel_ready,
        "userspace_ready": userspace_ready,
        "inference_ready": inference_ready,
        "next_step": next_step,
        "amdxdna_module_loaded": _module_loaded("amdxdna"),
        "device": device,
        "firmware": firmware,
        "groups": groups,
        "xrt": xrt,
        "onnxruntime": ort,
    }


def npu_ready_for_inference(snapshot: Dict[str, Any] | None = None) -> bool:
    snap = snapshot if snapshot is not None else probe_npu_stack()
    return bool(snap.get("inference_ready"))


def resolve_nlp_backend(*, requested: Any = None, snapshot: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Fail-closed NLP inference backend selection.

    Returns the NPU/ONNX-VitisAI backend only when it is both opted-in and fully
    installed; otherwise returns the CPU torch backend with the reason it fell
    back. Never raises; never selects a half-installed accelerator.
    """
    want_npu = _normalize(requested) == NPU_BACKEND_NAME if requested is not None else npu_inference_enabled()
    if not want_npu:
        return {"backend": CPU_BACKEND_NAME, "reason": "npu_not_requested", "npu": None}
    snap = snapshot if snapshot is not None else probe_npu_stack()
    if not snap.get("inference_ready"):
        return {"backend": CPU_BACKEND_NAME, "reason": f"npu_stack_incomplete:{snap.get('next_step')}", "npu": snap}
    return {"backend": NPU_BACKEND_NAME, "reason": "", "npu": snap}


__all__: List[str] = [
    "NPU_BACKEND_NAME",
    "CPU_BACKEND_NAME",
    "NpuReadiness",
    "npu_inference_enabled",
    "npu_ready_for_inference",
    "probe_npu_stack",
    "resolve_nlp_backend",
]
