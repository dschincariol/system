from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest


class _FakeCuda:
    def __init__(self, available: bool, count: int = 0) -> None:
        self._available = bool(available)
        self._count = int(count)

    def is_available(self) -> bool:
        return bool(self._available)

    def device_count(self) -> int:
        return int(self._count)

    def get_device_name(self, idx: int) -> str:
        return f"fake-device-{idx}"


class _FakeVersion:
    def __init__(self, hip: str = "", cuda: str = "") -> None:
        self.hip = hip
        self.cuda = cuda


class _FakeTorch:
    def __init__(self, available: bool = False, *, hip: str = "", count: int = 0) -> None:
        self.version = _FakeVersion(hip=hip)
        self.cuda = _FakeCuda(available, count=count)

    def get_num_threads(self) -> int:
        return 8

    def get_num_interop_threads(self) -> int:
        return 4


def test_resolve_torch_device_defaults_to_cpu_even_when_cuda_available(monkeypatch) -> None:
    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.delenv("RUNTIME_HARDWARE_PROFILE", raising=False)
    from engine.runtime.hardware import resolve_torch_device

    resolution = resolve_torch_device(_FakeTorch(available=True))

    assert resolution.resolved == "cpu"
    assert resolution.disabled_accelerator_reason == "cpu_first_default"


def test_auto_requires_explicit_nvidia_profile(monkeypatch) -> None:
    monkeypatch.setenv("TORCH_DEVICE", "auto")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "cpu")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "cpu")
    from engine.runtime.hardware import resolve_torch_device

    resolution = resolve_torch_device(_FakeTorch(available=True))

    assert resolution.resolved == "cpu"
    assert resolution.disabled_accelerator_reason == "accelerator_profile_not_enabled"

    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "nvidia")
    resolution = resolve_torch_device(_FakeTorch(available=True))

    assert resolution.resolved == "cpu"
    assert resolution.disabled_accelerator_reason == "dependency_profile_not_enabled"

    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "nvidia-cuda")
    resolution = resolve_torch_device(_FakeTorch(available=True))

    assert resolution.resolved == "cuda"
    assert resolution.accelerator_enabled is True


def test_explicit_cuda_falls_back_to_cpu_when_unavailable(monkeypatch) -> None:
    monkeypatch.setenv("TORCH_DEVICE", "cuda")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "nvidia")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "nvidia-cuda")
    from engine.runtime.hardware import resolve_torch_device

    resolution = resolve_torch_device(_FakeTorch(available=False))

    assert resolution.resolved == "cpu"
    assert resolution.disabled_accelerator_reason == "cuda_unavailable"


def test_cpu_first_defaults_and_snapshot(monkeypatch) -> None:
    for key in (
        "TORCH_DEVICE",
        "EMBED_DEVICE",
        "NLP_DEVICE",
        "FINBERT_DEVICE",
        "TS_FOUNDATION_DEVICE",
        "TORCH_CPU_THREADS",
        "TORCH_INTEROP_THREADS",
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "ENGINE_SUPERVISED_PROCESS_COUNT",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RUNTIME_CPUS", "12")
    monkeypatch.setenv("START_INGESTION_WITH_SERVER", "0")
    monkeypatch.setenv("TRADING_CPU_THREAD_POLICY", "auto")

    from engine.runtime.hardware import apply_cpu_first_runtime_defaults, runtime_hardware_snapshot

    apply_cpu_first_runtime_defaults()
    snapshot = runtime_hardware_snapshot(_FakeTorch(available=True))

    assert snapshot["dependency_profile"] == "cpu"
    assert snapshot["devices"]["TORCH_DEVICE"]["resolved"] == "cpu"
    assert snapshot["devices"]["EMBED_DEVICE"]["resolved"] == "cpu"
    assert snapshot["threads"]["cpu_threads"] == 4
    assert snapshot["threads"]["interop_threads"] == 2
    assert snapshot["thread_policy"]["role"] == "runtime"
    assert snapshot["thread_policy"]["supervised_process_count"] == 1
    assert snapshot["nvidia_telemetry_enabled"] is False


def test_hardware_diagnostics_logs_resolved_devices(monkeypatch, caplog) -> None:
    monkeypatch.setenv("TORCH_DEVICE", "cpu")
    from engine.runtime.hardware import log_runtime_hardware_diagnostics

    logger = logging.getLogger("tests.runtime_hardware")
    with caplog.at_level(logging.INFO, logger="tests.runtime_hardware"):
        snapshot = log_runtime_hardware_diagnostics(logger, torch_module=_FakeTorch(), component="unit")

    assert snapshot["devices"]["TORCH_DEVICE"]["resolved"] == "cpu"
    assert "dependency_profile=cpu" in caplog.text
    assert "torch_device=cpu" in caplog.text
    assert "disabled_accelerator_reason=" in caplog.text


def test_nvidia_telemetry_requires_nvidia_dependency_profile(monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "nvidia")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "cpu")
    monkeypatch.setenv("NVIDIA_TELEMETRY_ENABLED", "1")
    from engine.runtime.hardware import nvidia_telemetry_enabled, runtime_hardware_snapshot

    assert nvidia_telemetry_enabled(_FakeTorch(available=True)) is False
    snapshot = runtime_hardware_snapshot(_FakeTorch(available=True))
    assert snapshot["nvidia_profile_enabled"] is True
    assert snapshot["nvidia_dependency_profile_enabled"] is False
    assert snapshot["accelerator_profile_error"] == "nvidia_runtime_requires_nvidia_dependency_profile"


def test_nvidia_telemetry_requires_explicit_feature_flag(monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "nvidia")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "nvidia-cuda")
    monkeypatch.delenv("NVIDIA_TELEMETRY_ENABLED", raising=False)
    from engine.runtime.hardware import nvidia_telemetry_enabled, runtime_hardware_snapshot

    assert nvidia_telemetry_enabled(_FakeTorch(available=True)) is False
    snapshot = runtime_hardware_snapshot(_FakeTorch(available=True))
    assert snapshot["nvidia_telemetry_enabled"] is False

    monkeypatch.setenv("NVIDIA_TELEMETRY_ENABLED", "1")
    assert nvidia_telemetry_enabled(_FakeTorch(available=True)) is True


def test_amd_rocm_runtime_profile_requires_matching_dependency_profile(monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "amd-rocm")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "cpu")
    from engine.runtime.hardware import runtime_hardware_snapshot

    snapshot = runtime_hardware_snapshot(_FakeTorch(available=False))

    assert snapshot["amd_profile_selected"] is True
    assert snapshot["amd_dependency_profile_enabled"] is False
    assert snapshot["accelerator_profile_error"] == "amd_rocm_runtime_requires_amd_rocm_dependency_profile"


def test_amd_rocm_dependency_profile_without_rocm_torch_raises_hard(monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "amd-rocm")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "amd-rocm")
    from engine.runtime import acceleration
    from engine.runtime.hardware import runtime_hardware_snapshot

    expected = "amd_rocm_python_runtime_unsupported" if sys.version_info[:2] < (3, 12) else "amd_rocm_torch_not_hip_build"
    with pytest.raises(acceleration.AccelerationProfileError) as excinfo:
        runtime_hardware_snapshot(_FakeTorch(available=False))

    assert expected in str(excinfo.value)


def test_amd_rocm_auto_selects_hip_device_when_profile_is_enabled(monkeypatch) -> None:
    monkeypatch.setenv("TORCH_DEVICE", "auto")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "amd-rocm")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "amd-rocm")
    from engine.runtime import acceleration
    from engine.runtime.hardware import resolve_torch_device, runtime_hardware_snapshot

    monkeypatch.setattr(acceleration, "amd_rocm_python_marker_error", lambda **_: "")
    torch = _FakeTorch(available=True, hip="7.2.4", count=1)
    resolution = resolve_torch_device(torch)
    snapshot = runtime_hardware_snapshot(torch)

    assert resolution.resolved == "cuda"
    assert resolution.accelerator_enabled is True
    assert resolution.disabled_accelerator_reason == ""
    assert resolution.hip_version == "7.2.4"
    assert resolution.rocm_available is True
    assert resolution.torch_cuda_device_count == 1
    assert snapshot["devices"]["TORCH_DEVICE"]["resolved"] == "cuda"
    assert snapshot["devices"]["TORCH_DEVICE"]["rocm_available"] is True
    assert snapshot["rocm_available"] is True
    assert snapshot["amd_rocm_acceleration_profile_enabled"] is True


def test_explicit_rocm_device_raises_when_no_hip_device_is_visible(monkeypatch) -> None:
    monkeypatch.setenv("TORCH_DEVICE", "rocm")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "amd-rocm")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "amd-rocm")
    from engine.runtime import acceleration
    from engine.runtime.hardware import resolve_torch_device

    monkeypatch.setattr(acceleration, "amd_rocm_python_marker_error", lambda **_: "")
    with pytest.raises(acceleration.AccelerationProfileError) as excinfo:
        resolve_torch_device(_FakeTorch(available=True, hip="7.2.4", count=0))

    assert "amd_rocm_torch_device_count_zero" in str(excinfo.value)


def test_amd_rocm_dependency_profile_under_python_lt_312_raises_hard(monkeypatch) -> None:
    if sys.version_info[:2] >= (3, 12):
        pytest.skip("Python <3.12 marker check is exercised by the 3.11 host/runtime path.")
    monkeypatch.setenv("TORCH_DEVICE", "auto")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "amd-rocm")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "amd-rocm")
    from engine.runtime import acceleration
    from engine.runtime.hardware import runtime_hardware_snapshot

    with pytest.raises(acceleration.AccelerationProfileError) as excinfo:
        runtime_hardware_snapshot(_FakeTorch(available=True, hip="7.2.4", count=1))

    assert "amd_rocm_python_runtime_unsupported" in str(excinfo.value)
    assert "required_python=>=3.12" in str(excinfo.value)


@pytest.mark.requires_rocm
def test_py312_rocm_torch_runtime_selects_hip_device(monkeypatch) -> None:
    if sys.version_info[:2] < (3, 12):
        pytest.skip("requires Python 3.12 ROCm runtime image")
    torch = pytest.importorskip("torch")
    monkeypatch.setenv("TORCH_DEVICE", "auto")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "amd-rocm")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "amd-rocm")
    from engine.runtime.hardware import resolve_torch_device, torch_hip_version

    assert torch_hip_version(torch)
    resolution = resolve_torch_device(torch)

    assert resolution.resolved == "cuda"
    assert resolution.accelerator_enabled is True
    assert resolution.rocm_available is True
    assert resolution.torch_cuda_device_count > 0


def test_explicit_hip_device_requires_amd_rocm_dependency_profile(monkeypatch) -> None:
    monkeypatch.setenv("TORCH_DEVICE", "hip")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "amd-rocm")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "cpu")
    from engine.runtime.hardware import resolve_torch_device

    resolution = resolve_torch_device(_FakeTorch(available=True, hip="7.2.4", count=1))

    assert resolution.resolved == "cpu"
    assert resolution.disabled_accelerator_reason == "dependency_profile_not_enabled"


def test_event_workers_do_not_default_to_cuda() -> None:
    repo = Path(__file__).resolve().parents[1]
    for rel in (
        "engine/data/jobs/process_events.py",
        "engine/data/jobs/process_events_enriched.py",
        "engine/data/jobs/process_events_live.py",
        "engine/data/jobs/process_events_shadow.py",
    ):
        text = (repo / rel).read_text(encoding="utf-8")
        assert 'setdefault("TORCH_DEVICE", "cuda")' not in text
        assert 'setdefault("CUDA_VISIBLE_DEVICES", "0")' not in text


def test_runtime_entrypoints_apply_and_log_hardware_profile() -> None:
    repo = Path(__file__).resolve().parents[1]
    for rel in ("start_system.py", "start_ingestion.py"):
        text = (repo / rel).read_text(encoding="utf-8")
        assert "apply_cpu_first_runtime_defaults(" in text
        assert "log_runtime_hardware_diagnostics" in text
