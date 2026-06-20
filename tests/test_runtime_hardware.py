from __future__ import annotations

import logging
from pathlib import Path


class _FakeCuda:
    def __init__(self, available: bool) -> None:
        self._available = bool(available)

    def is_available(self) -> bool:
        return bool(self._available)


class _FakeTorch:
    def __init__(self, available: bool = False) -> None:
        self.cuda = _FakeCuda(available)

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
    ):
        monkeypatch.delenv(key, raising=False)

    from engine.runtime.hardware import apply_cpu_first_runtime_defaults, runtime_hardware_snapshot

    apply_cpu_first_runtime_defaults()
    snapshot = runtime_hardware_snapshot(_FakeTorch(available=True))

    assert snapshot["dependency_profile"] == "cpu"
    assert snapshot["devices"]["TORCH_DEVICE"]["resolved"] == "cpu"
    assert snapshot["devices"]["EMBED_DEVICE"]["resolved"] == "cpu"
    assert snapshot["threads"]["cpu_threads"] == 8
    assert snapshot["threads"]["interop_threads"] == 4
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


def test_amd_rocm_profile_is_reported_as_not_validated(monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "amd-rocm")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "cpu")
    from engine.runtime.hardware import runtime_hardware_snapshot

    snapshot = runtime_hardware_snapshot(_FakeTorch(available=False))

    assert snapshot["amd_profile_selected"] is True
    assert snapshot["accelerator_profile_error"] == "amd_rocm_profile_not_validated"


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
        assert "apply_cpu_first_runtime_defaults()" in text
        assert "log_runtime_hardware_diagnostics" in text
