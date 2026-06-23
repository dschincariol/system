from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from engine.runtime import acceleration

REPO_ROOT = Path(__file__).resolve().parents[2]


class _FakeVersion:
    def __init__(self, *, hip: str = "", cuda: str = "") -> None:
        self.hip = hip
        self.cuda = cuda


class _FakeCuda:
    def __init__(self, *, available: bool, count: int = 0) -> None:
        self._available = bool(available)
        self._count = int(count)

    def is_available(self) -> bool:
        return bool(self._available)

    def device_count(self) -> int:
        return int(self._count)

    def get_device_name(self, idx: int) -> str:
        return f"fake-gpu-{idx}"


class _FakeTorch:
    __version__ = "2.9.1+rocm7.2.4"

    def __init__(self, *, hip: str = "", cuda_available: bool = False, count: int = 0) -> None:
        self.version = _FakeVersion(hip=hip)
        self.cuda = _FakeCuda(available=cuda_available, count=count)


def _json_from_stdout(stdout: str) -> dict:
    start = stdout.find("{")
    assert start >= 0, stdout
    return json.loads(stdout[start:])


def _host_rocm_available() -> bool:
    try:
        import torch

        snapshot = acceleration.probe_torch_acceleration(
            torch_module=torch,
            persist_env=False,
            emit_log=False,
            profile="amd-rocm",
        )
    except Exception:
        return False
    return bool(snapshot.get("rocm_available"))


def test_cpu_profile_keeps_default_device_cpu_even_when_torch_can_see_gpu() -> None:
    torch = _FakeTorch(hip="6.4", cuda_available=True, count=1)

    resolved = acceleration.resolve_torch_device(torch, requested="auto", profile="cpu")

    assert resolved["effective_device"] == "cpu"
    assert resolved["fallback_reason"] == "cpu_profile"
    assert resolved["torch_cuda_is_available"] is True


def test_amd_rocm_profile_selects_cuda_api_for_hip_runtime() -> None:
    torch = _FakeTorch(hip="6.4", cuda_available=True, count=1)

    resolved = acceleration.resolve_torch_device(torch, requested="auto", profile="amd-rocm")

    assert resolved["effective_device"] == "cuda"
    assert resolved["hip_version"] == "6.4"
    assert resolved["torch_cuda_device_count"] == 1
    assert resolved["fallback_reason"] == ""


def test_amd_rocm_profile_falls_back_to_cpu_for_cpu_torch() -> None:
    torch = _FakeTorch(hip="", cuda_available=False, count=0)

    resolved = acceleration.resolve_torch_device(torch, requested="auto", profile="amd-rocm")

    assert resolved["effective_device"] == "cpu"
    assert resolved["fallback_reason"] == "torch_cuda_unavailable"


def test_amd_rocm_profile_requires_visible_device_count() -> None:
    torch = _FakeTorch(hip="7.2.4", cuda_available=True, count=0)

    resolved = acceleration.resolve_torch_device(torch, requested="auto", profile="amd-rocm")

    assert resolved["effective_device"] == "cpu"
    assert resolved["fallback_reason"] == "torch_cuda_device_count_zero"
    assert resolved["torch_cuda_is_available"] is True
    assert resolved["torch_cuda_device_count"] == 0


def test_probe_persists_effective_cpu_env_for_missing_rocm(monkeypatch: pytest.MonkeyPatch) -> None:
    torch = _FakeTorch(hip="", cuda_available=False, count=0)
    monkeypatch.setenv("TRADING_ACCELERATION_PROFILE", "amd-rocm")

    snapshot = acceleration.probe_torch_acceleration(torch_module=torch, strict_profile=False)

    assert snapshot["effective_device"] == "cpu"
    assert snapshot["rocm_available"] is False
    assert snapshot["fallback_reason"] == "torch_cuda_unavailable"
    assert snapshot["torch_cuda_is_available"] is False
    assert snapshot["torch_cuda_device_count"] == 0
    assert snapshot["torch_imported"] is True


def test_probe_strict_amd_rocm_profile_raises_for_cpu_torch(monkeypatch: pytest.MonkeyPatch) -> None:
    torch = _FakeTorch(hip="", cuda_available=False, count=0)
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "amd-rocm")
    monkeypatch.setattr(acceleration, "amd_rocm_python_marker_error", lambda **_: "")

    with pytest.raises(acceleration.AccelerationProfileError) as excinfo:
        acceleration.probe_torch_acceleration(torch_module=torch)

    assert "amd_rocm_torch_not_hip_build" in str(excinfo.value)


@pytest.mark.linux_only
def test_rocm_validation_harness_allows_missing_gpu() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "tools/validate_rocm_acceleration.py",
            "--json",
            "--allow-missing-gpu",
            "--matmul-size",
            "16",
            "--model-batch",
            "1",
            "--model-seq-len",
            "8",
            "--model-features",
            "3",
            "--model-horizons",
            "2",
            "--repeat",
            "1",
        ],
        cwd=str(REPO_ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = _json_from_stdout(result.stdout)
    assert "torch" in payload
    assert payload["benchmarks"]["cpu"]["matmul"]["device"] == "cpu"


def test_rocm_validation_harness_uses_dependency_light_patchtst_core() -> None:
    text = (REPO_ROOT / "tools" / "validate_rocm_acceleration.py").read_text(encoding="utf-8")

    assert "engine.strategy.patchtst_core import PatchTST" in text
    assert "engine.strategy.models.patchtst import PatchTST" not in text
    assert "engine.strategy.models.patchtst_core import PatchTST" not in text


@pytest.mark.linux_only
@pytest.mark.requires_rocm
def test_rocm_validation_harness_require_gpu() -> None:
    if not _host_rocm_available():
        pytest.skip("ROCm torch GPU is unavailable on this host")

    result = subprocess.run(
        [
            sys.executable,
            "tools/validate_rocm_acceleration.py",
            "--json",
            "--require-gpu",
            "--matmul-size",
            "16",
            "--model-batch",
            "1",
            "--model-seq-len",
            "8",
            "--model-features",
            "3",
            "--model-horizons",
            "2",
            "--repeat",
            "1",
        ],
        cwd=str(REPO_ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = _json_from_stdout(result.stdout)
    assert payload["torch"]["rocm_available"] is True
    assert payload["benchmarks"]["gpu"]["skipped"] is False
