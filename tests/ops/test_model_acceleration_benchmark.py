from __future__ import annotations

"""Suite coverage for the opt-in model acceleration benchmark and training driver.

These mirror tests/ops/test_rocm_acceleration.py: the default lane runs the
tools in CPU/`--allow-missing-gpu` mode (always available, proves the CPU
baseline and clean machine-readable output), while the `requires_rocm` lane runs
the strict `--require-gpu` path only when a real ROCm GPU is visible.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from engine.runtime import acceleration

REPO_ROOT = Path(__file__).resolve().parents[2]


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


def _run(tool: str, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, f"tools/{tool}", "--json", *extra],
        cwd=str(REPO_ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


# --------------------------------------------------------------------------- #
# benchmark_model_acceleration.py
# --------------------------------------------------------------------------- #


@pytest.mark.linux_only
def test_benchmark_allows_missing_gpu_cpu_baseline() -> None:
    result = _run(
        "benchmark_model_acceleration.py",
        "--allow-missing-gpu",
        "--skip", "finbert",
        "--batch", "4",
        "--seq-len", "16",
        "--features", "6",
        "--horizons", "2",
        "--d-model", "16",
        "--layers", "1",
        "--heads", "2",
        "--repeat", "2",
        "--warmup", "1",
    )

    assert result.returncode == 0, result.stderr
    payload = _json_from_stdout(result.stdout)
    assert payload["ok"] is True
    # CPU baseline is always produced for both time-series families.
    for name in ("patchtst", "itransformer"):
        cpu = payload["results"][name]["cpu"]
        assert cpu["skipped"] is False
        assert cpu["inference"]["per_iter_ms"] > 0.0
        assert cpu["train"]["per_iter_ms"] > 0.0
        # No GPU on a CPU-only host: the GPU scope is skipped, never faked.
        assert payload["results"][name]["gpu"]["skipped"] is True


def test_benchmark_builds_real_model_families() -> None:
    text = (REPO_ROOT / "tools" / "benchmark_model_acceleration.py").read_text(encoding="utf-8")
    # The benchmark must exercise the real networks / pipeline, not stand-ins.
    assert "engine.strategy.patchtst_core import PatchTST" in text
    assert "engine.strategy.models.itransformer import ITransformer" in text
    assert "engine.data.finbert_sentiment import load_finbert_model" in text


@pytest.mark.linux_only
@pytest.mark.requires_rocm
def test_benchmark_require_gpu_reports_speedup() -> None:
    if not _host_rocm_available():
        pytest.skip("ROCm torch GPU is unavailable on this host")

    result = _run(
        "benchmark_model_acceleration.py",
        "--require-gpu",
        "--skip", "finbert",
        "--batch", "8",
        "--seq-len", "32",
        "--features", "8",
        "--horizons", "2",
        "--repeat", "3",
    )

    assert result.returncode == 0, result.stderr
    payload = _json_from_stdout(result.stdout)
    assert payload["gpu_available"] is True
    for name in ("patchtst", "itransformer"):
        gpu = payload["results"][name]["gpu"]
        assert gpu["skipped"] is False
        assert gpu["inference"]["per_iter_ms"] > 0.0
        assert payload["results"][name].get("speedup_inference") is not None


# --------------------------------------------------------------------------- #
# train_torch_models_gpu.py
# --------------------------------------------------------------------------- #


@pytest.mark.linux_only
def test_training_driver_trains_on_cpu_and_reduces_loss() -> None:
    result = _run(
        "train_torch_models_gpu.py",
        "--allow-missing-gpu",
        "--epochs", "5",
        "--samples", "48",
        "--seq-len", "32",
        "--horizons", "2",
    )

    assert result.returncode == 0, result.stderr
    payload = _json_from_stdout(result.stdout)
    assert payload["ok"] is True
    for name in ("patchtst", "itransformer"):
        entry = payload["results"][name]
        assert entry["skipped"] is False, entry
        assert entry["device"] == "cpu"
        assert entry["ran_on_cuda"] is False
        # The real production .fit() wrapper ran end-to-end and learned the signal.
        assert entry["loss_final"] <= entry["loss_initial"]


@pytest.mark.linux_only
@pytest.mark.requires_rocm
def test_training_driver_require_gpu_runs_on_cuda() -> None:
    if not _host_rocm_available():
        pytest.skip("ROCm torch GPU is unavailable on this host")

    result = _run(
        "train_torch_models_gpu.py",
        "--require-gpu",
        "--epochs", "5",
        "--samples", "48",
        "--seq-len", "32",
        "--horizons", "2",
    )

    assert result.returncode == 0, result.stderr
    payload = _json_from_stdout(result.stdout)
    assert payload["gpu_available"] is True
    for name in ("patchtst", "itransformer"):
        entry = payload["results"][name]
        assert entry["skipped"] is False, entry
        assert entry["ran_on_cuda"] is True
