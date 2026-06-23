from __future__ import annotations

"""Coverage for the XDNA2 NPU readiness probe and fail-closed backend resolver.

The critical invariant: enabling the NPU must NEVER break or change the default
runtime path unless the full NPU stack is actually installed. These tests lock
that contract in regardless of host hardware.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from engine.runtime import npu

REPO_ROOT = Path(__file__).resolve().parents[2]


def _clear_npu_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "TRADING_NPU_INFERENCE",
        "TRADING_ACCELERATION_PROFILE",
        "FINBERT_BACKEND",
        "NLP_BACKEND",
    ):
        monkeypatch.delenv(key, raising=False)


def test_probe_returns_layered_readiness_keys() -> None:
    snap = npu.probe_npu_stack()
    for key in ("kernel_ready", "userspace_ready", "inference_ready", "next_step", "device", "firmware"):
        assert key in snap
    assert isinstance(snap["kernel_ready"], bool)


def test_default_runtime_does_not_select_npu(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_npu_env(monkeypatch)
    assert npu.npu_inference_enabled() is False
    decision = npu.resolve_nlp_backend()
    assert decision["backend"] == npu.CPU_BACKEND_NAME
    assert decision["reason"] == "npu_not_requested"


def test_opt_in_but_stack_incomplete_falls_back_to_cpu(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_npu_env(monkeypatch)
    monkeypatch.setenv("FINBERT_BACKEND", "onnx-vitisai")
    assert npu.npu_inference_enabled() is True
    # Force an incomplete stack so the test is independent of host state.
    incomplete = {"inference_ready": False, "next_step": "install_xrt_xdna_userspace"}
    decision = npu.resolve_nlp_backend(snapshot=incomplete)
    assert decision["backend"] == npu.CPU_BACKEND_NAME
    assert "npu_stack_incomplete" in decision["reason"]


def test_resolver_selects_npu_only_when_fully_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_npu_env(monkeypatch)
    monkeypatch.setenv("TRADING_NPU_INFERENCE", "1")
    ready = {"inference_ready": True, "next_step": "ready_export_quantize_and_run_int8_model"}
    decision = npu.resolve_nlp_backend(snapshot=ready)
    assert decision["backend"] == npu.NPU_BACKEND_NAME
    assert decision["reason"] == ""


def test_finbert_npu_branch_is_noop_without_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_npu_env(monkeypatch)
    from engine.data import finbert_sentiment

    assert finbert_sentiment._try_npu_probabilities(["earnings beat expectations"]) is None


@pytest.mark.linux_only
def test_npu_validator_tool_runs() -> None:
    result = subprocess.run(
        [sys.executable, "tools/validate_npu_acceleration.py", "--json"],
        cwd=str(REPO_ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    # Exit code reflects kernel readiness; either way it must emit valid JSON.
    payload = json.loads(result.stdout[result.stdout.find("{"):])
    assert "kernel_ready" in payload
    assert "next_step" in payload
