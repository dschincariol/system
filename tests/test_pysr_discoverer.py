from __future__ import annotations

import importlib.util
import shutil

import numpy as np
import pandas as pd
import pytest

from engine.strategy.discovery.pysr_discoverer import PySRDiscoverer, expression_complexity

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("pysr") is None or shutil.which("julia") is None,
    reason="PySR and Julia are required to exercise the real symbolic engine",
)


def _symbolic_frame(n: int = 140) -> pd.DataFrame:
    idx = np.arange(n, dtype=float)
    x0 = np.sin(idx / 7.0)
    x1 = np.cos(idx / 11.0)
    x2 = (idx - idx.mean()) / idx.std()
    x3 = np.sqrt(np.abs(x0)) + 0.1 * x2
    target = (1.7 * x0) - (0.8 * x1) + (0.25 * x0 * x2)
    return pd.DataFrame(
        {
            "price.last": x0,
            "price.rv_20": x1,
            "price.momentum_1h": x2,
            "macro.policy_rate_upper_z": x3,
            "target": target,
        }
    )


def test_pysr_discoverer_returns_bounded_symbolic_candidates() -> None:
    frame = _symbolic_frame()
    discoverer = PySRDiscoverer(timeout_seconds=30, niterations=5, max_complexity=12, top_k=5)

    candidates = discoverer.propose("AAPL", frame)

    assert len(candidates) >= 5
    assert all(candidate.feature_id.startswith("discovered.pysr.") for candidate in candidates)
    assert all(int(candidate.params["complexity"]) <= 12 for candidate in candidates)
    assert all(expression_complexity(candidate.expression) <= 12 for candidate in candidates)
    assert len({candidate.hash for candidate in candidates}) == len(candidates)


def test_pysr_candidate_evaluates_on_oos_frame() -> None:
    frame = _symbolic_frame()
    discoverer = PySRDiscoverer(timeout_seconds=30, niterations=5, max_complexity=12, top_k=5)
    candidate = discoverer.propose("AAPL", frame.iloc[:100].reset_index(drop=True))[0]

    result = discoverer.evaluate(candidate, frame.iloc[100:].reset_index(drop=True), "target")

    assert result.candidate_hash == candidate.hash
    assert result.n_obs >= 8
    assert 0.0 <= result.p_value <= 1.0


def test_pysr_uses_explicit_registry_primitives_when_supplied() -> None:
    frame = _symbolic_frame()
    frame["non_registry_leak"] = frame["target"] * 100.0
    primitives = ["price.last", "price.rv_20"]
    discoverer = PySRDiscoverer(
        timeout_seconds=30,
        niterations=5,
        max_complexity=12,
        top_k=5,
        primitive_columns=primitives,
    )

    candidates = discoverer.propose("AAPL", frame)

    assert candidates
    assert all(set(candidate.params["source_feature_ids"]).issubset(set(primitives)) for candidate in candidates)


@pytest.mark.skipif(
    importlib.util.find_spec("pysr") is None or shutil.which("julia") is None,
    reason="PySR and Julia are required to exercise the real symbolic engine",
)
def test_real_pysr_engine_path_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = _symbolic_frame(180)
    discoverer = PySRDiscoverer(timeout_seconds=30, niterations=10, max_complexity=12, top_k=5)

    def fail_fallback(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("PySR fell back instead of returning real-engine candidates")

    monkeypatch.setattr(PySRDiscoverer, "_propose_with_fallback", fail_fallback)
    candidates = discoverer.propose("AAPL", frame)

    assert len(candidates) >= 5
    assert all(candidate.params["engine"] == "pysr" for candidate in candidates)
