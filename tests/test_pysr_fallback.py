from __future__ import annotations

import numpy as np
import pandas as pd

from engine.strategy.discovery.pysr_discoverer import PySRDiscoverer


def _symbolic_frame(n: int = 80) -> pd.DataFrame:
    idx = np.arange(n, dtype=float)
    x0 = np.sin(idx / 7.0)
    x1 = np.cos(idx / 11.0)
    target = (1.7 * x0) - (0.8 * x1)
    return pd.DataFrame(
        {
            "price.last": x0,
            "price.rv_20": x1,
            "target": target,
        }
    )


def test_pysr_exception_always_returns_fallback_candidates(monkeypatch) -> None:
    from engine.runtime import metrics as runtime_metrics

    emitted = []

    def capture_metric(metric: str, value=1, **kwargs):  # noqa: ANN001, ANN003
        emitted.append((metric, value, kwargs))

    def fail_pysr(self, symbol, x, y, feature_columns):  # noqa: ANN001
        raise RuntimeError("forced PySR failure")

    monkeypatch.delenv("PYSR_ALLOW_FALLBACK", raising=False)
    monkeypatch.setattr(runtime_metrics, "emit_counter", capture_metric)
    monkeypatch.setattr(PySRDiscoverer, "_propose_with_pysr", fail_pysr)

    candidates = PySRDiscoverer(top_k=3).propose("AAPL", _symbolic_frame())

    assert candidates
    assert all(candidate.params["engine"] == "fallback" for candidate in candidates)
    assert any(metric == "pysr_fallback_used" for metric, _value, _kwargs in emitted)


def test_pysr_fallback_failure_returns_empty_candidates(monkeypatch) -> None:
    from engine.runtime import metrics as runtime_metrics

    def fail_pysr(self, symbol, x, y, feature_columns):  # noqa: ANN001
        raise RuntimeError("forced PySR failure")

    def fail_fallback(self, symbol, x, y, feature_columns):  # noqa: ANN001
        raise RuntimeError("forced fallback failure")

    monkeypatch.setattr(runtime_metrics, "emit_counter", lambda *args, **kwargs: None)
    monkeypatch.setattr(PySRDiscoverer, "_propose_with_pysr", fail_pysr)
    monkeypatch.setattr(PySRDiscoverer, "_propose_with_fallback", fail_fallback)

    assert PySRDiscoverer(top_k=3).propose("AAPL", _symbolic_frame()) == []
