from __future__ import annotations

import importlib.util

import numpy as np
import pandas as pd
import pytest

from engine.strategy.discovery.tsfresh_discoverer import TsfreshDiscoverer


pytestmark = pytest.mark.skipif(importlib.util.find_spec("tsfresh") is None, reason="tsfresh not installed")


def _synthetic_price_frame(n: int = 240) -> pd.DataFrame:
    idx = np.arange(n, dtype=float)
    close = 100.0 + (0.05 * idx) + np.sin(idx / 5.0) + (0.25 * np.cos(idx / 13.0))
    frame = pd.DataFrame(
        {
            "ts_ms": 1_710_000_000_000 + (idx.astype(int) * 60_000),
            "close": close,
        }
    )
    frame["target"] = pd.Series(close).pct_change().shift(-1)
    return frame.dropna().reset_index(drop=True)


def test_tsfresh_proposes_many_candidates_with_stable_hashes() -> None:
    frame = _synthetic_price_frame()
    discoverer = TsfreshDiscoverer(window=180, n_jobs=0)

    first = discoverer.propose("AAPL", frame)
    second = discoverer.propose("AAPL", frame)

    assert len(first) >= 50
    assert [candidate.hash for candidate in first] == [candidate.hash for candidate in second]
    assert len({candidate.hash for candidate in first}) == len(first)
    assert all(candidate.feature_id.startswith("discovered.tsfresh.") for candidate in first)
    assert all(candidate.params["window"] == 180 for candidate in first)


def test_tsfresh_evaluate_returns_oos_evidence() -> None:
    frame = _synthetic_price_frame(260)
    train = frame.iloc[:220].reset_index(drop=True)
    test = frame.iloc[20:].reset_index(drop=True)
    discoverer = TsfreshDiscoverer(window=180, n_jobs=0, max_candidates=80)
    candidate = discoverer.propose("AAPL", train)[0]

    result = discoverer.evaluate(candidate, test, "target")

    assert result.candidate_hash == candidate.hash
    assert result.n_obs > 0
    assert 0.0 <= result.p_value <= 1.0
    assert result.decision in {"pending", "degenerate"}
