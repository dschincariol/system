from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd

from engine.strategy.discovery.base import CandidateFeature, EvaluationResult
from engine.strategy.discovery.registry import ensure_discovery_schema, list_evaluations, list_registered_features
from engine.strategy.jobs.discover_features import run_discovery


class FixedDiscoverer:
    source = "fixed"

    def propose(self, symbol: str, train_df: pd.DataFrame) -> list[CandidateFeature]:
        del train_df
        return [
            CandidateFeature(source=self.source, symbol=symbol, expression="accepted", params={"kind": "strong"}),
            CandidateFeature(source=self.source, symbol=symbol, expression="tstat_failed", params={"kind": "weak_t"}),
            CandidateFeature(source=self.source, symbol=symbol, expression="fdr_failed", params={"kind": "weak_p"}),
            CandidateFeature(source=self.source, symbol=symbol, expression="degenerate", params={"kind": "flat"}),
        ]

    def evaluate(self, candidate: CandidateFeature, test_df: pd.DataFrame, target: str):
        del test_df, target
        if candidate.expression == "accepted":
            return EvaluationResult(candidate.hash, candidate.feature_id, t_stat=4.5, p_value=0.001, oos_ic=0.40, n_obs=80)
        if candidate.expression == "tstat_failed":
            return EvaluationResult(candidate.hash, candidate.feature_id, t_stat=1.2, p_value=0.001, oos_ic=0.10, n_obs=80)
        if candidate.expression == "fdr_failed":
            return EvaluationResult(candidate.hash, candidate.feature_id, t_stat=4.1, p_value=0.90, oos_ic=0.01, n_obs=80)
        return EvaluationResult(
            candidate.hash,
            candidate.feature_id,
            t_stat=0.0,
            p_value=1.0,
            decision="degenerate",
            n_obs=0,
        )


def _frame(n: int = 100) -> pd.DataFrame:
    idx = np.arange(n, dtype=float)
    return pd.DataFrame(
        {
            "close": 100.0 + idx,
            "feature_a": idx / 10.0,
            "target": idx / 20.0,
        }
    )


def test_discovery_job_gates_registers_shadow_only_and_reruns_noop() -> None:
    con = sqlite3.connect(":memory:")
    ensure_discovery_schema(con)
    train = {"AAPL": _frame()}
    test = {"AAPL": _frame()}

    first = run_discovery(
        symbols=["AAPL"],
        train_frames=train,
        test_frames=test,
        target="target",
        discoverers=[FixedDiscoverer()],
        con=con,
        feature_ids=["feature_a"],
    )
    second = run_discovery(
        symbols=["AAPL"],
        train_frames=train,
        test_frames=test,
        target="target",
        discoverers=[FixedDiscoverer()],
        con=con,
        feature_ids=["feature_a"],
    )

    assert first["proposed"] == 4
    assert first["evaluated"] == 4
    assert first["accepted"] == 1
    assert first["registered_shadow"] == 1
    assert second["proposed"] == 4
    assert second["evaluated"] == 0
    assert second["skipped"] == 4

    evaluations = list_evaluations(con=con)
    decisions = {row["decision"] for row in evaluations}
    assert "accepted" in decisions
    assert {"fdr_failed", "tstat_failed", "degenerate"}.issubset(decisions)

    registered = list_registered_features(con=con)
    assert len(registered) == 1
    assert registered[0].stage == "shadow"
    assert not list_registered_features(stage="live", con=con)
