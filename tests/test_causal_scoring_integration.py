from __future__ import annotations

import sqlite3

import pytest

from engine.causal import dowhy_runner
from engine.causal.dag import CausalDAG
from engine.causal.dowhy_runner import DoWhyResult
from engine.causal.granger import GrangerResult
from engine.causal.scores import (
    CausalScoreRecord,
    causal_score,
    ensure_causal_schema,
    latest_causal_scores,
    upsert_causal_dag,
    upsert_causal_score,
)
from engine.strategy.ensemble.ridge_meta import RidgeStackEnsemble
from engine.strategy.jobs import causal_scoring
from engine.strategy.jobs.causal_scoring import run_causal_scoring
from engine.strategy.promotion_audit import _augment_reason_with_causal_scores


def _fixture_rows(n: int = 120) -> list[tuple[int, str, str, str, float, float]]:
    rows: list[tuple[int, str, str, str, float, float]] = []
    x_prev = 0.0
    y_prev = 0.0
    for idx in range(n):
        x = 0.35 * x_prev + ((idx % 7) - 3) / 10.0
        y = 0.25 * y_prev + 0.8 * x_prev + ((idx % 5) - 2) / 20.0
        rows.append((idx, "feature.x", "target.ret", "30d", float(x), float(y)))
        x_prev = x
        y_prev = y
    return rows


def test_causal_score_bounds_and_nontrivial_threshold() -> None:
    weak = causal_score(granger_p=1.0, dowhy_t=0.0)
    strong = causal_score(granger_p=1e-6, dowhy_t=0.0)

    assert 0.0 <= weak <= 1.0
    assert 0.0 <= strong <= 1.0
    assert weak < 0.5
    assert strong >= 0.5


def test_dowhy_missing_dependency_is_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_missing():
        raise ModuleNotFoundError("dowhy")

    monkeypatch.setattr(dowhy_runner, "_load_causal_model", _raise_missing)
    dag = CausalDAG(name="x_to_y", nodes=("x", "y"), edges=(("x", "y"),), treatment="x", outcome="y")

    result = dowhy_runner.run_dowhy({"x": [0.0, 1.0], "y": [1.0, 2.0]}, dag)

    assert result.decision == "skipped_no_dependency"


def test_causal_scoring_job_writes_scores_and_audit_payload_includes_them() -> None:
    con = sqlite3.connect(":memory:")
    ensure_causal_schema(con)
    con.execute(
        """
        CREATE TABLE causal_observations (
            ts INTEGER,
            feature TEXT,
            target TEXT,
            window TEXT,
            feature_value REAL,
            target_value REAL
        )
        """
    )
    con.executemany(
        """
        INSERT INTO causal_observations(ts, feature, target, window, feature_value, target_value)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        _fixture_rows(),
    )

    summary = run_causal_scoring(
        con=con,
        features=["feature.x"],
        targets=["target.ret"],
        windows=["30d"],
        now_ms=123456,
        min_obs=30,
        max_lag=3,
    )

    assert summary["written"] == 1
    row = con.execute(
        """
        SELECT feature, target, window, granger_p, granger_lag, score, decision
        FROM causal_scores
        WHERE feature='feature.x' AND target='target.ret' AND window='30d'
        """
    ).fetchone()
    assert row is not None
    assert row[0:3] == ("feature.x", "target.ret", "30d")
    assert 0.0 <= float(row[3]) <= 1.0
    assert int(row[4]) >= 1
    assert 0.0 <= float(row[5]) <= 1.0

    latest = latest_causal_scores(["feature.x"], con=con)
    assert latest["feature.x"] == pytest.approx(float(row[5]))

    reason = _augment_reason_with_causal_scores({"feature_ids": ["feature.x", "feature.missing"]}, con)
    assert set(reason["causal_scores"]) == {"feature.x", "feature.missing"}
    assert reason["causal_scores"]["feature.x"] == pytest.approx(float(row[5]))
    assert reason["causal_scores"]["feature.missing"] is None


def test_causal_scoring_job_passes_dag_confounders_to_granger(monkeypatch: pytest.MonkeyPatch) -> None:
    con = sqlite3.connect(":memory:")
    ensure_causal_schema(con)
    con.execute(
        """
        CREATE TABLE causal_observations (
            ts INTEGER,
            feature TEXT,
            target TEXT,
            window TEXT,
            feature_value REAL,
            target_value REAL,
            z REAL
        )
        """
    )
    rows = [
        (idx, "feature.x", "target.ret", "30d", float(idx % 5), float((idx % 5) * 0.5), float(idx % 3))
        for idx in range(60)
    ]
    con.executemany(
        """
        INSERT INTO causal_observations(ts, feature, target, window, feature_value, target_value, z)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    upsert_causal_dag(
        con,
        CausalDAG(
            name="x_to_ret_with_z",
            nodes=("feature.x", "target.ret", "z"),
            edges=(("z", "feature.x"), ("z", "target.ret"), ("feature.x", "target.ret")),
            treatment="feature.x",
            outcome="target.ret",
            confounders=("z",),
        ),
    )
    captured: dict[str, object] = {}

    def fake_granger(data, *, cause, effect, controls=None, max_lag=10):
        captured["controls"] = list(controls or [])
        captured["data_keys"] = sorted(data)
        return GrangerResult(p_value=0.01, lag=1, f_stat=7.0, hac_lag=1, n_obs=59, bic=0.0)

    monkeypatch.setattr(causal_scoring, "granger_causality", fake_granger)
    monkeypatch.setattr(
        causal_scoring,
        "run_dowhy",
        lambda data, dag: DoWhyResult(decision="skipped_no_dependency"),
    )

    summary = run_causal_scoring(
        con=con,
        features=["feature.x"],
        targets=["target.ret"],
        windows=["30d"],
        now_ms=123457,
        min_obs=30,
        max_lag=3,
    )

    assert summary["written"] == 1
    assert captured["controls"] == ["z"]
    assert captured["data_keys"] == ["feature.x", "target.ret", "z"]


def test_upsert_causal_score_round_trip() -> None:
    con = sqlite3.connect(":memory:")
    upsert_causal_score(
        con,
        CausalScoreRecord(
            feature="f",
            target="y",
            window="90d",
            ts=10,
            granger_p=0.01,
            granger_lag=1,
            dowhy_effect=None,
            dowhy_p=None,
            score=0.6,
            decision="granger_only",
        ),
    )

    assert latest_causal_scores(["f"], con=con)["f"] == pytest.approx(0.6)


def test_ridge_meta_no_prior_behavior_is_unchanged() -> None:
    data = [
        {"a": 0.1, "b": 0.2, "target": 0.12},
        {"a": 0.4, "b": 0.1, "target": 0.32},
        {"a": 0.3, "b": 0.5, "target": 0.31},
        {"a": 0.8, "b": 0.6, "target": 0.72},
    ]

    baseline = RidgeStackEnsemble(alpha=0.25, nonneg=False).fit(data)
    explicit_none = RidgeStackEnsemble(alpha=0.25, nonneg=False).fit(data, prior_weights=None)

    assert explicit_none.to_dict() == baseline.to_dict()
