from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from engine.strategy.discovery.llm_factor_generator import (
    cumulative_trial_count,
    run_llm_factor_discovery,
)
from engine.strategy.discovery.registry import ensure_discovery_schema, list_evaluations, list_registered_features

REPO_ROOT = Path(__file__).resolve().parents[1]
FEATURE_IDS = ["f0", "f1", "f2", "f3"]


def _frames(*, leakage: bool = False):
    n = 160
    idx = np.arange(n, dtype=float)
    ts_ms = 1_700_000_000_000 + np.arange(n, dtype=np.int64) * 86_400_000
    rng = np.random.default_rng(17 if not leakage else 23)
    f0 = np.sin(idx / 5.0)
    f1 = np.cos(idx / 7.0)
    f2 = np.sin(idx / 11.0) + 0.3
    f3 = np.cos(idx / 13.0)
    if leakage:
        target = np.empty(n, dtype=float)
        target[:80] = 3.0 * (f1[:80] * f2[:80])
        target[80:] = rng.normal(0.0, 1.0, size=80)
    else:
        target = (3.0 * (f1 * f2)) + (2.0 * (f0 * f3)) + rng.normal(0.0, 0.01, size=n)
    frame = pd.DataFrame(
        {
            "ts_ms": ts_ms,
            "f0": f0,
            "f1": f1,
            "f2": f2,
            "f3": f3,
            "target": target,
        }
    )
    return {"AAPL": frame.iloc[:80].reset_index(drop=True)}, {"AAPL": frame.iloc[80:].reset_index(drop=True)}, int(ts_ms[80])


def test_llm_factor_discovery_mocked_gauntlet_registers_only_novel(monkeypatch):
    train, test, cutoff = _frames()
    con = sqlite3.connect(":memory:")
    ensure_discovery_schema(con)
    monkeypatch.setenv("LLM_EVAL_MIN_TS", str(cutoff))

    def mocked_llm(**_kwargs):
        return json.dumps(
            {
                "candidates": [
                    {"expression": "x0", "hypothesis": "redundant copy of an existing feature"},
                    {"expression": "__import__('os')", "hypothesis": "not in the DSL"},
                    {"expression": "(x1*x2)", "hypothesis": "interaction captures convex carry pressure"},
                ]
            }
        )

    summary = run_llm_factor_discovery(
        symbols=["AAPL"],
        train_frames=train,
        test_frames=test,
        feature_ids=list(FEATURE_IDS),
        con=con,
        llm_client=mocked_llm,
        q_threshold=0.10,
        t_threshold=2.0,
    )

    assert summary["proposed"] == 2
    assert summary["parse_rejected"] == 1
    assert summary["redundant"] == 1
    assert summary["accepted"] == 1
    assert summary["registered_experimental"] == 1

    evaluations = list_evaluations(con=con)
    decisions = {row["decision"] for row in evaluations}
    assert "redundant" in decisions
    assert "accepted" in decisions

    registered = list_registered_features(con=con)
    assert len(registered) == 1
    assert registered[0].feature_id.startswith("discovered.llm.")
    assert registered[0].stage == "shadow"
    assert registered[0].source == "llm_factor"


def test_llm_factor_discovery_cumulative_fdr_denominator_grows(monkeypatch):
    train, test, cutoff = _frames()
    con = sqlite3.connect(":memory:")
    ensure_discovery_schema(con)
    monkeypatch.setenv("LLM_EVAL_MIN_TS", str(cutoff))

    def first_llm(**_kwargs):
        return json.dumps({"candidates": [{"expression": "(x1*x2)", "hypothesis": "first interaction"}]})

    def second_llm(**_kwargs):
        return json.dumps({"candidates": [{"expression": "(x0*x3)", "hypothesis": "second interaction"}]})

    first = run_llm_factor_discovery(
        symbols=["AAPL"],
        train_frames=train,
        test_frames=test,
        feature_ids=list(FEATURE_IDS),
        con=con,
        llm_client=first_llm,
        q_threshold=0.10,
        t_threshold=2.0,
    )
    second = run_llm_factor_discovery(
        symbols=["AAPL"],
        train_frames=train,
        test_frames=test,
        feature_ids=list(FEATURE_IDS),
        con=con,
        llm_client=second_llm,
        q_threshold=0.10,
        t_threshold=2.0,
    )

    assert first["cumulative_n_tests"] == 1
    assert second["cumulative_n_tests"] == 2
    assert cumulative_trial_count(con=con) == 2


def test_llm_factor_discovery_scores_only_post_cutoff_for_qualification(monkeypatch):
    train, test, cutoff = _frames(leakage=True)
    con = sqlite3.connect(":memory:")
    ensure_discovery_schema(con)
    monkeypatch.setenv("LLM_EVAL_MIN_TS", str(cutoff))

    def mocked_llm(**_kwargs):
        return json.dumps({"candidates": [{"expression": "(x1*x2)", "hypothesis": "works only before cutoff"}]})

    summary = run_llm_factor_discovery(
        symbols=["AAPL"],
        train_frames=train,
        test_frames=test,
        feature_ids=list(FEATURE_IDS),
        con=con,
        llm_client=mocked_llm,
        q_threshold=0.10,
        t_threshold=2.0,
    )

    assert summary["accepted"] == 0
    assert summary["registered_experimental"] == 0
    assert not list_registered_features(con=con)
    assert list_evaluations(con=con)[0]["decision"] != "accepted"


def test_llm_factor_discovery_noops_without_key(monkeypatch):
    import engine.strategy.discovery.llm_factor_generator as module

    monkeypatch.setenv("LLM_FACTOR_DISCOVERY", "1")
    monkeypatch.setattr(module, "load_anthropic_api_key", lambda: "")

    summary = module.run_llm_factor_discovery(symbols=["AAPL"], con=sqlite3.connect(":memory:"))

    assert summary["reason"] == "anthropic_api_key_missing"
    assert summary["proposed"] == 0


def test_load_anthropic_api_key_warns_on_secret_loader_failure(monkeypatch):
    import engine.strategy.discovery.llm_factor_generator as module
    import services.secrets.loader as loader

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(module, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))
    monkeypatch.setattr(loader, "load_secret", lambda _name: (_ for _ in ()).throw(RuntimeError("vault down")))
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "unit")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    assert module.load_anthropic_api_key() == ""
    assert calls
    assert calls[0][0][0] == "LLM_FACTOR_SECRET_LOAD_FAILED"


def test_execution_and_broker_modules_do_not_import_discovery() -> None:
    offenders = []
    for root in (REPO_ROOT / "engine" / "execution",):
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if "engine.strategy.discovery" in text or "strategy.discovery" in text:
                offenders.append(path.relative_to(REPO_ROOT).as_posix())
    for path in (REPO_ROOT / "engine" / "execution").glob("broker*.py"):
        text = path.read_text(encoding="utf-8")
        if "llm_factor" in text or "LLM_FACTOR" in text:
            offenders.append(path.relative_to(REPO_ROOT).as_posix())

    assert offenders == []
