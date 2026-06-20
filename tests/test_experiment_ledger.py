from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.promotion_test_helpers import passing_deconfounded_payload


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


@pytest.fixture()
def ledger_stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "experiment_ledger.db"))
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")
    modules = _reload_modules(
        "engine.runtime.db_guard",
        "engine.runtime.storage",
        "engine.runtime.runtime_meta",
        "engine.strategy.promotion_audit",
        "engine.strategy.experiment_ledger",
        "engine.model_registry",
    )
    storage = modules[1]
    storage.init_db()
    try:
        yield modules
    finally:
        storage.close_pooled_connections()


def _record_stat_pass(promotion_audit: Any, *, model_id: str) -> None:
    evidence_ts = int(time.time() * 1000)
    promotion_audit.record_statistical_evidence(
        model_id=str(model_id),
        test_name="white_reality_check",
        ts=int(evidence_ts),
        p_value=0.01,
        decision="pass",
        payload={"source": "unit"},
    )
    promotion_audit.record_statistical_evidence(
        model_id=str(model_id),
        test_name="deconfounded_signal_validation",
        ts=int(evidence_ts),
        t_stat=4.2,
        p_value=0.01,
        decision="pass",
        payload={
            "source": "unit",
            "passed": True,
            "status": "evaluated",
            **passing_deconfounded_payload(12),
        },
    )


def _set_replay(runtime_meta: Any, *, model_id: str, model_kind: str, model_ts_ms: int) -> None:
    now_ms = int(time.time() * 1000)
    runtime_meta.meta_set(
        "competition_replay_validation",
        json.dumps(
            {
                "ok": True,
                "updated_ts_ms": int(now_ms),
                "models": {
                    f"{model_id}|AAPL|300|global": {
                        "model_name": str(model_id),
                        "model_id": str(model_id),
                        "symbol": "AAPL",
                        "horizon_s": 300,
                        "regime": "global",
                        "model_kind": str(model_kind),
                        "model_ts_ms": int(model_ts_ms),
                        "approved": True,
                    }
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )
    runtime_meta.meta_set(
        "competition_replay_validation_status",
        json.dumps({"ok": True, "status": "ready", "updated_ts_ms": int(now_ms)}, separators=(",", ":"), sort_keys=True),
    )


def test_generated_direct_promotion_requires_passing_experiment_ledger(ledger_stack: tuple[Any, ...]) -> None:
    _db_guard, storage, runtime_meta, promotion_audit, experiment_ledger, model_registry = ledger_stack
    model_id = "ledger_alpha_AAPL_1700000000000_abcdef1"
    model_ts = 1700000000000
    model_registry.register_model(
        model_name=model_id,
        model_kind="test_kind",
        model_ts_ms=model_ts,
        stage="challenger",
        metrics={"score": 1.0},
        regime="global",
    )
    storage.record_alpha_candidate(
        candidate_name=model_id,
        candidate_version=str(model_ts),
        model_family="gbm_regressor",
        feature_ids=["price.last"],
        generation_method="single_group_v1",
        status="registered_challenger",
        diagnostics={"source": "unit"},
        created_ts=model_ts,
    )
    _record_stat_pass(promotion_audit, model_id=model_id)
    _set_replay(runtime_meta, model_id=model_id, model_kind="test_kind", model_ts_ms=model_ts)

    with pytest.raises(RuntimeError, match="experiment ledger blocked"):
        model_registry.promote_to_champion(model_id, "test_kind", model_ts, regime="global")

    experiment_ledger.record_experiment_ledger(
        candidate_key=f"{model_id}:{model_ts}",
        candidate_name=model_id,
        candidate_version=str(model_ts),
        candidate_type="model_challenger",
        source="alpha_discovery",
        model_name=model_id,
        model_family="gbm_regressor",
        feature_ids=["price.last"],
        search_space={"generation_method": "single_group_v1"},
        trial_budget=4,
        trial_count=1,
        cpcv={"enabled": True, "status": "evaluated"},
        fdr={"n_competing_trials": 1, "deflated_sharpe": 1.2},
        redundancy={"checked": True, "method": "unit"},
        evidence={"statistical_gate": {"passed": True}},
        promotion_decision="pass",
        status="validated",
    )

    model_registry.promote_to_champion(model_id, "test_kind", model_ts, regime="global")
    champion = model_registry.get_stage_latest(model_id, "champion", regime="global")
    assert champion is not None
    rows = experiment_ledger.fetch_experiment_ledger(candidate_name=model_id, candidate_version=str(model_ts), limit=3)
    assert rows[0]["promotion_decision"] == "promoted"


def test_experiment_ledger_gate_enforces_trial_budget_accounting(ledger_stack: tuple[Any, ...]) -> None:
    _db_guard, _storage, _runtime_meta, _promotion_audit, experiment_ledger, _model_registry = ledger_stack
    model_id = "budget_alpha_AAPL_1700000000001_abcdef2"
    experiment_ledger.record_experiment_ledger(
        candidate_key=f"{model_id}:v1",
        candidate_name=model_id,
        candidate_version="v1",
        candidate_type="model_challenger",
        source="alpha_discovery",
        model_name=model_id,
        feature_ids=["price.last"],
        trial_budget=0,
        trial_count=1,
        cpcv={"status": "evaluated"},
        redundancy={"checked": True},
        evidence={"statistical_gate": {"passed": True}},
        promotion_decision="pass",
        status="validated",
    )

    passed, diagnostics = experiment_ledger.evaluate_experiment_ledger_promotion_gate(
        model_name=model_id,
        candidate_version="v1",
        generated_hint=True,
    )

    assert passed is False
    assert "trial_budget_missing" in diagnostics["blockers"]


def test_feature_discovery_registry_records_candidate_and_evaluation_ledger() -> None:
    import sqlite3

    from engine.strategy.discovery.base import CandidateFeature, EvaluationResult
    from engine.strategy.discovery.registry import ensure_discovery_schema, record_candidate, record_evaluation
    from engine.strategy.experiment_ledger import fetch_experiment_ledger

    con = sqlite3.connect(":memory:")
    ensure_discovery_schema(con)
    candidate = CandidateFeature(
        source="llm_factor",
        symbol="AAPL",
        expression="x0*x1",
        params={
            "feature_map": {"x0": "price.last", "x1": "price.rv_20"},
            "source_feature_ids": ["price.last", "price.rv_20"],
            "prompt_hash": "prompt-1",
            "model_id": "llm-model-1",
            "trial_budget": 3,
        },
    )
    record = record_candidate(candidate, con=con, ts=123)
    result = EvaluationResult(
        candidate_hash=candidate.hash,
        feature_id=candidate.feature_id,
        t_stat=4.2,
        p_value=0.001,
        q_value=0.004,
        decision="accepted",
        n_obs=64,
        diagnostics={"cpcv_ic_mean": 0.2},
    )
    record_evaluation(record.id, result, con=con, ts=456)

    rows = fetch_experiment_ledger(candidate_key=candidate.hash, con=con, limit=5)
    assert [row["promotion_decision"] for row in rows[:2]] == ["accepted", "pending"]
    assert rows[0]["trial_budget"] == 3
    assert rows[0]["fdr_json"]["q_value"] == 0.004
    assert rows[0]["prompt_hash"] == "prompt-1"
