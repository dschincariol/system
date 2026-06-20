from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from engine.audit.chain import append_chain_row


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        if name in sys.modules:
            modules.append(importlib.reload(sys.modules[name]))
        else:
            modules.append(importlib.import_module(name))
    return modules


@pytest.fixture()
def strategy_stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "strategy_governance.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_INIT_ON_STARTUP", "0")
    monkeypatch.setenv("SHADOW_PROMOTION_MIN_RUNS", "2")
    monkeypatch.setenv("SHADOW_PROMOTION_THRESHOLD", "1.10")
    monkeypatch.setenv("SHADOW_PROMOTION_LOOKBACK", "3600")
    monkeypatch.setenv("STRAT_PROMOTE_STREAK", "1")
    monkeypatch.setenv("PROMOTE_COOLDOWN_S", "0")
    monkeypatch.setenv("PROMOTION_COOLDOWN_S", "0")
    monkeypatch.setenv("STRATEGY_PROMOTION_OPERATOR_APPROVAL_REQUIRED", "1")
    monkeypatch.setenv("STRATEGY_PROMOTION_OPE_EVIDENCE_REQUIRED", "1")
    monkeypatch.setenv("STRATEGY_PROMOTION_MIN_REALIZED_PNL", "0.0")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("ENV", "dev")

    modules = _reload_modules(
        "engine.runtime.state_cache",
        "engine.runtime.db_guard",
        "engine.runtime.storage",
        "engine.runtime.runtime_meta",
        "engine.strategy.portfolio",
        "engine.strategy.strategy_promotion_governance",
        "engine.strategy.promotion_audit",
        "engine.strategy.jobs.strategy_governance_job",
    )
    storage = modules[2]
    portfolio = modules[4]
    storage.init_db()
    portfolio.init_portfolio_db()
    try:
        yield {
            "storage": storage,
            "runtime_meta": modules[3],
            "portfolio": portfolio,
            "governance": modules[5],
            "promotion_audit": modules[6],
            "job": modules[7],
        }
    finally:
        try:
            storage.close_pooled_connections()
        except Exception:
            pass


def _seed_registry(con, *, now_ms: int) -> None:
    con.execute(
        """
        INSERT INTO strategy_registry(strategy_name, enabled, stage, created_ts_ms, updated_ts_ms, meta_json)
        VALUES(?,?,?,?,?,?)
        """,
        ("baseline", 1, "live", int(now_ms), int(now_ms), "{}"),
    )
    con.execute(
        """
        INSERT INTO strategy_registry(strategy_name, enabled, stage, created_ts_ms, updated_ts_ms, meta_json)
        VALUES(?,?,?,?,?,?)
        """,
        ("shadow_alpha", 1, "shadow", int(now_ms), int(now_ms), "{}"),
    )


def _seed_strategy_metrics(con, *, now_ms: int, realized_shadow: bool) -> None:
    baseline = {
        "net_calmar": 0.20,
        "sharpe_simple": 0.60,
        "max_drawdown": 0.10,
        "total_return": 0.02,
        "efficiency_score": 0.10,
        "return_per_risk_unit": 0.10,
        "drawdown_contribution": 0.0,
        "rolling_realized_pnl": 5.0,
        "rolling_total_pnl": 5.0,
    }
    shadow = {
        "net_calmar": 1.50,
        "sharpe_simple": 2.00,
        "max_drawdown": 0.05,
        "total_return": 0.30,
        "efficiency_score": 0.75,
        "return_per_risk_unit": 1.25,
        "drawdown_contribution": 0.0,
        "shadow_proxy_score": 1.50,
        "rolling_realized_pnl": 50.0 if realized_shadow else 0.0,
        "rolling_total_pnl": 50.0 if realized_shadow else 0.0,
    }
    for name, metrics in (("baseline", baseline), ("shadow_alpha", shadow)):
        con.execute(
            """
            INSERT INTO strategy_metrics(strategy_name, window_days, ts_ms, metrics_json, is_active)
            VALUES(?,?,?,?,?)
            """,
            (name, 0, int(now_ms), json.dumps(metrics, separators=(",", ":"), sort_keys=True), 1),
        )


def _seed_shadow_runs(con, *, now_ms: int) -> None:
    for idx in range(2):
        con.execute(
            """
            INSERT INTO strategy_shadow_runs(ts_ms, strategy_name, desired_json, metrics_json)
            VALUES(?,?,?,?)
            """,
            (
                int(now_ms + idx),
                "shadow_alpha",
                "{}",
                json.dumps({"proxy_score": 1.5}, separators=(",", ":"), sort_keys=True),
            ),
        )


def _stage(con, strategy_name: str) -> str:
    row = con.execute(
        "SELECT stage FROM strategy_registry WHERE strategy_name=?",
        (str(strategy_name),),
    ).fetchone()
    return str((row or [""])[0] or "")


def _candidate(con) -> dict[str, Any]:
    row = con.execute(
        """
        SELECT strategy_name, candidate_version, candidate_model_id, status, operator_approved_ts_ms
        FROM strategy_promotion_candidates
        WHERE strategy_name='shadow_alpha'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    return {
        "strategy_name": str(row[0]),
        "candidate_version": str(row[1]),
        "candidate_model_id": str(row[2]),
        "status": str(row[3]),
        "operator_approved_ts_ms": int(row[4] or 0),
    }


def _seed_complete_evidence(stack: dict[str, Any], con, *, candidate: dict[str, Any]) -> None:
    now_ms = int(time.time() * 1000)
    candidate_model_id = str(candidate["candidate_model_id"])
    candidate_version = str(candidate["candidate_version"])
    promotion_audit = stack["promotion_audit"]
    runtime_meta = stack["runtime_meta"]

    promotion_audit.record_statistical_evidence(
        con=con,
        model_id=candidate_model_id,
        ts=now_ms,
        test_name="white_reality_check",
        p_value=0.01,
        q_value=0.02,
        bootstrap_samples=1000,
        decision="pass",
        payload={"source": "unit"},
    )
    promotion_audit.record_statistical_evidence(
        con=con,
        model_id=candidate_model_id,
        ts=now_ms,
        test_name="deconfounded_signal_validation",
        t_stat=4.2,
        p_value=0.01,
        decision="pass",
        payload={"source": "unit", "passed": True},
    )

    append_chain_row(
        "policy_ope_evidence",
        {
            "ts_ms": now_ms,
            "candidate_key": candidate_model_id,
            "model_id": candidate_model_id,
            "model_name": "shadow_alpha",
            "candidate_type": "strategy",
            "candidate_version": candidate_version,
            "symbol": None,
            "horizon_s": 0,
            "regime": "global",
            "policy_value": 0.08,
            "standard_error": 0.01,
            "ci_lower": 0.04,
            "ci_upper": 0.12,
            "n_obs": 100,
            "effective_n": 80.0,
            "support": 0.95,
            "max_importance_weight": 2.0,
            "confidence_z": 1.645,
            "decision": "pass",
            "reason": "pass",
            "config_json": {"source": "unit"},
            "diagnostics_json": {"passed": True},
        },
        con,
    )
    con.commit()

    replay_payload = {
        "ok": True,
        "updated_ts_ms": now_ms,
        "models": {
            f"{candidate_model_id}|global": {
                "model_name": candidate_model_id,
                "model_id": candidate_model_id,
                "model_kind": "strategy",
                "model_ts_ms": int(candidate_version),
                "regime": "global",
                "approved": True,
            }
        },
    }
    runtime_meta.meta_set("competition_replay_validation", json.dumps(replay_payload, separators=(",", ":"), sort_keys=True))
    runtime_meta.meta_set(
        "competition_replay_validation_status",
        json.dumps({"ok": True, "status": "ready", "updated_ts_ms": now_ms}, separators=(",", ":"), sort_keys=True),
    )


def test_shadow_outperformance_records_candidate_without_live_stage(strategy_stack: dict[str, Any]) -> None:
    storage = strategy_stack["storage"]
    portfolio = strategy_stack["portfolio"]
    now_ms = int(time.time() * 1000)
    con = storage.connect()
    try:
        _seed_registry(con, now_ms=now_ms)
        _seed_strategy_metrics(con, now_ms=now_ms, realized_shadow=False)
        _seed_shadow_runs(con, now_ms=now_ms)
        con.commit()

        portfolio._auto_promote_shadow_strategies(con)
        con.commit()

        assert _stage(con, "shadow_alpha") == "shadow"
        candidate = _candidate(con)
        assert candidate["status"] == "candidate"
        assert candidate["operator_approved_ts_ms"] == 0

        result = strategy_stack["job"].main()
        assert result == 0
        assert _stage(con, "shadow_alpha") == "shadow"
        validation_row = con.execute(
            "SELECT value FROM portfolio_meta WHERE key='last_strategy_validation'"
        ).fetchone()
        validation = json.loads(str(validation_row[0]))
        blockers = validation["strategy_promotion_governance"]["blockers"]
        assert "operator_approval_required" in blockers
    finally:
        con.close()


def test_approved_governance_path_promotes_with_complete_evidence(strategy_stack: dict[str, Any]) -> None:
    storage = strategy_stack["storage"]
    portfolio = strategy_stack["portfolio"]
    governance = strategy_stack["governance"]
    now_ms = int(time.time() * 1000)
    con = storage.connect()
    try:
        _seed_registry(con, now_ms=now_ms)
        _seed_strategy_metrics(con, now_ms=now_ms, realized_shadow=True)
        _seed_shadow_runs(con, now_ms=now_ms)
        portfolio._auto_promote_shadow_strategies(con)
        candidate = _candidate(con)
        _seed_complete_evidence(strategy_stack, con, candidate=candidate)
        governance.approve_strategy_promotion_candidate(
            con,
            strategy_name="shadow_alpha",
            actor="unit_operator",
            reason="complete evidence reviewed",
            candidate_version=candidate["candidate_version"],
        )
        con.commit()

        result = strategy_stack["job"].main()
        assert result == 0

        assert _stage(con, "shadow_alpha") == "live"
        promoted = con.execute(
            """
            SELECT status, promoted_ts_ms
            FROM strategy_promotion_candidates
            WHERE candidate_model_id=?
            """,
            (candidate["candidate_model_id"],),
        ).fetchone()
        assert promoted is not None
        assert str(promoted[0]) == "promoted"
        assert int(promoted[1] or 0) > 0

        audit_actions = [
            str(row[0])
            for row in con.execute(
                """
                SELECT action
                FROM model_promotion_audit
                WHERE model_name=?
                ORDER BY id ASC
                """,
                (candidate["candidate_model_id"],),
            ).fetchall()
        ]
        assert "candidate" in audit_actions
        assert "approve" in audit_actions
        assert "promote" in audit_actions
    finally:
        con.close()
