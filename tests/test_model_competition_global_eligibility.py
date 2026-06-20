from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path
from unittest.mock import patch

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
def competition_stack(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "model_competition_global.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("CHAMPION_PROMOTION_COOLDOWN_S", "0")
    monkeypatch.setenv("CHAMPION_PROMOTION_MIN_TRADES", "3")
    monkeypatch.setenv("CHAMPION_PROMOTION_MIN_OBSERVATION_S", "1")
    monkeypatch.setenv("CHAMPION_PROMOTION_MIN_NET_PNL_DELTA", "0")
    storage, champion_manager = _reload_modules(
        "engine.runtime.db_guard",
        "engine.runtime.storage",
        "engine.strategy.champion_manager",
    )[1:]
    storage.init_db()
    con = storage.connect()
    try:
        con.execute("DROP TABLE IF EXISTS champion_assignments")
        con.execute(
            """
            CREATE TABLE champion_assignments (
              scope TEXT NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL DEFAULT 0,
              model_name TEXT NOT NULL,
              challenger_name TEXT,
              regime TEXT NOT NULL DEFAULT 'global',
              state TEXT NOT NULL DEFAULT 'champion',
              assigned_ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER NOT NULL,
              meta_json TEXT,
              PRIMARY KEY (scope, symbol, horizon_s)
            )
            """
        )
        con.commit()
    finally:
        con.close()
    yield storage, champion_manager
    try:
        storage.close_pooled_connections()
    except Exception:
        pass


def _flat_returns(value: float, n_obs: int = 60) -> list[float]:
    return [float(value)] * int(n_obs)


def _insert_marketplace_row(storage, *, model_name: str) -> None:
    now_ms = int(time.time() * 1000)
    first_ts = now_ms - 60_000
    returns = _flat_returns(0.2)
    meta = {
        "score_source": "pnl_attribution",
        "risk_adjusted_score": 0.99,
        "rolling_realized_pnl": float(sum(returns)),
        "rolling_unrealized_pnl": 0.0,
        "rolling_total_pnl": float(sum(returns)),
        "realized_pnl": float(sum(returns)),
        "unrealized_pnl": 0.0,
        "total_pnl": float(sum(returns)),
        "transaction_cost": 0.0,
        "rolling_window_ms": 86_400_000,
        "observation_duration_ms": int(now_ms - first_ts),
        "first_signal_ts_ms": int(first_ts),
        "last_signal_ts_ms": int(now_ms),
        "recent_total_pnl": float(sum(returns)),
        "prior_total_pnl": 0.0,
        "max_drawdown": 0.0,
        "model_kind": "test_model",
        "model_ts_ms": int(first_ts),
        "net_cost_label_count": len(returns),
        "net_cost_evidence_available": True,
        "net_cost_evidence": {"available": True, "n": len(returns)},
        "realized_trade_pnls": returns,
        "deconfounded_validation": passing_deconfounded_payload(len(returns)),
    }
    con = storage.connect()
    try:
        con.execute(
            """
            INSERT INTO model_marketplace_scores(
              model_id, model_name, symbol, horizon_s, regime, stage, score, trades, wins, losses,
              gross_pnl, net_pnl, avg_confidence, last_signal_ts_ms, updated_ts_ms, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(model_name),
                str(model_name),
                "AAPL",
                300,
                "global",
                "challenger",
                0.99,
                len(returns),
                len(returns),
                0,
                float(sum(returns)),
                float(sum(returns)),
                0.9,
                int(now_ms),
                int(now_ms),
                json.dumps(meta, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()


def _approved_replay(model_name: str) -> dict:
    return {
        f"{model_name}|baseline|AAPL|300|global": {
            "approved": True,
            "model_name": str(model_name),
            "symbol": "AAPL",
            "horizon_s": 300,
            "regime": "global",
            "n": 60,
        }
    }


def _run_cycle(champion_manager, *, replay_models: dict, replay_fresh: bool, blocked_keys: list[str]):
    with patch.object(champion_manager, "promotion_allowed", return_value=(True, {"allowed": True})), patch.object(
        champion_manager,
        "_learned_alpha_candidate_gate",
        return_value={"allowed": True, "available": False},
    ), patch.object(
        champion_manager,
        "_evaluate_candidate_graph_gate",
        return_value=(True, {"applied": False, "passed": True}),
    ), patch.object(
        champion_manager,
        "_evaluate_candidate_ope_gate",
        return_value=(True, {"applied": False, "passed": True}),
    ), patch.object(
        champion_manager,
        "_evaluate_promotion_stat_gate",
        return_value=(True, {"enabled": True, "validation_enabled": True, "passed": True, "status": "passed"}),
    ), patch.object(
        champion_manager,
        "get_cached_replay_validation_snapshot",
        return_value={"fresh": bool(replay_fresh), "snapshot": {"models": replay_models}},
    ), patch.object(
        champion_manager,
        "run_self_critic",
        return_value={"blocked_keys": list(blocked_keys)},
    ), patch.object(
        champion_manager,
        "compute_capital_plan",
        return_value={},
    ), patch.object(
        champion_manager,
        "_sync_assignment_to_model_registry",
        return_value=None,
    ), patch.object(
        champion_manager,
        "_sync_registry_runtime",
        return_value=None,
    ), patch.object(
        champion_manager,
        "audit",
        return_value=None,
    ):
        return champion_manager.evaluate_competition_cycle()


def _global_assignment(champion_manager) -> dict:
    return champion_manager.get_champion_assignment(
        champion_manager.MODEL_COMPETITION_SCOPE,
        champion_manager.MODEL_COMPETITION_SYMBOL,
        champion_manager.MODEL_COMPETITION_HORIZON_S,
    )


def _global_change(champion_manager, result: dict) -> dict:
    return next(
        (
            row
            for row in list(result.get("changes") or [])
            if str((row or {}).get("scope") or "") == champion_manager.MODEL_COMPETITION_SCOPE
        ),
        {},
    )


def test_global_best_blocked_by_self_critic(competition_stack) -> None:
    storage, champion_manager = competition_stack
    model_name = "global_self_critic_block"
    _insert_marketplace_row(storage, model_name=model_name)

    result = _run_cycle(
        champion_manager,
        replay_models=_approved_replay(model_name),
        replay_fresh=True,
        blocked_keys=[f"{model_name}|baseline|AAPL|300|global"],
    )
    change = _global_change(champion_manager, result)

    assert _global_assignment(champion_manager) == {}
    assert change["reason"] == "best_blocked_self_critic"
    assert "self_critic_blocked" in change["best_promotion_eligibility"]["block_reasons"]


def test_global_best_blocked_by_missing_replay(competition_stack) -> None:
    storage, champion_manager = competition_stack
    model_name = "global_missing_replay"
    _insert_marketplace_row(storage, model_name=model_name)

    result = _run_cycle(champion_manager, replay_models={}, replay_fresh=True, blocked_keys=[])
    change = _global_change(champion_manager, result)

    assert _global_assignment(champion_manager) == {}
    assert change["reason"] == "replay_gate_blocked"
    assert "replay_missing" in change["best_promotion_eligibility"]["block_reasons"]


def test_global_best_blocked_by_stale_replay(competition_stack) -> None:
    storage, champion_manager = competition_stack
    model_name = "global_stale_replay"
    _insert_marketplace_row(storage, model_name=model_name)

    result = _run_cycle(
        champion_manager,
        replay_models=_approved_replay(model_name),
        replay_fresh=False,
        blocked_keys=[],
    )
    change = _global_change(champion_manager, result)

    assert result["status"] == "replay_stale"
    assert _global_assignment(champion_manager) == {}
    assert change["reason"] == "replay_stale"
    assert "replay_stale" in change["best_promotion_eligibility"]["block_reasons"]


def test_global_best_assigns_valid_replay_approved_candidate(competition_stack) -> None:
    storage, champion_manager = competition_stack
    model_name = "global_replay_approved"
    _insert_marketplace_row(storage, model_name=model_name)

    result = _run_cycle(
        champion_manager,
        replay_models=_approved_replay(model_name),
        replay_fresh=True,
        blocked_keys=[],
    )
    change = _global_change(champion_manager, result)
    assignment = _global_assignment(champion_manager)

    assert assignment["model_name"] == model_name
    assert change["reason"] == "bootstrap_best"
    assert change["promotion_eligibility"]["eligible"] is True
    assert assignment["meta"]["promotion_eligibility"]["eligible"] is True
