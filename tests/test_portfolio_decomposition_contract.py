from __future__ import annotations

import importlib
import inspect
import json
from typing import Any

import pytest


@pytest.fixture()
def portfolio(monkeypatch: pytest.MonkeyPatch):
    module = importlib.import_module("engine.strategy.portfolio")
    module = importlib.reload(module)
    monkeypatch.setattr(module, "_capital_mode", lambda: "normal")
    return module


@pytest.fixture()
def explain_payload() -> dict[str, Any]:
    return {
        "portfolio_decision": {
            "selection_score": "1.25",
            "target_weight": "0.20",
            "size_factor": "0.50",
            "confidence": "0.77",
            "prediction_strength": "1.11",
            "predicted_z": "-1.40",
            "direction": "sell",
            "trade": "yes",
            "when": "enter_now",
            "features_used": ["f1", "", None, "f2"],
            "include_in_universe": "true",
            "universe_rank": "9.5",
        },
        "event_meta": {"novelty": 0.40},
        "tradability": {
            "expected_ret_net": 0.02,
            "p_win": 0.60,
            "expected_dd": 0.05,
        },
    }


def test_facade_signatures_lock_public_portfolio_helpers(portfolio) -> None:
    expected = {
        "apply_max_position_constraint": "(desired: Optional[Dict[str, Dict[str, Any]]], *, max_positions: int | None = None) -> Dict[str, Dict[str, Any]]",
        "_extract_model_intent_from_explain": "(explain_json: str) -> Dict[str, Any]",
        "_pick_best_per_symbol": "(alerts: List[Dict]) -> Dict[str, Dict]",
        "_resolve_desired_weight": "(alert: Dict[str, Any], score: float, symbol: str) -> float",
        "_apply_flip_flop_penalty": "(con, desired: Dict[str, Dict], state: Dict[str, Dict]) -> Tuple[Dict[str, Dict], Dict[str, Any]]",
        "_apply_capital_at_risk_gate": "(desired: Dict[str, Dict]) -> Tuple[Dict[str, Dict], Dict]",
        "_build_rebalance_result": "(ctx: engine.strategy.portfolio._RebalanceContext, *, execution_blocked: bool, execution_blocked_codes: List[str]) -> Dict[str, Any]",
    }
    for name, signature in expected.items():
        assert hasattr(portfolio, name)
        assert str(inspect.signature(getattr(portfolio, name))) == signature


def test_signal_normalization_and_candidate_ranking_fixture(
    portfolio,
    monkeypatch: pytest.MonkeyPatch,
    explain_payload: dict[str, Any],
) -> None:
    monkeypatch.setattr(portfolio, "PORTFOLIO_MIN_CONF", 0.50)
    monkeypatch.setattr(portfolio, "PORTFOLIO_MIN_ABS_Z", 0.70)
    monkeypatch.setattr(portfolio, "PORTFOLIO_NOVELTY_ALPHA", 0.50)

    explain_json = json.dumps(explain_payload, separators=(",", ":"), sort_keys=True)
    intent = portfolio._extract_model_intent_from_explain(explain_json)

    assert intent == {
        "score": 1.25,
        "target_weight": 0.20,
        "size_mult": 0.50,
        "confidence": 0.77,
        "prediction_strength": 1.11,
        "expected_z": -1.40,
        "side": "SHORT",
        "should_trade": True,
        "timing": "enter_now",
        "selected_features": ["f1", "f2"],
        "include_in_universe": True,
        "universe_score": 9.5,
    }
    assert portfolio._alert_effective_signal(
        {"expected_z": 0.1, "confidence": 0.2, "_model_intent": intent}
    ) == (
        -1.4,
        0.77,
    )
    assert portfolio._tradability_from_explain(explain_json) == {
        "expected_ret_net": 0.02,
        "p_win": 0.6,
        "expected_dd": 0.05,
    }
    assert portfolio._score_from_alert(
        1.25, 0.80, "HIGH", explain_json
    ) == pytest.approx(1.296)

    baseline_explain = json.dumps(
        {
            "event_meta": {"novelty": 0.40},
            "tradability": {
                "expected_ret_net": 0.02,
                "p_win": 0.60,
                "expected_dd": 0.05,
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    best = portfolio._pick_best_per_symbol(
        [
            {
                "id": 1,
                "symbol": "AAPL",
                "expected_z": -1.4,
                "confidence": 0.77,
                "severity": "LOW",
                "explain_json": explain_json,
                "_model_intent": intent,
            },
            {
                "id": 2,
                "symbol": "AAPL",
                "expected_z": 1.25,
                "confidence": 0.80,
                "severity": "HIGH",
                "explain_json": baseline_explain,
                "_model_intent": {},
            },
            {
                "id": 3,
                "symbol": "MSFT",
                "expected_z": 2.0,
                "confidence": 0.95,
                "severity": "CRIT",
                "explain_json": "{}",
                "_model_intent": {"should_trade": False, "score": 100.0},
            },
            {
                "id": 4,
                "symbol": "TSLA",
                "expected_z": -1.0,
                "confidence": 0.90,
                "severity": "LOW",
                "explain_json": json.dumps(
                    {
                        "tradability": {
                            "expected_ret_net": -0.01,
                            "p_win": 0.40,
                            "expected_dd": 0.10,
                        }
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "_model_intent": {"score": 0.90, "side": "SHORT", "should_trade": True},
            },
        ]
    )

    assert list(best) == ["AAPL", "TSLA"]
    assert best["AAPL"]["id"] == 2
    assert best["AAPL"]["_score"] == pytest.approx(1.08864)
    assert best["AAPL"]["expected_z"] == pytest.approx(1.25)
    assert best["TSLA"]["_score"] == pytest.approx(0.21375)


def test_sizing_caps_and_selected_alert_serialization_fixture(
    portfolio,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(portfolio, "PORTFOLIO_SYMBOL_CAPS", {"AAPL": 0.12})
    monkeypatch.setattr(portfolio, "PORTFOLIO_MAX_W_PER_SYMBOL", 0.30)
    monkeypatch.setattr(portfolio, "PORTFOLIO_SCORE_NORM", 2.0)
    monkeypatch.setattr(portfolio, "PORTFOLIO_GROSS_CAP", 1.0)

    assert portfolio._safe_float("nan", 7.0) == 7.0
    assert portfolio._safe_int("bad", 9) == 9
    assert portfolio._dict_str_any({1: "one", "two": 2}) == {"1": "one", "two": 2}
    assert portfolio._signed_weight({"side": "SHORT", "weight": 0.25}) == pytest.approx(
        -0.25
    )
    assert portfolio._normalize_nonnegative_weights([2.0, -1.0, 2.0]) == pytest.approx(
        [0.5, 0.0, 0.5]
    )

    assert portfolio._desired_weight(1.0, "AAPL") == pytest.approx(0.12)
    assert portfolio._desired_weight(1.0, "MSFT") == pytest.approx(0.30)
    assert portfolio._resolve_desired_weight(
        {"_model_intent": {"target_weight": 0.50, "size_mult": 2.0}},
        1.0,
        "AAPL",
    ) == pytest.approx(0.12)
    assert portfolio._resolve_desired_weight(
        {"_model_intent": {"target_weight": -0.09}}, 1.0, "AAPL"
    ) == pytest.approx(0.09)

    capped = portfolio.apply_max_position_constraint(
        {
            "a": {"weight": 0.10},
            "b": {"weight": -0.20},
            "c": {"weight": 0.15},
        },
        max_positions=2,
    )
    assert list(capped) == ["b", "c"]

    ids = portfolio._selected_alert_ids_from_desired(
        {
            "a": {"side": "LONG", "weight": 0.10, "source_alert_id": "7"},
            "b": {"side": "SHORT", "weight": -0.20, "source_alert_id": 5},
            "flat": {"side": "FLAT", "weight": 0.30, "source_alert_id": 99},
            "zero": {"side": "LONG", "weight": 0.0, "source_alert_id": 8},
            "dupe": {"side": "LONG", "weight": 0.01, "source_alert_id": 7},
        }
    )
    assert ids == [5, 7]


def test_risk_gate_and_flip_flop_penalty_fixture(
    portfolio,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(portfolio, "PORTFOLIO_CAR_MAX_PER_SYMBOL", 0.02)
    monkeypatch.setattr(portfolio, "PORTFOLIO_CAR_MAX", 0.03)
    monkeypatch.setattr(portfolio, "PORTFOLIO_GROSS_CAP", 0.50)
    monkeypatch.setattr(portfolio, "_portfolio_flip_lambda", lambda: 0.10)

    desired = {
        "m1:AAPL": {
            "model_id": "m1",
            "symbol": "AAPL",
            "side": "LONG",
            "weight": 0.20,
            "reason": {},
            "explain_json": json.dumps(
                {"tradability": {"expected_dd": 0.20}}, separators=(",", ":")
            ),
        },
        "m1:TSLA": {
            "model_id": "m1",
            "symbol": "TSLA",
            "side": "SHORT",
            "weight": -0.18,
            "reason": {},
            "explain_json": json.dumps(
                {"tradability": {"expected_dd": 0.10}}, separators=(",", ":")
            ),
        },
    }
    gated, car_meta = portfolio._apply_capital_at_risk_gate(desired)
    assert car_meta["car_scaled"] is True
    assert car_meta["car_scale"] == pytest.approx(0.7894736842105263)
    assert gated["m1:AAPL"]["weight"] == pytest.approx(0.07894736842105263)
    assert gated["m1:TSLA"]["weight"] == pytest.approx(-0.14210526315789473)
    assert gated["m1:AAPL"]["reason"]["car_symbol_cap"] is True
    assert car_meta["car_by_symbol"]["m1:AAPL"]["risk"] == pytest.approx(0.02)

    written_meta: dict[str, str] = {}
    monkeypatch.setattr(
        portfolio,
        "_put_meta",
        lambda _con, key, value: written_meta.__setitem__(key, value),
    )
    penalized, flip_meta = portfolio._apply_flip_flop_penalty(
        object(),
        {
            "m1:AAPL": {
                "model_id": "m1",
                "symbol": "AAPL",
                "side": "LONG",
                "weight": 0.25,
                "reason": {},
            }
        },
        {
            "prev": {
                "model_id": "m1",
                "symbol": "AAPL",
                "side": "SHORT",
                "weight": 0.10,
            }
        },
    )

    assert flip_meta["flip_count"] == 1
    assert flip_meta["turnover"] == pytest.approx(0.35)
    assert flip_meta["penalty"] == pytest.approx(0.035)
    assert penalized["m1:AAPL"]["reason"]["flip_flop_penalty"]["prev_side"] == "SHORT"
    assert "last_flip_flop_penalty" in written_meta
    assert json.loads(written_meta["last_flip_flop_penalty"])["flip_count"] == 1


def test_rebalance_result_shape_fixture(portfolio) -> None:
    ctx = portfolio._RebalanceContext(con=object())
    ctx.desired = {
        "m1:AAPL": {"symbol": "AAPL", "side": "LONG", "weight": 0.20},
        "m1:MSFT": {"symbol": "MSFT", "side": "SHORT", "weight": -0.10},
    }
    ctx.changed = ["AAPL"]
    ctx.orders_n = 1
    ctx.degraded_reasons = [{"phase": "risk_gate", "code": "UNIT"}]
    ctx.flip_penalty = {"flip_count": 1}
    ctx.portfolio_diag = {
        "position_summary": {"gross": 0.30},
        "model_summary": {"m1": {"gross": 0.30}},
    }

    clean = portfolio._build_rebalance_result(
        ctx, execution_blocked=False, execution_blocked_codes=[]
    )
    blocked = portfolio._build_rebalance_result(
        ctx,
        execution_blocked=True,
        execution_blocked_codes=["PORTFOLIO_RISK_GATE_FAILED"],
    )

    assert clean["changed"] == ["AAPL"]
    assert clean["orders_n"] == 1
    assert clean["selected"] == ["AAPL", "MSFT"]
    assert clean["portfolio_diagnostics"]["position_summary"] == {"gross": 0.30}
    assert clean["portfolio_diagnostics"]["flip_flop_penalty"] == {"flip_count": 1}
    assert blocked["changed"] == []
    assert blocked["orders_n"] == 0
    assert blocked["execution_blocked_codes"] == ["PORTFOLIO_RISK_GATE_FAILED"]
