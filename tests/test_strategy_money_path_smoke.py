from __future__ import annotations

import math

import numpy as np


class _Cursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(self, row):
        self.calls = []
        self.closed = False
        self._row = row

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return _Cursor(self._row)

    def close(self):
        self.closed = True


def test_social_risk_blocks_manipulation_and_scales_attention_shock() -> None:
    from engine.strategy.social_risk import social_gate_for_symbol

    blocked = social_gate_for_symbol(_Connection((0.9, 0.2, 0.0)), "aapl", 123)

    assert blocked["block"] is True
    assert blocked["factor"] == 0.0
    assert blocked["manip_risk"] == 0.9

    scaled = social_gate_for_symbol(
        _Connection((0.1, 0.95, 0.0)),
        "msft",
        123,
        shock_factor=0.4,
    )

    assert scaled["block"] is False
    assert scaled["factor"] == 0.4
    assert scaled["attention_shock"] == 0.95


def test_options_and_social_context_read_asof_rows(monkeypatch) -> None:
    from engine.strategy import options_context, social_context

    options_con = _Connection(
        (
            0.11,
            0.12,
            -0.25,
            0.03,
            1.4,
            0.8,
            0.9,
            0.5,
            0.2,
            -0.1,
            1.0,
            0.33,
            0.44,
        )
    )
    monkeypatch.setattr(options_context, "connect", lambda: options_con)

    options = options_context.get_options_feature_vector(
        symbol="aapl",
        ts_ms=900_000,
        bucket_sec=900,
    )

    assert options["iv_rank"] == 0.11
    assert options["skew_25d"] == -0.25
    assert options["opt_flow_imbalance_z"] == 0.44
    assert options_con.calls[0][1] == ("AAPL", 900, 900_000, 900_000)
    assert options_con.closed is True

    social_con = _Connection(
        (
            7,
            3,
            0.2,
            12.0,
            -0.1,
            0.4,
            1.5,
            0.05,
            0.07,
            0.3,
            0.6,
            1.0,
            1.0,
        )
    )
    monkeypatch.setattr(social_context, "connect", lambda: social_con)

    social = social_context.get_social_feature_vector(
        symbol="tsla",
        ts_ms=600_000,
        bucket_sec=300,
    )

    assert social["mention_count"] == 7
    assert social["mention_rate_z"] == 1.5
    assert social["attention_shock"] == 0.6
    assert social_con.calls[0][1] == ("TSLA", 300, 600_000)
    assert social_con.closed is True


def test_opportunity_weight_bounds_convex_scaling() -> None:
    from engine.strategy.opportunity_allocation import opportunity_weight

    weight = opportunity_weight(0.8, 1.2, 0.5, max_cap=0.5)

    assert 0.0 < weight <= 0.5
    assert math.isclose(weight, 0.192)
    assert opportunity_weight("bad", 1.0, 0.5) == 0.0


def test_corr_optimizer_preserves_cap_and_marks_adjusted_targets(monkeypatch) -> None:
    from engine.strategy import corr_opt
    from engine.strategy import risk as strategy_risk

    monkeypatch.setattr(
        strategy_risk,
        "realized_vol_from_prices",
        lambda _con, _symbol, lookback: 0.2,
    )
    monkeypatch.setattr(
        strategy_risk,
        "corr_from_prices",
        lambda _con, _left, _right, lookback: 0.8,
    )
    desired = {
        "AAPL": {
            "side": "LONG",
            "weight": 0.5,
            "weight_cap": 0.5,
            "explain_json": '{"expected_ret_net": 0.05, "expected_dd": 0.10}',
            "reason": {},
        },
        "MSFT": {
            "side": "LONG",
            "weight": 0.5,
            "weight_cap": 0.5,
            "explain_json": '{"expected_ret_net": 0.03, "expected_dd": 0.10}',
            "reason": {},
        },
    }

    out = corr_opt.corr_aware_optimize_desired(
        object(),
        desired,
        gross_cap=0.6,
        lookback=20,
        corr_max=0.3,
        iters=3,
    )

    assert sum(target["weight"] for target in out.values()) <= 0.600001
    assert out["AAPL"]["reason"]["corr_opt"] is True
    assert out["MSFT"]["reason"]["corr_opt"] is True


def test_strategy_variant_models_build_desired_with_shared_portfolio_helpers(
    monkeypatch,
) -> None:
    from engine.strategy.models import baseline, conservative

    alerts = [
        {
            "id": 1,
            "symbol": "AAPL",
            "expected_z": 2.0,
            "confidence": 0.9,
            "_score": 1.8,
            "severity": "high",
            "horizon_s": 300,
            "explain_json": "{}",
        }
    ]

    monkeypatch.setattr(
        baseline.P,
        "_pick_best_per_symbol",
        lambda values: {item["symbol"]: dict(item) for item in values},
    )
    monkeypatch.setattr(
        baseline.P,
        "_strategy_candidate_limit",
        lambda candidates, default: min(len(candidates), int(default)),
    )
    monkeypatch.setattr(
        baseline.P,
        "_resolve_desired_weight",
        lambda _alert, _score, _symbol: 0.4,
    )
    monkeypatch.setattr(
        baseline.P,
        "_merge_model_intent_reason",
        lambda reason, _alert: dict(reason),
    )
    monkeypatch.setattr(baseline.P, "PORTFOLIO_MAX_POSITIONS", 3)
    monkeypatch.setattr(baseline.P, "PORTFOLIO_GROSS_CAP", 1.0)

    baseline_out = baseline.build_desired(alerts, now_ms=123)

    assert baseline_out["AAPL"]["side"] == "LONG"
    assert baseline_out["AAPL"]["weight"] == 0.4
    assert baseline_out["AAPL"]["_strategy"] == "baseline"

    monkeypatch.setattr(
        conservative.P,
        "_alert_effective_signal",
        lambda alert: (float(alert["expected_z"]), float(alert["confidence"])),
    )
    monkeypatch.setattr(
        conservative.P,
        "_has_explicit_model_trade_intent",
        lambda _intent: False,
    )
    monkeypatch.setattr(
        conservative.P,
        "_model_intent_trade_allowed",
        lambda _alert: True,
    )
    monkeypatch.setattr(
        conservative.P,
        "_coerce_float",
        lambda value: None if value is None else float(value),
    )
    monkeypatch.setattr(
        conservative.P,
        "_score_from_alert",
        lambda expected_z, confidence, _severity, _explain_json: abs(expected_z) * confidence,
    )
    monkeypatch.setattr(conservative.P, "_symbol_cap", lambda _symbol: 0.5)
    monkeypatch.setattr(
        conservative.P,
        "_merge_model_intent_reason",
        lambda reason, _alert: dict(reason),
    )
    monkeypatch.setattr(conservative.P, "PORTFOLIO_GROSS_CAP", 1.0)
    monkeypatch.setattr(conservative, "GROSS_CAP", 0.7)
    monkeypatch.setattr(conservative, "SCORE_NORM", 10.0)
    monkeypatch.setattr(conservative, "MAX_POS", 2)

    conservative_out = conservative.build_desired(alerts, now_ms=456)

    assert conservative_out["AAPL"]["side"] == "LONG"
    assert conservative_out["AAPL"]["weight"] > 0.0
    assert conservative_out["AAPL"]["_strategy"] == "conservative"


def test_rl_strategy_policy_predicts_shadow_choice_without_live_execution() -> None:
    from engine.strategy.rl_strategy_policy import predict_strategy

    fallback_choice, fallback_score = predict_strategy(
        {"prev_drawdown": -0.2},
        None,
        threshold=0.1,
    )

    assert fallback_choice == "conservative"
    assert fallback_score == 0.2

    policy = {
        "weights": np.asarray([1.0, -1.0], dtype=np.float32),
        "bias": 0.0,
        "feature_names": ["momentum", "drawdown"],
    }
    choice, score = predict_strategy(
        {"momentum": 0.1, "drawdown": 0.5},
        policy,
        threshold=0.0,
    )

    assert choice == "baseline"
    assert score < 0.0
