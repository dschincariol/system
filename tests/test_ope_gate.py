from __future__ import annotations

import sqlite3

import pytest

from engine.strategy.ope_gate import (
    ensure_ope_schema,
    evaluate_policy_ope_gate,
    record_policy_ope_observation,
)


def _config(**overrides):
    base = {
        "min_obs": 10,
        "min_effective_n": 5.0,
        "min_support": 0.80,
        "max_importance_weight": 10.0,
        "confidence_z": 1.64,
        "min_policy_value_lower_bound": 0.0,
        "max_standard_error": 0.10,
        "max_ci_width": 0.50,
        "max_model_optimism": 0.10,
        "lookback_ms": 0,
    }
    base.update(overrides)
    return base


def _con(tmp_path):
    db = sqlite3.connect(tmp_path / "ope.sqlite")
    ensure_ope_schema(db)
    return db


def _record_rows(con, *, n: int, prefix: str = "row", **row):
    for idx in range(n):
        record_policy_ope_observation(
            con=con,
            model_id="policy_model",
            model_name="rl_policy_AAPL",
            candidate_type="rl",
            candidate_version="v1",
            symbol="AAPL",
            horizon_s=300,
            regime="global",
            logged_action=row.get("logged_action", "long"),
            target_action=row.get("target_action", "long"),
            behavior_propensity=row.get("behavior_propensity"),
            target_propensity=row.get("target_propensity"),
            outcome=row.get("outcome"),
            logged_model_estimate=row.get("logged_model_estimate"),
            target_model_estimate=row.get("target_model_estimate"),
            source_table="unit",
            source_id=f"{prefix}-{idx}",
            ts_ms=1_700_000_000_000 + idx,
        )


def _evaluate(con, **config):
    return evaluate_policy_ope_gate(
        con=con,
        model_id="policy_model",
        model_name="rl_policy_AAPL",
        candidate_type="rl",
        candidate_version="v1",
        symbol="AAPL",
        horizon_s=300,
        regime="global",
        config=_config(**config),
    )


def test_ope_gate_blocks_missing_propensities(tmp_path):
    con = _con(tmp_path)
    try:
        _record_rows(
            con,
            n=12,
            target_propensity=1.0,
            outcome=0.02,
            logged_model_estimate=0.01,
            target_model_estimate=0.01,
        )

        passed, payload = _evaluate(con)

        assert passed is False
        assert payload["status"] == "missing_propensities"
        assert "missing_propensities" in payload["blockers"]
        assert payload["missing_behavior_propensity"] == 12
    finally:
        con.close()


def test_ope_gate_blocks_insufficient_support(tmp_path):
    con = _con(tmp_path)
    try:
        _record_rows(
            con,
            n=4,
            prefix="covered",
            behavior_propensity=0.5,
            target_propensity=0.5,
            outcome=0.04,
            logged_model_estimate=0.01,
            target_model_estimate=0.01,
        )
        _record_rows(
            con,
            n=16,
            prefix="unsupported",
            behavior_propensity=0.5,
            target_propensity=0.0,
            outcome=0.04,
            logged_model_estimate=0.01,
            target_model_estimate=0.01,
        )

        passed, payload = _evaluate(con, min_obs=20, min_effective_n=3.0, min_support=0.80)

        assert passed is False
        assert payload["status"] == "insufficient_support"
        assert payload["support"] == pytest.approx(0.20)
        assert "insufficient_support" in payload["blockers"]
    finally:
        con.close()


def test_ope_gate_blocks_optimistic_policy_estimates(tmp_path):
    con = _con(tmp_path)
    try:
        _record_rows(
            con,
            n=15,
            behavior_propensity=0.5,
            target_propensity=0.5,
            outcome=0.0,
            logged_model_estimate=1.0,
            target_model_estimate=1.0,
        )

        passed, payload = _evaluate(con, min_policy_value_lower_bound=-0.10, max_model_optimism=0.20)

        assert passed is False
        assert payload["status"] == "optimistic_model_estimates"
        assert payload["direct_method_value"] == pytest.approx(1.0)
        assert payload["policy_value"] == pytest.approx(0.0)
        assert "optimistic_model_estimates" in payload["blockers"]
    finally:
        con.close()


def test_ope_gate_passes_conservative_estimates(tmp_path):
    con = _con(tmp_path)
    try:
        _record_rows(
            con,
            n=30,
            behavior_propensity=0.5,
            target_propensity=0.5,
            outcome=0.04,
            logged_model_estimate=0.01,
            target_model_estimate=0.01,
        )

        passed, payload = _evaluate(con, min_obs=20, min_effective_n=10.0)

        assert passed is True
        assert payload["status"] == "pass"
        assert payload["policy_value"] == pytest.approx(0.04)
        assert payload["ci_lower"] > 0.0
    finally:
        con.close()
