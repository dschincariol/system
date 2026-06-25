from __future__ import annotations

import importlib
import json
import os
import sqlite3
import uuid
from contextlib import ExitStack
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.safety_critical

NY = ZoneInfo("America/New_York")


def _ms_et(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=NY).timestamp() * 1000)


def _decision() -> dict:
    return {
        "order_type": "MARKET",
        "aggressiveness": "AGGRESSIVE",
        "latency_mult": 1.0,
        "chunk_pct": 1.0,
        "sim_extra_slippage_bps": 0.0,
        "size_mult": 1.0,
        "execution_policy": "balanced",
        "entry_strategy": "immediate",
        "entry_delay_ms": 0,
        "expected_slippage_bps": 0.0,
        "expected_fill_latency_ms": 0,
        "limit_offset_bps": 0.0,
    }


class _PolicyHarness:
    def __init__(self) -> None:
        self.epe = importlib.reload(importlib.import_module("engine.execution.execution_policy_engine"))

    def _patch_dependencies(self, suppressions: list[dict]) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(patch.object(self.epe, "init_db", return_value=None))
        stack.enter_context(patch.object(self.epe, "execution_allowed", return_value=(True, "", {})))
        stack.enter_context(
            patch.object(
                self.epe,
                "evaluate_trade_suppression",
                return_value={"state": "NONE", "action": "NONE", "size_mult": 1.0, "throttle_mult": 1.0, "hard_block": False},
            )
        )
        stack.enter_context(patch.object(self.epe, "update_capital_preservation_mode", return_value={}))
        stack.enter_context(patch.object(self.epe, "get_state", return_value="normal"))
        stack.enter_context(patch.object(self.epe, "_regime_compatibility", return_value=(1.0, {"regime": "test"})))
        stack.enter_context(patch.object(self.epe, "load_execution_feedback_snapshot", return_value={}))
        stack.enter_context(patch.object(self.epe, "build_alpha_handoff", side_effect=lambda *_args, **kwargs: dict(kwargs)))
        stack.enter_context(patch.object(self.epe, "decide_execution_strategy", return_value=_decision()))
        stack.enter_context(patch.object(self.epe, "get_execution_mode", return_value="paper"))
        stack.enter_context(
            patch.object(
                self.epe,
                "execution_adjustment_for_order",
                return_value={"blocked": False, "ttl_ms": 60_000, "half_life_ms": 60_000, "size_multiplier": 1.0},
            )
        )
        stack.enter_context(patch.object(self.epe, "live_ai_order_guard", return_value={"ok": True}))
        stack.enter_context(patch.object(self.epe, "score_order_meta_label", return_value={"applied": False, "multiplier": 1.0}))
        stack.enter_context(patch.object(self.epe, "conformal_gate_from_payload", return_value={"hard_block": False, "size_mult": 1.0}))
        stack.enter_context(patch.object(self.epe, "ood_gate_from_payload", return_value={"hard_block": False, "applied": False, "multiplier": 1.0}))
        stack.enter_context(patch.object(self.epe, "uncertainty_gate_from_payload", return_value={"hard_block": False, "applied": False, "multiplier": 1.0, "action": "NONE"}))
        stack.enter_context(patch.object(self.epe, "deeplob_shadow_enabled", return_value=False))
        stack.enter_context(patch.object(self.epe, "append_chain_row", return_value=None))
        stack.enter_context(patch.object(self.epe, "log_suppression", side_effect=lambda **kwargs: suppressions.append(dict(kwargs))))
        return stack

    def run(self, order: dict, now_ms: int, *, enforce: bool = True, env: dict[str, str] | None = None) -> tuple[list[dict], list[dict]]:
        suppressions: list[dict] = []
        con = sqlite3.connect(":memory:")
        old_flag = self.epe.EPE_EQUITY_SESSION_ENFORCE
        canary = f"eq04-canary-{uuid.uuid4()}"
        try:
            self.epe.EPE_EQUITY_SESSION_ENFORCE = bool(enforce)
            with patch.dict(
                os.environ,
                {
                    "EPE_FX_SESSION_ENFORCE": "0",
                    "EPE_CRYPTO_SESSION_ENFORCE": "0",
                    "POLYGON_API_KEY": canary,
                    **dict(env or {}),
                },
                clear=False,
            ):
                with self._patch_dependencies(suppressions):
                    shaped = self.epe.apply_execution_policy(
                        [order],
                        con=con,
                        actor="test",
                        mode="paper",
                        broker="sim",
                        now_ms=int(now_ms),
                        initialize_storage=False,
                    )
            serialized = json.dumps({"shaped": shaped, "suppressions": suppressions}, sort_keys=True, default=str)
            assert canary not in serialized
        finally:
            self.epe.EPE_EQUITY_SESSION_ENFORCE = old_flag
            con.close()
        return shaped, suppressions


def test_closed_session_equity_order_is_suppressed_via_existing_path() -> None:
    harness = _PolicyHarness()
    now_ms = _ms_et(2026, 7, 3, 10, 0)
    order = {"symbol": "SPY", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}

    shaped, suppressions = harness.run(order, now_ms)

    assert shaped == []
    assert len(suppressions) == 1
    assert suppressions[0]["suppression_reason"].startswith("equity_session_closed")
    assert suppressions[0]["decision_json"]["blocked_by"] == "equity_session"
    assert suppressions[0]["decision_json"]["equity_session"]["session"] == "closed_holiday"


def test_near_close_equity_order_gets_more_passive_timing_bias() -> None:
    harness = _PolicyHarness()
    now_ms = _ms_et(2026, 6, 24, 15, 55)
    order = {"symbol": "SPY", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}

    baseline, baseline_suppressions = harness.run(order, now_ms, enforce=False)
    shaped, suppressions = harness.run(order, now_ms, enforce=True)

    assert baseline_suppressions == []
    assert suppressions == []
    assert len(baseline) == len(shaped) == 1
    assert shaped[0]["order_type"] == "LIMIT"
    assert shaped[0]["aggressiveness"] == "PASSIVE"
    assert shaped[0]["equity_session_timing_bias"] is True
    assert shaped[0]["equity_session"]["session"] == "regular"
    assert shaped[0]["epe_broker_sim_overrides"]["latency_ms"] > baseline[0]["epe_broker_sim_overrides"]["latency_ms"]
    assert shaped[0]["epe_broker_sim_overrides"]["chunk_pct"] < baseline[0]["epe_broker_sim_overrides"]["chunk_pct"]


def test_uncovered_year_can_fail_closed_for_equity_orders() -> None:
    harness = _PolicyHarness()
    now_ms = _ms_et(2035, 1, 3, 10, 0)
    order = {"symbol": "SPY", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}

    shaped, suppressions = harness.run(order, now_ms, env={"EQUITY_SESSION_UNKNOWN_YEAR_POLICY": "fail_closed"})

    assert shaped == []
    assert len(suppressions) == 1
    assert suppressions[0]["suppression_reason"] == "equity_session_closed:holiday_table_uncovered"
    assert suppressions[0]["decision_json"]["equity_session"]["holiday_table_covered"] is False
