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

    def _patch_dependencies(self, suppressions: list[dict], audits: list[dict]) -> ExitStack:
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
        stack.enter_context(patch.object(self.epe, "append_chain_row", side_effect=lambda table, row, con: audits.append({"table": table, "row": row})))
        stack.enter_context(patch.object(self.epe, "log_suppression", side_effect=lambda **kwargs: suppressions.append(dict(kwargs))))
        return stack

    def capture(self, order: dict, now_ms: int, *, enforce: bool) -> dict:
        suppressions: list[dict] = []
        audits: list[dict] = []
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
                },
                clear=False,
            ):
                with self._patch_dependencies(suppressions, audits):
                    shaped = self.epe.apply_execution_policy(
                        [order],
                        con=con,
                        actor="test",
                        mode="paper",
                        broker="sim",
                        now_ms=int(now_ms),
                        initialize_storage=False,
                    )
            captured = {"shaped": shaped, "suppressions": suppressions, "audits": audits}
            assert canary not in json.dumps(captured, sort_keys=True, default=str)
            return captured
        finally:
            self.epe.EPE_EQUITY_SESSION_ENFORCE = old_flag
            con.close()


def test_crypto_and_unknown_symbols_are_identical_with_equity_hook_enabled() -> None:
    harness = _PolicyHarness()
    now_ms = _ms_et(2026, 7, 3, 10, 0)
    crypto = {"symbol": "BTC", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}
    unknown = {"symbol": "UNKNOWN_TEST_X", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}

    assert harness.capture(crypto, now_ms, enforce=True) == harness.capture(crypto, now_ms, enforce=False)
    assert harness.capture(unknown, now_ms, enforce=True) == harness.capture(unknown, now_ms, enforce=False)


def test_equity_flag_off_matches_pre_hook_shape_and_has_no_equity_metadata() -> None:
    harness = _PolicyHarness()
    now_ms = _ms_et(2026, 7, 3, 10, 0)
    order = {"symbol": "SPY", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}

    baseline = harness.capture(order, now_ms, enforce=False)
    repeat = harness.capture(order, now_ms, enforce=False)

    assert repeat == baseline
    assert baseline["suppressions"] == []
    assert len(baseline["shaped"]) == 1
    assert "equity_session" not in baseline["shaped"][0]
    assert "equity_out_of_session_mark" not in baseline["shaped"][0]
