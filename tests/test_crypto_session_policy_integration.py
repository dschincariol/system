from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


def _utc_ms(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


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


def _maintenance_disabled_env() -> dict[str, str]:
    return {
        "EPE_CRYPTO_SESSION_ENFORCE": "1",
        "EPE_FX_SESSION_ENFORCE": "1",
        "CRYPTO_MAINTENANCE_ENABLED": "0",
        "CRYPTO_MAINTENANCE_START_UTC": "",
        "CRYPTO_MAINTENANCE_START_HOUR_UTC": "",
        "CRYPTO_MAINTENANCE_START_MINUTE_UTC": "",
        "CRYPTO_MAINTENANCE_DURATION_MINUTES": "0",
        "CRYPTO_MAINTENANCE_SYMBOLS": "",
    }


class CryptoSessionPolicyIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.epe = importlib.reload(importlib.import_module("engine.execution.execution_policy_engine"))

    def _patch_policy_dependencies(self, suppressions: list[dict]) -> ExitStack:
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

    def _run_policy(self, order: dict, now_ms: int, *, env: dict[str, str] | None = None) -> tuple[list[dict], list[dict]]:
        suppressions: list[dict] = []
        con = sqlite3.connect(":memory:")
        try:
            with patch.dict(os.environ, {**_maintenance_disabled_env(), **dict(env or {})}, clear=False):
                with self._patch_policy_dependencies(suppressions):
                    shaped = self.epe.apply_execution_policy(
                        [order],
                        con=con,
                        actor="test",
                        mode="paper",
                        broker="sim",
                        now_ms=int(now_ms),
                        initialize_storage=False,
                    )
        finally:
            con.close()
        return shaped, suppressions

    def test_crypto_weekend_order_is_not_suppressed_while_fx_would_be(self) -> None:
        now_ms = _utc_ms(2026, 6, 27, 16)
        crypto_order = {"symbol": "BTC", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}
        fx_order = {"symbol": "EURUSD", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}

        crypto_shaped, crypto_suppressions = self._run_policy(crypto_order, now_ms)
        fx_shaped, fx_suppressions = self._run_policy(fx_order, now_ms)

        self.assertEqual(crypto_suppressions, [])
        self.assertEqual(len(crypto_shaped), 1)
        self.assertEqual(crypto_shaped[0]["crypto_session"]["session"], "open")
        self.assertFalse(bool(crypto_shaped[0]["crypto_session_blocked"]))
        self.assertEqual(fx_shaped, [])
        self.assertEqual(len(fx_suppressions), 1)
        self.assertEqual(fx_suppressions[0]["decision_json"]["blocked_by"], "fx_session")

    def test_crypto_maintenance_window_suppresses_via_existing_audit_path(self) -> None:
        now_ms = _utc_ms(2026, 6, 24, 16, 15)
        order = {"symbol": "BTC", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}
        env = {
            "CRYPTO_MAINTENANCE_ENABLED": "1",
            "CRYPTO_MAINTENANCE_START_HOUR_UTC": "16",
            "CRYPTO_MAINTENANCE_START_MINUTE_UTC": "0",
            "CRYPTO_MAINTENANCE_DURATION_MINUTES": "30",
        }

        shaped, suppressions = self._run_policy(order, now_ms, env=env)

        self.assertEqual(shaped, [])
        self.assertEqual(len(suppressions), 1)
        self.assertEqual(suppressions[0]["suppression_reason"], "maintenance")
        self.assertEqual(suppressions[0]["decision_json"]["blocked_by"], "crypto_session")
        self.assertEqual(suppressions[0]["decision_json"]["crypto_session"]["session"], "maintenance")

    def test_equity_order_is_identical_with_crypto_session_gate_enabled(self) -> None:
        now_ms = _utc_ms(2026, 6, 27, 16)
        order = {"symbol": "AAPL", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}

        disabled, disabled_suppressions = self._run_policy(order, now_ms, env={"EPE_CRYPTO_SESSION_ENFORCE": "0"})
        enabled, enabled_suppressions = self._run_policy(order, now_ms, env={"EPE_CRYPTO_SESSION_ENFORCE": "1"})

        self.assertEqual(enabled_suppressions, disabled_suppressions)
        self.assertEqual(enabled, disabled)

    def test_fx_order_is_identical_with_crypto_session_gate_enabled(self) -> None:
        now_ms = _utc_ms(2026, 6, 24, 16)
        order = {"symbol": "EURUSD", "to_weight": 0.10, "side": "BUY", "signal_ts_ms": now_ms, "alpha_ttl_ms": 60_000, "alpha_half_life_ms": 60_000}

        disabled, disabled_suppressions = self._run_policy(order, now_ms, env={"EPE_CRYPTO_SESSION_ENFORCE": "0"})
        enabled, enabled_suppressions = self._run_policy(order, now_ms, env={"EPE_CRYPTO_SESSION_ENFORCE": "1"})

        self.assertEqual(enabled_suppressions, disabled_suppressions)
        self.assertEqual(enabled, disabled)
