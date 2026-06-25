from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _occ(underlying: str, expiry: datetime, right: str, strike: float) -> str:
    return f"{underlying.upper()}{expiry:%y%m%d}{right.upper()}{int(round(float(strike) * 1000.0)):08d}"


def _seed_predictor_inputs(con: sqlite3.Connection, *, now_ms: int) -> None:
    expiry = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc) + timedelta(days=40)
    expiry_text = expiry.date().isoformat()
    con.execute("CREATE TABLE runtime_meta(key TEXT PRIMARY KEY, value TEXT, updated_ts_ms INTEGER)")
    con.execute(
        """
        CREATE TABLE options_surface(
          ts_ms INTEGER,
          underlying TEXT,
          atm_iv_near REAL,
          atm_iv_next REAL,
          skew_25d REAL,
          term_structure_slope REAL
        )
        """
    )
    con.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, px REAL, price REAL)")
    con.execute(
        """
        CREATE TABLE options_chain_v2(
          ts_ms INTEGER,
          underlying TEXT,
          contract TEXT,
          expiration TEXT,
          contract_type TEXT,
          strike REAL,
          iv REAL,
          open_interest REAL,
          volume REAL,
          delta REAL,
          gamma REAL
        )
        """
    )
    for idx in range(12):
        con.execute(
            "INSERT INTO options_surface VALUES (?, ?, ?, ?, ?, ?)",
            (int(now_ms - ((11 - idx) * 86_400_000)), "SPY", 0.55, 0.60, 0.01, 0.02),
        )
    px = 100.0
    for idx in range(40):
        px *= 1.001 if idx % 2 else 0.999
        con.execute(
            "INSERT INTO prices VALUES (?, ?, ?, ?)",
            (int(now_ms - ((39 - idx) * 86_400_000)), "SPY", float(px), float(px)),
        )
    chain_rows = [
        (_occ("SPY", expiry, "P", 495.0), "put", 495.0, -0.30),
        (_occ("SPY", expiry, "P", 485.0), "put", 485.0, -0.12),
        (_occ("SPY", expiry, "C", 510.0), "call", 510.0, 0.30),
        (_occ("SPY", expiry, "C", 520.0), "call", 520.0, 0.12),
    ]
    for contract, contract_type, strike, delta in chain_rows:
        con.execute(
            """
            INSERT INTO options_chain_v2(
              ts_ms, underlying, contract, expiration, contract_type, strike,
              iv, open_interest, volume, delta, gamma
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(now_ms), "SPY", contract, expiry_text, contract_type, strike, 0.35, 500, 100, delta, 0.01),
        )


class OptionsPredictorShadowGateTest(unittest.TestCase):
    ENV_KEYS = (
        "USE_OPTIONS_PREDICTOR",
        "OPTIONS_MIN_DTE_DAYS",
        "OPTIONS_MAX_DTE_DAYS",
        "OPTIONS_PRED_TARGET_DELTA",
    )

    def setUp(self) -> None:
        self.env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}

    def tearDown(self) -> None:
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _reload_predictor(self, **env: str):
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)
        for key, value in env.items():
            os.environ[key] = str(value)
        import engine.strategy.options_predictor as options_predictor

        return importlib.reload(options_predictor)

    def test_run_options_predictor_noop_when_disabled_or_evidence_missing(self) -> None:
        con = sqlite3.connect(":memory:")
        now_ms = int(time.time() * 1000)
        _seed_predictor_inputs(con, now_ms=now_ms)

        disabled = self._reload_predictor()
        self.assertEqual(disabled.run_options_predictor(con, ["SPY"]), {"forecasts": 0, "intents": 0})

        enabled = self._reload_predictor(USE_OPTIONS_PREDICTOR="1")
        self.assertEqual(enabled.run_options_predictor(con, ["SPY"]), {"forecasts": 0, "intents": 0})

    def test_emitted_intent_is_shadow_stamped_when_evidence_passes(self) -> None:
        from engine.execution.options_readiness import is_options_order

        con = sqlite3.connect(":memory:")
        now_ms = int(time.time() * 1000)
        _seed_predictor_inputs(con, now_ms=now_ms)
        con.execute(
            "INSERT INTO runtime_meta(key, value, updated_ts_ms) VALUES (?, ?, ?)",
            (
                "options_feature_ablation_report",
                json.dumps({"status": "ok", "verdict": "ENABLE_SUPPORTED", "dataset": {"usable_rows": 1000}}),
                int(now_ms),
            ),
        )
        predictor = self._reload_predictor(
            USE_OPTIONS_PREDICTOR="1",
            OPTIONS_MIN_DTE_DAYS="20",
            OPTIONS_MAX_DTE_DAYS="60",
            OPTIONS_PRED_TARGET_DELTA="0.30",
        )

        summary = predictor.run_options_predictor(con, ["SPY"])

        self.assertEqual(summary, {"forecasts": 1, "intents": 1})
        row = con.execute(
            "SELECT structure_json, evidence_gate_ok FROM options_predictor_shadow WHERE underlying='SPY'"
        ).fetchone()
        self.assertIsNotNone(row)
        payload = json.loads(row[0])
        intent = payload["intent"]
        self.assertEqual(row[1], 1)
        self.assertEqual(intent["execution_target"], "shadow")
        self.assertTrue(intent["competition"]["blocked"])
        self.assertTrue(is_options_order(intent))


if __name__ == "__main__":
    unittest.main()
