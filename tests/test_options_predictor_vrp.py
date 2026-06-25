from __future__ import annotations

from pathlib import Path
import importlib
import os
import sqlite3
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _seed_surface(con: sqlite3.Connection, *, symbol: str = "SPY", ts_ms: int = 1_700_000_000_000) -> None:
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
    for idx in range(12):
        con.execute(
            """
            INSERT INTO options_surface(
              ts_ms, underlying, atm_iv_near, atm_iv_next, skew_25d, term_structure_slope
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (int(ts_ms - ((11 - idx) * 86_400_000)), symbol, 0.55 + (idx * 0.002), 0.60, 0.01, 0.02),
        )


def _seed_prices(con: sqlite3.Connection, *, symbol: str = "SPY", ts_ms: int = 1_700_000_000_000) -> None:
    con.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, px REAL, price REAL)")
    px = 100.0
    for idx in range(40):
        px *= 1.001 if idx % 2 else 0.999
        con.execute(
            "INSERT INTO prices(ts_ms, symbol, px, price) VALUES (?, ?, ?, ?)",
            (int(ts_ms - ((39 - idx) * 86_400_000)), symbol, float(px), float(px)),
        )


class OptionsPredictorVrpTest(unittest.TestCase):
    ENV_KEYS = (
        "USE_OPTIONS_FEATURES",
        "USE_OPTIONS_PREDICTOR",
        "USE_FINBERT_SENTIMENT",
        "USE_SYMBOL_SNAPSHOT_FEATURES",
        "USE_TECH_FEATURES",
        "USE_STRESS_FEATURES",
        "USE_MACRO_FEATURES",
        "USE_SOCIAL_FEATURES",
        "USE_SOCIAL_REGIME",
        "USE_WEATHER_FEATURES",
        "USE_FACTOR_UNIVERSE",
        "USE_TSFRESH_FEATURES",
        "USE_NLP_FEATURES",
        "USE_INSIDER_FEATURES",
        "USE_SHORT_FEATURES",
        "USE_FUNDING_FEATURES",
        "USE_NEWS_FLOW_FEATURES",
        "USE_ETF_FLOW_FEATURES",
        "USE_COT_FEATURES",
        "USE_13F_FEATURES",
        "USE_GOV_FEATURES",
        "USE_FUNDAMENTALS_PIT_FEATURES",
        "USE_BOCPD_FEATURES",
        "USE_FX_FEATURES",
        "USE_FUTURES_FEATURES",
    )

    def setUp(self) -> None:
        self.env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}

    def tearDown(self) -> None:
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_forecast_vrp_positive_when_implied_exceeds_realized(self) -> None:
        from engine.strategy.options_predictor import forecast_vrp

        con = sqlite3.connect(":memory:")
        ts_ms = 1_700_000_000_000
        _seed_surface(con, ts_ms=ts_ms)
        _seed_prices(con, ts_ms=ts_ms)

        forecast = forecast_vrp(con, "SPY", ts_ms=ts_ms)

        self.assertIsNotNone(forecast)
        assert forecast is not None
        self.assertGreater(forecast["vrp_signal"], 0.0)
        self.assertGreaterEqual(forecast["vrp_signal"], -1.0)
        self.assertLessEqual(forecast["vrp_signal"], 1.0)
        self.assertGreater(forecast["iv_forecast"], forecast["realized_vol"])
        self.assertGreater(forecast["confidence"], 0.0)

    def test_forecast_vrp_returns_none_when_surface_or_prices_missing(self) -> None:
        from engine.strategy.options_predictor import forecast_vrp

        ts_ms = 1_700_000_000_000
        no_surface = sqlite3.connect(":memory:")
        _seed_prices(no_surface, ts_ms=ts_ms)
        self.assertIsNone(forecast_vrp(no_surface, "SPY", ts_ms=ts_ms))

        no_prices = sqlite3.connect(":memory:")
        _seed_surface(no_prices, ts_ms=ts_ms)
        self.assertIsNone(forecast_vrp(no_prices, "SPY", ts_ms=ts_ms))

    def test_import_does_not_change_feature_registry_or_options_feature_gate(self) -> None:
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)
        import engine.data.finbert_sentiment as finbert_sentiment
        import engine.strategy.feature_registry as feature_registry

        importlib.reload(finbert_sentiment)
        feature_registry = importlib.reload(feature_registry)
        feature_registry.invalidate_feature_registry_cache()
        with patch.object(feature_registry, "_discovered_feature_ids", return_value=[]):
            before = list(feature_registry.default_feature_ids())
            self.assertEqual(len(before), 111)
            self.assertFalse(feature_registry.USE_OPTIONS_FEATURES)

            import engine.strategy.options_predictor as options_predictor

            options_predictor = importlib.reload(options_predictor)
            self.assertTrue(hasattr(options_predictor, "forecast_vrp"))
            feature_registry.invalidate_feature_registry_cache()
            after = list(feature_registry.default_feature_ids())
            self.assertEqual(after, before)
            self.assertFalse(feature_registry.USE_OPTIONS_FEATURES)
            self.assertFalse(options_predictor.USE_OPTIONS_PREDICTOR)


if __name__ == "__main__":
    unittest.main()
