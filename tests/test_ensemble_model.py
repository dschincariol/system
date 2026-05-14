from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class EnsembleModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "ensemble_model.db")
        self.storage, self.engine_module, self.public_module = _reload_modules(
            "engine.runtime.storage",
            "engine.ensemble_model",
            "ensemble_model",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _insert_model_performance(
        self,
        *,
        model_name: str,
        model_version: str,
        symbol: str,
        rolling_score: float,
        directional_accuracy: int,
        pnl_impact: float,
        error: float,
        tracked_prediction_id_start: int,
        rows: int = 16,
        volatility_regime: str = "unknown",
        trend_regime: str = "unknown",
        liquidity_regime: str = "unknown",
    ) -> None:
        con = self.storage.connect()
        try:
            now_ms = 1_700_000_000_000
            for idx in range(rows):
                con.execute(
                    """
                    INSERT INTO model_performance(
                      tracked_prediction_id, prediction_id, outcome_id, "time", prediction_time,
                      symbol, model_id, model_name, model_version, horizon_s,
                      prediction, realized_return, error, directional_accuracy,
                      pnl_impact, rolling_score,
                      regime_time_ms, volatility_regime, trend_regime, liquidity_regime,
                      metadata_json, created_ts_ms, updated_ts_ms
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(tracked_prediction_id_start + idx),
                        None,
                        None,
                        int(now_ms + idx),
                        int(now_ms + idx - 1),
                        str(symbol),
                        f"{model_name}:{symbol}:{model_version}",
                        str(model_name),
                        str(model_version),
                        300,
                        0.0,
                        0.0,
                        float(error),
                        int(directional_accuracy),
                        float(pnl_impact),
                        float(rolling_score),
                        int(now_ms + idx),
                        str(volatility_regime),
                        str(trend_regime),
                        str(liquidity_regime),
                        "{}",
                        int(now_ms + idx),
                        int(now_ms + idx),
                    ),
                )
            con.commit()
        finally:
            con.close()

    def test_combine_prefers_model_with_stronger_model_performance_history(self) -> None:
        self._insert_model_performance(
            model_name="stable_alpha",
            model_version="v1",
            symbol="AMD",
            rolling_score=0.93,
            directional_accuracy=1,
            pnl_impact=0.04,
            error=0.01,
            tracked_prediction_id_start=1,
        )
        self._insert_model_performance(
            model_name="noisy_beta",
            model_version="v1",
            symbol="AMD",
            rolling_score=0.08,
            directional_accuracy=0,
            pnl_impact=-0.04,
            error=1.25,
            tracked_prediction_id_start=100,
        )

        result = self.public_module.EnsembleModel().combine(
            [
                {
                    "symbol": "AMD",
                    "model_name": "stable_alpha",
                    "model_version": "v1",
                    "prediction": 0.25,
                    "confidence": 0.82,
                    "performance_metrics": {"accuracy": 0.51},
                    "metadata": {"recent_performance": 0.42},
                },
                {
                    "symbol": "AMD",
                    "model_name": "noisy_beta",
                    "model_version": "v1",
                    "prediction": 1.60,
                    "confidence": 0.83,
                    "performance_metrics": {"accuracy": 0.99},
                    "metadata": {"recent_performance": 0.98},
                },
            ]
        )

        member_weights = {
            str(member.get("model_name") or ""): float(member.get("weight") or 0.0)
            for member in (result.get("members") or [])
        }
        self.assertGreater(member_weights["stable_alpha"], member_weights["noisy_beta"])
        self.assertEqual(
            {
                str(member.get("weight_source") or "")
                for member in (result.get("members") or [])
            },
            {"model_performance"},
        )
        self.assertLess(abs(float(result["final_prediction"]) - 0.25), abs(float(result["final_prediction"]) - 1.60))
        self.assertLess(float(result["final_prediction"]), 0.925)

    def test_combine_falls_back_to_metric_weights_without_model_performance_history(self) -> None:
        result = self.public_module.EnsembleModel().combine(
            [
                {
                    "model_name": "alpha",
                    "prediction": 1.0,
                    "confidence": 0.90,
                    "performance_metrics": {"accuracy": 0.80},
                    "metadata": {"recent_performance": 0.70},
                },
                {
                    "model_name": "beta",
                    "prediction": -0.5,
                    "confidence": 0.60,
                    "performance_metrics": {"accuracy": 0.50},
                    "metadata": {"recent_performance": 0.30},
                },
            ]
        )

        self.assertAlmostEqual(float(result["final_prediction"]), 0.4661016949, places=6)
        self.assertAlmostEqual(float(result["aggregated_confidence"]), 0.6164406779, places=6)
        self.assertEqual(
            {
                str(member.get("weight_source") or "")
                for member in (result.get("members") or [])
            },
            {"metric_fallback"},
        )

    def test_combine_shifts_member_weights_by_current_regime(self) -> None:
        self._insert_model_performance(
            model_name="alpha",
            model_version="v1",
            symbol="AMD",
            rolling_score=0.95,
            directional_accuracy=1,
            pnl_impact=0.05,
            error=0.01,
            tracked_prediction_id_start=1,
            volatility_regime="high",
            trend_regime="bearish",
            liquidity_regime="thin",
        )
        self._insert_model_performance(
            model_name="alpha",
            model_version="v1",
            symbol="AMD",
            rolling_score=0.10,
            directional_accuracy=0,
            pnl_impact=-0.05,
            error=1.10,
            tracked_prediction_id_start=100,
            volatility_regime="low",
            trend_regime="bullish",
            liquidity_regime="deep",
        )
        self._insert_model_performance(
            model_name="beta",
            model_version="v1",
            symbol="AMD",
            rolling_score=0.12,
            directional_accuracy=0,
            pnl_impact=-0.05,
            error=1.20,
            tracked_prediction_id_start=200,
            volatility_regime="high",
            trend_regime="bearish",
            liquidity_regime="thin",
        )
        self._insert_model_performance(
            model_name="beta",
            model_version="v1",
            symbol="AMD",
            rolling_score=0.94,
            directional_accuracy=1,
            pnl_impact=0.05,
            error=0.01,
            tracked_prediction_id_start=300,
            volatility_regime="low",
            trend_regime="bullish",
            liquidity_regime="deep",
        )

        bear_result = self.public_module.EnsembleModel().combine(
            [
                {
                    "symbol": "AMD",
                    "model_name": "alpha",
                    "model_version": "v1",
                    "prediction": -0.8,
                    "confidence": 0.70,
                    "performance_metrics": {"accuracy": 0.60},
                    "metadata": {
                        "recent_performance": 0.55,
                        "regime": {
                            "volatility_regime": "high",
                            "trend_regime": "bearish",
                            "liquidity_regime": "thin",
                        },
                    },
                },
                {
                    "symbol": "AMD",
                    "model_name": "beta",
                    "model_version": "v1",
                    "prediction": 0.8,
                    "confidence": 0.70,
                    "performance_metrics": {"accuracy": 0.60},
                    "metadata": {
                        "recent_performance": 0.55,
                        "regime": {
                            "volatility_regime": "high",
                            "trend_regime": "bearish",
                            "liquidity_regime": "thin",
                        },
                    },
                },
            ]
        )
        bull_result = self.public_module.EnsembleModel().combine(
            [
                {
                    "symbol": "AMD",
                    "model_name": "alpha",
                    "model_version": "v1",
                    "prediction": -0.8,
                    "confidence": 0.70,
                    "performance_metrics": {"accuracy": 0.60},
                    "metadata": {
                        "recent_performance": 0.55,
                        "regime": {
                            "volatility_regime": "low",
                            "trend_regime": "bullish",
                            "liquidity_regime": "deep",
                        },
                    },
                },
                {
                    "symbol": "AMD",
                    "model_name": "beta",
                    "model_version": "v1",
                    "prediction": 0.8,
                    "confidence": 0.70,
                    "performance_metrics": {"accuracy": 0.60},
                    "metadata": {
                        "recent_performance": 0.55,
                        "regime": {
                            "volatility_regime": "low",
                            "trend_regime": "bullish",
                            "liquidity_regime": "deep",
                        },
                    },
                },
            ]
        )

        bear_weights = {
            str(member.get("model_name") or ""): float(member.get("weight") or 0.0)
            for member in (bear_result.get("members") or [])
        }
        bull_weights = {
            str(member.get("model_name") or ""): float(member.get("weight") or 0.0)
            for member in (bull_result.get("members") or [])
        }

        self.assertGreater(bear_weights["alpha"], bear_weights["beta"])
        self.assertGreater(bull_weights["beta"], bull_weights["alpha"])
        self.assertEqual(
            {
                str(member.get("weight_source") or "")
                for member in (bear_result.get("members") or [])
            },
            {"model_performance_regime"},
        )
        self.assertLess(float(bear_result["final_prediction"]), 0.0)
        self.assertGreater(float(bull_result["final_prediction"]), 0.0)


if __name__ == "__main__":
    unittest.main()
