from __future__ import annotations

import copy
import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class BlackLittermanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "BLACK_LITTERMAN_ENABLED",
                "BLACK_LITTERMAN_TAU",
                "BLACK_LITTERMAN_VIEW_CONFIDENCE",
                "PORTFOLIO_ALLOC_OPT",
                "PORTFOLIO_USE_EXEC_REGIME",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "black_litterman.db")
        os.environ["BLACK_LITTERMAN_ENABLED"] = "1"
        os.environ["BLACK_LITTERMAN_TAU"] = "0.05"
        os.environ["BLACK_LITTERMAN_VIEW_CONFIDENCE"] = "0.60"
        os.environ["PORTFOLIO_ALLOC_OPT"] = "1"
        os.environ["PORTFOLIO_USE_EXEC_REGIME"] = "0"

        _, self.storage, _, self.black_litterman, self.portfolio = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.runtime.risk_state",
            "engine.strategy.black_litterman",
            "engine.strategy.portfolio",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_posterior_stability_with_near_singular_covariance(self) -> None:
        cov = np.asarray(
            [
                [0.0400, 0.0399, 0.0397],
                [0.0399, 0.0401, 0.0398],
                [0.0397, 0.0398, 0.0402],
            ],
            dtype=np.float64,
        )
        equilibrium = self.black_litterman.compute_equilibrium_returns(cov)
        views = self.black_litterman.build_view_matrix(
            {
                "AAPL": {"prediction": 0.045, "confidence": 0.75},
                "MSFT": {"prediction": 0.015, "confidence": 0.55},
                "NVDA": {"prediction": -0.010, "confidence": 0.60},
            }
        )

        posterior = self.black_litterman.black_litterman_posterior(
            cov,
            equilibrium,
            views,
            views["uncertainty"],
        )

        self.assertTrue(bool(posterior["applied"]))
        self.assertEqual(list(posterior["assets"]), ["AAPL", "MSFT", "NVDA"])
        self.assertTrue(np.all(np.isfinite(posterior["posterior_returns"])))
        self.assertTrue(np.all(np.isfinite(posterior["posterior_covariance"])))
        self.assertTrue(
            np.allclose(
                posterior["posterior_covariance"],
                posterior["posterior_covariance"].T,
                atol=1e-10,
            )
        )
        self.assertTrue(np.all(np.diag(posterior["posterior_covariance"]) > 0.0))

    def test_portfolio_overlay_falls_back_when_covariance_missing(self) -> None:
        desired = {
            "baseline:AAPL": {
                "symbol": "AAPL",
                "side": "LONG",
                "weight": 0.25,
                "reason": {},
                "explain_json": json.dumps(
                    {
                        "tradability": {"expected_ret_net": 0.015, "expected_dd": 0.050},
                        "model_intent": {"expected_ret_net": 0.015, "confidence": 0.70, "uncertainty": 0.30},
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            }
        }

        with patch("engine.strategy.risk.realized_vol_from_prices", return_value=None):
            result = self.portfolio._apply_black_litterman_overlay(None, copy.deepcopy(desired))

        row = result["baseline:AAPL"]
        overlay = row["reason"]["black_litterman"]
        self.assertFalse(bool(overlay["applied"]))
        self.assertEqual(str(overlay["fallback_reason"]), "missing_covariance")
        self.assertAlmostEqual(float(row["adjusted_expected_ret_net"]), 0.015, places=6)

    def test_adjusted_returns_flow_into_allocation_optimizer(self) -> None:
        desired = {
            "model:AAPL": {
                "symbol": "AAPL",
                "side": "LONG",
                "weight": 0.25,
                "reason": {},
                "explain_json": json.dumps(
                    {
                        "tradability": {"expected_ret_net": 0.030, "expected_dd": 0.050},
                        "model_intent": {"expected_ret_net": 0.030, "confidence": 0.60, "uncertainty": 0.40},
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            },
            "model:MSFT": {
                "symbol": "MSFT",
                "side": "LONG",
                "weight": 0.25,
                "reason": {},
                "explain_json": json.dumps(
                    {
                        "tradability": {"expected_ret_net": 0.010, "expected_dd": 0.050},
                        "model_intent": {"expected_ret_net": 0.010, "confidence": 0.60, "uncertainty": 0.40},
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            },
        }
        posterior_payload = {
            "applied": True,
            "tau": 0.05,
            "assets": ["model:AAPL", "model:MSFT"],
            "view_assets": ["model:AAPL", "model:MSFT"],
            "posterior_returns": np.asarray([0.010, 0.050], dtype=np.float64),
            "posterior_covariance": np.asarray([[0.040, 0.0], [0.0, 0.040]], dtype=np.float64),
            "equilibrium_returns": np.asarray([0.020, 0.020], dtype=np.float64),
            "view_returns": np.asarray([0.030, 0.010], dtype=np.float64),
            "view_confidence": np.asarray([0.60, 0.60], dtype=np.float64),
            "uncertainty": np.diag([0.001, 0.001]).astype(np.float64),
        }

        with patch("engine.strategy.risk.realized_vol_from_prices", return_value=0.20), patch(
            "engine.strategy.risk.corr_from_prices",
            return_value=0.0,
        ), patch.object(
            self.portfolio,
            "black_litterman_posterior",
            return_value=posterior_payload,
        ):
            blended = self.portfolio._apply_black_litterman_overlay(None, copy.deepcopy(desired))
            optimized = self.portfolio._optimize_capital_allocation(None, blended)

        self.assertAlmostEqual(float(blended["model:AAPL"]["adjusted_expected_ret_net"]), 0.010, places=6)
        self.assertAlmostEqual(float(blended["model:MSFT"]["adjusted_expected_ret_net"]), 0.050, places=6)
        self.assertGreater(
            float(optimized["model:MSFT"]["weight"]),
            float(optimized["model:AAPL"]["weight"]),
        )
        self.assertGreater(
            float(optimized["model:MSFT"]["reason"]["alloc_util"]),
            float(optimized["model:AAPL"]["reason"]["alloc_util"]),
        )


if __name__ == "__main__":
    unittest.main()
