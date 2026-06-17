from __future__ import annotations

import importlib
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module():
    import engine.strategy.portfolio_backtest as portfolio_backtest

    return importlib.reload(portfolio_backtest)


class PortfolioBacktestContractTests(unittest.TestCase):
    def test_init_portfolio_backtest_schema_skips_write_txn_when_ready(self) -> None:
        portfolio_backtest = _reload_module()
        with patch.object(portfolio_backtest, "_portfolio_backtest_schema_ready", return_value=True):
            with patch.object(portfolio_backtest, "run_write_txn", side_effect=AssertionError("write txn should be skipped")):
                portfolio_backtest.init_portfolio_backtest_schema()

    def test_init_portfolio_backtest_schema_uses_retrying_direct_write_txn_when_missing(self) -> None:
        portfolio_backtest = _reload_module()
        with patch.object(portfolio_backtest, "_portfolio_backtest_schema_ready", return_value=False):
            with patch.object(portfolio_backtest, "run_write_txn") as run_write_txn:
                portfolio_backtest.init_portfolio_backtest_schema()

        run_write_txn.assert_called_once_with(
            portfolio_backtest._init_portfolio_backtest_schema,
            table="portfolio_bt_runs",
            operation="init_portfolio_backtest_schema",
            direct=True,
        )

    def test_persist_backtest_results_writes_run_and_curve_points(self) -> None:
        portfolio_backtest = _reload_module()

        class _FakeCursor:
            def __init__(self, lastrowid=None):
                self.lastrowid = lastrowid

        class _FakeConnection:
            def __init__(self) -> None:
                self.calls = []

            def execute(self, sql, params=()):
                self.calls.append((sql, params))
                text = " ".join(str(sql).split()).lower()
                if "insert into portfolio_bt_runs" in text:
                    return _FakeCursor(lastrowid=17)
                return _FakeCursor()

        fake_con = _FakeConnection()
        run_id = portfolio_backtest._persist_backtest_results(
            fake_con,
            now_ms=2000,
            start_ms=1000,
            curve=[
                (1100, 0.1, 1.1, 0.0, {"exec_cost": 0.01, "slippage": 0.02, "fees": 0.03}),
                (1200, -0.2, 0.9, 0.1, {"exec_cost": 0.04, "slippage": 0.05, "fees": 0.06}),
            ],
            metrics={"final_equity": 0.9},
        )

        point_inserts = [
            call
            for call in fake_con.calls
            if "insert or replace into portfolio_bt_points" in " ".join(str(call[0]).split()).lower()
        ]
        updates = [
            call
            for call in fake_con.calls
            if "update portfolio_bt_runs" in " ".join(str(call[0]).split()).lower()
        ]

        self.assertEqual(run_id, 17)
        self.assertEqual(len(point_inserts), 2)
        self.assertEqual(len(updates), 1)

    def test_run_backtest_skips_write_txn_when_no_curve_points_exist(self) -> None:
        portfolio_backtest = _reload_module()

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return list(self._rows)

        class _FakeConnection:
            def execute(self, sql, params=()):
                return _FakeCursor([])

            def close(self):
                return None

        with patch.object(portfolio_backtest, "init_portfolio_db"):
            with patch.object(portfolio_backtest, "init_portfolio_backtest_schema"):
                with patch.object(portfolio_backtest, "connect", return_value=_FakeConnection()):
                    with patch.object(portfolio_backtest, "run_write_txn", side_effect=AssertionError("write txn should be skipped")):
                        with patch.object(portfolio_backtest, "_now_ms", return_value=2_000):
                            result = portfolio_backtest.run_backtest()

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "no_points")
        self.assertIsNone(result["run_id"])

    def test_portfolio_pro_chart_view_model_annotations_and_fallback_mode(self) -> None:
        script = r"""
import assert from "node:assert/strict";
import {
  PORTFOLIO_DRAWDOWN_THROTTLE,
  buildPortfolioBacktestProViewModel,
  resolvePortfolioBacktestRenderMode
} from "./ui/portfolio_backtest.js";

const run = {
  id: 7,
  metrics: {
    sharpe_simple: 1.234,
    sortino_simple: 2.5,
    calmar_simple: 0.75,
    n_returns: 3,
  },
  points: [
    {
      ts_ms: 1_700_000_000_000,
      equity: 1.0,
      drawdown: 0,
      detail: { positions: [{ symbol: "AAPL", side: "LONG", weight: 0.2 }] },
    },
    {
      ts_ms: 1_700_000_060_000,
      equity: 1.04,
      drawdown: 0.02,
      detail: {
        positions: [
          { symbol: "AAPL", side: "LONG", weight: 0.1 },
          { symbol: "MSFT", side: "SHORT", weight: 0.2 },
        ],
      },
    },
    {
      ts_ms: 1_700_000_120_000,
      equity: 1.03,
      drawdown: -0.03,
      detail: {
        positions: [
          { symbol: "AAPL", side: "LONG", weight: 0.1 },
          { symbol: "MSFT", side: "SHORT", weight: 0.2 },
        ],
        trade_costs: [{ symbol: "AAPL", delta_weight: -0.1, status: "estimated" }],
      },
    },
  ],
};

const vm = buildPortfolioBacktestProViewModel(run);
assert.equal(vm.ok, true);
assert.equal(vm.drawdownThrottle, PORTFOLIO_DRAWDOWN_THROTTLE);
assert.equal(vm.drawdownSeries[1].value, -0.02);
assert.equal(vm.maxDrawdown, -0.03);
assert.equal(vm.benchmarkSeries.length, 0);
assert.match(vm.benchmarkState.text, /Benchmark unavailable/);
assert.ok(vm.markers.some((row) => row.kind === "intended"));
assert.ok(vm.markers.some((row) => row.kind === "filled" && row.side === "SELL"));

const annotations = Object.fromEntries(vm.annotations.map((row) => [row.key, row]));
assert.equal(annotations.sharpe.value, "1.23");
assert.equal(annotations.sortino.value, "2.50");
assert.equal(annotations.calmar.value, "0.75");
assert.equal(annotations.turnover.value, "0.075");
assert.equal(annotations.sample_count.value, "3");

assert.deepEqual(
  resolvePortfolioBacktestRenderMode({ proEnabled: false, lightweightAvailable: true, hasRenderableSeries: true }),
  { mode: "canvas", reason: "feature_flag_disabled" },
);
assert.deepEqual(
  resolvePortfolioBacktestRenderMode({ proEnabled: true, lightweightAvailable: false, hasRenderableSeries: true }),
  { mode: "canvas", reason: "lightweight_charts_unavailable" },
);
assert.deepEqual(
  resolvePortfolioBacktestRenderMode({ proEnabled: true, lightweightAvailable: true, hasRenderableSeries: true }),
  { mode: "pro", reason: "pro_renderer_enabled" },
);

const benchmarkVm = buildPortfolioBacktestProViewModel({
  ...run,
  benchmark_symbol: "QQQ",
  benchmark_points: [
    { ts_ms: 1_700_000_000_000, close: 100 },
    { ts_ms: 1_700_000_060_000, close: 105 },
  ],
});
assert.equal(benchmarkVm.benchmarkState.available, true);
assert.equal(benchmarkVm.benchmarkState.label, "QQQ");
assert.equal(benchmarkVm.benchmarkSeries[0].value, 1.0);
assert.equal(benchmarkVm.benchmarkSeries[1].value, 1.05);
"""
        result = subprocess.run(
            ["node", "--input-type=module", "-e", script],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)


if __name__ == "__main__":
    unittest.main()
