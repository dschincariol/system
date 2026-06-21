from __future__ import annotations

import importlib
import json
import sqlite3
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


def _make_latest_backtest_connection(*, include_prices: bool = True, include_benchmark_rows: bool = True):
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE portfolio_bt_runs (
          id INTEGER PRIMARY KEY,
          ts_ms INTEGER NOT NULL,
          start_ts_ms INTEGER NOT NULL,
          end_ts_ms INTEGER NOT NULL,
          metrics_json TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE portfolio_bt_points (
          run_id INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          ret REAL,
          equity REAL,
          drawdown REAL,
          detail_json TEXT
        )
        """
    )
    con.execute(
        "INSERT INTO portfolio_bt_runs VALUES (?, ?, ?, ?, ?)",
        (7, 1_700_000_060_000, 1_700_000_000_000, 1_700_000_120_000, json.dumps({"steps": 3})),
    )
    con.executemany(
        "INSERT INTO portfolio_bt_points VALUES (?, ?, ?, ?, ?, ?)",
        [
            (7, 1_700_000_000_000, 0.0, 1000.0, 0.0, "{}"),
            (7, 1_700_000_060_000, 0.1, 1100.0, 0.0, "{}"),
            (7, 1_700_000_120_000, -0.05, 1045.0, -0.05, "{}"),
        ],
    )
    if include_prices:
        con.execute(
            """
            CREATE TABLE prices (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              price REAL,
              px REAL,
              source TEXT
            )
            """
        )
        if include_benchmark_rows:
            con.executemany(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?, ?, ?, ?, ?)",
                [
                    (1_700_000_000_000, "SPY", 400.0, None, "unit"),
                    (1_700_000_060_000, "SPY", 420.0, None, "unit"),
                    (1_700_000_120_000, "SPY", None, 380.0, "unit_px"),
                    (1_700_000_060_000, "QQQ", 300.0, None, "unit"),
                ],
            )
    return con


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

    def test_latest_portfolio_backtest_api_returns_spy_benchmark_shape(self) -> None:
        import engine.api.api_read_advanced as api_read_advanced

        api_read_advanced = importlib.reload(api_read_advanced)
        con = _make_latest_backtest_connection()
        with patch.object(api_read_advanced, "db_connect", return_value=con):
            payload = api_read_advanced.get_latest_portfolio_backtest()

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["meta"]["benchmark_ready"])
        self.assertEqual(payload["meta"]["benchmark_symbol"], "SPY")
        benchmark = payload["run"]["benchmark"]
        self.assertEqual(
            {
                "available": benchmark["available"],
                "symbol": benchmark["symbol"],
                "source": benchmark["source"],
                "normalization": benchmark["normalization"],
                "price_field": benchmark["price_field"],
            },
            {
                "available": True,
                "symbol": "SPY",
                "source": "prices",
                "normalization": "portfolio_start_value",
                "price_field": "COALESCE(price, px)",
            },
        )
        self.assertEqual(benchmark["point_count"], 3)
        self.assertEqual(len(benchmark["points"]), 3)
        self.assertEqual(benchmark["start_value"], 1000.0)
        self.assertEqual(benchmark["start_price"], 400.0)
        self.assertEqual(benchmark["points"][0]["value"], 1000.0)
        self.assertEqual(benchmark["points"][1]["value"], 1050.0)
        self.assertEqual(benchmark["points"][2]["value"], 950.0)
        self.assertEqual(benchmark["points"][2]["price"], 380.0)
        self.assertEqual(benchmark["points"][2]["source"], "unit_px")

    def test_latest_portfolio_backtest_api_marks_missing_benchmark_data(self) -> None:
        import engine.api.api_read_advanced as api_read_advanced

        api_read_advanced = importlib.reload(api_read_advanced)
        con = _make_latest_backtest_connection(include_prices=False)
        with patch.object(api_read_advanced, "db_connect", return_value=con):
            payload = api_read_advanced.get_latest_portfolio_backtest()

        self.assertTrue(payload["ok"])
        self.assertFalse(payload["meta"]["benchmark_ready"])
        benchmark = payload["run"]["benchmark"]
        self.assertFalse(benchmark["available"])
        self.assertEqual(benchmark["symbol"], "SPY")
        self.assertEqual(benchmark["source"], "prices")
        self.assertEqual(benchmark["points"], [])
        self.assertEqual(benchmark["unavailable_reason"], "prices_table_missing")

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
  benchmark: {
    available: true,
    symbol: "SPY",
    source: "prices",
    normalization: "portfolio_start_value",
    points: [
      { ts_ms: 1_700_000_000_000, price: 400, value: 1.0, source: "unit" },
      { ts_ms: 1_700_000_060_000, price: 420, value: 1.05, source: "unit" },
    ],
  },
});
assert.equal(benchmarkVm.benchmarkState.available, true);
assert.equal(benchmarkVm.benchmarkState.label, "SPY");
assert.equal(benchmarkVm.benchmarkState.source, "prices");
assert.equal(benchmarkVm.benchmarkState.normalization, "portfolio_start_value");
assert.match(benchmarkVm.benchmarkState.text, /from prices/);
assert.equal(benchmarkVm.benchmarkSeries[0].value, 1.0);
assert.equal(benchmarkVm.benchmarkSeries[1].value, 1.05);

const missingBenchmarkVm = buildPortfolioBacktestProViewModel({
  ...run,
  benchmark: {
    available: false,
    symbol: "SPY",
    source: "prices",
    normalization: "portfolio_start_value",
    unavailable_reason: "benchmark_prices_missing",
    points: [],
  },
});
assert.equal(missingBenchmarkVm.benchmarkState.available, false);
assert.equal(missingBenchmarkVm.benchmarkSeries.length, 0);
assert.match(missingBenchmarkVm.benchmarkState.text, /SPY benchmark unavailable from prices: benchmark prices missing\./);

const gapVm = buildPortfolioBacktestProViewModel({
  id: 8,
  points: [
    { ts_ms: 1_700_000_000_000, equity: 1.0, drawdown: 0 },
    { ts_ms: 1_700_000_060_000, equity: null, drawdown: null },
    { ts_ms: 1_700_000_120_000, equity: 1.1, drawdown: -0.02 },
  ],
});
assert.equal(gapVm.ok, true);
assert.equal(gapVm.equitySeries.length, 3);
assert.equal(gapVm.drawdownSeries.length, 3);
assert.equal("value" in gapVm.equitySeries[1], false);
assert.equal("value" in gapVm.drawdownSeries[1], false);
assert.equal(gapVm.finiteEquitySeries.length, 2);
assert.equal(gapVm.finiteDrawdownSeries.length, 2);
assert.equal(gapVm.latestEquity, 1.1);
assert.equal(gapVm.latestDrawdown, -0.02);
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
