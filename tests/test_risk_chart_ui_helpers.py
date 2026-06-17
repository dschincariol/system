from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_node(code: str, *paths: Path) -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node executable is not available")
    result = subprocess.run(
        [node, "--input-type=module", "-e", code, *[str(path) for path in paths]],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    return json.loads(result.stdout)


def test_risk_history_view_model_uses_all_timestamped_rows() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildRiskHistoryViewModel({
  ok: true,
  history: [
    { ts_ms: 3000, gross: 0.50, net: 0.15, drawdown: 0.03, blocked: false },
    { ts_ms: 1000, gross: 0.30, net: 0.10, drawdown: 0.01, blocked: false },
    { ts_ms: 2000, gross: 0.40, net: -0.20, drawdown: 0.02, blocked: true },
  ],
});
console.log(JSON.stringify({
  ready: vm.ready,
  pointCount: vm.pointCount,
  times: vm.rows.map((row) => row.ts_ms),
  blockedCount: vm.blockedCount,
  latestGross: vm.latest.gross,
  summary: vm.summary,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["ready"] is True
    assert parsed["pointCount"] == 3
    assert parsed["times"] == [1000, 2000, 3000]
    assert parsed["blockedCount"] == 1
    assert parsed["latestGross"] == 0.5
    assert "3 timestamped rows" in parsed["summary"]


def test_monte_carlo_summary_model_reports_missing_fan_detail() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildMonteCarloRiskViewModel({
  ok: true,
  ready: true,
  status: "ok",
  simulations: 1500,
  horizon: 10,
  var_95: -0.01,
  var_99: -0.02,
  cvar_95: -0.03,
  cvar_99: -0.04,
  worst_simulated_drawdown: 0.05,
  drawdown_percentiles: { p95: 0.04 },
  stress: { var_95: -0.05, cvar_95: -0.07, drawdown_percentiles: { p95: 0.08 } },
  chart_detail: {
    mode: "summary",
    has_fan: false,
    has_distribution: false,
    unavailable: [{ field: "fan_chart", reason: "summary only" }],
  },
});
console.log(JSON.stringify({
  mode: vm.mode,
  hasFan: vm.hasFan,
  hasDistribution: vm.hasDistribution,
  bars: vm.bars.map((bar) => [bar.key, bar.value]),
  unavailableFields: vm.unavailable.map((row) => row.field),
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["mode"] == "summary"
    assert parsed["hasFan"] is False
    assert parsed["hasDistribution"] is False
    assert ["cvar_95", 0.03] in parsed["bars"]
    assert "fan_chart" in parsed["unavailableFields"]
    assert "distribution" in parsed["unavailableFields"]


def test_monte_carlo_fan_requires_real_quantile_rows() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildMonteCarloRiskViewModel({
  ok: true,
  ready: true,
  status: "ok",
  var_95: -0.01,
  chart_detail: { has_fan: true },
  fan: [
    { step: 1, p50: 0.01 },
    { step: 2, p50: 0.02 },
  ],
});
console.log(JSON.stringify({
  mode: vm.mode,
  hasFan: vm.hasFan,
  fanRows: vm.fanRows.length,
  unavailableFields: vm.unavailable.map((row) => row.field),
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["mode"] == "summary"
    assert parsed["hasFan"] is False
    assert parsed["fanRows"] == 0
    assert "fan_chart" in parsed["unavailableFields"]


def test_alpha_decay_view_model_selects_chartable_strategy_history() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildAlphaDecayViewModel({
  ok: true,
  runtime: { status: "warn" },
  strategy_history: [
    { strategy: "short_lived", ts_ms: 3000, rolling_sharpe: 0.1, severity: "warn" },
    { strategy: "mean_reversion", ts_ms: 1000, rolling_sharpe: 0.4, half_life_buckets: 5, severity: "ok" },
    { strategy: "mean_reversion", ts_ms: 2000, rolling_sharpe: 0.2, half_life_buckets: 2, severity: "warn" },
  ],
});
console.log(JSON.stringify({
  ready: vm.ready,
  selectedStrategy: vm.selectedStrategy,
  times: vm.rows.map((row) => row.ts_ms),
  latestSharpe: vm.latest.rolling_sharpe,
  strategies: vm.strategies,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["ready"] is True
    assert parsed["selectedStrategy"] == "mean_reversion"
    assert parsed["times"] == [1000, 2000]
    assert parsed["latestSharpe"] == 0.2
    assert parsed["strategies"][0]["points"] == 2


def test_regime_history_view_model_preserves_labeled_layers_over_time() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildRegimeHistoryViewModel({
  ok: true,
  symbol: "SPY",
  rows: [
    {
      ts_ms: 2000,
      source_symbol: "SPY",
      layers: {
        macro: { label: "RISK_OFF", confidence: 0.7 },
        asset: { label: "BREAK", confidence: 0.6 },
        micro: { label: "THIN", confidence: 0.5 },
      },
    },
    {
      ts_ms: 1000,
      source_symbol: "SPY",
      layers: {
        macro: { label: "RISK_ON", confidence: 0.8 },
        asset: { label: "STABLE", confidence: 0.9 },
        micro: { label: "NORMAL", confidence: 0.6 },
      },
    },
  ],
});
console.log(JSON.stringify({
  ready: vm.ready,
  times: vm.rows.map((row) => row.ts_ms),
  latest: vm.latest,
  summary: vm.summary,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["ready"] is True
    assert parsed["times"] == [1000, 2000]
    assert parsed["latest"]["macro"] == "RISK_OFF"
    assert "latest macro RISK_OFF" in parsed["summary"]
