from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from engine.api.model_performance_divergence import build_model_performance_divergence


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_performance_divergence_aggregation_handles_missing_sources() -> None:
    payload = build_model_performance_divergence(
        model_id="alpha",
        shadow_payload=[],
        backtest_payload={"ok": False, "error": "no portfolio backtest runs", "run": None},
        pnl_payload={"ok": True, "data": {"source": "missing", "ts_ms": 0}},
        execution_metrics_payload={"ok": False, "error": "metrics missing"},
        execution_stats_payload={"ok": False, "error": "stats missing"},
        execution_advisories_payload={"ok": True, "items": []},
        model_registry_payload={
            "ok": True,
            "rows": [
                {
                    "model_name": "alpha",
                    "stage": "champion",
                    "metrics": {},
                    "created_ts_ms": 1_700_000_000_000,
                }
            ],
        },
        now_ms=1_700_000_010_000,
    )

    assert payload["ok"] is True
    assert payload["status"]["state"] == "incomplete"
    assert payload["selection"]["model_id"] == "alpha"
    assert "shadow_eval" in payload["missing_sources"]
    assert "portfolio_backtest" in payload["missing_sources"]
    assert "live_pnl" in payload["missing_sources"]
    assert all("status" in row for row in payload["comparisons"])


def test_performance_divergence_aggregation_flags_live_decay() -> None:
    payload = build_model_performance_divergence(
        model_id="alpha",
        shadow_payload=[
            {
                "model_id": "alpha",
                "ts_ms": 1_700_000_005_000,
                "directional_acc": 0.62,
                "avg_slippage_impact": 1.5,
            }
        ],
        backtest_payload={
            "ok": True,
            "run": {
                "ts_ms": 1_700_000_000_000,
                "metrics": {"total_return": 0.12, "hit_rate": 0.64},
                "points": [],
            },
            "meta": {"count": 10},
        },
        pnl_payload={
            "ok": True,
            "meta": {"count": 8},
            "data": {
                "model_id": "alpha",
                "total_return": 0.01,
                "source": "canonical",
                "ts_ms": 1_700_000_009_000,
            },
        },
        execution_metrics_payload={
            "ok": True,
            "n_fills": 8,
            "avg_slippage_bps": 8.0,
            "by_strategy": [{"strategy_name": "mean_reversion", "n_fills": 8}],
        },
        execution_stats_payload={
            "ok": True,
            "ts_ms": 1_700_000_009_500,
            "orders": {"total": 10},
            "fills": {"total": 8, "last_fill_ts_ms": 1_700_000_009_500},
            "metrics": {"avg_slippage_bps": 8.0},
        },
        execution_advisories_payload={
            "ok": True,
            "items": [{"ts_ms": 1_700_000_004_000, "expected_slippage_bps": 2.0}],
        },
        model_registry_payload={
            "ok": True,
            "rows": [
                {
                    "model_name": "alpha",
                    "stage": "champion",
                    "metrics": {"expected_fill_rate": 0.95},
                    "created_ts_ms": 1_700_000_000_500,
                }
            ],
        },
        now_ms=1_700_000_010_000,
    )

    statuses = {row["key"]: row["status"] for row in payload["comparisons"]}
    assert payload["status"]["state"] == "diverged"
    assert payload["selection"]["strategy"] == "mean_reversion"
    assert statuses["return"] == "diverged"
    assert statuses["slippage_bps"] == "diverged"
    assert statuses["fill_rate"] == "diverged"


def test_performance_divergence_frontend_handles_partial_and_failure() -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for dashboard frontend helper test")

    script = r"""
import assert from "node:assert/strict";
import {
  buildPerformanceDivergenceViewModel,
  loadPerformanceDivergence,
  renderPerformanceDivergencePanel,
} from "./ui/model_performance_divergence.mjs";

class El {
  constructor() {
    this.textContent = "";
    this.innerHTML = "";
    this.className = "";
    this.title = "";
    this.value = "";
    this.disabled = false;
    this.onchange = null;
  }
}

const elements = new Map();
const doc = {
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, new El());
    return elements.get(id);
  },
};

const partial = {
  ok: true,
  selection: { model_id: "alpha" },
  status: { state: "incomplete", reason: "partial data", ts_ms: 1700000010000 },
  comparisons: [
    {
      key: "return",
      label: "Return",
      unit: "pct",
      expected: { value: 0.12, unit: "pct", source: "portfolio_backtest", ts_ms: 1700000000000 },
      realized: null,
      status: "incomplete",
      explanation: "Missing realized return source.",
    },
  ],
  missing_sources: ["live_pnl"],
};

const vm = buildPerformanceDivergenceViewModel(partial);
assert.equal(vm.rows[0].realizedText, "-");
assert.equal(vm.missing.some((item) => item.includes("live_pnl")), true);

const rendered = renderPerformanceDivergencePanel(partial, doc);
assert.equal(rendered.state, "incomplete");
assert.match(elements.get("performanceDivergenceRows").innerHTML, /Missing realized return source/);

await loadPerformanceDivergence(async () => {
  throw new Error("route down");
}, doc);
assert.match(elements.get("performanceDivergenceStatus").textContent, /endpoint unavailable/i);
assert.match(elements.get("performanceDivergenceRows").innerHTML, /No comparable performance data/);
"""
    result = subprocess.run(
        [node, "--input-type=module", "-e", script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_performance_divergence_frontend_ranks_and_switches_chart_metric() -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for dashboard frontend helper test")

    script = r"""
import assert from "node:assert/strict";
import {
  buildPerformanceDivergenceViewModel,
  renderPerformanceDivergencePanel,
} from "./ui/model_performance_divergence.mjs";

class El {
  constructor() {
    this.textContent = "";
    this.innerHTML = "";
    this.className = "";
    this.title = "";
    this.value = "";
    this.disabled = false;
    this.onchange = null;
  }
}

const elements = new Map();
const doc = {
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, new El());
    return elements.get(id);
  },
};

const payload = {
  ok: true,
  selection: { model_id: "alpha", strategy: "mean_reversion" },
  status: { state: "diverged", reason: "at least one metric diverged", ts_ms: 1700000020000 },
  comparisons: [
    {
      key: "hit_rate",
      label: "Hit Rate",
      unit: "pct",
      expected: { value: 0.60, unit: "pct", source: "backtest", ts_ms: 1700000010000 },
      realized: { value: 0.61, unit: "pct", source: "live", ts_ms: 1700000015000 },
      delta: 0.01,
      status: "ok",
      explanation: "aligned",
    },
    {
      key: "return",
      label: "Return",
      unit: "pct",
      expected: { value: 0.10, unit: "pct", source: "backtest", ts_ms: 1700000010000 },
      realized: { value: 0.04, unit: "pct", source: "live", ts_ms: 1700000014000 },
      delta: -0.06,
      status: "diverged",
      explanation: "return diverged",
    },
    {
      key: "fill_rate",
      label: "Fill Rate",
      unit: "pct",
      expected: { value: 0.95, unit: "pct", source: "registry", ts_ms: 1700000009000 },
      realized: { value: 0.75, unit: "pct", source: "execution", ts_ms: 1700000013000 },
      delta: -0.20,
      status: "diverged",
      explanation: "fill rate diverged",
    },
  ],
};

const vm = buildPerformanceDivergenceViewModel(payload);
assert.equal(vm.selectedMetricKey, "fill_rate");
assert.equal(vm.chart.label, "Fill Rate");
assert.deepEqual(vm.chartOptions.map((option) => option.key), ["fill_rate", "return", "hit_rate"]);

const selectedVm = buildPerformanceDivergenceViewModel(payload, { selectedMetricKey: "return" });
assert.equal(selectedVm.chart.label, "Return");
assert.equal(selectedVm.state, "diverged");
assert.equal(selectedVm.rows.length, 3);

const importanceVm = buildPerformanceDivergenceViewModel({
  ok: true,
  status: { state: "ok", reason: "aligned", ts_ms: 1700000020000 },
  comparisons: [
    {
      key: "fill_rate",
      label: "Fill Rate",
      unit: "pct",
      expected: { value: 0.95, unit: "pct", source: "registry", ts_ms: 1700000010000 },
      realized: { value: 0.94, unit: "pct", source: "execution", ts_ms: 1700000015000 },
      delta: -0.01,
      status: "ok",
      explanation: "aligned",
    },
    {
      key: "return",
      label: "Return",
      unit: "pct",
      expected: { value: 0.10, unit: "pct", source: "backtest", ts_ms: 1700000010000 },
      realized: { value: 0.09, unit: "pct", source: "live", ts_ms: 1700000015000 },
      delta: -0.01,
      status: "ok",
      explanation: "aligned",
    },
  ],
});
assert.equal(importanceVm.selectedMetricKey, "return");

const freshnessVm = buildPerformanceDivergenceViewModel({
  ok: true,
  status: { state: "ok", reason: "aligned", ts_ms: 1700000030000 },
  comparisons: [
    {
      key: "return",
      label: "Return",
      unit: "pct",
      expected: { value: 0.10, unit: "pct", source: "backtest", ts_ms: 1700000010000 },
      realized: { value: 0.09, unit: "pct", source: "live", ts_ms: 1700000015000 },
      delta: -0.01,
      status: "ok",
      explanation: "aligned",
    },
    {
      key: "fill_rate",
      label: "Fill Rate",
      unit: "pct",
      expected: { value: 0.95, unit: "pct", source: "registry", ts_ms: 1700000020000 },
      realized: { value: 0.94, unit: "pct", source: "execution", ts_ms: 1700000025000 },
      delta: -0.01,
      status: "ok",
      explanation: "aligned",
    },
  ],
});
assert.equal(freshnessVm.selectedMetricKey, "fill_rate");

const chartCalls = [];
const rendered = renderPerformanceDivergencePanel(payload, doc, (_canvas, values, opts) => {
  chartCalls.push({ values, opts });
});

assert.equal(rendered.selectedMetricKey, "fill_rate");
assert.equal(elements.get("performanceDivergenceMetric").value, "fill_rate");
assert.equal(elements.get("performanceDivergenceMetric").disabled, false);
assert.match(elements.get("performanceDivergenceMetric").innerHTML, /Fill Rate \| diverged/);
assert.equal(chartCalls[chartCalls.length - 1].opts.valueLabel, "Fill Rate");
assert.equal(elements.get("performanceDivergenceMeta").textContent, "diverged");
assert.equal(elements.get("performanceDivergenceStatus").textContent, "at least one metric diverged");

elements.get("performanceDivergenceMetric").value = "return";
elements.get("performanceDivergenceMetric").onchange();
assert.equal(chartCalls[chartCalls.length - 1].opts.valueLabel, "Return");
assert.equal(elements.get("performanceDivergenceMetric").value, "return");
assert.equal(elements.get("performanceDivergenceMeta").textContent, "diverged");
assert.equal(elements.get("performanceDivergenceStatus").textContent, "at least one metric diverged");
assert.match(elements.get("performanceDivergenceRows").innerHTML, /Fill Rate/);
"""
    result = subprocess.run(
        [node, "--input-type=module", "-e", script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr or result.stdout
