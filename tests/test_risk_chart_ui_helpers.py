from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


FAKE_DOM_JS = r"""
const nodes = new Map();
function makeNode(id) {
  const node = {
    id,
    className: "",
    innerHTML: "",
    hidden: false,
    style: {},
    attrs: {},
    ownerDocument: null,
    parentNode: {
      insertBefore(child) {
        if (child && child.id) nodes.set(child.id, child);
        if (child) child.parentNode = this;
      },
    },
    classList: null,
    setAttribute(name, value) { this.attrs[name] = String(value); },
    getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null; },
    hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name); },
  };
  node.classList = {
    add(cls) {
      const classes = new Set(String(node.className || "").split(/\s+/).filter(Boolean));
      classes.add(cls);
      node.className = Array.from(classes).join(" ");
    },
  };
  return node;
}
const document = {
  getElementById(id) { return nodes.get(id) || null; },
  createElement(tag) {
    const node = makeNode("");
    node.tagName = String(tag || "").toUpperCase();
    node.ownerDocument = document;
    return node;
  },
};
globalThis.document = document;
function makeContext() {
  return new Proxy({}, {
    get(_target, prop) {
      if (prop === "measureText") return (text) => ({ width: String(text).length * 7 });
      if (prop === "canvas") return { width: 320, height: 180 };
      return () => undefined;
    },
    set() { return true; },
  });
}
function makeCanvas(id) {
  const canvas = makeNode(id);
  canvas.ownerDocument = document;
  canvas.width = 320;
  canvas.height = 180;
  canvas.getContext = () => makeContext();
  nodes.set(id, canvas);
  const a11y = makeNode(`${id}A11y`);
  a11y.ownerDocument = document;
  nodes.set(a11y.id, a11y);
  return canvas;
}
"""


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


def test_chart_focus_metadata_tolerates_canvas_without_has_attribute() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const minimalAttrs = [];
const minimal = {
  id: "minimalChart",
  setAttribute(name, value) { minimalAttrs.push([name, value]); },
};
const existingAttrs = [];
const existing = {
  id: "existingChart",
  getAttribute(name) { return name === "tabindex" ? "-1" : null; },
  setAttribute(name, value) { existingAttrs.push([name, value]); },
};
mod.applyChartFocusMetadata(minimal, { title: "Minimal", summary: "Minimal chart", pointCount: 1 });
mod.applyChartFocusMetadata(existing, { title: "Existing", summary: "Existing chart", pointCount: 1 });
console.log(JSON.stringify({
  minimalTabindex: minimalAttrs.some((row) => row[0] === "tabindex" && row[1] === "0"),
  existingTabindexWrites: existingAttrs.filter((row) => row[0] === "tabindex").length,
  minimalRole: minimalAttrs.find((row) => row[0] === "role")?.[1],
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "chart_a11y.js")

    assert parsed["minimalTabindex"] is True
    assert parsed["existingTabindexWrites"] == 0
    assert parsed["minimalRole"] == "img"


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


def test_risk_history_zero_reference_line_maps_to_zero() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const nodes = new Map();
const calls = [];
function makeNode(id) {
  const node = {
    id,
    className: "",
    innerHTML: "",
    hidden: false,
    style: {},
    attrs: {},
    ownerDocument: null,
    parentNode: {
      insertBefore(child) {
        if (child && child.id) nodes.set(child.id, child);
        if (child) child.parentNode = this;
      },
    },
    classList: null,
    setAttribute(name, value) { this.attrs[name] = String(value); },
    getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null; },
    hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name); },
  };
  node.classList = {
    add(cls) {
      const classes = new Set(String(node.className || "").split(/\s+/).filter(Boolean));
      classes.add(cls);
      node.className = Array.from(classes).join(" ");
    },
  };
  return node;
}
const document = {
  getElementById(id) { return nodes.get(id) || null; },
  createElement(tag) {
    const node = makeNode("");
    node.tagName = String(tag || "").toUpperCase();
    node.ownerDocument = document;
    return node;
  },
};
globalThis.document = document;
const ctx = {
  fillStyle: "",
  strokeStyle: "",
  lineWidth: 1,
  font: "",
  clearRect(...args) { calls.push(["clearRect", ...args]); },
  strokeRect(...args) { calls.push(["strokeRect", ...args]); },
  fillRect(...args) { calls.push(["fillRect", this.fillStyle, ...args]); },
  beginPath() { calls.push(["beginPath"]); },
  moveTo(...args) { calls.push(["moveTo", ...args]); },
  lineTo(...args) { calls.push(["lineTo", ...args]); },
  stroke() { calls.push(["stroke", this.strokeStyle]); },
  fillText(...args) { calls.push(["fillText", ...args]); },
  measureText(text) { return { width: String(text).length * 7 }; },
};
const canvas = makeNode("riskHistoryChart");
canvas.ownerDocument = document;
canvas.width = 320;
canvas.height = 180;
canvas.getContext = () => ctx;
nodes.set(canvas.id, canvas);
const a11y = makeNode("riskHistoryChartA11y");
a11y.ownerDocument = document;
nodes.set(a11y.id, a11y);

const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildRiskHistoryViewModel({
  ok: true,
  history: [
    { ts_ms: 1000, gross: 0.30, net: 0.10, drawdown: 0.01, blocked: false },
    { ts_ms: 2000, gross: 0.40, net: -0.20, drawdown: 0.02, blocked: false },
    { ts_ms: 3000, gross: 0.50, net: 0.15, drawdown: 0.03, blocked: false },
  ],
});
mod.renderRiskHistoryChart(canvas, vm);

const values = [0.30, 0.10, 0.01, 0.40, -0.20, 0.02, 0.50, 0.15, 0.03];
const min = Math.min(...values);
const max = Math.max(...values);
const pad = (max - min) * 0.08;
const yMin = min - pad;
const yMax = max + pad;
const padT = 22;
const plotH = 180 - padT - 30;
const expectedY = padT + plotH * (1 - ((0 - yMin) / Math.max(1e-9, yMax - yMin)));
const midpointY = padT + plotH / 2;
const strokeIndex = calls.findIndex((call) => call[0] === "stroke" && call[1] === "#20252c");
const beforeStroke = strokeIndex >= 0 ? calls.slice(0, strokeIndex) : [];
const move = [...beforeStroke].reverse().find((call) => call[0] === "moveTo");
const line = [...beforeStroke].reverse().find((call) => call[0] === "lineTo");
console.log(JSON.stringify({
  expectedY,
  midpointY,
  strokeY: move ? move[2] : null,
  lineY: line ? line[2] : null,
  zeroLabel: calls.some((call) => call[0] === "fillText" && call[1] === "0.0%"),
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["zeroLabel"] is True
    assert abs(parsed["strokeY"] - parsed["expectedY"]) < 0.001
    assert abs(parsed["lineY"] - parsed["expectedY"]) < 0.001
    assert abs(parsed["strokeY"] - parsed["midpointY"]) > 1.0


def test_risk_history_accessibility_summary_and_table_cover_all_visible_series() -> None:
    code = FAKE_DOM_JS + r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const canvas = makeCanvas("riskHistoryChart");
const vm = mod.buildRiskHistoryViewModel({
  ok: true,
  history: [
    { ts_ms: 1000, gross: 0.30, net: 0.10, drawdown: 0.01, blocked: false },
    { ts_ms: 2000, gross: null, net: 0.07, drawdown: 0.04, blocked: true },
    { ts_ms: 3000, gross: 0.50, net: -0.20, drawdown: 0.03, blocked: false },
  ],
});
mod.renderRiskHistoryChart(canvas, vm);
console.log(JSON.stringify({
  summary: canvas.attrs["aria-label"],
  html: nodes.get("riskHistoryChartA11y").innerHTML,
  vmSummary: vm.summary,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert "latest gross 50.0%" in parsed["summary"]
    assert "net -20.0%" in parsed["summary"]
    assert "drawdown 3.0%" in parsed["summary"]
    assert "blocked context 1 of 3 rows" in parsed["summary"]
    assert "Gross" in parsed["html"]
    assert "Net" in parsed["html"]
    assert "Drawdown" in parsed["html"]
    assert "Blocked" in parsed["html"]
    assert "+7.00%" in parsed["html"]
    assert "4.00%" in parsed["html"]
    assert ">yes<" in parsed["html"]


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


def test_monte_carlo_model_accepts_populated_fan_and_distribution_rows() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildMonteCarloRiskViewModel({
  ok: true,
  ready: true,
  status: "ok",
  var_95: -0.01,
  cvar_95: -0.03,
  fan: [
    { step: 1, p05: -0.02, p50: 0.0, p95: 0.02 },
    { step: 2, p05: -0.04, p50: 0.01, p95: 0.05 },
  ],
  distribution: [
    { bucket: "-4% to 0%", lower: -0.04, upper: 0.0, value: -0.02, count: 12, probability: 0.24 },
    { bucket: "0% to 4%", lower: 0.0, upper: 0.04, value: 0.02, count: 38, probability: 0.76 },
  ],
});
console.log(JSON.stringify({
  mode: vm.mode,
  hasFan: vm.hasFan,
  hasDistribution: vm.hasDistribution,
  fanRows: vm.fanRows.length,
  distributionRows: vm.distributionRows.length,
  unavailableFields: vm.unavailable.map((row) => row.field),
  summary: vm.summary,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["mode"] == "fan_distribution"
    assert parsed["hasFan"] is True
    assert parsed["hasDistribution"] is True
    assert parsed["fanRows"] == 2
    assert parsed["distributionRows"] == 2
    assert "fan_chart" not in parsed["unavailableFields"]
    assert "distribution" not in parsed["unavailableFields"]
    assert "distribution data available" in parsed["summary"]


def test_monte_carlo_fan_accessibility_summary_and_table_include_band_quantiles() -> None:
    code = FAKE_DOM_JS + r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const canvas = makeCanvas("monteCarloFanChart");
const vm = mod.buildMonteCarloRiskViewModel({
  ok: true,
  ready: true,
  status: "ok",
  var_95: -0.01,
  cvar_95: -0.03,
  fan: [
    { step: 1, p05: -0.02, p50: 0.0, p95: 0.02 },
    { step: 2, p05: -0.04, p50: 0.01, p95: 0.05 },
  ],
});
mod.renderMonteCarloFanChart(canvas, vm);
console.log(JSON.stringify({
  summary: canvas.attrs["aria-label"],
  html: nodes.get("monteCarloFanChartA11y").innerHTML,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert "p05 -4.00%" in parsed["summary"]
    assert "p50 +1.00%" in parsed["summary"]
    assert "p95 +5.00%" in parsed["summary"]
    assert "shaded band spans p05 to p95" in parsed["summary"]
    assert "P05" in parsed["html"]
    assert "P50" in parsed["html"]
    assert "P95" in parsed["html"]
    assert "-4.00%" in parsed["html"]
    assert "+5.00%" in parsed["html"]


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


def test_alpha_decay_view_model_prioritizes_severity_over_row_count_and_honors_selection() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const payload = {
  ok: true,
  runtime: { status: "severe" },
  strategy_history: [
    { strategy: "benign_carry", ts_ms: 1000, rolling_sharpe: 0.6, half_life_buckets: 8, severity: "ok", severity_score: 0.0, throttle_mult: 1.0 },
    { strategy: "benign_carry", ts_ms: 2000, rolling_sharpe: 0.5, half_life_buckets: 8, severity: "ok", severity_score: 0.0, throttle_mult: 1.0 },
    { strategy: "benign_carry", ts_ms: 3000, rolling_sharpe: 0.4, half_life_buckets: 8, severity: "ok", severity_score: 0.0, throttle_mult: 1.0 },
    { strategy: "benign_carry", ts_ms: 4000, rolling_sharpe: 0.3, half_life_buckets: 8, severity: "ok", severity_score: 0.0, throttle_mult: 1.0 },
    { strategy: "breakout_decay", ts_ms: 2500, rolling_sharpe: -0.1, half_life_buckets: 2, structural_break_z: -1.5, severity: "severe", severity_score: 0.8, throttle_mult: 0.3 },
    { strategy: "breakout_decay", ts_ms: 3500, rolling_sharpe: -0.2, half_life_buckets: 1, structural_break_z: -2.4, severity: "severe", severity_score: 0.9, throttle_mult: 0.2 },
  ],
};
const auto = mod.buildAlphaDecayViewModel(payload);
const selected = mod.buildAlphaDecayViewModel(payload, { selectedStrategy: "benign_carry" });
const select = { value: "", innerHTML: "", disabled: true, hidden: false, style: {}, onchange: null };
let userSelected = "";
mod.renderAlphaDecayStrategySelector(select, payload, auto, (strategy) => { userSelected = strategy; });
select.value = "benign_carry";
select.onchange();
console.log(JSON.stringify({
  autoStrategy: auto.selectedStrategy,
  selectedStrategy: selected.selectedStrategy,
  selectedRows: selected.rows.map((row) => row.ts_ms),
  firstOption: auto.strategies[0].strategy,
  optionsHtml: select.innerHTML,
  selectorDisabled: select.disabled,
  userSelected,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["autoStrategy"] == "breakout_decay"
    assert parsed["selectedStrategy"] == "benign_carry"
    assert parsed["selectedRows"] == [1000, 2000, 3000, 4000]
    assert parsed["firstOption"] == "breakout_decay"
    assert "benign_carry (OK, 4 pts)" in parsed["optionsHtml"]
    assert parsed["selectorDisabled"] is False
    assert parsed["userSelected"] == "benign_carry"


def test_alpha_decay_view_model_and_renderer_preserve_gaps_and_zero_throttle() -> None:
    code = r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildAlphaDecayViewModel({
  ok: true,
  runtime: { status: "severe" },
  strategy_history: [
    { strategy: "blocked_alpha", ts_ms: 1000, rolling_sharpe: 0.1, half_life_buckets: 4, severity: "ok", throttle_mult: 0.7 },
    { strategy: "blocked_alpha", ts_ms: 2000, rolling_sharpe: null, half_life_buckets: null, severity: "severe", throttle_mult: 0.0 },
    { strategy: "blocked_alpha", ts_ms: 3000, rolling_sharpe: 0.0, half_life_buckets: 0.0, severity: "severe", throttle_mult: 0.0 },
  ],
});
const calls = [];
const ctx = new Proxy({}, {
  get(_target, prop) {
    if (prop === "measureText") return (text) => ({ width: String(text).length * 7 });
    if (prop === "canvas") return { width: 320, height: 180 };
    return (...args) => {
      calls.push([String(prop), ...args]);
      return undefined;
    };
  },
  set() { return true; },
});
const canvas = {
  width: 320,
  height: 180,
  id: "alphaDecayChart",
  getContext: () => ctx,
  getAttribute: () => null,
  hasAttribute: () => false,
  setAttribute() {},
};
mod.renderAlphaDecayChart(canvas, vm);
console.log(JSON.stringify({
  ready: vm.ready,
  rows: vm.rows.length,
  chartablePoints: vm.strategies[0].points,
  zeroThrottle: vm.rows[1].throttle_mult,
  zeroSharpe: vm.rows[2].rolling_sharpe,
  moveToCount: calls.filter((row) => row[0] === "moveTo").length,
  lineToCount: calls.filter((row) => row[0] === "lineTo").length,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert parsed["ready"] is True
    assert parsed["rows"] == 3
    assert parsed["chartablePoints"] == 2
    assert parsed["zeroThrottle"] == 0
    assert parsed["zeroSharpe"] == 0
    assert parsed["moveToCount"] >= 4
    assert parsed["lineToCount"] == 0


def test_alpha_decay_accessibility_summary_and_table_include_sharpe_and_half_life() -> None:
    code = FAKE_DOM_JS + r"""
import { pathToFileURL } from "node:url";
const mod = await import(pathToFileURL(process.argv[1]).href);
const canvas = makeCanvas("alphaDecayChart");
const vm = mod.buildAlphaDecayViewModel({
  ok: true,
  runtime: { status: "warn" },
  strategy_history: [
    { strategy: "mean_reversion", ts_ms: 1000, rolling_sharpe: 0.4, half_life_buckets: 5, severity: "ok" },
    { strategy: "mean_reversion", ts_ms: 2000, rolling_sharpe: null, half_life_buckets: 3, severity: "warn" },
    { strategy: "mean_reversion", ts_ms: 3000, rolling_sharpe: 0.2, half_life_buckets: 2, severity: "warn" },
  ],
});
mod.renderAlphaDecayChart(canvas, vm);
console.log(JSON.stringify({
  summary: canvas.attrs["aria-label"],
  html: nodes.get("alphaDecayChartA11y").innerHTML,
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "risk_charts.js")

    assert "latest rolling Sharpe 0.20" in parsed["summary"]
    assert "half-life 2.0 buckets" in parsed["summary"]
    assert "Rolling Sharpe" in parsed["html"]
    assert "Half-life Buckets" in parsed["html"]
    assert "0.200" in parsed["html"]
    assert "3.00" in parsed["html"]
    assert "unavailable" in parsed["html"]


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
