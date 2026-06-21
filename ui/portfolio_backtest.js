/*
  FILE: ui/portfolio_backtest.js

  Latest portfolio-backtest panel loader. This module fetches the newest
  backtest run, renders summary metrics, and draws equity/drawdown charts for
  dashboard inspection.
*/

import { renderLineChart } from "./charts.js";
import { renderChartAccessibility } from "./chart_a11y.js";
import {
  normalizeDecisionMarker,
} from "./decision_overlays.js";
import {
  addSeriesCompat,
  applyMarkersToState,
  clearMarkerLayer,
  createProChart,
  disconnectResizeObserver,
  ensureLightweightCharts,
} from "./pro_chart_core.js";
import { DEFAULT_RISK_CAPS } from "./risk_headroom_thresholds.js";
import { _fmtPct } from "./utils.js";

let _lastPortfolioBacktestSummary = null;

const PORTFOLIO_PRO_FLAG_KEY = "dashboard.portfolioBacktest.proCharts.enabled";
export const PORTFOLIO_DRAWDOWN_THROTTLE = -DEFAULT_RISK_CAPS.drawdown;

const _PRO = {
  equityChart: null,
  drawdownChart: null,
  equityResizeObserver: null,
  drawdownResizeObserver: null,
  markerLayer: null,
  markerSeries: null,
  ddPriceLine: null,
};

export function getLastPortfolioBacktestSummary() {
  return _lastPortfolioBacktestSummary;
}

function _fmtMoney(x) {
  const v = Number(x || 0);
  const s = Math.abs(v) >= 1000 ? v.toFixed(0) : v.toFixed(2);
  return (v < 0 ? "-" : "") + "$" + String(s).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function _num(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function _escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function _normalizeTime(value, fallbackIndex = 0) {
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) {
    return n > 10_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed) && parsed > 0) return Math.floor(parsed / 1000);
  }
  return Math.max(1, Number(fallbackIndex || 0) + 1);
}

function _formatChartTime(time) {
  const n = Number(time);
  if (Number.isFinite(n) && n > 10_000) {
    try { return new Date(n * 1000).toLocaleString(); } catch {}
  }
  return String(time ?? "unavailable");
}

function _normalizeDrawdownValue(value) {
  const n = _num(value);
  if (n == null) return null;
  if (n === 0) return 0;
  return n > 0 ? -Math.abs(n) : n;
}

function _finiteMetric(metrics, keys) {
  const source = metrics && typeof metrics === "object" ? metrics : {};
  for (const key of keys) {
    const n = _num(source[key]);
    if (n != null) return n;
  }
  return null;
}

function _fmtRatio(value) {
  const n = _num(value);
  return n == null ? "n/a" : n.toFixed(2);
}

function _fmtTurnover(value) {
  const n = _num(value);
  return n == null ? "n/a" : n.toFixed(3);
}

function _fmtCount(value) {
  const n = _num(value);
  return n == null ? "n/a" : String(Math.max(0, Math.round(n)));
}

function _asDetail(point) {
  const detail = point && typeof point.detail === "object" ? point.detail : null;
  if (detail) return detail;
  const raw = point && (point.detail_json || point.detailJson);
  if (typeof raw === "string" && raw.trim()) {
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {}
  }
  return {};
}

function _signedPositionsMap(positions) {
  const out = new Map();
  for (const row of Array.isArray(positions) ? positions : []) {
    const symbol = String(row && row.symbol || "").trim().toUpperCase();
    if (!symbol) continue;
    const weight = _num(row.weight, 0) || 0;
    const side = String(row.side || "").toUpperCase();
    out.set(symbol, side === "SHORT" ? -Math.abs(weight) : Math.abs(weight));
  }
  return out;
}

function _positionChangeCount(prev, next) {
  if (!prev) return next && next.size ? next.size : 0;
  let count = 0;
  const keys = new Set([...prev.keys(), ...next.keys()]);
  for (const key of keys) {
    if (Math.abs(Number(prev.get(key) || 0) - Number(next.get(key) || 0)) > 1e-9) count += 1;
  }
  return count;
}

function _computeTurnoverFromPoints(points) {
  let prev = null;
  let total = 0;
  let samples = 0;
  for (const point of Array.isArray(points) ? points : []) {
    const detail = _asDetail(point);
    const cur = _signedPositionsMap(detail.positions || []);
    if (prev) {
      const keys = new Set([...prev.keys(), ...cur.keys()]);
      let step = 0;
      for (const key of keys) {
        step += Math.abs(Number(cur.get(key) || 0) - Number(prev.get(key) || 0));
      }
      total += 0.5 * step;
      samples += 1;
    }
    prev = cur;
  }
  return samples > 0 ? total / samples : null;
}

function _extractBenchmarkRows(run) {
  const metrics = run && typeof run.metrics === "object" ? run.metrics : {};
  const benchmark = run && typeof run.benchmark === "object" ? run.benchmark : {};
  const candidates = [
    run && run.benchmark_points,
    run && run.benchmarkPoints,
    benchmark.points,
    benchmark.series,
    metrics.benchmark_points,
    metrics.benchmark_series,
  ];
  for (const rows of candidates) {
    if (Array.isArray(rows) && rows.length) return rows;
  }
  return [];
}

function _extractBenchmarkLabel(run) {
  const metrics = run && typeof run.metrics === "object" ? run.metrics : {};
  const benchmark = run && typeof run.benchmark === "object" ? run.benchmark : {};
  return String(
    (run && (run.benchmark_symbol || run.benchmarkLabel)) ||
    benchmark.symbol ||
    benchmark.label ||
    metrics.benchmark_symbol ||
    "Benchmark"
  );
}

function _benchmarkUnavailableText(run, fallback = "Benchmark unavailable: endpoint returned no benchmark series.") {
  const benchmark = run && typeof run.benchmark === "object" ? run.benchmark : {};
  const label = _extractBenchmarkLabel(run || {});
  const source = String(benchmark.source || "").trim();
  const reason = String(benchmark.unavailable_reason || benchmark.reason || "").trim();
  if (!reason) return fallback;
  const reasonText = reason.replace(/_/g, " ");
  const sourceText = source ? ` from ${source}` : "";
  return `${label} benchmark unavailable${sourceText}: ${reasonText}.`;
}

function _valueFromPoint(point, keys) {
  if (point && typeof point === "object") {
    for (const key of keys) {
      const n = _num(point[key]);
      if (n != null) return n;
    }
    return null;
  }
  return _num(point);
}

function _buildBenchmarkSeries(run, portfolioStartValue) {
  const rows = _extractBenchmarkRows(run);
  const label = _extractBenchmarkLabel(run);
  const benchmark = run && typeof run.benchmark === "object" ? run.benchmark : {};
  const source = String(benchmark.source || "").trim();
  const normalization = String(benchmark.normalization || "").trim();
  if (!rows.length) {
    return {
      series: [],
      state: {
        available: false,
        label,
        source,
        normalization,
        unavailableReason: String(benchmark.unavailable_reason || ""),
        text: _benchmarkUnavailableText(run),
      },
    };
  }

  const rawSeries = rows
    .map((point, index) => {
      const value = _valueFromPoint(point, ["value", "equity", "benchmark", "close", "price", "v"]);
      if (value == null) return null;
      return {
        time: _normalizeTime(point && (point.ts_ms ?? point.time ?? point.t ?? point.date), index),
        value,
      };
    })
    .filter(Boolean)
    .sort((a, b) => Number(a.time) - Number(b.time));

  if (rawSeries.length < 2) {
    return {
      series: [],
      state: {
        available: false,
        label,
        source,
        normalization,
        unavailableReason: String(benchmark.unavailable_reason || "benchmark_points_insufficient"),
        text: _benchmarkUnavailableText(
          run,
          `${label} benchmark unavailable${source ? ` from ${source}` : ""}: fewer than two numeric points.`,
        ),
      },
    };
  }

  const first = rawSeries[0].value;
  const start = _num(portfolioStartValue, 1) || 1;
  const series = first > 0
    ? rawSeries.map((point) => ({ time: point.time, value: (point.value / first) * start }))
    : rawSeries;

  return {
    series,
    state: {
      available: true,
      label,
      source,
      normalization,
      text: `${label} benchmark overlay available${source ? ` from ${source}` : ""}; normalized to the portfolio start value.`,
    },
  };
}

function _explicitMarkerRows(detail) {
  const candidates = [
    detail.markers,
    detail.decision_markers,
    detail.decisions,
    detail.orders,
  ];
  return candidates.flatMap((rows) => Array.isArray(rows) ? rows : []);
}

export function buildPortfolioBacktestMarkers(points = []) {
  const out = [];
  let prevPositions = null;

  for (const [index, point] of (Array.isArray(points) ? points : []).entries()) {
    const time = _normalizeTime(point && (point.ts_ms ?? point.time ?? point.t), index);
    const tsMs = time * 1000;
    const detail = _asDetail(point);
    let explicitCount = 0;

    for (const marker of _explicitMarkerRows(detail)) {
      const markerTime = marker && (marker.time ?? marker.t ?? marker.ts ?? marker.ts_ms);
      const normalized = normalizeDecisionMarker({
        ...marker,
        time: markerTime || time,
        ts_ms: marker && marker.ts_ms ? marker.ts_ms : (markerTime ? undefined : tsMs),
        kind: marker && (marker.kind || marker.type) ? (marker.kind || marker.type) : "intended",
      });
      if (normalized) {
        out.push(normalized);
        explicitCount += 1;
      }
    }

    const tradeCosts = Array.isArray(detail.trade_costs) ? detail.trade_costs : [];
    for (const trade of tradeCosts) {
      const delta = _num(trade && trade.delta_weight, 0) || 0;
      const symbol = String(trade && trade.symbol || "").trim().toUpperCase();
      if (!symbol && Math.abs(delta) <= 1e-12) continue;
      const side = delta >= 0 ? "BUY" : "SELL";
      const normalized = normalizeDecisionMarker({
        time,
        ts_ms: tsMs,
        kind: "filled",
        side,
        qty: Math.abs(delta),
        text: `${side} ${symbol || "trade"}`.slice(0, 12),
        label: "Backtest transition trade",
        reason_code: String(trade && trade.status || "transition_trade"),
      });
      if (normalized) out.push(normalized);
    }

    const curPositions = _signedPositionsMap(detail.positions || []);
    const changed = _positionChangeCount(prevPositions, curPositions);
    if (changed > 0 && !tradeCosts.length && explicitCount === 0) {
      const normalized = normalizeDecisionMarker({
        time,
        ts_ms: tsMs,
        kind: "intended",
        side: "BUY",
        qty: changed,
        text: `DEC ${changed}`.slice(0, 12),
        label: "Backtest portfolio decision",
        reason_code: "positions_changed",
      });
      if (normalized) out.push(normalized);
    }
    prevPositions = curPositions;
  }

  return out.sort((a, b) => Number(a.time) - Number(b.time)).slice(-300);
}

export function buildPortfolioMetricAnnotations(run = {}, points = []) {
  const metrics = run && typeof run.metrics === "object" ? run.metrics : {};
  const sampleCount =
    _finiteMetric(metrics, ["sample_count", "samples", "n_returns", "steps", "steps_used"]) ??
    (Array.isArray(points) ? points.length : null);
  const turnover =
    _finiteMetric(metrics, ["turnover_avg", "avg_turnover", "turnover", "mean_turnover"]) ??
    _computeTurnoverFromPoints(points);

  return [
    {
      key: "sharpe",
      label: "Sharpe",
      value: _fmtRatio(_finiteMetric(metrics, ["sharpe_simple", "sharpe", "sharpe_ratio"])),
      rawValue: _finiteMetric(metrics, ["sharpe_simple", "sharpe", "sharpe_ratio"]),
      meta: "run risk-adjusted return",
    },
    {
      key: "sortino",
      label: "Sortino",
      value: _fmtRatio(_finiteMetric(metrics, ["sortino_simple", "sortino", "sortino_ratio"])),
      rawValue: _finiteMetric(metrics, ["sortino_simple", "sortino", "sortino_ratio"]),
      meta: "downside-adjusted return",
    },
    {
      key: "calmar",
      label: "Calmar",
      value: _fmtRatio(_finiteMetric(metrics, ["calmar_simple", "calmar", "calmar_ratio"])),
      rawValue: _finiteMetric(metrics, ["calmar_simple", "calmar", "calmar_ratio"]),
      meta: "return vs max drawdown",
    },
    {
      key: "turnover",
      label: "Turnover",
      value: _fmtTurnover(turnover),
      rawValue: turnover,
      meta: "average step turnover",
    },
    {
      key: "sample_count",
      label: "Samples",
      value: _fmtCount(sampleCount),
      rawValue: sampleCount,
      meta: "points in this run",
    },
  ];
}

export function buildPortfolioBacktestProViewModel(run = {}) {
  const points = Array.isArray(run && run.points) ? run.points : [];
  const equitySeries = [];
  const drawdownSeries = [];
  const finiteEquitySeries = [];
  const finiteDrawdownSeries = [];

  for (const [index, point] of points.entries()) {
    const time = _normalizeTime(point && (point.ts_ms ?? point.time ?? point.t), index);
    const equity = _num(point && point.equity);
    if (equity != null) {
      const row = { time, value: equity };
      equitySeries.push(row);
      finiteEquitySeries.push(row);
    } else {
      equitySeries.push({ time });
    }
    const drawdown = _normalizeDrawdownValue(point && point.drawdown);
    if (drawdown != null) {
      const row = { time, value: drawdown };
      drawdownSeries.push(row);
      finiteDrawdownSeries.push(row);
    } else {
      drawdownSeries.push({ time });
    }
  }

  equitySeries.sort((a, b) => Number(a.time) - Number(b.time));
  drawdownSeries.sort((a, b) => Number(a.time) - Number(b.time));
  finiteEquitySeries.sort((a, b) => Number(a.time) - Number(b.time));
  finiteDrawdownSeries.sort((a, b) => Number(a.time) - Number(b.time));
  const benchmark = _buildBenchmarkSeries(run || {}, finiteEquitySeries.length ? finiteEquitySeries[0].value : 1);
  const maxDrawdown = finiteDrawdownSeries.length ? Math.min(...finiteDrawdownSeries.map((point) => Number(point.value))) : null;
  const latestEquity = finiteEquitySeries.length ? finiteEquitySeries[finiteEquitySeries.length - 1].value : null;
  const latestDrawdown = finiteDrawdownSeries.length ? finiteDrawdownSeries[finiteDrawdownSeries.length - 1].value : null;
  const throttleGap = latestDrawdown == null ? null : latestDrawdown - PORTFOLIO_DRAWDOWN_THROTTLE;

  return {
    ok: finiteEquitySeries.length >= 2 && finiteDrawdownSeries.length >= 2,
    runId: run && run.id != null ? String(run.id) : "",
    equitySeries,
    drawdownSeries,
    finiteEquitySeries,
    finiteDrawdownSeries,
    benchmarkSeries: benchmark.series,
    benchmarkState: benchmark.state,
    markers: buildPortfolioBacktestMarkers(points),
    annotations: buildPortfolioMetricAnnotations(run, points),
    drawdownThrottle: PORTFOLIO_DRAWDOWN_THROTTLE,
    latestEquity,
    latestDrawdown,
    maxDrawdown,
    throttleGap,
    pointCount: points.length,
  };
}

export function resolvePortfolioBacktestRenderMode({ proEnabled = true, lightweightAvailable = true, hasRenderableSeries = true } = {}) {
  if (!proEnabled) return { mode: "canvas", reason: "feature_flag_disabled" };
  if (!hasRenderableSeries) return { mode: "canvas", reason: "insufficient_series" };
  if (!lightweightAvailable) return { mode: "canvas", reason: "lightweight_charts_unavailable" };
  return { mode: "pro", reason: "pro_renderer_enabled" };
}

function _portfolioProEnabled() {
  try {
    if (typeof window !== "undefined" && typeof window.__PORTFOLIO_BACKTEST_PRO_CHARTS__ === "boolean") {
      return !!window.__PORTFOLIO_BACKTEST_PRO_CHARTS__;
    }
    if (typeof localStorage !== "undefined") {
      const raw = localStorage.getItem(PORTFOLIO_PRO_FLAG_KEY);
      if (raw === "0" || String(raw).toLowerCase() === "false") return false;
      if (raw === "1" || String(raw).toLowerCase() === "true") return true;
    }
  } catch {}
  return true;
}

function _setChartDisplay({ pro }) {
  const cEq = document.getElementById("portfolioEquityCanvas");
  const cDd = document.getElementById("portfolioDdCanvas");
  const pEq = document.getElementById("portfolioEquityPro");
  const pDd = document.getElementById("portfolioDdPro");
  if (cEq) cEq.style.display = pro ? "none" : "block";
  if (cDd) cDd.style.display = pro ? "none" : "block";
  if (pEq) pEq.style.display = pro ? "block" : "none";
  if (pDd) pDd.style.display = pro ? "block" : "none";
}

function _destroyPortfolioProCharts() {
  const pEq = typeof document !== "undefined" ? document.getElementById("portfolioEquityPro") : null;
  const pDd = typeof document !== "undefined" ? document.getElementById("portfolioDdPro") : null;
  clearMarkerLayer(_PRO);
  disconnectResizeObserver(_PRO.equityResizeObserver, pEq);
  disconnectResizeObserver(_PRO.drawdownResizeObserver, pDd);
  _PRO.equityResizeObserver = null;
  _PRO.drawdownResizeObserver = null;
  try { if (_PRO.equityChart) _PRO.equityChart.remove(); } catch {}
  try { if (_PRO.drawdownChart) _PRO.drawdownChart.remove(); } catch {}
  _PRO.equityChart = null;
  _PRO.drawdownChart = null;
  _PRO.ddPriceLine = null;
}

function _buildChart(container, height = 220) {
  return createProChart(container, {
    includeInitialSize: true,
    initialSizeFallback: { width: 420, height },
    chartOptions: {
      rightPriceScale: {
        scaleMargins: { top: 0.12, bottom: 0.12 },
      },
    },
    resizeObserverOptions: {
      sizeFallback: { width: 420, height },
    },
  });
}

function _hoverSeriesMap(vm) {
  const out = new Map();
  const put = (time, key, value) => {
    const k = String(time);
    const row = out.get(k) || {};
    row[key] = value;
    out.set(k, row);
  };
  for (const point of vm.equitySeries || []) put(point.time, "equity", point.value);
  for (const point of vm.drawdownSeries || []) put(point.time, "drawdown", point.value);
  for (const point of vm.benchmarkSeries || []) put(point.time, "benchmark", point.value);
  return out;
}

function _installPortfolioHover(vm, refs) {
  const hoverEl = document.getElementById("portfolioBtHover");
  if (!hoverEl) return;
  const byTime = _hoverSeriesMap(vm);
  const benchmarkLabel = vm.benchmarkState && vm.benchmarkState.available ? vm.benchmarkState.label : "benchmark";

  const update = (param) => {
    if (!param || !param.time) {
      hoverEl.textContent = "hover portfolio charts";
      return;
    }
    const row = byTime.get(String(param.time)) || {};
    const equity = param.seriesData && refs.equitySeries ? param.seriesData.get(refs.equitySeries) : null;
    const drawdown = param.seriesData && refs.drawdownSeries ? param.seriesData.get(refs.drawdownSeries) : null;
    const benchmark = param.seriesData && refs.benchmarkSeries ? param.seriesData.get(refs.benchmarkSeries) : null;
    const equityValue = _num(equity && equity.value, _num(row.equity));
    const drawdownValue = _num(drawdown && drawdown.value, _num(row.drawdown));
    const benchmarkValue = _num(benchmark && benchmark.value, _num(row.benchmark));
    const gap = drawdownValue == null ? null : drawdownValue - PORTFOLIO_DRAWDOWN_THROTTLE;
    hoverEl.textContent = [
      `time ${_formatChartTime(param.time)}`,
      `equity ${equityValue == null ? "n/a" : equityValue.toFixed(4)}`,
      `${benchmarkLabel} ${benchmarkValue == null ? "n/a" : benchmarkValue.toFixed(4)}`,
      `drawdown ${drawdownValue == null ? "n/a" : _fmtPct(drawdownValue)}`,
      `throttle gap ${gap == null ? "n/a" : _fmtPct(gap)}`,
    ].join(" | ");
  };

  try { refs.equityChart.subscribeCrosshairMove(update); } catch {}
  try { refs.drawdownChart.subscribeCrosshairMove(update); } catch {}
}

function _renderPortfolioMetricAnnotations(vm = null) {
  const el = document.getElementById("portfolioBtAnnotations");
  if (!el) return;
  if (!vm || !Array.isArray(vm.annotations) || !vm.annotations.length) {
    el.innerHTML = '<span class="pill dim">run metrics unavailable</span>';
    return;
  }
  el.innerHTML = vm.annotations.map((item) => `
    <span class="portfolioBtAnnotation" data-metric="${_escapeHtml(item.key || "")}">
      <span class="portfolioBtAnnotationLabel">${_escapeHtml(item.label || "")}</span>
      <span class="portfolioBtAnnotationValue mono">${_escapeHtml(item.value || "n/a")}</span>
      <span class="portfolioBtAnnotationMeta">${_escapeHtml(item.meta || "")}</span>
    </span>
  `).join("");
}

function _renderBenchmarkState(vm = null) {
  const el = document.getElementById("portfolioBtBenchmarkState");
  if (!el) return;
  const state = vm && vm.benchmarkState ? vm.benchmarkState : {
    available: false,
    text: "Benchmark unavailable: endpoint returned no benchmark series.",
  };
  el.textContent = state.text;
  el.className = state.available ? "pill ok" : "pill warn";
}

async function _renderPortfolioProCharts(vm) {
  const pEq = document.getElementById("portfolioEquityPro");
  const pDd = document.getElementById("portfolioDdPro");
  if (!pEq || !pDd) return false;

  const mode = resolvePortfolioBacktestRenderMode({
    proEnabled: _portfolioProEnabled(),
    lightweightAvailable: true,
    hasRenderableSeries: !!(vm && vm.ok),
  });
  if (mode.mode !== "pro") return false;

  _destroyPortfolioProCharts();
  _setChartDisplay({ pro: true });
  pEq.innerHTML = "";
  pDd.innerHTML = "";

  await ensureLightweightCharts();
  const eqBuilt = _buildChart(pEq, 220);
  const ddBuilt = _buildChart(pDd, 220);
  _PRO.equityChart = eqBuilt.chart;
  _PRO.drawdownChart = ddBuilt.chart;
  _PRO.equityResizeObserver = eqBuilt.resizeObserver;
  _PRO.drawdownResizeObserver = ddBuilt.resizeObserver;

  const equitySeries = addSeriesCompat(_PRO.equityChart, "line", {
    color: "#56B4E9",
    lineWidth: 2,
    priceLineVisible: false,
    title: "Portfolio equity",
  });
  equitySeries.setData(vm.equitySeries || []);

  let benchmarkSeries = null;
  if (Array.isArray(vm.benchmarkSeries) && vm.benchmarkSeries.length >= 2) {
    benchmarkSeries = addSeriesCompat(_PRO.equityChart, "line", {
      color: "#E69F00",
      lineWidth: 2,
      lineStyle: 2,
      priceLineVisible: false,
      title: vm.benchmarkState.label || "Benchmark",
    });
    benchmarkSeries.setData(vm.benchmarkSeries);
  }

  const drawdownSeries = addSeriesCompat(_PRO.drawdownChart, "area", {
    lineColor: "#D55E00",
    topColor: "rgba(213,94,0,0.10)",
    bottomColor: "rgba(213,94,0,0.36)",
    lineWidth: 2,
    priceLineVisible: false,
    priceFormat: {
      type: "custom",
      formatter: (price) => _fmtPct(Number(price)),
    },
  });
  drawdownSeries.setData(vm.drawdownSeries || []);
  if (typeof drawdownSeries.createPriceLine === "function") {
    _PRO.ddPriceLine = drawdownSeries.createPriceLine({
      price: PORTFOLIO_DRAWDOWN_THROTTLE,
      color: "#CC79A7",
      lineWidth: 2,
      lineStyle: 2,
      axisLabelVisible: true,
      title: "6% throttle",
    });
  }

  if (Array.isArray(vm.markers) && vm.markers.length) {
    applyMarkersToState(_PRO, vm.markers, { anchor: equitySeries });
  }

  try { _PRO.equityChart.timeScale().fitContent(); } catch {}
  try { _PRO.drawdownChart.timeScale().fitContent(); } catch {}
  _installPortfolioHover(vm, {
    equityChart: _PRO.equityChart,
    drawdownChart: _PRO.drawdownChart,
    equitySeries,
    drawdownSeries,
    benchmarkSeries,
  });

  renderChartAccessibility(pEq, {
    title: "Portfolio equity curve",
    series: vm.equitySeries || [],
    valueLabel: "equity",
    valueFormatter: (v) => Number(v).toFixed(4),
    containerId: "portfolioEquityCanvasA11y",
    chartType: "lightweight-chart",
    summary: `Portfolio equity curve: latest equity ${vm.latestEquity == null ? "n/a" : Number(vm.latestEquity).toFixed(4)} across ${vm.equitySeries.length} points. ${vm.benchmarkState.text}`,
  });
  renderChartAccessibility(pDd, {
    title: "Portfolio drawdown",
    series: vm.drawdownSeries || [],
    valueLabel: "drawdown",
    valueFormatter: (v) => _fmtPct(Number(v)),
    containerId: "portfolioDdCanvasA11y",
    chartType: "lightweight-chart",
    summary: `Portfolio drawdown: latest ${vm.latestDrawdown == null ? "n/a" : _fmtPct(vm.latestDrawdown)}; 6% throttle reference shown at ${_fmtPct(PORTFOLIO_DRAWDOWN_THROTTLE)}.`,
  });
  return true;
}

function _renderPortfolioFallbackCharts(cEq, cDd, vm, { equityEmpty = "", drawdownEmpty = "" } = {}) {
  _destroyPortfolioProCharts();
  _setChartDisplay({ pro: false });
  renderLineChart(cEq, (vm && vm.equitySeries ? vm.equitySeries.map((p) => p.value) : []), {
    xValues: vm && vm.equitySeries ? vm.equitySeries.map((p) => p.time) : [],
    fmtX: (value) => _formatChartTime(value),
    topLabel: "equity",
    a11yTitle: "Portfolio equity curve",
    a11ySeries: vm ? vm.equitySeries : [],
    a11yTimeKey: "time",
    valueLabel: "equity",
    a11yValueFormatter: (v) => Number(v).toFixed(2),
    fmtY: (v) => Number(v).toFixed(3),
    stroke: "#56B4E9",
    emptyMessage: equityEmpty || "No portfolio backtest runs are available.",
  });
  renderLineChart(cDd, (vm && vm.drawdownSeries ? vm.drawdownSeries.map((p) => p.value) : []), {
    xValues: vm && vm.drawdownSeries ? vm.drawdownSeries.map((p) => p.time) : [],
    fmtX: (value) => _formatChartTime(value),
    topLabel: "drawdown",
    bottomLabel: "6% throttle",
    a11yTitle: "Portfolio drawdown",
    a11ySeries: vm ? vm.drawdownSeries : [],
    a11yTimeKey: "time",
    valueLabel: "drawdown",
    a11yValueFormatter: (v) => _fmtPct(Number(v)),
    fmtY: (v) => _fmtPct(v),
    stroke: "#D55E00",
    yMax: 0,
    yMin: Math.min(PORTFOLIO_DRAWDOWN_THROTTLE, vm && vm.maxDrawdown != null ? vm.maxDrawdown : PORTFOLIO_DRAWDOWN_THROTTLE),
    emptyMessage: drawdownEmpty || "No portfolio backtest runs are available.",
  });
}

export async function loadPortfolioBacktestLatest(fetchJSON) {

  const meta = document.getElementById("portfolioBtMeta");
  const sumBody = document.getElementById("portfolioBtSummaryBody");
  const cEq = document.getElementById("portfolioEquityCanvas");
  const cDd = document.getElementById("portfolioDdCanvas");

  if (!meta || !sumBody || !cEq || !cDd) return;

  try {

    const res = await fetchJSON("/api/portfolio/backtest/latest");

    if (!res || !res.ok || !res.run) {
      _lastPortfolioBacktestSummary = null;

      meta.textContent = "no runs";
      meta.className = "pill dim";

      sumBody.innerHTML = "";
      _renderPortfolioMetricAnnotations(null);
      _renderBenchmarkState(null);
      _renderPortfolioFallbackCharts(cEq, cDd, null, {
        equityEmpty: "No portfolio backtest runs are available.",
        drawdownEmpty: "No portfolio backtest runs are available.",
      });

      return;
    }

    meta.textContent = "ok";
    meta.className = "pill ok";

    const run = res.run || {};
    const metrics = run.metrics || {};
    const pts = Array.isArray(run.points) ? run.points : [];
    const viewModel = buildPortfolioBacktestProViewModel(run);
    const equitySeries = viewModel.finiteEquitySeries;
    const equity = equitySeries.map((p) => p.value);
    const drawdownSeries = viewModel.finiteDrawdownSeries;
    const maxDd = viewModel.maxDrawdown == null ? 0 : viewModel.maxDrawdown;
    _renderPortfolioMetricAnnotations(viewModel);
    _renderBenchmarkState(viewModel);

    let proRendered = false;
    try {
      proRendered = await _renderPortfolioProCharts(viewModel);
    } catch (e) {
      console.warn("portfolio pro charts failed; falling back to canvas", e);
      proRendered = false;
    }
    if (!proRendered) {
      _renderPortfolioFallbackCharts(cEq, cDd, viewModel);
    }

    const startTs = Number(run.start_ts_ms);
    const endTs = Number(run.end_ts_ms);

    const windowStr =
      (Number.isFinite(startTs) && Number.isFinite(endTs))
        ? `${new Date(startTs).toLocaleDateString()} → ${new Date(endTs).toLocaleDateString()}`
        : "—";

    const totalReturn =
      (equity.length >= 2)
        ? ((equity[equity.length - 1] / equity[0]) - 1.0)
        : (Number.isFinite(metrics.total_return) ? Number(metrics.total_return) : NaN);

    const sharpe = Number.isFinite(metrics.sharpe_simple) ? Number(metrics.sharpe_simple) : NaN;
    const sortino = Number.isFinite(metrics.sortino_simple) ? Number(metrics.sortino_simple) : NaN;
    const calmar = Number.isFinite(metrics.calmar_simple) ? Number(metrics.calmar_simple) : NaN;
    const turnover = Number.isFinite(metrics.turnover_avg) ? Number(metrics.turnover_avg) : NaN;

    const trades = _finiteMetric(metrics, ["steps_used", "steps", "n_returns"]) ?? (pts.length || NaN);

    _lastPortfolioBacktestSummary = {
      totalReturn: Number.isFinite(totalReturn) ? totalReturn : null,
      maxDrawdown: Number.isFinite(maxDd) ? maxDd : null,
      sharpe: Number.isFinite(sharpe) ? sharpe : null,
      sortino: Number.isFinite(sortino) ? sortino : null,
      calmar: Number.isFinite(calmar) ? calmar : null,
      latestEquity: equity.length ? equity[equity.length - 1] : null,
    };

    sumBody.innerHTML = "";

    sumBody.insertAdjacentHTML("beforeend", `
      <tr>
        <td class="small">${windowStr}</td>
        <td class="mono">${Number.isFinite(totalReturn) ? _fmtPct(totalReturn) : "?"}</td>
        <td class="mono">${_fmtPct(maxDd)}</td>
        <td class="mono">
          S=${Number.isFinite(sharpe) ? sharpe.toFixed(2) : "?"}
          &nbsp;So=${Number.isFinite(sortino) ? sortino.toFixed(2) : "?"}
          &nbsp;C=${Number.isFinite(calmar) ? calmar.toFixed(2) : "?"}
        </td>
        <td class="mono">
          n=${Number.isFinite(trades) ? trades : "?"}
          &nbsp;τ=${Number.isFinite(turnover) ? turnover.toFixed(3) : "?"}
        </td>
      </tr>
    `);

  } catch (e) {
    _lastPortfolioBacktestSummary = null;

    meta.textContent = "error";
    meta.className = "pill bad";

    sumBody.innerHTML = "";
    _renderPortfolioMetricAnnotations(null);
    _renderBenchmarkState(null);
    _renderPortfolioFallbackCharts(cEq, cDd, null, {
      equityEmpty: "Portfolio backtest equity failed to load.",
      drawdownEmpty: "Portfolio backtest drawdown failed to load.",
    });
  }
}
