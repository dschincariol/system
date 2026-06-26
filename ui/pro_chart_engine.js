/*
  FILE: ui/pro_chart_engine.js

  Advanced chart-engine helpers for the dashboard. This module owns the richer
  chart overlays, persisted preferences, and dashboard-side chart state used by
  the browser UI.
*/

import { apiEventSource, apiFetch } from "./api_client.js";
import { renderChartAccessibility } from "./chart_a11y.js";
import {
  addSeriesCompat,
  applyIndicatorSeries,
  applyMarkersToState,
  applyPriceLinesToState,
  applyWindowBandsToState,
  clearMarkerLayer,
  clearPriceLinesForState,
  clearRetryTimer,
  clearWindowBandsForState,
  closeEventSource,
  createIndicatorState,
  createProChart,
  disconnectResizeObserver,
  ensureIndicatorStateForRows,
  ensureLightweightCharts,
  installProChartHealthTicker,
  installVisibilityReconnect,
  normalizeCandle,
  proChartVolumeColor,
  removeVisibilityHandler,
  scheduleStreamReconnect,
  updateIndicatorSeriesTail,
  updateIndicatorState,
  upsertSeriesPoint
} from "./pro_chart_core.js";
import {
  applyDashboardProChartOverlayInputs,
  getDashboardProChartOverlayState,
  getProChartsState,
  setDashboardProChartOverlayState
} from "./pro_chart_prefs.js";
import {
  buildIndicatorAccessibilitySummary,
  buildOverlayAccessibilitySummary,
  decisionOverlayLegendItems,
  decisionWindowLegendItems,
  indicatorOverlayLegendItems,
  normalizeDecisionOverlayPayload,
  VWAP_OVERLAY_LABEL,
} from "./decision_overlays.js";
import {
  getSelectedSymbolContext,
  normalizeSelectedSymbol
} from "./symbol_context.mjs";

const _DASH = {
  key: "",
  container: null,
  chart: null,
  resizeObserver: null,

  candleSeries: null,
  lineSeries: null,
  volumeSeries: null,
  vwapSeries: null,
  ema20Series: null,
  ema50Series: null,
  pnlSeries: null,
  markerLayer: null,
  markerSeries: null,
  priceLineHandles: [],
  windowBandLayer: null,
  windowBandSeries: null,
  decisionOverlayPayload: null,

  es: null,
  retryTimer: null,
  retryBackoffMs: 500,
  overlayTimer: null,
  healthTimer: null,
  lastUpdateMs: 0,
  lastBar: null,
  streamConnected: false,
  candlesData: [],
  volumeData: [],
  pnlData: [],
  indicatorState: createIndicatorState([]),

  crosshairEl: null,
  healthEl: null,
  visHandler: null,
  visKey: "",
};

function _destroyChartState() {
  clearRetryTimer(_DASH);

  try { if (_DASH.overlayTimer) clearInterval(_DASH.overlayTimer); } catch {}
  _DASH.overlayTimer = null;

  try { if (_DASH.healthTimer) clearInterval(_DASH.healthTimer); } catch {}
  _DASH.healthTimer = null;

  closeEventSource(_DASH);
  removeVisibilityHandler(_DASH);

  disconnectResizeObserver(_DASH.resizeObserver, _DASH.container);
  _DASH.resizeObserver = null;

  clearMarkerLayer(_DASH);
  clearPriceLinesForState(_DASH);
  clearWindowBandsForState(_DASH);
  _DASH.decisionOverlayPayload = null;

  try { if (_DASH.chart) _DASH.chart.remove(); } catch {}
  _DASH.chart = null;

  _DASH.candleSeries = null;
  _DASH.lineSeries = null;
  _DASH.volumeSeries = null;
  _DASH.vwapSeries = null;
  _DASH.ema20Series = null;
  _DASH.ema50Series = null;
  _DASH.pnlSeries = null;
  _DASH.lastUpdateMs = 0;
  _DASH.lastBar = null;
  _DASH.streamConnected = false;
  _DASH.candlesData = [];
  _DASH.volumeData = [];
  _DASH.pnlData = [];
  _DASH.indicatorState = createIndicatorState([]);
}

function _volColor(c) {
  return proChartVolumeColor(c, 0.45);
}

function _setMeta(text, cls = "pill dim") {
  const el = document.getElementById("proChartsMeta");
  if (!el) return;
  el.textContent = text;
  el.className = cls;
}

function _setHealthText(text) {
  const el = _DASH.healthEl || document.getElementById("proChartsHealth");
  if (!el) return;
  el.textContent = text;
  try {
    const notify = typeof window !== "undefined" ? window.__updateDashboardProChartState__ : null;
    if (typeof notify === "function") notify();
  } catch {}
}

export function getDashboardChartRuntime() {
  const healthText = String((_DASH.healthEl || document.getElementById("proChartsHealth"))?.textContent || "").trim();
  const hardError = /failed|error|unavailable/i.test(healthText) ? healthText : "";
  return {
    lastUpdateMs: Number(_DASH.lastUpdateMs || 0),
    hasHistory: Array.isArray(_DASH.candlesData) && _DASH.candlesData.length > 0,
    healthText,
    error: hardError,
  };
}

function _installHealthTicker() {
  _DASH.healthEl = document.getElementById("proChartsHealth");
  installProChartHealthTicker(_DASH, _DASH.healthEl, {
    timerKey: "healthTimer",
    setText: (text) => _setHealthText(text),
  });
}

function _buildChart(container) {
  const { chart, resizeObserver } = createProChart(container, {
    includeInitialSize: true,
    chartOptions: {
      rightPriceScale: {
        borderColor: "#30363d",
        scaleMargins: { top: 0.08, bottom: 0.28 },
      },
    },
  });
  _DASH.resizeObserver = resizeObserver;
  return chart;
}

function _clearTradeMarkers() {
  clearMarkerLayer(_DASH);
}

function _markerAnchorSeries() {
  return _DASH.candleSeries || _DASH.lineSeries || null;
}

function _clearPriceLines() {
  clearPriceLinesForState(_DASH, { anchor: _markerAnchorSeries() });
}

function _clearWindowBands() {
  clearWindowBandsForState(_DASH);
}

function _applyWindowBands(payload = _DASH.decisionOverlayPayload, candles = _DASH.candlesData) {
  const normalized = normalizeDecisionOverlayPayload(payload || {});
  return applyWindowBandsToState(_DASH, normalized.windows || [], {
    anchor: _markerAnchorSeries(),
    rows: candles || _DASH.candlesData,
  });
}

function _setCrosshairPanel() {
  const xhairEl = document.getElementById("proChartsCrosshair");
  _DASH.crosshairEl = xhairEl || null;
  if (!_DASH.chart || !xhairEl) return;

  _DASH.chart.subscribeCrosshairMove((param) => {
    try {
      if (!param || !param.time) {
        xhairEl.innerHTML = "<span class=\"mono\">hover chart</span>";
        return;
      }

      const time = String(param.time);
      let pricePoint = null;
      if (_DASH.candleSeries) pricePoint = param.seriesData.get(_DASH.candleSeries);
      else if (_DASH.lineSeries) pricePoint = param.seriesData.get(_DASH.lineSeries);

      const volumePoint = _DASH.volumeSeries ? param.seriesData.get(_DASH.volumeSeries) : null;
      const pnlPoint = _DASH.pnlSeries ? param.seriesData.get(_DASH.pnlSeries) : null;

      const open = pricePoint && pricePoint.open !== undefined ? Number(pricePoint.open) : null;
      const high = pricePoint && pricePoint.high !== undefined ? Number(pricePoint.high) : null;
      const low = pricePoint && pricePoint.low !== undefined ? Number(pricePoint.low) : null;
      const close = pricePoint && pricePoint.close !== undefined
        ? Number(pricePoint.close)
        : (pricePoint && pricePoint.value !== undefined ? Number(pricePoint.value) : null);
      const volume = volumePoint && volumePoint.value !== undefined ? Number(volumePoint.value) : null;
      const pnl = pnlPoint && pnlPoint.value !== undefined ? Number(pnlPoint.value) : null;

      xhairEl.innerHTML =
        `<div class="mono"><b>T</b> ${time}</div>` +
        `<div class="mono"><b>O</b> ${Number.isFinite(open) ? open.toFixed(4) : "—"}  <b>H</b> ${Number.isFinite(high) ? high.toFixed(4) : "—"}</div>` +
        `<div class="mono"><b>L</b> ${Number.isFinite(low) ? low.toFixed(4) : "—"}  <b>C</b> ${Number.isFinite(close) ? close.toFixed(4) : "—"}</div>` +
        `<div class="mono"><b>V</b> ${Number.isFinite(volume) ? volume.toFixed(0) : "—"}  <b>PnL</b> ${Number.isFinite(pnl) ? pnl.toFixed(2) : "—"}</div>`;
    } catch {
      xhairEl.innerHTML = "<span class=\"mono\">hover chart</span>";
    }
  });
}

function _ensureOverlaySeries() {
  if (!_DASH.chart) return;

  if (!_DASH.volumeSeries && _DASH.candleSeries) {
    _DASH.volumeSeries = addSeriesCompat(_DASH.chart, "histogram", {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      scaleMargins: { top: 0.78, bottom: 0.0 },
    });
  }

  if (!_DASH.vwapSeries) {
    _DASH.vwapSeries = addSeriesCompat(_DASH.chart, "line", { lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
  }

  if (!_DASH.ema20Series) {
    _DASH.ema20Series = addSeriesCompat(_DASH.chart, "line", { lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
  }

  if (!_DASH.ema50Series) {
    _DASH.ema50Series = addSeriesCompat(_DASH.chart, "line", { lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
  }

  if (!_DASH.pnlSeries) {
    _DASH.pnlSeries = addSeriesCompat(_DASH.chart, "line", {
      lineWidth: 2,
      priceScaleId: "pnl",
      lastValueVisible: true,
      priceLineVisible: false,
    });
    try {
      _DASH.chart.priceScale("pnl").applyOptions({
        borderColor: "#30363d",
        scaleMargins: { top: 0.08, bottom: 0.28 },
      });
    } catch {}
  }
}

function _normalizePriceResponse(rows) {
  const candles = [];
  const volume = [];

  for (const row of rows || []) {
    const c = normalizeCandle(row);
    if (!c) continue;

    candles.push({
      time: c.time,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
      volume: c.volume,
    });

    volume.push({
      time: c.time,
      value: c.volume,
      color: _volColor(c),
    });
  }

  return { candles, volume };
}

function _dashboardChartKeyParts() {
  const parts = String(_DASH.key || "").split("::");
  return {
    symbol: parts[0] || "SPY",
    tf: parts[1] || "",
    type: parts[2] || "",
  };
}

function _fmtFixed(value, digits = 4) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : "unavailable";
}

function _seriesValueMap(rows) {
  return new Map(
    (Array.isArray(rows) ? rows : [])
      .filter((row) => Number.isFinite(Number(row && row.time)) && Number.isFinite(Number(row && row.value)))
      .map((row) => [Number(row.time), Number(row.value)])
  );
}

function _latestFieldText(rows, field) {
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    const value = Number(rows[index] && rows[index][field.key]);
    if (Number.isFinite(value)) {
      const formatter = field.formatter || ((v) => _fmtFixed(v, 4));
      return `${field.label} ${formatter(value)}`;
    }
  }
  return "";
}

function _renderDashboardChartA11y({ errorMessage = "", emptyMessage = "" } = {}) {
  const container = _DASH.container || document.getElementById("liveMarketChart");
  if (!container) return;
  const { symbol, tf, type } = _dashboardChartKeyParts();
  const title = `Live market chart ${symbol}`;
  const overlayState = getDashboardProChartOverlayState();
  const vwapByTime = _seriesValueMap(_DASH.indicatorState && _DASH.indicatorState.vwap);
  const ema20ByTime = _seriesValueMap(_DASH.indicatorState && _DASH.indicatorState.ema20);
  const ema50ByTime = _seriesValueMap(_DASH.indicatorState && _DASH.indicatorState.ema50);
  const pnlByTime = _seriesValueMap(_DASH.pnlData);
  const series = (_DASH.candlesData || []).map((c) => ({
    time: Number(c.time),
    value: Number(c.close),
    open: Number(c.open),
    high: Number(c.high),
    low: Number(c.low),
    close: Number(c.close),
    volume: Number(c.volume || 0),
    vwap: vwapByTime.get(Number(c.time)),
    ema20: ema20ByTime.get(Number(c.time)),
    ema50: ema50ByTime.get(Number(c.time)),
    pnl: pnlByTime.get(Number(c.time)),
  })).filter((c) => Number.isFinite(c.time) && Number.isFinite(c.value));
  const indicatorSummary = buildIndicatorAccessibilitySummary(overlayState);
  const overlaySummary = buildOverlayAccessibilitySummary(_DASH.decisionOverlayPayload || {});
  const seriesFields = [
    { key: "close", label: "Close", formatter: (v) => _fmtFixed(v, 4) },
    ...(_DASH.volumeSeries ? [{ key: "volume", label: "Volume", formatter: (v) => _fmtFixed(v, 0) }] : []),
    ...(overlayState.vwap ? [{ key: "vwap", label: VWAP_OVERLAY_LABEL, formatter: (v) => _fmtFixed(v, 4) }] : []),
    ...(overlayState.ema ? [
      { key: "ema20", label: "EMA20", formatter: (v) => _fmtFixed(v, 4) },
      { key: "ema50", label: "EMA50", formatter: (v) => _fmtFixed(v, 4) },
    ] : []),
    ...(overlayState.pnl ? [{ key: "pnl", label: "PnL", formatter: (v) => _fmtFixed(v, 2) }] : []),
  ];
  const latestVisible = seriesFields.map((field) => _latestFieldText(series, field)).filter(Boolean).join(", ");
  const columns = [
    { label: "Time", value: (row) => row.timeText || row.index },
    { label: "Open", value: (row) => _fmtFixed(row.raw && row.raw.open, 4) },
    { label: "High", value: (row) => _fmtFixed(row.raw && row.raw.high, 4) },
    { label: "Low", value: (row) => _fmtFixed(row.raw && row.raw.low, 4) },
    { label: "Close", value: (row) => _fmtFixed(row.raw && row.raw.close, 4) },
    ...(_DASH.volumeSeries ? [{ label: "Volume", value: (row) => _fmtFixed(row.raw && row.raw.volume, 0) }] : []),
    ...(overlayState.vwap ? [{ label: VWAP_OVERLAY_LABEL, value: (row) => _fmtFixed(row.raw && row.raw.vwap, 4) }] : []),
    ...(overlayState.ema ? [
      { label: "EMA20", value: (row) => _fmtFixed(row.raw && row.raw.ema20, 4) },
      { label: "EMA50", value: (row) => _fmtFixed(row.raw && row.raw.ema50, 4) },
    ] : []),
    ...(overlayState.pnl ? [{ label: "PnL", value: (row) => _fmtFixed(row.raw && row.raw.pnl, 2) }] : []),
  ];

  renderChartAccessibility(container, {
    title,
    series,
    seriesFields,
    timeKey: "time",
    valueKey: "value",
    valueLabel: "close",
    valueFormatter: (v) => Number(v).toFixed(4),
    emptyMessage: emptyMessage || `No candle history is available for ${symbol}${tf ? ` ${tf}` : ""}.`,
    errorMessage,
    chartType: "lightweight-chart",
    columns,
    summary: series.length
      ? `${title}: latest ${latestVisible || `close ${Number(series[series.length - 1].close).toFixed(4)}`} across ${series.length} candles${tf ? ` on ${tf}` : ""}${type ? ` as ${type}` : ""}. ${indicatorSummary} ${overlaySummary}`
      : "",
  });
}

async function _fetchCandles(symbol, tf, limit = 1200) {
  try {
    const res = await apiFetch(`/api/market/candles?symbol=${encodeURIComponent(symbol)}&tf=${encodeURIComponent(tf)}&limit=${encodeURIComponent(limit)}&max_points=${encodeURIComponent(limit)}`, { cache: "no-store" });
    const text = await res.text();
    let json = null;

    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      return { candles: [], volume: [], error: `invalid candle response (${res.status})` };
    }

    if (!res.ok) {
      return { candles: [], volume: [], error: (json && json.error) ? String(json.error) : `candle http ${res.status}` };
    }

    if (!json || !json.ok) {
      return { candles: [], volume: [], error: (json && json.error) ? String(json.error) : "candle api error" };
    }

    if (!Array.isArray(json.candles)) {
      return { candles: [], volume: [], error: "candle shape invalid" };
    }

    return { ..._normalizePriceResponse(json.candles), error: null };
  } catch (e) {
    return { candles: [], volume: [], error: e && e.message ? e.message : "candle fetch failed" };
  }
}

async function _fetchTrades(symbol) {
  try {
    const res = await apiFetch(`/api/terminal/decision_overlays?symbol=${encodeURIComponent(symbol)}`, { cache: "no-store" });
    const text = await res.text();
    let json = null;

    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      return { markers: [], overlay: null, error: `invalid marker response (${res.status})` };
    }

    if (!res.ok) {
      return { markers: [], overlay: null, error: (json && json.error) ? String(json.error) : `marker http ${res.status}` };
    }

    if (!json || !json.ok || !Array.isArray(json.markers)) {
      return { markers: [], overlay: null, error: (json && json.error) ? String(json.error) : "marker api error" };
    }

    return { markers: json.markers, overlay: json, error: null };
  } catch (e) {
    try {
      const res = await apiFetch(`/api/terminal/markers?symbol=${encodeURIComponent(symbol)}`, { cache: "no-store" });
      const text = await res.text();
      const json = text ? JSON.parse(text) : null;
      if (res.ok && json && json.ok && Array.isArray(json.markers)) {
        return { markers: json.markers, overlay: json, error: null };
      }
    } catch {}
    return { markers: [], overlay: null, error: e && e.message ? e.message : "marker fetch failed" };
  }
}

async function _fetchPortfolioOverlay() {
  try {
    const eqRes = await apiFetch(`/api/terminal/equity?limit=3000`, { cache: "no-store" });
    const eqText = await eqRes.text();
    let eqJson = null;

    try {
      eqJson = eqText ? JSON.parse(eqText) : null;
    } catch {
      eqJson = null;
    }

    if (eqRes.ok && eqJson && eqJson.ok && Array.isArray(eqJson.series) && eqJson.series.length) {
      return {
        series: eqJson.series
          .map((p) => ({ time: Number(p.t), value: Number(p.v) }))
          .filter((p) => Number.isFinite(p.time) && Number.isFinite(p.value)),
        error: null,
      };
    }
  } catch {}

  try {
    const btRes = await apiFetch(`/api/portfolio/backtest/latest`, { cache: "no-store" });
    const btText = await btRes.text();
    let btJson = null;

    try {
      btJson = btText ? JSON.parse(btText) : null;
    } catch {
      return { series: [], error: `invalid backtest overlay response (${btRes.status})` };
    }

    const pts = btJson?.run?.points;
    if (btRes.ok && btJson && btJson.ok && Array.isArray(pts) && pts.length) {
      return {
        series: pts
          .map((p) => ({ time: Math.floor(Number(p.ts_ms || 0) / 1000), value: Number(p.equity) }))
          .filter((p) => Number.isFinite(p.time) && Number.isFinite(p.value)),
        error: null,
      };
    }

    return { series: [], error: (btJson && btJson.error) ? String(btJson.error) : "portfolio overlay unavailable" };
  } catch (e) {
    return { series: [], error: e && e.message ? e.message : "portfolio overlay fetch failed" };
  }
}

export async function renderPriceChart(containerId) {
  const container = document.getElementById(containerId);
  if (!container) return null;

  await ensureLightweightCharts();

  _DASH.container = container;
  _DASH.chart = _buildChart(container);
  _ensureOverlaySeries();

  const st = getProChartsState();
  const type = String(st.type || "candle").trim();

  if (type === "line") {
    _DASH.lineSeries = addSeriesCompat(_DASH.chart, "line", { lineWidth: 2 });
    _DASH.candleSeries = null;
  } else if (type === "area") {
    _DASH.lineSeries = addSeriesCompat(_DASH.chart, "area");
    _DASH.candleSeries = null;
  } else if (type === "bar") {
    _DASH.candleSeries = addSeriesCompat(_DASH.chart, "bar");
    _DASH.lineSeries = null;
  } else {
    _DASH.candleSeries = addSeriesCompat(_DASH.chart, "candlestick");
    _DASH.lineSeries = null;
  }

  if (_DASH.candleSeries && !_DASH.volumeSeries) {
    _DASH.volumeSeries = addSeriesCompat(_DASH.chart, "histogram", {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      scaleMargins: { top: 0.78, bottom: 0.0 },
    });
  }

  _setCrosshairPanel();
  _installHealthTicker();

  return _DASH.chart;
}

export function renderVolumeOverlay(chart, volumeData) {
  if (!chart || !_DASH.volumeSeries) return;
  _DASH.volumeData = (volumeData || [])
    .map((v) => ({
      time: Number(v.time),
      value: Number(v.value),
      color: v.color,
    }))
    .filter((v) => Number.isFinite(v.time) && Number.isFinite(v.value));
  try {
    _DASH.volumeSeries.setData(_DASH.volumeData);
  } catch {}
}

export function renderTradeMarkers(chart, trades) {
  if (!chart) return;
  applyMarkersToState(_DASH, trades || [], { anchor: _markerAnchorSeries() });
}

export function renderPortfolioOverlay(chart, pnlData) {
  if (!chart || !_DASH.pnlSeries) return;
  _DASH.pnlData = (pnlData || [])
    .map((p) => ({ time: Number(p.time), value: Number(p.value) }))
    .filter((p) => Number.isFinite(p.time) && Number.isFinite(p.value));
  try {
    _DASH.pnlSeries.setData(_DASH.pnlData);
  } catch {}
}

function _legendShapeGlyph(shape) {
  const raw = String(shape || "");
  if (raw === "line") return "---";
  if (raw === "arrowUp") return "^";
  if (raw === "arrowDown") return "v";
  if (raw === "square") return "[]";
  if (raw === "circle") return "o";
  if (raw === "band") return "band";
  return "-";
}

function _escapeAttr(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function _renderOverlayLegend(payload, overlays = getDashboardProChartOverlayState()) {
  const el = document.getElementById("proChartsOverlayLegend");
  if (!el) return;
  const normalized = normalizeDecisionOverlayPayload(payload || {});
  const indicatorSummary = buildIndicatorAccessibilitySummary(overlays || {});
  const decisionSummary = buildOverlayAccessibilitySummary(normalized);
  const summary = `${indicatorSummary} ${decisionSummary}`;
  const items = [
    ...indicatorOverlayLegendItems(overlays || {}),
    ...decisionOverlayLegendItems(normalized),
    ...decisionWindowLegendItems(normalized),
  ];
  el.setAttribute("aria-label", summary);
  el.innerHTML =
    `<div class="overlayLegendItems">${items.map((item) => (
      `<span class="overlayLegendItem">` +
      `<span class="overlayLegendGlyph" style="border-color:${_escapeAttr(item.color)}; color:${_escapeAttr(item.color)}; background:${_escapeAttr(item.fillColor || "transparent")}">${_escapeAttr(_legendShapeGlyph(item.shape))}</span>` +
      `<span>${_escapeAttr(item.label)}</span>` +
      `<span class="mono muted">${_escapeAttr(item.text)}</span>` +
      `<span class="mono">${_escapeAttr(String(item.count))}</span>` +
      `</span>`
    )).join("")}</div>` +
    `<div class="overlayLegendSummary">${_escapeAttr(summary)} ${normalized.windows.length ? `Windows ${normalized.windows.length}.` : ""} ${normalized.price_lines.length ? `Levels ${normalized.price_lines.length}.` : ""}</div>`;
}

function _applyPriceSeries(candles) {
  _DASH.candlesData = (candles || [])
    .map((c) => ({
      time: Number(c.time),
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close),
      volume: Number(c.volume || 0),
    }))
    .filter((c) => Number.isFinite(c.time) && Number.isFinite(c.close));
  _DASH.volumeData = _DASH.candlesData.map((c) => ({
    time: Number(c.time),
    value: Number(c.volume || 0),
    color: _volColor(c),
  }));
  _DASH.indicatorState = createIndicatorState(_DASH.candlesData);

  if (_DASH.candleSeries) {
    try {
      _DASH.candleSeries.setData(
        _DASH.candlesData.map((c) => ({
          time: Number(c.time),
          open: Number(c.open),
          high: Number(c.high),
          low: Number(c.low),
          close: Number(c.close),
        }))
      );
    } catch {}
  } else if (_DASH.lineSeries) {
    try {
      _DASH.lineSeries.setData(
        _DASH.candlesData.map((c) => ({
          time: Number(c.time),
          value: Number(c.close),
        }))
      );
    } catch {}
  }
  _renderDashboardChartA11y();
}

function _applyIndicatorOverlays(candles, overlays) {
  if (!_DASH.chart) return;
  _ensureOverlaySeries();
  const src = Array.isArray(candles) ? candles : [];
  _DASH.indicatorState = ensureIndicatorStateForRows(_DASH.indicatorState, src);
  applyIndicatorSeries({
    indicatorState: _DASH.indicatorState,
    overlays,
    vwapSeries: _DASH.vwapSeries,
    ema20Series: _DASH.ema20Series,
    ema50Series: _DASH.ema50Series,
  });
}

async function _refreshOverlays(symbol, candles = _DASH.candlesData) {
  const overlays = getDashboardProChartOverlayState();

  if (overlays.trades) {
    const tradesRes = await _fetchTrades(symbol);
    const normalizedOverlay = normalizeDecisionOverlayPayload(tradesRes.overlay || { markers: tradesRes.markers || [] });
    _DASH.decisionOverlayPayload = normalizedOverlay;
    renderTradeMarkers(_DASH.chart, normalizedOverlay.markers);
    applyPriceLinesToState(_DASH, normalizedOverlay.price_lines || [], { anchor: _markerAnchorSeries() });
    _applyWindowBands(normalizedOverlay, candles);
    _renderOverlayLegend(normalizedOverlay, overlays);
    if (tradesRes.error) {
      _setHealthText(`markers: ${tradesRes.error}`);
    }
  } else {
    try {
      const anchor = _markerAnchorSeries();
      if (anchor && typeof anchor.setMarkers === "function") {
        anchor.setMarkers([]);
      } else {
        _clearTradeMarkers();
      }
    } catch {}
    _clearPriceLines();
    _clearWindowBands();
    _DASH.decisionOverlayPayload = null;
    _renderOverlayLegend({ markers: [] }, overlays);
  }

  if (overlays.pnl) {
    const pnlRes = await _fetchPortfolioOverlay();
    renderPortfolioOverlay(_DASH.chart, pnlRes.series);
    if (pnlRes.error) {
      _setHealthText(`overlay: ${pnlRes.error}`);
    }
  } else if (_DASH.pnlSeries) {
    _DASH.pnlData = [];
    try { _DASH.pnlSeries.setData([]); } catch {}
  }

  _applyIndicatorOverlays(candles, overlays);
  _renderDashboardChartA11y();
}

function _updateBar(bar) {
  if (!bar) return;
  _DASH.lastBar = bar;
  _DASH.lastUpdateMs = Date.now();
  _DASH.candlesData = upsertSeriesPoint(_DASH.candlesData, {
    time: Number(bar.time),
    open: Number(bar.open),
    high: Number(bar.high),
    low: Number(bar.low),
    close: Number(bar.close),
    volume: Number(bar.volume || 0),
  });
  const indicatorUpdate = updateIndicatorState(_DASH.indicatorState, {
    time: Number(bar.time),
    close: Number(bar.close),
    volume: Number(bar.volume || 0),
  });
  _DASH.indicatorState = indicatorUpdate.needsRebuild ? createIndicatorState(_DASH.candlesData) : indicatorUpdate.state;
  _DASH.volumeData = upsertSeriesPoint(_DASH.volumeData, {
    time: Number(bar.time),
    value: Number(bar.volume || 0),
    color: _volColor(bar),
  });

  if (_DASH.candleSeries) {
    try {
      _DASH.candleSeries.update({
        time: Number(bar.time),
        open: Number(bar.open),
        high: Number(bar.high),
        low: Number(bar.low),
        close: Number(bar.close),
      });
    } catch {}
  } else if (_DASH.lineSeries) {
    try {
      _DASH.lineSeries.update({
        time: Number(bar.time),
        value: Number(bar.close),
      });
    } catch {}
  }

  if (_DASH.volumeSeries) {
    try {
      _DASH.volumeSeries.update({
        time: Number(bar.time),
        value: Number(bar.volume || 0),
        color: _volColor(bar),
      });
    } catch {}
  }

  const overlays = getDashboardProChartOverlayState();
  _ensureOverlaySeries();
  updateIndicatorSeriesTail({
    indicatorState: _DASH.indicatorState,
    overlays,
    vwapSeries: _DASH.vwapSeries,
    ema20Series: _DASH.ema20Series,
    ema50Series: _DASH.ema50Series,
  });
  if (_DASH.decisionOverlayPayload && overlays.trades) {
    _applyWindowBands(_DASH.decisionOverlayPayload, _DASH.candlesData);
  }
  _renderDashboardChartA11y();
}

function _clearRetry() {
  clearRetryTimer(_DASH);
}

function _closeES() {
  closeEventSource(_DASH);
}

function _scheduleReconnect(openFn, key) {
  scheduleStreamReconnect(_DASH, { key, open: openFn });
}

function _installVisibilityHandler(openFn, key) {
  installVisibilityReconnect(_DASH, {
    key,
    open: openFn,
    close: _closeES,
    clearRetry: _clearRetry,
  });
}

async function _startLiveStream(symbol, tf) {
  const key = _DASH.key;
  const url = `/api/market/stream?symbol=${encodeURIComponent(symbol)}&tf=${encodeURIComponent(tf)}`;

  function openES() {
    _closeES();
    _clearRetry();

    try {
      const es = apiEventSource(url);
      _DASH.es = es;
      _DASH.streamConnected = true;

      es.addEventListener("hello", () => {
        _DASH.retryBackoffMs = 500;
        _DASH.streamConnected = true;
        _setHealthText("live");
      });

      es.addEventListener("candle", (ev) => {
        let payload = null;
        try { payload = JSON.parse(ev.data); } catch { return; }
        const bar = normalizeCandle(payload);
        if (!bar) return;
        _DASH.streamConnected = true;
        _updateBar(bar);
      });

      es.addEventListener("error", () => {
        if (_DASH.key !== key) return;
        _closeES();
        _DASH.streamConnected = false;
        _setHealthText("reconnecting…");
        _scheduleReconnect(openES, key);
      });
    } catch {
      _DASH.streamConnected = false;
      _setHealthText("reconnecting…");
      _scheduleReconnect(openES, key);
    }
  }

  _installVisibilityHandler(openES, key);
  openES();
}

export async function loadProCharts(fetchJSON) {
  const card = document.getElementById("proChartsCard");
  const chartEl = document.getElementById("liveMarketChart");
  const symEl = document.getElementById("globalSymbol");

  applyDashboardProChartOverlayInputs();

  const st = getProChartsState();
  if (!st.enabled) {
    if (card) card.style.display = "none";
    _setMeta("disabled", "pill dim");
    _destroyChartState();
    return;
  }

  if (card) card.style.display = "block";
  if (!chartEl) return;

  const contextSymbol = getSelectedSymbolContext().symbol;
  const inputSymbol = normalizeSelectedSymbol(symEl && symEl.value ? symEl.value : "");
  const symbol = contextSymbol || inputSymbol || "SPY";
  if (contextSymbol && symEl && symEl.value !== contextSymbol) {
    symEl.value = contextSymbol;
  }
  const tf = String(st.tf || "1m").trim() || "1m";
  const type = String(st.type || "candle").trim() || "candle";
  const key = `${symbol}::${tf}::${type}`;

  if (_DASH.key === key && _DASH.chart) {
    _setMeta(`${symbol} • ${tf} • ${type}`, "pill ok");
    await _refreshOverlays(symbol, _DASH.candlesData);
    return;
  }

  _destroyChartState();
  _DASH.key = key;

  try {
    const chart = await renderPriceChart("liveMarketChart");
    if (!chart) return;

    const candleRes = await _fetchCandles(symbol, tf, 1200);
    const candles = Array.isArray(candleRes.candles) ? candleRes.candles : [];
    const volume = Array.isArray(candleRes.volume) ? candleRes.volume : [];

    _applyPriceSeries(candles);
    renderVolumeOverlay(chart, volume);

    if (candles.length) {
      _DASH.lastBar = candles[candles.length - 1];
      _DASH.lastUpdateMs = Date.now();
    }

    await _refreshOverlays(symbol, candles);

    _DASH.overlayTimer = setInterval(async () => {
      if (_DASH.key !== key) return;
      if (document.hidden) return;
      await _refreshOverlays(symbol, _DASH.candlesData);
    }, 15000);

    await _startLiveStream(symbol, tf);

    if (candleRes.error) {
      _setMeta(`${symbol} • ${tf} • ${type} • load failed`, "pill warn");
      _setHealthText(`candles: ${candleRes.error}`);
      _renderDashboardChartA11y({ errorMessage: `Candles failed to load: ${candleRes.error}` });
    } else if (!candles.length) {
      _setMeta(`${symbol} • ${tf} • ${type} • no candles`, "pill warn");
      _setHealthText("no data");
      _renderDashboardChartA11y({ emptyMessage: `No candles are available for ${symbol} ${tf}.` });
    } else {
      _setMeta(`${symbol} • ${tf} • ${type}`, "pill ok");
      _setHealthText(_DASH.lastUpdateMs ? "live" : "no data");
      _renderDashboardChartA11y();
    }
  } catch (e) {
    console.error("loadProCharts failed", e);
    _setMeta("error", "pill bad");
    _setHealthText(e && e.message ? e.message : "error");
    renderChartAccessibility(chartEl, {
      title: "Live market chart",
      series: [],
      emptyMessage: "Live market chart failed to load.",
      errorMessage: e && e.message ? e.message : "Live market chart failed to load.",
      valueLabel: "close",
      chartType: "lightweight-chart",
    });
  }
}

export function bindProChartSymbolWatcher() {
  const bindRefresh = (id, patchKey) => {
    const el = document.getElementById(id);
    if (!el || el._proChartBound) return;
    el._proChartBound = true;
    el.addEventListener("change", async () => {
      setDashboardProChartOverlayState({ [patchKey]: !!el.checked });
      if (typeof window._refreshProCharts === "function") {
        await window._refreshProCharts();
      }
    });
  };

  const symEl = document.getElementById("globalSymbol");
  if (symEl && !symEl._proChartSymBound) {
    symEl._proChartSymBound = true;
    symEl.addEventListener("change", async () => {
      if (typeof window._refreshProCharts === "function") {
        await window._refreshProCharts();
      }
    });
  }

  bindRefresh("proChartsOvVWAP", "vwap");
  bindRefresh("proChartsOvEMA", "ema");
  bindRefresh("proChartsOvTrades", "trades");
  bindRefresh("proChartsOvPnL", "pnl");

  applyDashboardProChartOverlayInputs();
}
