/*
  FILE: ui/pro_chart_engine.js

  Advanced chart-engine helpers for the dashboard. This module owns the richer
  chart overlays, persisted preferences, and dashboard-side chart state used by
  the browser UI.
*/

import { getProChartsState } from "./terminal/pro_charting.js";
import {
  getSelectedSymbolContext,
  normalizeSelectedSymbol
} from "./symbol_context.mjs";

const LS_OV_VWAP = "dashboard.proCharts.ov.vwap";
const LS_OV_EMA = "dashboard.proCharts.ov.ema";
const LS_OV_TRADES = "dashboard.proCharts.ov.trades";
const LS_OV_PNL = "dashboard.proCharts.ov.pnl";

const DEFAULTS = {
  libLocal: "/ui/vendor/lightweight-charts.standalone.production.js",
  libCdn: "https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js",
  overlays: {
    vwap: true,
    ema: true,
    trades: true,
    pnl: true,
  },
};

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

  crosshairEl: null,
  healthEl: null,
  visHandler: null,
  visKey: "",
};

function _lsBoolGet(key, fallback) {
  try {
    const v = localStorage.getItem(key);
    if (v === null || v === undefined) return !!fallback;
    return v === "1";
  } catch {
    return !!fallback;
  }
}

function _lsBoolSet(key, value) {
  try { localStorage.setItem(key, value ? "1" : "0"); } catch {}
}

function _getOverlayState() {
  return {
    vwap: _lsBoolGet(LS_OV_VWAP, DEFAULTS.overlays.vwap),
    ema: _lsBoolGet(LS_OV_EMA, DEFAULTS.overlays.ema),
    trades: _lsBoolGet(LS_OV_TRADES, DEFAULTS.overlays.trades),
    pnl: _lsBoolGet(LS_OV_PNL, DEFAULTS.overlays.pnl),
  };
}

function _setOverlayState(patch) {
  const next = { ..._getOverlayState(), ...(patch || {}) };
  _lsBoolSet(LS_OV_VWAP, !!next.vwap);
  _lsBoolSet(LS_OV_EMA, !!next.ema);
  _lsBoolSet(LS_OV_TRADES, !!next.trades);
  _lsBoolSet(LS_OV_PNL, !!next.pnl);
  return next;
}

function _setOverlayInputs() {
  const ov = _getOverlayState();
  const map = {
    proChartsOvVWAP: !!ov.vwap,
    proChartsOvEMA: !!ov.ema,
    proChartsOvTrades: !!ov.trades,
    proChartsOvPnL: !!ov.pnl,
  };

  for (const [id, checked] of Object.entries(map)) {
    const el = document.getElementById(id);
    if (el) el.checked = checked;
  }
}

async function _ensureLightweightCharts() {
  if (window.LightweightCharts) return window.LightweightCharts;

  async function load(src) {
    return await new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = src;
      s.async = true;
      s.onload = () => resolve(true);
      s.onerror = () => reject(new Error("failed to load " + src));
      document.head.appendChild(s);
    });
  }

  try { await load(DEFAULTS.libLocal); }
  catch { await load(DEFAULTS.libCdn); }

  if (!window.LightweightCharts) throw new Error("LightweightCharts not available");
  return window.LightweightCharts;
}

function _destroyChartState() {
  try { if (_DASH.retryTimer) clearTimeout(_DASH.retryTimer); } catch {}
  _DASH.retryTimer = null;

  try { if (_DASH.overlayTimer) clearInterval(_DASH.overlayTimer); } catch {}
  _DASH.overlayTimer = null;

  try { if (_DASH.healthTimer) clearInterval(_DASH.healthTimer); } catch {}
  _DASH.healthTimer = null;

  try { if (_DASH.es) _DASH.es.close(); } catch {}
  _DASH.es = null;

  try {
    if (_DASH.visHandler) {
      document.removeEventListener("visibilitychange", _DASH.visHandler);
    }
  } catch {}
  _DASH.visHandler = null;
  _DASH.visKey = "";

  try { if (_DASH.resizeObserver) _DASH.resizeObserver.disconnect(); } catch {}
  _DASH.resizeObserver = null;

  try {
    if (_DASH.markerLayer && typeof _DASH.markerLayer.detach === "function") {
      _DASH.markerLayer.detach();
    }
  } catch {}
  _DASH.markerLayer = null;
  _DASH.markerSeries = null;

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
}

function _safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function _normCandle(c) {
  const time = Number(c?.t ?? c?.time ?? 0);
  const open = _safeNum(c?.o ?? c?.open ?? c?.price ?? c?.last ?? c?.close);
  const high = _safeNum(c?.h ?? c?.high ?? c?.price ?? c?.last ?? c?.close);
  const low = _safeNum(c?.l ?? c?.low ?? c?.price ?? c?.last ?? c?.close);
  const close = _safeNum(c?.c ?? c?.close ?? c?.price ?? c?.last);
  const volume = _safeNum(c?.v ?? c?.volume ?? 0) ?? 0;

  if (!Number.isFinite(time) || open === null || high === null || low === null || close === null) {
    return null;
  }

  return {
    time: Number(time),
    open: Number(open),
    high: Number(high),
    low: Number(low),
    close: Number(close),
    volume: Number(volume),
  };
}

function _volColor(c) {
  if (!c) return "rgba(157,167,179,0.35)";
  return Number(c.close) >= Number(c.open)
    ? "rgba(46,160,67,0.45)"
    : "rgba(255,107,107,0.45)";
}

function _computeVWAP(candles) {
  let pv = 0;
  let vv = 0;
  const out = [];

  for (const c of candles || []) {
    const time = Number(c.time);
    const close = Number(c.close);
    const volume = Number(c.volume || 0);
    if (!Number.isFinite(time) || !Number.isFinite(close)) continue;

    if (Number.isFinite(volume) && volume > 0) {
      pv += close * volume;
      vv += volume;
    }

    out.push({
      time,
      value: vv > 0 ? (pv / vv) : close,
    });
  }

  return out;
}

function _computeEMA(candles, period) {
  const k = 2 / (Number(period) + 1);
  let ema = null;
  const out = [];

  for (const c of candles || []) {
    const time = Number(c.time);
    const close = Number(c.close);
    if (!Number.isFinite(time) || !Number.isFinite(close)) continue;

    if (ema === null) ema = close;
    else ema = (close * k) + (ema * (1 - k));

    out.push({ time, value: ema });
  }

  return out;
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

function _upsertSeriesPoint(rows, point, limit = 1500) {
  const next = Array.isArray(rows) ? rows.slice() : [];
  if (!point) return next;
  const time = Number(point.time);
  if (!Number.isFinite(time)) return next;

  const last = next[next.length - 1];
  if (last && Number(last.time) === time) {
    next[next.length - 1] = point;
  } else if (last && time < Number(last.time)) {
    const idx = next.findIndex((row) => Number(row.time) === time);
    if (idx >= 0) next[idx] = point;
  } else {
    next.push(point);
  }

  if (next.length > limit) {
    next.splice(0, next.length - limit);
  }

  return next;
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
  if (_DASH.healthTimer) {
    try { clearInterval(_DASH.healthTimer); } catch {}
  }

  _DASH.healthTimer = setInterval(() => {
    if (!_DASH.healthEl) return;
    if (!_DASH.lastUpdateMs) {
      _setHealthText(_DASH.streamConnected ? "live stream" : "no data");
      return;
    }

    const age = Date.now() - Number(_DASH.lastUpdateMs || 0);
    if (_DASH.streamConnected) {
      if (age < 1500) _setHealthText("live");
      else if (age < 60_000) _setHealthText(`live • last candle ${Math.floor(age / 1000)}s`);
      else _setHealthText(`stale ${Math.floor(age / 1000)}s`);
      return;
    }

    if (age < 1500) _setHealthText("live");
    else if (age < 5000) _setHealthText(`lag ${Math.floor(age)}ms`);
    else _setHealthText(`stale ${Math.floor(age / 1000)}s`);
  }, 750);
}

function _buildChart(container) {
  const { createChart } = window.LightweightCharts;
  container.innerHTML = "";

  const chart = createChart(container, {
    layout: {
      background: { color: "#0a0d12" },
      textColor: "#9da7b3",
    },
    grid: {
      vertLines: { color: "#1f2630" },
      horzLines: { color: "#1f2630" },
    },
    rightPriceScale: {
      borderColor: "#30363d",
      scaleMargins: { top: 0.08, bottom: 0.28 },
    },
    timeScale: {
      borderColor: "#30363d",
      timeVisible: true,
      secondsVisible: false,
      rightBarStaysOnScroll: true,
    },
    crosshair: { mode: 1 },
    width: Math.max(10, Math.floor(container.clientWidth || 1200)),
    height: Math.max(10, Math.floor(container.clientHeight || 420)),
  });

  const ro = new ResizeObserver(() => {
    const r = container.getBoundingClientRect();
    chart.applyOptions({
      width: Math.max(10, Math.floor(r.width)),
      height: Math.max(10, Math.floor(r.height)),
    });
  });
  ro.observe(container);
  _DASH.resizeObserver = ro;

  return chart;
}

function _seriesDef(type) {
  const lw = window.LightweightCharts || {};
  const defs = {
    line: lw.LineSeries,
    area: lw.AreaSeries,
    bar: lw.BarSeries,
    histogram: lw.HistogramSeries,
    candlestick: lw.CandlestickSeries,
  };
  return defs[type] || null;
}

function _addSeriesCompat(chart, type, options = {}) {
  if (!chart) throw new Error("chart_missing");

  const legacyFns = {
    line: "addLineSeries",
    area: "addAreaSeries",
    bar: "addBarSeries",
    histogram: "addHistogramSeries",
    candlestick: "addCandlestickSeries",
  };

  const legacyName = legacyFns[type];
  if (legacyName && typeof chart[legacyName] === "function") {
    return chart[legacyName](options);
  }

  if (typeof chart.addSeries === "function") {
    const def = _seriesDef(type);
    if (def) return chart.addSeries(def, options);
  }

  throw new Error(`unsupported_series_type:${type}`);
}

function _clearTradeMarkers() {
  try {
    if (_DASH.markerLayer && typeof _DASH.markerLayer.detach === "function") {
      _DASH.markerLayer.detach();
    }
  } catch {}
  _DASH.markerLayer = null;
  _DASH.markerSeries = null;
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
    _DASH.volumeSeries = _addSeriesCompat(_DASH.chart, "histogram", {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      scaleMargins: { top: 0.78, bottom: 0.0 },
    });
  }

  if (!_DASH.vwapSeries) {
    _DASH.vwapSeries = _addSeriesCompat(_DASH.chart, "line", { lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
  }

  if (!_DASH.ema20Series) {
    _DASH.ema20Series = _addSeriesCompat(_DASH.chart, "line", { lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
  }

  if (!_DASH.ema50Series) {
    _DASH.ema50Series = _addSeriesCompat(_DASH.chart, "line", { lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
  }

  if (!_DASH.pnlSeries) {
    _DASH.pnlSeries = _addSeriesCompat(_DASH.chart, "line", {
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
    const c = _normCandle(row);
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

async function _fetchCandles(symbol, tf, limit = 1200) {
  try {
    const res = await fetch(`/api/market/candles?symbol=${encodeURIComponent(symbol)}&tf=${encodeURIComponent(tf)}&limit=${encodeURIComponent(limit)}&max_points=${encodeURIComponent(limit)}`, { cache: "no-store" });
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
    const res = await fetch(`/api/terminal/markers?symbol=${encodeURIComponent(symbol)}`, { cache: "no-store" });
    const text = await res.text();
    let json = null;

    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      return { markers: [], error: `invalid marker response (${res.status})` };
    }

    if (!res.ok) {
      return { markers: [], error: (json && json.error) ? String(json.error) : `marker http ${res.status}` };
    }

    if (!json || !json.ok || !Array.isArray(json.markers)) {
      return { markers: [], error: (json && json.error) ? String(json.error) : "marker api error" };
    }

    return { markers: json.markers, error: null };
  } catch (e) {
    return { markers: [], error: e && e.message ? e.message : "marker fetch failed" };
  }
}

async function _fetchPortfolioOverlay() {
  try {
    const eqRes = await fetch(`/api/terminal/equity?limit=3000`, { cache: "no-store" });
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
    const btRes = await fetch(`/api/portfolio/backtest/latest`, { cache: "no-store" });
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

  await _ensureLightweightCharts();

  _DASH.container = container;
  _DASH.chart = _buildChart(container);
  _ensureOverlaySeries();

  const st = getProChartsState();
  const type = String(st.type || "candle").trim();

  if (type === "line") {
    _DASH.lineSeries = _addSeriesCompat(_DASH.chart, "line", { lineWidth: 2 });
    _DASH.candleSeries = null;
  } else if (type === "area") {
    _DASH.lineSeries = _addSeriesCompat(_DASH.chart, "area");
    _DASH.candleSeries = null;
  } else if (type === "bar") {
    _DASH.candleSeries = _addSeriesCompat(_DASH.chart, "bar");
    _DASH.lineSeries = null;
  } else {
    _DASH.candleSeries = _addSeriesCompat(_DASH.chart, "candlestick");
    _DASH.lineSeries = null;
  }

  if (_DASH.candleSeries && !_DASH.volumeSeries) {
    _DASH.volumeSeries = _addSeriesCompat(_DASH.chart, "histogram", {
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
  if (!chart || !_DASH.candleSeries) return;

  const markers = (trades || []).map((m) => {
    const time = Number(m.t ?? m.time ?? 0);
    const side = String(m.side || "").toUpperCase();
    const kind = String(m.kind || "").toUpperCase();
    const isBuy = side.includes("BUY") || side.includes("LONG") || Number(m.qty || 0) > 0;
    const isEntry = kind === "FILL" || kind === "ENTRY" || kind === "INTENT";

    return {
      time,
      position: isBuy ? "belowBar" : "aboveBar",
      shape: isBuy ? "arrowUp" : "arrowDown",
      color: isBuy ? "rgba(46,160,67,0.95)" : "rgba(255,107,107,0.95)",
      text: String(m.text || (isEntry ? (isBuy ? "ENTRY" : "EXIT") : side || kind || "TRADE")).slice(0, 10),
    };
  }).filter((m) => Number.isFinite(m.time));

  if (typeof _DASH.candleSeries.setMarkers === "function") {
    try { _DASH.candleSeries.setMarkers(markers); } catch {}
    return;
  }

  const createSeriesMarkers = window.LightweightCharts && window.LightweightCharts.createSeriesMarkers;
  if (typeof createSeriesMarkers !== "function") return;

  try {
    if (!_DASH.markerLayer || _DASH.markerSeries !== _DASH.candleSeries) {
      _clearTradeMarkers();
      _DASH.markerLayer = createSeriesMarkers(_DASH.candleSeries, markers);
      _DASH.markerSeries = _DASH.candleSeries;
      return;
    }
    if (typeof _DASH.markerLayer.setMarkers === "function") {
      _DASH.markerLayer.setMarkers(markers);
    }
  } catch {}
}

export function renderPortfolioOverlay(chart, pnlData) {
  if (!chart || !_DASH.pnlSeries) return;
  try {
    _DASH.pnlSeries.setData(
      (pnlData || [])
        .map((p) => ({ time: Number(p.time), value: Number(p.value) }))
        .filter((p) => Number.isFinite(p.time) && Number.isFinite(p.value))
    );
  } catch {}
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
}

function _applyIndicatorOverlays(candles, overlays) {
  if (!_DASH.chart) return;
  _ensureOverlaySeries();

  if (overlays.vwap && _DASH.vwapSeries) {
    try { _DASH.vwapSeries.setData(_computeVWAP(candles)); } catch {}
  } else if (_DASH.vwapSeries) {
    try { _DASH.vwapSeries.setData([]); } catch {}
  }

  if (overlays.ema && _DASH.ema20Series && _DASH.ema50Series) {
    try { _DASH.ema20Series.setData(_computeEMA(candles, 20)); } catch {}
    try { _DASH.ema50Series.setData(_computeEMA(candles, 50)); } catch {}
  } else {
    try { if (_DASH.ema20Series) _DASH.ema20Series.setData([]); } catch {}
    try { if (_DASH.ema50Series) _DASH.ema50Series.setData([]); } catch {}
  }
}

async function _refreshOverlays(symbol, candles = _DASH.candlesData) {
  const overlays = _getOverlayState();

  if (overlays.trades) {
    const tradesRes = await _fetchTrades(symbol);
    renderTradeMarkers(_DASH.chart, tradesRes.markers);
    if (tradesRes.error) {
      _setHealthText(`markers: ${tradesRes.error}`);
    }
  } else if (_DASH.candleSeries) {
    try {
      if (typeof _DASH.candleSeries.setMarkers === "function") {
        _DASH.candleSeries.setMarkers([]);
      } else {
        _clearTradeMarkers();
      }
    } catch {}
  }

  if (overlays.pnl) {
    const pnlRes = await _fetchPortfolioOverlay();
    renderPortfolioOverlay(_DASH.chart, pnlRes.series);
    if (pnlRes.error) {
      _setHealthText(`overlay: ${pnlRes.error}`);
    }
  } else if (_DASH.pnlSeries) {
    try { _DASH.pnlSeries.setData([]); } catch {}
  }

  _applyIndicatorOverlays(candles, overlays);
}

function _updateBar(bar) {
  if (!bar) return;
  _DASH.lastBar = bar;
  _DASH.lastUpdateMs = Date.now();
  _DASH.candlesData = _upsertSeriesPoint(_DASH.candlesData, {
    time: Number(bar.time),
    open: Number(bar.open),
    high: Number(bar.high),
    low: Number(bar.low),
    close: Number(bar.close),
    volume: Number(bar.volume || 0),
  });
  _DASH.volumeData = _upsertSeriesPoint(_DASH.volumeData, {
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

  _applyIndicatorOverlays(_DASH.candlesData, _getOverlayState());
}

function _clearRetry() {
  try { if (_DASH.retryTimer) clearTimeout(_DASH.retryTimer); } catch {}
  _DASH.retryTimer = null;
}

function _closeES() {
  try { if (_DASH.es) _DASH.es.close(); } catch {}
  _DASH.es = null;
}

function _scheduleReconnect(openFn, key) {
  _clearRetry();
  const ms = Math.max(250, Math.min(15000, Number(_DASH.retryBackoffMs || 500)));
  _DASH.retryBackoffMs = Math.min(15000, Math.floor(ms * 1.7));
  _DASH.retryTimer = setTimeout(() => {
    _DASH.retryTimer = null;
    if (_DASH.key !== key) return;
    if (document.hidden) return;
    openFn();
  }, ms);
}

function _installVisibilityHandler(openFn, key) {
  if (_DASH.visKey === key && _DASH.visHandler) return;

  try {
    if (_DASH.visHandler) {
      document.removeEventListener("visibilitychange", _DASH.visHandler);
    }
  } catch {}

  _DASH.visKey = key;
  _DASH.visHandler = () => {
    if (_DASH.key !== key) return;
    if (document.hidden) {
      _clearRetry();
      _closeES();
    } else {
      _DASH.retryBackoffMs = 500;
      openFn();
    }
  };

  document.addEventListener("visibilitychange", _DASH.visHandler);
}

async function _startLiveStream(symbol, tf) {
  const key = _DASH.key;
  const url = `/api/market/stream?symbol=${encodeURIComponent(symbol)}&tf=${encodeURIComponent(tf)}`;

  function openES() {
    _closeES();
    _clearRetry();

    try {
      const es = new EventSource(url);
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
        const bar = _normCandle(payload);
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

  _setOverlayInputs();

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
    } else if (!candles.length) {
      _setMeta(`${symbol} • ${tf} • ${type} • no candles`, "pill warn");
      _setHealthText("no data");
    } else {
      _setMeta(`${symbol} • ${tf} • ${type}`, "pill ok");
      _setHealthText(_DASH.lastUpdateMs ? "live" : "no data");
    }
  } catch (e) {
    console.error("loadProCharts failed", e);
    _setMeta("error", "pill bad");
    _setHealthText(e && e.message ? e.message : "error");
  }
}

export function bindProChartSymbolWatcher() {
  const bindRefresh = (id, patchKey) => {
    const el = document.getElementById(id);
    if (!el || el._proChartBound) return;
    el._proChartBound = true;
    el.addEventListener("change", async () => {
      _setOverlayState({ [patchKey]: !!el.checked });
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

  _setOverlayInputs();
}
