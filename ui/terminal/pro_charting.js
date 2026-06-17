/* ui/pro_charting.js
   Institutional chart core (Lightweight Charts)

   Implements:
     - SSE auto-reconnect + pause on hidden tabs
     - emit-only-when-changed server side (paired with api_market patch)
     - VWAP overlay (client)
     - EMA20/EMA50 overlays (client)
     - Fill/intent markers (from /api/terminal/markers)
     - Equity overlay (secondary right scale; from /api/terminal/equity)
     - Crosshair info panel
     - Chart health indicator (update age)
     - State-safe teardown (no timer leaks)

   Exports:
     - getProChartsState / setProChartsState
     - startLiveMarketChart(opts)
   - applyTerminalOverlays({ overlays, markers, equitySeries })
*/

import { renderChartAccessibility } from "../chart_a11y.js";
import {
  addSeriesCompat,
  applyIndicatorSeries,
  applyMarkersToState,
  applyPriceLinesToState,
  clearMarkerLayer,
  clearPriceLinesForState,
  clearRetryTimer,
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
} from "../pro_chart_core.js";
import {
  buildOverlayAccessibilitySummary,
  normalizeDecisionOverlayPayload,
} from "../decision_overlays.js";
import {
  applyProChartsVisibility,
  getProChartsState,
  PRO_CHART_PREF_DEFAULTS,
  setProChartsState
} from "../pro_chart_prefs.js";

export { applyProChartsVisibility, getProChartsState, setProChartsState } from "../pro_chart_prefs.js";

const DEFAULTS = PRO_CHART_PREF_DEFAULTS;

async function _readJsonResponse(response, url) {
  const raw = await response.text();
  let data = null;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch {}
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText || "request_failed"}: ${raw || ""}`.trim());
  }
  if (!data || typeof data !== "object") {
    throw new Error(`invalid_json_response: ${url}`);
  }
  if (data.ok === false) {
    throw new Error(String(data.error || `api_error: ${url}`));
  }
  return data;
}

export function stopLiveMarketChart() {
  _destroyLiveState(_LIVE);
  _LIVE.key = "";
  _LIVE.container = null;
  _LIVE._healthEl = null;
  _LIVE._xhairEl = null;
}

function _safeEl(id) {
  const el = document.getElementById(id);
  if (!el) throw new Error("missing_el:" + id);
  return el;
}

function _mkChart(containerEl) {
  const { chart } = createProChart(containerEl);
  return chart;
}

function _destroyLiveState(st) {
  clearRetryTimer(st);
  closeEventSource(st, { markDisconnected: true });
  removeVisibilityHandler(st, { handlerKey: "_visHandler", keyKey: "_visKey" });

  try { if (st && st._healthTimer) clearInterval(st._healthTimer); } catch {}
  try { if (st) st._healthTimer = null; } catch {}

  clearMarkerLayer(st);
  clearPriceLinesForState(st);
  try {
    if (st) {
      st.decisionOverlayPayload = null;
    }
  } catch {}

  try { if (st && st.chart) st.chart.remove(); } catch {}
  try { if (st) st.chart = null; } catch {}

  disconnectResizeObserver(st && st.container ? st.container._proChartResizeObserver : null, st && st.container);

  // Clear series refs
  if (st) {
    st.candleSeries = null;
    st.lineSeries = null;
    st.volumeSeries = null;

    st.vwapSeries = null;
    st.ema20Series = null;
    st.ema50Series = null;
    st.equitySeries = null;
    st.history = [];
    st.volumeHistory = [];
    st.indicatorState = createIndicatorState([]);
  }
}

const _LIVE = {
  key: "",
  container: null,
  chart: null,

  candleSeries: null,
  lineSeries: null,
  volumeSeries: null,

  vwapSeries: null,
  ema20Series: null,
  ema50Series: null,
  equitySeries: null,
  markerLayer: null,
  markerSeries: null,
  priceLineHandles: [],
  decisionOverlayPayload: null,

  es: null,
  retryTimer: null,
  retryBackoffMs: 500,

  lastCandle: null,
  lastUpdateMs: 0,
  streamConnected: false,
  history: [],
  volumeHistory: [],
  indicatorState: createIndicatorState([]),

  _visKey: "",
  _visHandler: null,

  _healthTimer: null,
  _healthEl: null,
  _xhairEl: null,
};

function _volColor(c) {
  try {
    return proChartVolumeColor(c, 0.45);
  } catch {
    return "rgba(167,176,188,0.35)";
  }
}

function _liveChartKeyParts() {
  const parts = String(_LIVE.key || "").split("::");
  return {
    symbol: parts[1] || "SPY",
    tf: parts[2] || "",
    type: parts[3] || "",
  };
}

function _fmtFixed(value, digits = 4) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : "unavailable";
}

function _renderLiveChartA11y({ errorMessage = "", emptyMessage = "" } = {}) {
  const container = _LIVE.container || (typeof document !== "undefined" ? document.getElementById("terminalChart") : null);
  if (!container) return;
  const { symbol, tf, type } = _liveChartKeyParts();
  const title = `Terminal market chart ${symbol}`;
  const series = (_LIVE.history || []).map((c) => ({
    time: Number(c.time),
    value: Number(c.close),
    open: Number(c.open),
    high: Number(c.high),
    low: Number(c.low),
    close: Number(c.close),
    volume: Number(c.volume || 0),
  })).filter((c) => Number.isFinite(c.time) && Number.isFinite(c.value));
  const overlaySummary = buildOverlayAccessibilitySummary(_LIVE.decisionOverlayPayload || {});

  renderChartAccessibility(container, {
    title,
    series,
    timeKey: "time",
    valueKey: "value",
    valueLabel: "close",
    valueFormatter: (v) => Number(v).toFixed(4),
    emptyMessage: emptyMessage || `No candle history is available for ${symbol}${tf ? ` ${tf}` : ""}.`,
    errorMessage,
    chartType: "lightweight-chart",
    columns: [
      { label: "Time", value: (row) => row.timeText || row.index },
      { label: "Open", value: (row) => _fmtFixed(row.raw && row.raw.open, 4) },
      { label: "High", value: (row) => _fmtFixed(row.raw && row.raw.high, 4) },
      { label: "Low", value: (row) => _fmtFixed(row.raw && row.raw.low, 4) },
      { label: "Close", value: (row) => _fmtFixed(row.raw && row.raw.close, 4) },
      { label: "Volume", value: (row) => _fmtFixed(row.raw && row.raw.volume, 0) },
    ],
    summary: series.length
      ? `${title}: latest close ${Number(series[series.length - 1].close).toFixed(4)} across ${series.length} candles${tf ? ` on ${tf}` : ""}${type ? ` as ${type}` : ""}. ${overlaySummary}`
      : "",
  });
}

function _setCrosshairPanel(chart, xhairEl) {
  if (!chart || !xhairEl) return;
  _LIVE._xhairEl = xhairEl;

  chart.subscribeCrosshairMove((param) => {
    try {
      if (!param || !param.time) {
        xhairEl.classList.remove("show");
        return;
      }

      const t = param.time;
      let p = null;

      if (_LIVE.candleSeries) p = param.seriesData.get(_LIVE.candleSeries);
      else if (_LIVE.lineSeries) p = param.seriesData.get(_LIVE.lineSeries);

      const vol = _LIVE.volumeSeries ? param.seriesData.get(_LIVE.volumeSeries) : null;

      const o = p && p.open !== undefined ? Number(p.open) : null;
      const h = p && p.high !== undefined ? Number(p.high) : null;
      const l = p && p.low !== undefined ? Number(p.low) : null;
      const c = p && p.close !== undefined ? Number(p.close) : (p && p.value !== undefined ? Number(p.value) : null);
      const v = vol && vol.value !== undefined ? Number(vol.value) : null;

      xhairEl.innerHTML =
        `<div class="mono"><b>T</b> ${String(t)}</div>` +
        `<div class="mono"><b>O</b> ${Number.isFinite(o) ? o.toFixed(4) : "—"}  <b>H</b> ${Number.isFinite(h) ? h.toFixed(4) : "—"}</div>` +
        `<div class="mono"><b>L</b> ${Number.isFinite(l) ? l.toFixed(4) : "—"}  <b>C</b> ${Number.isFinite(c) ? c.toFixed(4) : "—"}</div>` +
        `<div class="mono"><b>V</b> ${Number.isFinite(v) ? v.toFixed(0) : "—"}</div>`;

      xhairEl.classList.add("show");
    } catch {
      try { xhairEl.classList.remove("show"); } catch {}
    }
  });
}

function _installHealthTicker(healthEl) {
  _LIVE._healthEl = healthEl || null;
  installProChartHealthTicker(_LIVE, healthEl, { timerKey: "_healthTimer" });
}

function _markerAnchorSeries() {
  return _LIVE.candleSeries || _LIVE.lineSeries || null;
}

function _clearPriceLines() {
  clearPriceLinesForState(_LIVE, { anchor: _markerAnchorSeries() });
}

function _applyMarkers(markers) {
  applyMarkersToState(_LIVE, markers || [], { anchor: _markerAnchorSeries() });
}

function _ensureOverlaySeries() {
  if (!_LIVE.chart) return;

  // Create overlay series lazily; reuse if exists
  if (!_LIVE.vwapSeries) _LIVE.vwapSeries = addSeriesCompat(_LIVE.chart, "line", { lineWidth: 1 });
  if (!_LIVE.ema20Series) _LIVE.ema20Series = addSeriesCompat(_LIVE.chart, "line", { lineWidth: 1 });
  if (!_LIVE.ema50Series) _LIVE.ema50Series = addSeriesCompat(_LIVE.chart, "line", { lineWidth: 1 });

  // Equity uses separate right scale; keep margins stable
  if (!_LIVE.equitySeries) {
    _LIVE.equitySeries = addSeriesCompat(_LIVE.chart, "line", { lineWidth: 2, priceScaleId: "right" });
    try {
      _LIVE.chart.priceScale("right").applyOptions({ scaleMargins: { top: 0.12, bottom: 0.22 } });
    } catch {}
  }
}

function _hideOverlaySeries() {
  try { if (_LIVE.vwapSeries) _LIVE.vwapSeries.setData([]); } catch {}
  try { if (_LIVE.ema20Series) _LIVE.ema20Series.setData([]); } catch {}
  try { if (_LIVE.ema50Series) _LIVE.ema50Series.setData([]); } catch {}
  try { if (_LIVE.equitySeries) _LIVE.equitySeries.setData([]); } catch {}
}

export async function startLiveMarketChart(opts) {
  const {
    containerId,
    symbol,
    tf,
    type,
    crosshairElId,
    healthElId
  } = opts || {};

  const sym = String(symbol || "").trim().toUpperCase();
  if (!sym) return;

  const state = getProChartsState();
  if (!state.enabled) return;

  await ensureLightweightCharts();

  const tf2 = (tf || state.tf || DEFAULTS.tf).trim();
  const type2 = (type || state.type || DEFAULTS.type).trim();

  const key = `${containerId}::${sym}::${tf2}::${type2}`;
  if (_LIVE.key === key && _LIVE.es) return;

  _destroyLiveState(_LIVE);
  _LIVE.key = key;
  _LIVE.container = _safeEl(containerId);

  // Health/crosshair UI hooks
  const healthEl = healthElId ? document.getElementById(healthElId) : null;
  const xhairEl = crosshairElId ? document.getElementById(crosshairElId) : null;

  // Build chart + base series
  _LIVE.chart = _mkChart(_LIVE.container);

  if (type2 === "line") {
    _LIVE.lineSeries = addSeriesCompat(_LIVE.chart, "line", { lineWidth: 2 });
    _LIVE.candleSeries = null;
    _LIVE.volumeSeries = null;
  } else if (type2 === "area") {
    _LIVE.lineSeries = addSeriesCompat(_LIVE.chart, "area");
    _LIVE.candleSeries = null;
    _LIVE.volumeSeries = null;
  } else if (type2 === "bar") {
    _LIVE.candleSeries = addSeriesCompat(_LIVE.chart, "bar");
    _LIVE.volumeSeries = addSeriesCompat(_LIVE.chart, "histogram", {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      scaleMargins: { top: 0.82, bottom: 0.0 }
    });
    _LIVE.lineSeries = null;
  } else {
    _LIVE.candleSeries = addSeriesCompat(_LIVE.chart, "candlestick");
    _LIVE.volumeSeries = addSeriesCompat(_LIVE.chart, "histogram", {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      scaleMargins: { top: 0.82, bottom: 0.0 }
    });
    _LIVE.lineSeries = null;
  }

  // Crosshair panel + health ticker
  if (xhairEl) _setCrosshairPanel(_LIVE.chart, xhairEl);
  _installHealthTicker(healthEl);

  // Bootstrap historical candles
  let src = [];
  try {
    const url = `/api/market/candles?symbol=${encodeURIComponent(sym)}&tf=${encodeURIComponent(tf2)}&limit=1200&max_points=1200`;
    const r = await fetch(url, { cache: "no-store" });
    const j = await _readJsonResponse(r, url);
    if (j && j.ok && Array.isArray(j.candles)) {
      src = j.candles.map(c => ({
        t: Number(c.t),
        o: Number(c.o),
        h: Number(c.h),
        l: Number(c.l),
        c: Number(c.c),
        v: Number(c.v || 0)
      })).filter(c => Number.isFinite(c.t) && Number.isFinite(c.c));
    }
  } catch {}

  const candles = src.map(c => ({
    time: c.t,
    open: c.o,
    high: c.h,
    low: c.l,
    close: c.c
  }));
  _LIVE.history = src.map(c => ({
    time: c.t,
    open: c.o,
    high: c.h,
    low: c.l,
    close: c.c,
    volume: c.v,
  }));
  _LIVE.volumeHistory = src.map(c => ({
    time: c.t,
    value: c.v,
    color: _volColor(c),
  }));
  _LIVE.indicatorState = createIndicatorState(_LIVE.history);

  if (_LIVE.candleSeries) {
    try { _LIVE.candleSeries.setData(candles); } catch {}
    if (_LIVE.volumeSeries) {
      try { _LIVE.volumeSeries.setData(_LIVE.volumeHistory); } catch {}
    }
  } else if (_LIVE.lineSeries) {
    try { _LIVE.lineSeries.setData(src.map(c => ({ time: c.t, value: c.c }))); } catch {}
  }

  // Track last update from bootstrap
  if (src.length) {
    _LIVE.lastUpdateMs = Date.now();
    _LIVE.lastCandle = src[src.length - 1] || null;
  }
  _renderLiveChartA11y({
    emptyMessage: src.length ? "" : `No candles are available for ${sym} ${tf2}.`,
  });

  // Live stream via SSE (auto-reconnect + pause when hidden)
  const url = `/api/market/stream?symbol=${encodeURIComponent(sym)}&tf=${encodeURIComponent(tf2)}`;

  _LIVE.retryTimer = null;
  _LIVE.retryBackoffMs = 500;

  function _clearRetry() {
    clearRetryTimer(_LIVE);
  }

  function _closeES() {
    closeEventSource(_LIVE, { markDisconnected: true });
  }

  function _scheduleReconnect() {
    scheduleStreamReconnect(_LIVE, { key, open: _openES });
  }

  function _applyCandle(raw) {
    const c = normalizeCandle(raw);
    if (!c) return;

    _LIVE.lastCandle = c;
    _LIVE.lastUpdateMs = Date.now();
    _LIVE.history = upsertSeriesPoint(_LIVE.history, {
      time: Number(c.time),
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close),
      volume: Number(c.volume || 0),
    });
    const indicatorUpdate = updateIndicatorState(_LIVE.indicatorState, {
      time: Number(c.time),
      close: Number(c.close),
      volume: Number(c.volume || 0),
    });
    _LIVE.indicatorState = indicatorUpdate.needsRebuild ? createIndicatorState(_LIVE.history) : indicatorUpdate.state;
    _LIVE.volumeHistory = upsertSeriesPoint(_LIVE.volumeHistory, {
      time: Number(c.time),
      value: Number(c.volume || 0),
      color: _volColor(c),
    });

    if (_LIVE.candleSeries) {
      try {
        _LIVE.candleSeries.update({
          time: Number(c.time),
          open: Number(c.open),
          high: Number(c.high),
          low: Number(c.low),
          close: Number(c.close)
        });
      } catch {}

      if (_LIVE.volumeSeries) {
        try { _LIVE.volumeSeries.update({ time: Number(c.time), value: Number(c.volume || 0), color: _volColor(c) }); } catch {}
      }
    } else if (_LIVE.lineSeries) {
      try { _LIVE.lineSeries.update({ time: Number(c.time), value: Number(c.close) }); } catch {}
    }

    updateIndicatorSeriesTail({
      indicatorState: _LIVE.indicatorState,
      overlays: { vwap: true, ema: true },
      vwapSeries: _LIVE.vwapSeries,
      ema20Series: _LIVE.ema20Series,
      ema50Series: _LIVE.ema50Series,
    });
    _renderLiveChartA11y();
  }

  function _openES() {
    _closeES();
    _clearRetry();

    try {
      const es = new EventSource(url);
      _LIVE.es = es;
      _LIVE.streamConnected = true;

      es.addEventListener("hello", () => {
        _LIVE.retryBackoffMs = 500;
        _LIVE.streamConnected = true;
        if (_LIVE._healthEl) _LIVE._healthEl.textContent = "live";
      });

      es.addEventListener("candle", (ev) => {
        let c = null;
        try { c = JSON.parse(ev.data); } catch { return; }
        _LIVE.streamConnected = true;
        _applyCandle(c);
      });

      es.addEventListener("error", () => {
        if (_LIVE.key !== key) return;
        _closeES();
        if (_LIVE._healthEl) _LIVE._healthEl.textContent = "reconnecting…";
        _scheduleReconnect();
      });
    } catch {
      _LIVE.streamConnected = false;
      if (_LIVE._healthEl) _LIVE._healthEl.textContent = "reconnecting…";
      _scheduleReconnect();
    }
  }

  installVisibilityReconnect(_LIVE, {
    key,
    open: _openES,
    close: _closeES,
    clearRetry: _clearRetry,
    handlerKey: "_visHandler",
    keyKey: "_visKey",
  });

  _openES();
}

export function applyTerminalOverlays({ overlays, markers, equitySeries, decisionOverlay }) {
  const ov = overlays || {};
  const wantVWAP = !!ov.vwap;
  const wantEMA = !!ov.ema;
  const wantMarkers = !!ov.markers;
  const wantEquity = !!ov.equity;

  if (!_LIVE.chart) return;

  const overlayKey = String(_LIVE.key || "");

  if (wantVWAP || wantEMA || wantEquity) _ensureOverlaySeries();
  else _hideOverlaySeries();

  const normalizedOverlay = normalizeDecisionOverlayPayload(decisionOverlay || { markers: markers || [] });
  _LIVE.decisionOverlayPayload = wantMarkers ? normalizedOverlay : null;

  if (wantMarkers) {
    _applyMarkers(normalizedOverlay.markers || []);
    applyPriceLinesToState(_LIVE, normalizedOverlay.price_lines || [], { anchor: _markerAnchorSeries() });
  }
  else {
    try {
      const anchor = _markerAnchorSeries();
      if (anchor && typeof anchor.setMarkers === "function") {
        anchor.setMarkers([]);
      } else {
        clearMarkerLayer(_LIVE);
      }
    } catch {}
    _clearPriceLines();
  }

  const src = Array.isArray(_LIVE.history) ? _LIVE.history : [];
  _LIVE.indicatorState = ensureIndicatorStateForRows(_LIVE.indicatorState, src);

  if (_LIVE.key !== overlayKey) return;

  applyIndicatorSeries({
    indicatorState: _LIVE.indicatorState,
    overlays: { vwap: wantVWAP, ema: wantEMA },
    vwapSeries: _LIVE.vwapSeries,
    ema20Series: _LIVE.ema20Series,
    ema50Series: _LIVE.ema50Series,
  });

  if (wantEquity && _LIVE.equitySeries) {
    try {
      const series = Array.isArray(equitySeries) ? equitySeries : [];
      _LIVE.equitySeries.setData(
        series
          .map(p => ({ time: Number(p.t), value: Number(p.v) }))
          .filter(p => Number.isFinite(p.time) && Number.isFinite(p.value))
      );
    } catch {}
  } else {
    try { if (_LIVE.equitySeries) _LIVE.equitySeries.setData([]); } catch {}
  }
  _renderLiveChartA11y();
}
