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

const LS_ENABLED = "proCharts.enabled";
const LS_TF = "proCharts.tf";
const LS_TYPE = "proCharts.type";

const DEFAULTS = {
  enabled: true,
  tf: "1m",
  type: "candle",
  libLocal: "/ui/vendor/lightweight-charts.standalone.production.js",
  libCdn: "https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"
};

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

function _lsGet(k, d) {
  try {
    const v = localStorage.getItem(k);
    return v === null || v === undefined ? d : v;
  } catch { return d; }
}

function _lsSet(k, v) {
  try { localStorage.setItem(k, String(v)); } catch {}
}

export function getProChartsState() {
  const enabled = _lsGet(LS_ENABLED, DEFAULTS.enabled ? "1" : "0") === "1";
  const tf = (_lsGet(LS_TF, DEFAULTS.tf) || DEFAULTS.tf).trim();
  const type = (_lsGet(LS_TYPE, DEFAULTS.type) || DEFAULTS.type).trim();
  return { enabled, tf, type };
}

export function setProChartsState(patch) {
  const cur = getProChartsState();
  const next = { ...cur, ...(patch || {}) };
  _lsSet(LS_ENABLED, next.enabled ? "1" : "0");
  _lsSet(LS_TF, next.tf || DEFAULTS.tf);
  _lsSet(LS_TYPE, next.type || DEFAULTS.type);
  return next;
}

export function applyProChartsVisibility(cardId = "proChartsCard") {
  const st = getProChartsState();
  const el = document.getElementById(cardId);
  if (el) {
    el.style.display = st.enabled ? "" : "none";
  }
  return st;
}

export function stopLiveMarketChart() {
  _destroyLiveState(_LIVE);
  _LIVE.key = "";
  _LIVE.container = null;
  _LIVE._healthEl = null;
  _LIVE._xhairEl = null;
}

async function _ensureLightweightCharts() {
  if (window.LightweightCharts) return window.LightweightCharts;

  async function load(src) {
    return await new Promise((res, rej) => {
      const s = document.createElement("script");
      s.src = src;
      s.async = true;
      s.onload = () => res(true);
      s.onerror = () => rej(new Error("failed to load " + src));
      document.head.appendChild(s);
    });
  }

  try { await load(DEFAULTS.libLocal); }
  catch { await load(DEFAULTS.libCdn); }

  if (!window.LightweightCharts) throw new Error("LightweightCharts not available");
  return window.LightweightCharts;
}

function _safeEl(id) {
  const el = document.getElementById(id);
  if (!el) throw new Error("missing_el:" + id);
  return el;
}

function _mkChart(containerEl) {
  const { createChart } = window.LightweightCharts;
  containerEl.innerHTML = "";

  const chart = createChart(containerEl, {
    layout: {
      background: { color: "#0a0d12" },
      textColor: "#9da7b3"
    },
    grid: {
      vertLines: { color: "#1f2630" },
      horzLines: { color: "#1f2630" }
    },
    rightPriceScale: { borderColor: "#30363d" },
    timeScale: {
      borderColor: "#30363d",
      timeVisible: true,
      secondsVisible: false,
      rightBarStaysOnScroll: true
    },
    crosshair: { mode: 1 }
  });

  const ro = new ResizeObserver(() => {
    const r = containerEl.getBoundingClientRect();
    chart.applyOptions({
      width: Math.max(10, Math.floor(r.width)),
      height: Math.max(10, Math.floor(r.height))
    });
  });
  ro.observe(containerEl);
  containerEl._proChartResizeObserver = ro;

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

function _clearMarkerLayer() {
  try {
    if (_LIVE.markerLayer && typeof _LIVE.markerLayer.detach === "function") {
      _LIVE.markerLayer.detach();
    }
  } catch {}
  _LIVE.markerLayer = null;
  _LIVE.markerSeries = null;
}

function _destroyLiveState(st) {
  try { if (st && st.retryTimer) clearTimeout(st.retryTimer); } catch {}
  try { if (st) st.retryTimer = null; } catch {}

  try { if (st && st.es) st.es.close(); } catch {}
  try { if (st) st.es = null; } catch {}
  try { if (st) st.streamConnected = false; } catch {}

  try { if (st && st._visHandler) document.removeEventListener("visibilitychange", st._visHandler); } catch {}
  try { if (st) { st._visHandler = null; st._visKey = ""; } } catch {}

  try { if (st && st._healthTimer) clearInterval(st._healthTimer); } catch {}
  try { if (st) st._healthTimer = null; } catch {}

  try {
    if (st && st.markerLayer && typeof st.markerLayer.detach === "function") {
      st.markerLayer.detach();
    }
  } catch {}
  try {
    if (st) {
      st.markerLayer = null;
      st.markerSeries = null;
    }
  } catch {}

  try { if (st && st.chart) st.chart.remove(); } catch {}
  try { if (st) st.chart = null; } catch {}

  try { if (st && st.container && st.container._proChartResizeObserver) st.container._proChartResizeObserver.disconnect(); } catch {}
  try { if (st && st.container) st.container._proChartResizeObserver = null; } catch {}

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

  es: null,
  retryTimer: null,
  retryBackoffMs: 500,

  lastCandle: null,
  lastUpdateMs: 0,
  streamConnected: false,
  history: [],
  volumeHistory: [],

  _visKey: "",
  _visHandler: null,

  _healthTimer: null,
  _healthEl: null,
  _xhairEl: null,
};

function _volColor(c) {
  try {
    if (!c) return "rgba(157,167,179,0.35)";
    return (Number(c.c) >= Number(c.o)) ? "rgba(46,160,67,0.45)" : "rgba(210,153,34,0.45)";
  } catch {
    return "rgba(157,167,179,0.35)";
  }
}

function _computeVWAP(candles) {
  let pv = 0;
  let vv = 0;
  const out = [];
  for (const c of candles) {
    const t = Number(c.time);
    const v = Number(c.volume ?? c.v ?? 0);
    const px = Number(c.close ?? c.c);
    if (!Number.isFinite(t) || !Number.isFinite(px)) continue;
    if (Number.isFinite(v) && v > 0) {
      pv += px * v;
      vv += v;
    }
    const vwap = (vv > 0) ? (pv / vv) : px;
    out.push({ time: t, value: vwap });
  }
  return out;
}

function _computeEMA(candles, period) {
  const k = 2 / (Number(period) + 1);
  let ema = null;
  const out = [];
  for (const c of candles) {
    const t = Number(c.time);
    const px = Number(c.close ?? c.c);
    if (!Number.isFinite(t) || !Number.isFinite(px)) continue;
    if (ema === null) ema = px;
    else ema = (px * k) + (ema * (1 - k));
    out.push({ time: t, value: ema });
  }
  return out;
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
  if (_LIVE._healthTimer) {
    try { clearInterval(_LIVE._healthTimer); } catch {}
    _LIVE._healthTimer = null;
  }
  if (!healthEl) return;

  _LIVE._healthTimer = setInterval(() => {
    if (!_LIVE.lastUpdateMs) {
      healthEl.textContent = _LIVE.streamConnected ? "live stream" : "no data";
      return;
    }
    const age = Date.now() - Number(_LIVE.lastUpdateMs || 0);
    if (_LIVE.streamConnected) {
      if (age < 1500) healthEl.textContent = "live";
      else if (age < 60_000) healthEl.textContent = `live • last candle ${Math.floor(age / 1000)}s`;
      else healthEl.textContent = `stale ${Math.floor(age/1000)}s`;
      return;
    }
    if (age < 1500) healthEl.textContent = "live";
    else if (age < 5000) healthEl.textContent = `lag ${Math.floor(age)}ms`;
    else healthEl.textContent = `stale ${Math.floor(age/1000)}s`;
  }, 750);
}

function _applyMarkers(markers) {
  if (!_LIVE.candleSeries || !Array.isArray(markers)) return;

  // Lightweight Charts markers use time in seconds for UTCTimestamp (as number)
  const out = [];
  for (const m of markers) {
    const t = Number(m.t);
    if (!Number.isFinite(t)) continue;

    const side = String(m.side || "").toUpperCase();
    const isBuy = side.includes("BUY") || side.includes("LONG") || (Number(m.qty || 0) > 0);

    out.push({
      time: t,
      position: isBuy ? "belowBar" : "aboveBar",
      shape: isBuy ? "arrowUp" : "arrowDown",
      color: isBuy ? "rgba(46,160,67,0.95)" : "rgba(255,107,107,0.95)",
      text: String(m.text || (isBuy ? "BUY" : "SELL")).slice(0, 10),
    });
  }

  if (typeof _LIVE.candleSeries.setMarkers === "function") {
    try { _LIVE.candleSeries.setMarkers(out); } catch {}
    return;
  }

  const createSeriesMarkers = window.LightweightCharts && window.LightweightCharts.createSeriesMarkers;
  if (typeof createSeriesMarkers !== "function") return;

  try {
    if (!_LIVE.markerLayer || _LIVE.markerSeries !== _LIVE.candleSeries) {
      _clearMarkerLayer();
      _LIVE.markerLayer = createSeriesMarkers(_LIVE.candleSeries, out);
      _LIVE.markerSeries = _LIVE.candleSeries;
      return;
    }
    if (typeof _LIVE.markerLayer.setMarkers === "function") {
      _LIVE.markerLayer.setMarkers(out);
    }
  } catch {}
}

function _ensureOverlaySeries() {
  if (!_LIVE.chart) return;

  // Create overlay series lazily; reuse if exists
  if (!_LIVE.vwapSeries) _LIVE.vwapSeries = _addSeriesCompat(_LIVE.chart, "line", { lineWidth: 1 });
  if (!_LIVE.ema20Series) _LIVE.ema20Series = _addSeriesCompat(_LIVE.chart, "line", { lineWidth: 1 });
  if (!_LIVE.ema50Series) _LIVE.ema50Series = _addSeriesCompat(_LIVE.chart, "line", { lineWidth: 1 });

  // Equity uses separate right scale; keep margins stable
  if (!_LIVE.equitySeries) {
    _LIVE.equitySeries = _addSeriesCompat(_LIVE.chart, "line", { lineWidth: 2, priceScaleId: "right" });
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

  await _ensureLightweightCharts();

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
    _LIVE.lineSeries = _addSeriesCompat(_LIVE.chart, "line", { lineWidth: 2 });
    _LIVE.candleSeries = null;
    _LIVE.volumeSeries = null;
  } else if (type2 === "area") {
    _LIVE.lineSeries = _addSeriesCompat(_LIVE.chart, "area");
    _LIVE.candleSeries = null;
    _LIVE.volumeSeries = null;
  } else if (type2 === "bar") {
    _LIVE.candleSeries = _addSeriesCompat(_LIVE.chart, "bar");
    _LIVE.volumeSeries = _addSeriesCompat(_LIVE.chart, "histogram", {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      scaleMargins: { top: 0.82, bottom: 0.0 }
    });
    _LIVE.lineSeries = null;
  } else {
    _LIVE.candleSeries = _addSeriesCompat(_LIVE.chart, "candlestick");
    _LIVE.volumeSeries = _addSeriesCompat(_LIVE.chart, "histogram", {
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

  // Live stream via SSE (auto-reconnect + pause when hidden)
  const url = `/api/market/stream?symbol=${encodeURIComponent(sym)}&tf=${encodeURIComponent(tf2)}`;

  _LIVE.retryTimer = null;
  _LIVE.retryBackoffMs = 500;

  function _clearRetry() {
    try { if (_LIVE.retryTimer) clearTimeout(_LIVE.retryTimer); } catch {}
    _LIVE.retryTimer = null;
  }

  function _closeES() {
    try { if (_LIVE.es) _LIVE.es.close(); } catch {}
    _LIVE.es = null;
    _LIVE.streamConnected = false;
  }

  function _scheduleReconnect() {
    _clearRetry();
    const ms = Math.max(250, Math.min(15000, Number(_LIVE.retryBackoffMs || 500)));
    _LIVE.retryBackoffMs = Math.min(15000, Math.floor(ms * 1.7));
    _LIVE.retryTimer = setTimeout(() => {
      _LIVE.retryTimer = null;
      if (_LIVE.key !== key) return;
      if (document.hidden) return;
      _openES();
    }, ms);
  }

  function _applyCandle(c) {
    if (!c || !c.t) return;

    _LIVE.lastCandle = c;
    _LIVE.lastUpdateMs = Date.now();
    _LIVE.history = _upsertSeriesPoint(_LIVE.history, {
      time: Number(c.t),
      open: Number(c.o),
      high: Number(c.h),
      low: Number(c.l),
      close: Number(c.c),
      volume: Number(c.v || 0),
    });
    _LIVE.volumeHistory = _upsertSeriesPoint(_LIVE.volumeHistory, {
      time: Number(c.t),
      value: Number(c.v || 0),
      color: _volColor(c),
    });

    if (_LIVE.candleSeries) {
      try {
        _LIVE.candleSeries.update({
          time: Number(c.t),
          open: Number(c.o),
          high: Number(c.h),
          low: Number(c.l),
          close: Number(c.c)
        });
      } catch {}

      if (_LIVE.volumeSeries) {
        try { _LIVE.volumeSeries.update({ time: Number(c.t), value: Number(c.v || 0), color: _volColor(c) }); } catch {}
      }
    } else if (_LIVE.lineSeries) {
      try { _LIVE.lineSeries.update({ time: Number(c.t), value: Number(c.c) }); } catch {}
    }

    if (_LIVE.vwapSeries || _LIVE.ema20Series || _LIVE.ema50Series) {
      try { if (_LIVE.vwapSeries) _LIVE.vwapSeries.setData(_computeVWAP(_LIVE.history)); } catch {}
      try { if (_LIVE.ema20Series) _LIVE.ema20Series.setData(_computeEMA(_LIVE.history, 20)); } catch {}
      try { if (_LIVE.ema50Series) _LIVE.ema50Series.setData(_computeEMA(_LIVE.history, 50)); } catch {}
    }
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

  // visibility pause/resume
  if (!_LIVE._visKey || _LIVE._visKey !== key) {
    _LIVE._visKey = key;

    try {
      if (_LIVE._visHandler) document.removeEventListener("visibilitychange", _LIVE._visHandler);
    } catch {}

    _LIVE._visHandler = () => {
      if (_LIVE.key !== key) return;
      if (document.hidden) {
        _clearRetry();
        _closeES();
      } else {
        _LIVE.retryBackoffMs = 500;
        _openES();
      }
    };

    document.addEventListener("visibilitychange", _LIVE._visHandler);
  }

  _openES();
}

export function applyTerminalOverlays({ overlays, markers, equitySeries }) {
  const ov = overlays || {};
  const wantVWAP = !!ov.vwap;
  const wantEMA = !!ov.ema;
  const wantMarkers = !!ov.markers;
  const wantEquity = !!ov.equity;

  if (!_LIVE.chart) return;

  const overlayKey = String(_LIVE.key || "");

  if (wantVWAP || wantEMA || wantEquity) _ensureOverlaySeries();
  else _hideOverlaySeries();

  // Apply markers to candle series only
  if (wantMarkers) _applyMarkers(markers || []);
  else {
    try {
      if (_LIVE.candleSeries && typeof _LIVE.candleSeries.setMarkers === "function") {
        _LIVE.candleSeries.setMarkers([]);
      } else {
        _clearMarkerLayer();
      }
    } catch {}
  }

  const src = Array.isArray(_LIVE.history) ? _LIVE.history : [];

  if (_LIVE.key !== overlayKey) return;

  if (wantVWAP && _LIVE.vwapSeries) {
    try { _LIVE.vwapSeries.setData(_computeVWAP(src)); } catch {}
  } else {
    try { if (_LIVE.vwapSeries) _LIVE.vwapSeries.setData([]); } catch {}
  }

  if (wantEMA && _LIVE.ema20Series && _LIVE.ema50Series) {
    try { _LIVE.ema20Series.setData(_computeEMA(src, 20)); } catch {}
    try { _LIVE.ema50Series.setData(_computeEMA(src, 50)); } catch {}
  } else {
    try { if (_LIVE.ema20Series) _LIVE.ema20Series.setData([]); } catch {}
    try { if (_LIVE.ema50Series) _LIVE.ema50Series.setData([]); } catch {}
  }

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
}
