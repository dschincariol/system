/*
  FILE: ui/pro_chart_core.js

  Shared Lightweight Charts mechanics for dashboard and terminal pro charts.
  Surface-specific orchestration, fetching, legends, and crosshair copy stay in
  their existing wrappers.
*/

import { chartVolumeColor } from "./utils.js";
import {
  applyPriceLinesToSeries,
  createIndicatorState,
  toDecisionWindowBands,
  toLightweightMarkers,
  updateIndicatorState,
} from "./decision_overlays.js";

export { createIndicatorState, updateIndicatorState };

export const LIGHTWEIGHT_CHART_DEFAULTS = {
  libLocal: "/ui/vendor/lightweight-charts.standalone.production.js",
  libCdn: "https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js",
};

const BASE_CHART_OPTIONS = {
  layout: {
    background: { color: "#0a0d12" },
    textColor: "#9da7b3",
  },
  grid: {
    vertLines: { color: "#1f2630" },
    horzLines: { color: "#1f2630" },
  },
  rightPriceScale: { borderColor: "#30363d" },
  timeScale: {
    borderColor: "#30363d",
    timeVisible: true,
    secondsVisible: false,
    rightBarStaysOnScroll: true,
  },
  crosshair: { mode: 1 },
};

function _globalWindow() {
  try {
    return typeof window !== "undefined" ? window : null;
  } catch {
    return null;
  }
}

function _globalDocument() {
  try {
    return typeof document !== "undefined" ? document : null;
  } catch {
    return null;
  }
}

function _mergeOptions(base, patch) {
  const out = { ...(base || {}) };
  for (const [key, value] of Object.entries(patch || {})) {
    if (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      out[key] &&
      typeof out[key] === "object" &&
      !Array.isArray(out[key])
    ) {
      out[key] = _mergeOptions(out[key], value);
    } else if (value !== undefined) {
      out[key] = value;
    }
  }
  return out;
}

export async function ensureLightweightCharts({
  windowRef = _globalWindow(),
  documentRef = _globalDocument(),
  localSrc = LIGHTWEIGHT_CHART_DEFAULTS.libLocal,
  cdnSrc = LIGHTWEIGHT_CHART_DEFAULTS.libCdn,
} = {}) {
  if (windowRef && windowRef.LightweightCharts) return windowRef.LightweightCharts;
  if (!windowRef || !documentRef || !documentRef.createElement || !documentRef.head) {
    throw new Error("LightweightCharts not available");
  }

  async function load(src) {
    return await new Promise((resolve, reject) => {
      const script = documentRef.createElement("script");
      script.src = src;
      script.async = true;
      script.onload = () => resolve(true);
      script.onerror = () => reject(new Error("failed to load " + src));
      documentRef.head.appendChild(script);
    });
  }

  try {
    await load(localSrc);
  } catch {
    await load(cdnSrc);
  }

  if (!windowRef.LightweightCharts) throw new Error("LightweightCharts not available");
  return windowRef.LightweightCharts;
}

export function seriesDefinition(type, lightweightCharts = null) {
  const lw = lightweightCharts || (_globalWindow() && _globalWindow().LightweightCharts) || {};
  const defs = {
    line: lw.LineSeries,
    area: lw.AreaSeries,
    bar: lw.BarSeries,
    histogram: lw.HistogramSeries,
    candlestick: lw.CandlestickSeries,
  };
  return defs[type] || null;
}

export function addSeriesCompat(chart, type, options = {}, { windowRef = _globalWindow() } = {}) {
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
    const def = seriesDefinition(type, windowRef && windowRef.LightweightCharts);
    if (def) return chart.addSeries(def, options);
  }

  throw new Error(`unsupported_series_type:${type}`);
}

export function installChartResizeObserver(
  containerEl,
  chart,
  {
    ResizeObserverClass = typeof ResizeObserver !== "undefined" ? ResizeObserver : null,
    propertyName = "_proChartResizeObserver",
    sizeFallback = {},
  } = {}
) {
  if (!containerEl || !chart || !ResizeObserverClass) return null;
  const fallbackWidth = Number(sizeFallback.width);
  const fallbackHeight = Number(sizeFallback.height);

  const resizeObserver = new ResizeObserverClass(() => {
    const rect = typeof containerEl.getBoundingClientRect === "function"
      ? containerEl.getBoundingClientRect()
      : { width: containerEl.clientWidth || 0, height: containerEl.clientHeight || 0 };
    const width = (
      Number(rect.width) ||
      Number(containerEl.clientWidth) ||
      (Number.isFinite(fallbackWidth) && fallbackWidth > 0 ? fallbackWidth : 0)
    );
    const height = (
      Number(rect.height) ||
      Number(containerEl.clientHeight) ||
      (Number.isFinite(fallbackHeight) && fallbackHeight > 0 ? fallbackHeight : 0)
    );
    chart.applyOptions({
      width: Math.max(10, Math.floor(width)),
      height: Math.max(10, Math.floor(height)),
    });
  });
  resizeObserver.observe(containerEl);
  if (propertyName) containerEl[propertyName] = resizeObserver;
  return resizeObserver;
}

export function disconnectResizeObserver(resizeObserver, containerEl = null, propertyName = "_proChartResizeObserver") {
  try {
    if (resizeObserver && typeof resizeObserver.disconnect === "function") resizeObserver.disconnect();
  } catch {}
  try {
    if (containerEl && propertyName && containerEl[propertyName] === resizeObserver) {
      containerEl[propertyName] = null;
    }
  } catch {}
}

export function createProChart(
  containerEl,
  {
    windowRef = _globalWindow(),
    chartOptions = {},
    resizeObserverOptions = {},
    includeInitialSize = false,
    initialSizeFallback = {},
  } = {}
) {
  if (!containerEl) throw new Error("container_missing");
  const createChart = windowRef && windowRef.LightweightCharts && windowRef.LightweightCharts.createChart;
  if (typeof createChart !== "function") throw new Error("LightweightCharts not available");

  containerEl.innerHTML = "";
  const fallbackWidth = Number(initialSizeFallback.width);
  const fallbackHeight = Number(initialSizeFallback.height);
  const initialSize = includeInitialSize
    ? {
        width: Math.max(10, Math.floor(
          Number(containerEl.clientWidth) ||
          (Number.isFinite(fallbackWidth) && fallbackWidth > 0 ? fallbackWidth : 1200)
        )),
        height: Math.max(10, Math.floor(
          Number(containerEl.clientHeight) ||
          (Number.isFinite(fallbackHeight) && fallbackHeight > 0 ? fallbackHeight : 420)
        )),
      }
    : {};
  const chart = createChart(containerEl, _mergeOptions(_mergeOptions(BASE_CHART_OPTIONS, initialSize), chartOptions));
  const resizeObserver = installChartResizeObserver(containerEl, chart, resizeObserverOptions);
  return { chart, resizeObserver };
}

export function clearMarkerLayer(state) {
  try {
    if (state && state.markerLayer && typeof state.markerLayer.detach === "function") {
      state.markerLayer.detach();
    }
  } catch {}
  if (state) {
    state.markerLayer = null;
    state.markerSeries = null;
  }
}

function _timeCoordinate(timeScale, time) {
  if (!timeScale) return null;
  const t = Number(time);
  if (!Number.isFinite(t)) return null;

  try {
    if (typeof timeScale.timeToIndex === "function" && typeof timeScale.logicalToCoordinate === "function") {
      const index = timeScale.timeToIndex(t, true);
      const coord = Number.isFinite(index) ? timeScale.logicalToCoordinate(index) : null;
      if (Number.isFinite(coord)) return coord;
    }
  } catch {}

  try {
    if (typeof timeScale.timeToCoordinate === "function") {
      const coord = timeScale.timeToCoordinate(t);
      if (Number.isFinite(coord)) return coord;
    }
  } catch {}

  return null;
}

class DecisionWindowBandsRenderer {
  constructor(source) {
    this.source = source;
  }

  draw(target) {
    const source = this.source || {};
    const bands = Array.isArray(source.bands) ? source.bands : [];
    const chart = source.chart;
    if (!bands.length || !chart || typeof chart.timeScale !== "function") return;

    target.useMediaCoordinateSpace(({ context, mediaSize }) => {
      const timeScale = chart.timeScale();
      const width = Math.max(0, Number(mediaSize && mediaSize.width) || 0);
      const height = Math.max(0, Number(mediaSize && mediaSize.height) || 0);
      if (!width || !height) return;

      for (const band of bands) {
        const startX = _timeCoordinate(timeScale, band.startTime);
        const endX = _timeCoordinate(timeScale, band.endTime);
        if (!Number.isFinite(startX) && !Number.isFinite(endX)) continue;

        const leftRaw = Number.isFinite(startX) ? startX : 0;
        const rightRaw = Number.isFinite(endX) ? endX : width;
        const left = Math.max(0, Math.min(width, Math.min(leftRaw, rightRaw)));
        const right = Math.max(0, Math.min(width, Math.max(leftRaw, rightRaw)));
        const bandWidth = Math.max(3, right - left);

        context.save();
        context.fillStyle = band.fillColor || "rgba(139, 148, 158, 0.13)";
        context.fillRect(left, 0, bandWidth, height);
        context.strokeStyle = band.borderColor || "rgba(139, 148, 158, 0.35)";
        context.lineWidth = 1;
        context.beginPath();
        context.moveTo(left + 0.5, 0);
        context.lineTo(left + 0.5, height);
        context.moveTo(left + bandWidth - 0.5, 0);
        context.lineTo(left + bandWidth - 0.5, height);
        context.stroke();
        context.restore();
      }
    });
  }
}

class DecisionWindowBandsPaneView {
  constructor(primitive) {
    this.primitive = primitive;
  }

  renderer() {
    return new DecisionWindowBandsRenderer(this.primitive._source());
  }

  zOrder() {
    return "bottom";
  }
}

class DecisionWindowBandsPrimitive {
  constructor(bands = []) {
    this._bands = Array.isArray(bands) ? bands : [];
    this._chart = null;
    this._requestUpdate = null;
    this._paneViews = [new DecisionWindowBandsPaneView(this)];
  }

  attached({ chart, requestUpdate } = {}) {
    this._chart = chart || null;
    this._requestUpdate = typeof requestUpdate === "function" ? requestUpdate : null;
  }

  detached() {
    this._chart = null;
    this._requestUpdate = null;
  }

  paneViews() {
    return this._paneViews;
  }

  updateAllViews() {}

  setBands(bands = []) {
    this._bands = Array.isArray(bands) ? bands : [];
    try { if (this._requestUpdate) this._requestUpdate(); } catch {}
  }

  bands() {
    return this._bands.slice();
  }

  _source() {
    return {
      chart: this._chart,
      bands: this._bands,
    };
  }
}

export function clearWindowBandsForState(state) {
  if (!state) return;
  try {
    const anchor = state.windowBandSeries;
    if (anchor && state.windowBandLayer && typeof anchor.detachPrimitive === "function") {
      anchor.detachPrimitive(state.windowBandLayer);
    }
  } catch {}
  state.windowBandLayer = null;
  state.windowBandSeries = null;
}

export function applyWindowBandsToState(
  state,
  windows,
  {
    anchor = markerAnchorSeries(state),
    rows = (state && (state.candlesData || state.history)) || [],
    bandMapper = toDecisionWindowBands,
  } = {}
) {
  if (!state) return [];
  const bands = bandMapper(windows || [], rows || []);

  if (!bands.length || !anchor || typeof anchor.attachPrimitive !== "function") {
    clearWindowBandsForState(state);
    return bands;
  }

  try {
    if (!state.windowBandLayer || state.windowBandSeries !== anchor) {
      clearWindowBandsForState(state);
      state.windowBandLayer = new DecisionWindowBandsPrimitive(bands);
      state.windowBandSeries = anchor;
      anchor.attachPrimitive(state.windowBandLayer);
      return bands;
    }

    if (typeof state.windowBandLayer.setBands === "function") {
      state.windowBandLayer.setBands(bands);
    }
  } catch {
    clearWindowBandsForState(state);
  }

  return bands;
}

export function markerAnchorSeries(state) {
  return (state && (state.candleSeries || state.lineSeries)) || null;
}

export function applyMarkersToState(
  state,
  markers,
  {
    windowRef = _globalWindow(),
    markerMapper = toLightweightMarkers,
    anchor = markerAnchorSeries(state),
  } = {}
) {
  if (!state || !anchor || !Array.isArray(markers)) return false;
  const out = markerMapper(markers);

  if (typeof anchor.setMarkers === "function") {
    try {
      anchor.setMarkers(out);
      return true;
    } catch {
      return false;
    }
  }

  const createSeriesMarkers = windowRef && windowRef.LightweightCharts && windowRef.LightweightCharts.createSeriesMarkers;
  if (typeof createSeriesMarkers !== "function") return false;

  try {
    if (!state.markerLayer || state.markerSeries !== anchor) {
      clearMarkerLayer(state);
      state.markerLayer = createSeriesMarkers(anchor, out);
      state.markerSeries = anchor;
      return true;
    }
    if (typeof state.markerLayer.setMarkers === "function") {
      state.markerLayer.setMarkers(out);
      return true;
    }
  } catch {}

  return false;
}

export function applyPriceLinesToState(state, priceLines, { anchor = markerAnchorSeries(state) } = {}) {
  if (!state) return [];
  state.priceLineHandles = applyPriceLinesToSeries(anchor, state.priceLineHandles || [], priceLines || []);
  return state.priceLineHandles;
}

export function clearPriceLinesForState(state, options = {}) {
  return applyPriceLinesToState(state, [], options);
}

export function upsertSeriesPoint(rows, point, limit = 1500) {
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

export function safeNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

export function normalizeCandle(candle) {
  const time = safeNumber(candle?.t ?? candle?.time);
  const open = safeNumber(candle?.o ?? candle?.open ?? candle?.price ?? candle?.last ?? candle?.close);
  const high = safeNumber(candle?.h ?? candle?.high ?? candle?.price ?? candle?.last ?? candle?.close);
  const low = safeNumber(candle?.l ?? candle?.low ?? candle?.price ?? candle?.last ?? candle?.close);
  const close = safeNumber(candle?.c ?? candle?.close ?? candle?.price ?? candle?.last);
  const volume = safeNumber(candle?.v ?? candle?.volume ?? 0) ?? 0;

  if (time === null || time <= 0 || open === null || high === null || low === null || close === null) {
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

export function proChartVolumeColor(candle, alpha = 0.45) {
  if (!candle) return "rgba(167,176,188,0.35)";
  return chartVolumeColor(candle.close ?? candle.c, candle.open ?? candle.o, alpha);
}

export function ensureIndicatorStateForRows(state, rows) {
  const src = Array.isArray(rows) ? rows : [];
  if (!state || !Array.isArray(state.points) || state.points.length !== src.length) {
    return createIndicatorState(src);
  }
  return state;
}

export function applyIndicatorSeries({ indicatorState, overlays, vwapSeries, ema20Series, ema50Series }) {
  const state = indicatorState || createIndicatorState([]);
  const ov = overlays || {};

  if (ov.vwap && vwapSeries) {
    try { vwapSeries.setData(state.vwap || []); } catch {}
  } else {
    try { if (vwapSeries) vwapSeries.setData([]); } catch {}
  }

  if (ov.ema && ema20Series && ema50Series) {
    try { ema20Series.setData(state.ema20 || []); } catch {}
    try { ema50Series.setData(state.ema50 || []); } catch {}
  } else {
    try { if (ema20Series) ema20Series.setData([]); } catch {}
    try { if (ema50Series) ema50Series.setData([]); } catch {}
  }
}

export function updateIndicatorSeriesTail({ indicatorState, overlays, vwapSeries, ema20Series, ema50Series }) {
  const state = indicatorState || {};
  const ov = overlays || {};

  if (ov.vwap && vwapSeries && Array.isArray(state.vwap) && state.vwap.length) {
    try { vwapSeries.update(state.vwap[state.vwap.length - 1]); } catch {}
  }
  if (ov.ema && ema20Series && ema50Series) {
    if (Array.isArray(state.ema20) && state.ema20.length) {
      try { ema20Series.update(state.ema20[state.ema20.length - 1]); } catch {}
    }
    if (Array.isArray(state.ema50) && state.ema50.length) {
      try { ema50Series.update(state.ema50[state.ema50.length - 1]); } catch {}
    }
  }
}

export function formatProChartHealthText({ lastUpdateMs = 0, streamConnected = false, nowMs = Date.now() } = {}) {
  if (!lastUpdateMs) {
    return streamConnected ? "live stream" : "no data";
  }

  const age = Number(nowMs) - Number(lastUpdateMs || 0);
  if (streamConnected) {
    if (age < 1500) return "live";
    if (age < 60_000) return `live • last candle ${Math.floor(age / 1000)}s`;
    return `stale ${Math.floor(age / 1000)}s`;
  }

  if (age < 1500) return "live";
  if (age < 5000) return `lag ${Math.floor(age)}ms`;
  return `stale ${Math.floor(age / 1000)}s`;
}

export function installProChartHealthTicker(
  state,
  healthEl,
  {
    timerKey = "healthTimer",
    intervalMs = 750,
    setIntervalFn = setInterval,
    clearIntervalFn = clearInterval,
    setText = null,
  } = {}
) {
  if (!state) return null;
  try {
    if (state[timerKey]) clearIntervalFn(state[timerKey]);
  } catch {}
  state[timerKey] = null;
  if (!healthEl && !setText) return null;

  const write = typeof setText === "function"
    ? setText
    : (text) => {
        if (healthEl) healthEl.textContent = text;
      };

  state[timerKey] = setIntervalFn(() => {
    write(formatProChartHealthText(state));
  }, intervalMs);
  return state[timerKey];
}

export function clearRetryTimer(state, { timerKey = "retryTimer", clearTimeoutFn = clearTimeout } = {}) {
  try {
    if (state && state[timerKey]) clearTimeoutFn(state[timerKey]);
  } catch {}
  if (state) state[timerKey] = null;
}

export function closeEventSource(state, { esKey = "es", markDisconnected = false } = {}) {
  try {
    if (state && state[esKey] && typeof state[esKey].close === "function") state[esKey].close();
  } catch {}
  if (state) {
    state[esKey] = null;
    if (markDisconnected) state.streamConnected = false;
  }
}

export function scheduleStreamReconnect(
  state,
  {
    key,
    open,
    documentRef = _globalDocument(),
    timerKey = "retryTimer",
    backoffKey = "retryBackoffMs",
    setTimeoutFn = setTimeout,
    minMs = 250,
    maxMs = 15000,
    multiplier = 1.7,
  } = {}
) {
  if (!state || typeof open !== "function") return null;
  clearRetryTimer(state, { timerKey });
  const ms = Math.max(minMs, Math.min(maxMs, Number(state[backoffKey] || 500)));
  state[backoffKey] = Math.min(maxMs, Math.floor(ms * multiplier));
  state[timerKey] = setTimeoutFn(() => {
    state[timerKey] = null;
    if (state.key !== key) return;
    if (documentRef && documentRef.hidden) return;
    open();
  }, ms);
  return state[timerKey];
}

export function installVisibilityReconnect(
  state,
  {
    key,
    open,
    close,
    clearRetry,
    documentRef = _globalDocument(),
    handlerKey = "visHandler",
    keyKey = "visKey",
    backoffKey = "retryBackoffMs",
  } = {}
) {
  if (!state || !documentRef || typeof open !== "function") return null;
  if (state[keyKey] === key && state[handlerKey]) return state[handlerKey];

  try {
    if (state[handlerKey]) {
      documentRef.removeEventListener("visibilitychange", state[handlerKey]);
    }
  } catch {}

  state[keyKey] = key;
  state[handlerKey] = () => {
    if (state.key !== key) return;
    if (documentRef.hidden) {
      if (typeof clearRetry === "function") clearRetry();
      if (typeof close === "function") close();
    } else {
      state[backoffKey] = 500;
      open();
    }
  };

  documentRef.addEventListener("visibilitychange", state[handlerKey]);
  return state[handlerKey];
}

export function removeVisibilityHandler(state, {
  documentRef = _globalDocument(),
  handlerKey = "visHandler",
  keyKey = "visKey",
} = {}) {
  try {
    if (state && state[handlerKey] && documentRef) {
      documentRef.removeEventListener("visibilitychange", state[handlerKey]);
    }
  } catch {}
  if (state) {
    state[handlerKey] = null;
    state[keyKey] = "";
  }
}
