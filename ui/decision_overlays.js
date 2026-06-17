"use strict";

/*
  Shared normalization and rendering helpers for decision overlays on
  Lightweight Charts surfaces. This module is DOM-free so terminal and
  dashboard chart tests can exercise the production mapping directly.
*/

const COLORS = Object.freeze({
  fillBuy: "#56B4E9",
  fillSell: "#D55E00",
  intent: "#009E73",
  suppressed: "#E69F00",
  blocked: "#73B7E6",
  riskCapped: "#CC79A7",
  window: "#8B949E",
  averageCost: "#F0E442",
  stop: "#D55E00",
  takeProfit: "#009E73",
  maxRisk: "#CC79A7",
  cap: "#73B7E6",
  entry: "#56B4E9",
});

const LINE_STYLE = Object.freeze({
  solid: 0,
  dotted: 1,
  dashed: 2,
});

function finiteNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeEpochSeconds(value) {
  const n = finiteNumber(value);
  if (n == null || n <= 0) return null;
  return n > 10_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function cleanKind(value) {
  const raw = String(value || "").trim().toLowerCase().replace(/[\s-]+/g, "_");
  if (!raw) return "event";
  if (raw === "fill") return "filled";
  if (raw === "intent") return "intended";
  if (raw === "risk_cap" || raw === "capped") return "risk_capped";
  return raw;
}

function sideIsSell(side, qty = 0) {
  const raw = String(side || "").toUpperCase();
  const n = finiteNumber(qty, 0);
  return raw.includes("SELL") || raw.includes("SHORT") || n < 0;
}

export function decisionMarkerStyle(kind, side = "", qty = 0) {
  const normalizedKind = cleanKind(kind);
  const sell = sideIsSell(side, qty);

  if (normalizedKind === "filled") {
    return {
      label: sell ? "Filled sell" : "Filled buy",
      text: sell ? "FILL S" : "FILL B",
      color: sell ? COLORS.fillSell : COLORS.fillBuy,
      shape: sell ? "arrowDown" : "arrowUp",
      position: sell ? "aboveBar" : "belowBar",
    };
  }

  if (normalizedKind === "intended") {
    return {
      label: "Intended order",
      text: "INTENT",
      color: COLORS.intent,
      shape: "circle",
      position: sell ? "aboveBar" : "belowBar",
    };
  }

  if (normalizedKind === "suppressed") {
    return {
      label: "Suppressed decision",
      text: "SUPP",
      color: COLORS.suppressed,
      shape: "square",
      position: "aboveBar",
    };
  }

  if (normalizedKind === "blocked") {
    return {
      label: "Blocked decision",
      text: "BLOCK",
      color: COLORS.blocked,
      shape: "arrowDown",
      position: "aboveBar",
    };
  }

  if (normalizedKind === "risk_capped") {
    return {
      label: "Risk-capped decision",
      text: "CAP",
      color: COLORS.riskCapped,
      shape: "arrowUp",
      position: "belowBar",
    };
  }

  if (normalizedKind.includes("kill")) {
    return {
      label: "Kill-switch window",
      text: "KILL",
      color: COLORS.fillSell,
      shape: "square",
      position: "aboveBar",
    };
  }

  if (normalizedKind.includes("circuit")) {
    return {
      label: "Circuit-breaker window",
      text: "CB",
      color: COLORS.blocked,
      shape: "square",
      position: "aboveBar",
    };
  }

  if (normalizedKind.includes("drawdown")) {
    return {
      label: "Drawdown throttle window",
      text: "DD",
      color: COLORS.riskCapped,
      shape: "square",
      position: "aboveBar",
    };
  }

  if (normalizedKind.includes("suppression")) {
    return {
      label: "Suppression window",
      text: "TSE",
      color: COLORS.suppressed,
      shape: "square",
      position: "aboveBar",
    };
  }

  return {
    label: "Decision event",
    text: "EVENT",
    color: COLORS.window,
    shape: "circle",
    position: "aboveBar",
  };
}

export function normalizeDecisionMarker(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  const kind = cleanKind(source.kind || source.type);
  const time = normalizeEpochSeconds(source.t ?? source.time ?? source.ts ?? source.ts_ms);
  if (time == null) return null;

  const qty = finiteNumber(source.qty ?? source.size, 0);
  const style = decisionMarkerStyle(kind, source.side, qty);
  const text = String(source.text || style.text).slice(0, 12);
  const price = finiteNumber(source.price ?? source.px);

  return {
    ...source,
    t: time,
    time,
    ts: time,
    ts_ms: finiteNumber(source.ts_ms, time * 1000),
    kind,
    side: String(source.side || "").toUpperCase(),
    qty,
    size: finiteNumber(source.size, Math.abs(qty)),
    price,
    px: price,
    reason_code: String(source.reason_code || "unknown").trim() || "unknown",
    color: String(source.color || style.color),
    shape: String(source.shape || style.shape),
    position: String(source.position || style.position),
    text,
    label: String(source.label || style.label),
  };
}

export function toLightweightMarkers(markers) {
  return (Array.isArray(markers) ? markers : [])
    .map(normalizeDecisionMarker)
    .filter(Boolean)
    .map((marker) => ({
      time: marker.time,
      position: marker.position,
      shape: marker.shape,
      color: marker.color,
      text: marker.text,
      id: marker.id != null ? String(marker.id) : undefined,
    }));
}

export function normalizePriceLine(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  const price = finiteNumber(source.price ?? source.px ?? source.value);
  if (price == null) return null;
  const kind = cleanKind(source.kind || source.type || source.level_type);
  const color = source.color || (
    kind.includes("average") || kind.includes("avg") ? COLORS.averageCost :
      kind.includes("stop") ? COLORS.stop :
        kind.includes("take") || kind.includes("profit") ? COLORS.takeProfit :
          kind.includes("risk") ? COLORS.maxRisk :
            kind.includes("cap") ? COLORS.cap :
              COLORS.entry
  );
  const title = String(source.title || source.label || kind.replace(/_/g, " ")).slice(0, 36);
  return {
    ...source,
    kind,
    price,
    color,
    title,
    lineWidth: Math.max(1, Math.min(4, Math.round(finiteNumber(source.lineWidth, source.line_width ?? 1)))),
    lineStyle: finiteNumber(source.lineStyle, source.line_style ?? (kind.includes("cap") || kind.includes("risk") ? LINE_STYLE.dashed : LINE_STYLE.solid)),
    axisLabelVisible: source.axisLabelVisible !== false,
  };
}

export function normalizeWindow(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  const start = finiteNumber(source.start_ts_ms ?? source.ts_ms ?? source.start);
  if (start == null || start <= 0) return null;
  const end = finiteNumber(source.end_ts_ms ?? source.end);
  const kind = cleanKind(source.kind || source.type || "window");
  return {
    ...source,
    kind,
    start_ts_ms: start,
    end_ts_ms: end != null && end > start ? end : null,
    reason_code: String(source.reason_code || kind).trim() || kind,
    label: String(source.label || kind.replace(/_/g, " ")),
  };
}

export function normalizeDecisionOverlayPayload(payload) {
  const source = payload && typeof payload === "object" ? payload : {};
  const markers = (Array.isArray(source.markers) ? source.markers : [])
    .map(normalizeDecisionMarker)
    .filter(Boolean)
    .sort((a, b) => Number(a.time) - Number(b.time));
  const priceLines = (Array.isArray(source.price_lines) ? source.price_lines : Array.isArray(source.priceLines) ? source.priceLines : [])
    .map(normalizePriceLine)
    .filter(Boolean);
  const windows = (Array.isArray(source.windows) ? source.windows : [])
    .map(normalizeWindow)
    .filter(Boolean)
    .sort((a, b) => Number(a.start_ts_ms) - Number(b.start_ts_ms));

  return {
    ...source,
    markers,
    price_lines: priceLines,
    priceLines,
    windows,
  };
}

export function applyPriceLinesToSeries(series, existingHandles, priceLines) {
  const handles = Array.isArray(existingHandles) ? existingHandles : [];
  if (series && typeof series.removePriceLine === "function") {
    for (const handle of handles) {
      try { series.removePriceLine(handle); } catch {}
    }
  }
  if (!series || typeof series.createPriceLine !== "function") return [];

  const next = [];
  for (const line of (Array.isArray(priceLines) ? priceLines : []).map(normalizePriceLine).filter(Boolean)) {
    try {
      next.push(series.createPriceLine({
        price: line.price,
        color: line.color,
        lineWidth: line.lineWidth,
        lineStyle: line.lineStyle,
        axisLabelVisible: line.axisLabelVisible,
        title: line.title,
      }));
    } catch {}
  }
  return next;
}

export function decisionOverlayLegendItems(payload) {
  const normalized = normalizeDecisionOverlayPayload(payload);
  const counts = new Map();
  for (const marker of normalized.markers) {
    counts.set(marker.kind, (counts.get(marker.kind) || 0) + 1);
  }
  const kinds = ["filled", "intended", "suppressed", "blocked", "risk_capped"];
  return kinds.map((kind) => {
    const style = decisionMarkerStyle(kind);
    return {
      kind,
      label: style.label,
      text: style.text,
      color: style.color,
      shape: style.shape,
      count: counts.get(kind) || 0,
    };
  });
}

export function buildOverlayAccessibilitySummary(payload) {
  const normalized = normalizeDecisionOverlayPayload(payload);
  const counts = new Map();
  for (const marker of normalized.markers) {
    counts.set(marker.kind, (counts.get(marker.kind) || 0) + 1);
  }
  const parts = [];
  for (const [kind, label] of [
    ["filled", "filled"],
    ["intended", "intended"],
    ["suppressed", "suppressed"],
    ["blocked", "blocked"],
    ["risk_capped", "risk-capped"],
  ]) {
    const count = counts.get(kind) || 0;
    if (count) parts.push(`${count} ${label}`);
  }
  if (normalized.windows.length) parts.push(`${normalized.windows.length} active or recent windows`);
  if (normalized.price_lines.length) parts.push(`${normalized.price_lines.length} price levels`);
  return parts.length ? `Decision overlays: ${parts.join(", ")}.` : "Decision overlays: no automated decision events in the loaded range.";
}

function normalizeCandlePoint(point) {
  const source = point && typeof point === "object" ? point : {};
  const time = finiteNumber(source.time ?? source.t);
  const close = finiteNumber(source.close ?? source.c ?? source.value);
  const volume = finiteNumber(source.volume ?? source.v, 0);
  if (time == null || close == null) return null;
  return { time, close, volume: Math.max(0, volume || 0) };
}

function computeIndicatorPoint(base, point) {
  const prev = base || { pv: 0, vv: 0, ema20: null, ema50: null };
  const pv = prev.pv + (point.close * point.volume);
  const vv = prev.vv + point.volume;
  const ema20 = prev.ema20 == null ? point.close : (point.close * (2 / 21)) + (prev.ema20 * (1 - (2 / 21)));
  const ema50 = prev.ema50 == null ? point.close : (point.close * (2 / 51)) + (prev.ema50 * (1 - (2 / 51)));
  return {
    time: point.time,
    close: point.close,
    volume: point.volume,
    pv,
    vv,
    vwap: vv > 0 ? pv / vv : point.close,
    ema20,
    ema50,
  };
}

function trimIndicatorState(state, limit) {
  const maxLen = Math.max(0, Number(limit || 0));
  if (!maxLen || state.points.length <= maxLen) return state;
  const drop = state.points.length - maxLen;
  state.points.splice(0, drop);
  state.vwap.splice(0, drop);
  state.ema20.splice(0, drop);
  state.ema50.splice(0, drop);

  const rebuilt = createIndicatorState(state.points.map((p) => ({
    time: p.time,
    close: p.close,
    volume: p.volume,
  })));
  state.points = rebuilt.points;
  state.vwap = rebuilt.vwap;
  state.ema20 = rebuilt.ema20;
  state.ema50 = rebuilt.ema50;
  return state;
}

export function createIndicatorState(candles = []) {
  const state = { points: [], vwap: [], ema20: [], ema50: [] };
  for (const item of Array.isArray(candles) ? candles : []) {
    const point = normalizeCandlePoint(item);
    if (!point) continue;
    const computed = computeIndicatorPoint(state.points[state.points.length - 1] || null, point);
    state.points.push(computed);
    state.vwap.push({ time: computed.time, value: computed.vwap });
    state.ema20.push({ time: computed.time, value: computed.ema20 });
    state.ema50.push({ time: computed.time, value: computed.ema50 });
  }
  return state;
}

export function updateIndicatorState(state, candle, { limit = 1500 } = {}) {
  const point = normalizeCandlePoint(candle);
  if (!point) return { state, needsRebuild: false };
  const target = state && Array.isArray(state.points) ? state : createIndicatorState([]);
  const last = target.points[target.points.length - 1] || null;

  if (!last) {
    const computed = computeIndicatorPoint(null, point);
    target.points.push(computed);
    target.vwap.push({ time: computed.time, value: computed.vwap });
    target.ema20.push({ time: computed.time, value: computed.ema20 });
    target.ema50.push({ time: computed.time, value: computed.ema50 });
    return { state: trimIndicatorState(target, limit), needsRebuild: false };
  }

  if (point.time < last.time) {
    return { state: target, needsRebuild: true };
  }

  const replaceLast = point.time === last.time;
  const base = replaceLast ? (target.points[target.points.length - 2] || null) : last;
  const computed = computeIndicatorPoint(base, point);

  if (replaceLast) {
    target.points[target.points.length - 1] = computed;
    target.vwap[target.vwap.length - 1] = { time: computed.time, value: computed.vwap };
    target.ema20[target.ema20.length - 1] = { time: computed.time, value: computed.ema20 };
    target.ema50[target.ema50.length - 1] = { time: computed.time, value: computed.ema50 };
  } else {
    target.points.push(computed);
    target.vwap.push({ time: computed.time, value: computed.vwap });
    target.ema20.push({ time: computed.time, value: computed.ema20 });
    target.ema50.push({ time: computed.time, value: computed.ema50 });
  }

  return { state: trimIndicatorState(target, limit), needsRebuild: false };
}
