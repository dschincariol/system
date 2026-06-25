"use strict";

import { installChartPointInspector, renderChartAccessibility } from "./chart_a11y.js";
import { chartMarkerStyle, statusToken } from "./utils.js";

const CHART_COLORS = Object.freeze({
  bg: "#0a0d12",
  border: "#30363d",
  grid: "#20252c",
  axis: "#9da7b1",
  wick: "#9da7b1",
  up: statusToken("info").color,
  down: statusToken("crit").color,
  neutral: statusToken("neutral").color,
  selected: "rgba(255,255,255,.55)",
});

const STATE = {
  payload: null,
  selectedIndex: 0,
  fetchJSON: null,
  root: null,
  getSymbol: null,
};

function _arr(value) {
  return Array.isArray(value) ? value : [];
}

function _num(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function _tsMs(value) {
  const n = _num(value, null);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n < 10000000000 ? Math.round(n * 1000) : Math.round(n);
}

function _esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[ch]));
}

function _fmtNum(value, digits = 2) {
  const n = _num(value, null);
  return Number.isFinite(n) ? n.toFixed(digits) : "-";
}

function _fmtPct(value) {
  const n = _num(value, null);
  return Number.isFinite(n) ? `${(n * 100).toFixed(2)}%` : "-";
}

function _fmtTime(tsMs) {
  const ts = _tsMs(tsMs);
  if (!ts) return "-";
  try {
    return new Date(ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return String(ts);
  }
}

function _fmtOhlc(candle) {
  if (!candle) return "-";
  return `O ${_fmtNum(candle.open, 2)} / H ${_fmtNum(candle.high, 2)} / L ${_fmtNum(candle.low, 2)} / C ${_fmtNum(candle.close, 2)}`;
}

function _todayLocalISO() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function _normalizeCandle(row) {
  if (!row || typeof row !== "object") return null;
  const ts = _tsMs(row.ts_ms ?? row.ts ?? row.time ?? row.t);
  const close = _num(row.close ?? row.c ?? row.price ?? row.value, null);
  if (!ts || !Number.isFinite(close)) return null;
  const open = _num(row.open ?? row.o, close);
  const rawHigh = _num(row.high ?? row.h, Math.max(open, close));
  const rawLow = _num(row.low ?? row.l, Math.min(open, close));
  const high = Math.max(rawHigh, open, close);
  const low = Math.min(rawLow, open, close);
  const normalized = {
    ...row,
    ts_ms: ts,
    t: Math.floor(ts / 1000),
    open,
    high,
    low,
    close,
    volume: _num(row.volume ?? row.v, 0) || 0,
  };
  if (high !== rawHigh || low !== rawLow) {
    normalized.raw_high = rawHigh;
    normalized.raw_low = rawLow;
    normalized.ohlc_corrected = true;
  }
  return normalized;
}

function _normalizeEvent(row, kind) {
  if (!row || typeof row !== "object") return null;
  const ts = _tsMs(row.ts_ms ?? row.ts ?? row.time ?? row.t);
  if (!ts) return null;
  return {
    ...row,
    ts_ms: ts,
    t: Math.floor(ts / 1000),
    kind,
  };
}

function _uniqueSorted(values) {
  return Array.from(new Set(values.filter((v) => Number.isFinite(v) && v > 0))).sort((a, b) => a - b);
}

function _nearestIndex(values, target) {
  if (!values.length) return -1;
  const t = Number.isFinite(target) ? target : values[0];
  let best = 0;
  let bestDist = Math.abs(values[0] - t);
  for (let i = 1; i < values.length; i += 1) {
    const dist = Math.abs(values[i] - t);
    if (dist < bestDist) {
      best = i;
      bestDist = dist;
    }
  }
  return best;
}

function _nearestAtOrBefore(rows, selectedTsMs) {
  let best = null;
  for (const row of rows || []) {
    const ts = _tsMs(row && row.ts_ms);
    if (!ts || ts > selectedTsMs) continue;
    if (!best || ts > best.ts_ms) best = row;
  }
  return best;
}

function _nearestCandle(candles, selectedTsMs) {
  const idx = _nearestIndex((candles || []).map((c) => c.ts_ms), selectedTsMs);
  return idx >= 0 ? candles[idx] : null;
}

function _candleAtOrBefore(candles, selectedTsMs) {
  let best = null;
  for (const candle of candles || []) {
    const ts = _tsMs(candle && candle.ts_ms);
    if (!ts || ts > selectedTsMs) continue;
    if (!best || ts > best.ts_ms) best = candle;
  }
  return best || _nearestCandle(candles, selectedTsMs);
}

function _nearby(rows, selectedTsMs, windowMs = 15 * 60 * 1000) {
  return (rows || [])
    .filter((row) => {
      const ts = _tsMs(row && row.ts_ms);
      return ts && Math.abs(ts - selectedTsMs) <= windowMs;
    })
    .sort((a, b) => Math.abs(a.ts_ms - selectedTsMs) - Math.abs(b.ts_ms - selectedTsMs))
    .slice(0, 12);
}

function _clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function _hexToRgba(hex, alpha = 1) {
  const match = String(hex || "").match(/^#([0-9a-f]{6})$/i);
  if (!match) return String(hex || "");
  const raw = match[1];
  const r = Number.parseInt(raw.slice(0, 2), 16);
  const g = Number.parseInt(raw.slice(2, 4), 16);
  const b = Number.parseInt(raw.slice(4, 6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

function _textWidth(ctx, text) {
  if (ctx && typeof ctx.measureText === "function") {
    try {
      return ctx.measureText(String(text)).width;
    } catch {}
  }
  return String(text || "").length * 6;
}

function _hasGap(gaps, stream) {
  return (gaps || []).some((gap) => String(gap && gap.stream) === stream);
}

function _inferredGaps(streams, existingGaps) {
  const gaps = [..._arr(existingGaps)];
  const specs = [
    ["price", "no_price_data", "Price data is missing for this replay."],
    ["decisions", "no_decision_data", "Decision records are missing for this replay."],
    ["orders", "no_order_data", "Order records are missing for this replay."],
    ["fills", "no_fill_data", "Fill records are missing for this replay."],
    ["risk", "risk_history_missing", "Risk history is missing for this replay."],
  ];
  const streamMap = {
    price: streams.candles,
    decisions: streams.decisions,
    orders: streams.orders,
    fills: streams.fills,
    risk: streams.risk,
  };
  for (const [stream, code, message] of specs) {
    if (!_arr(streamMap[stream]).length && !_hasGap(gaps, stream)) {
      gaps.push({ stream, code, message, severity: stream === "price" ? "warn" : "info" });
    }
  }
  return gaps;
}

function _buildMarkerEvents(streams) {
  const markers = [];
  for (const row of streams.decisions) {
    markers.push({ ...row, markerKind: "decision", label: row.label || row.model_name || "D" });
  }
  for (const row of streams.orders) {
    markers.push({ ...row, markerKind: "order", label: row.action || row.state || "O" });
  }
  for (const row of streams.fills) {
    markers.push({ ...row, markerKind: "fill", label: row.side || "F", price: _num(row.price, null) });
  }
  return markers.sort((a, b) => a.ts_ms - b.ts_ms);
}

export function buildReplayViewModel(payload = {}, options = {}) {
  const rawStreams = payload.streams && typeof payload.streams === "object" ? payload.streams : {};
  const streams = {
    candles: _arr(payload.candles ?? rawStreams.candles).map(_normalizeCandle).filter(Boolean),
    decisions: _arr(payload.decisions ?? rawStreams.decisions).map((row) => _normalizeEvent(row, "decision")).filter(Boolean),
    orders: _arr(payload.orders ?? rawStreams.orders).map((row) => _normalizeEvent(row, "order")).filter(Boolean),
    fills: _arr(payload.fills ?? rawStreams.fills).map((row) => _normalizeEvent(row, "fill")).filter(Boolean),
    risk: _arr(payload.risk ?? rawStreams.risk).map((row) => _normalizeEvent(row, "risk")).filter(Boolean),
    pnl: _arr(payload.pnl ?? rawStreams.pnl).map((row) => _normalizeEvent(row, "pnl")).filter(Boolean),
  };
  streams.candles.sort((a, b) => a.ts_ms - b.ts_ms);
  for (const key of ["decisions", "orders", "fills", "risk", "pnl"]) {
    streams[key].sort((a, b) => a.ts_ms - b.ts_ms);
  }

  const timeline = _uniqueSorted([
    ...streams.candles.map((row) => row.ts_ms),
    ...streams.decisions.map((row) => row.ts_ms),
    ...streams.orders.map((row) => row.ts_ms),
    ...streams.fills.map((row) => row.ts_ms),
    ...streams.risk.map((row) => row.ts_ms),
    ...streams.pnl.map((row) => row.ts_ms),
  ]);
  const requestedTs = _tsMs(options.selectedTsMs);
  const selectedIndex = _nearestIndex(timeline, requestedTs || timeline[0]);
  const selectedTsMs = selectedIndex >= 0 ? timeline[selectedIndex] : null;
  const selectedCandle = selectedTsMs ? _candleAtOrBefore(streams.candles, selectedTsMs) : null;
  const selectedRisk = selectedTsMs ? _nearestAtOrBefore(streams.risk, selectedTsMs) : null;
  const selectedPnl = selectedTsMs ? _nearestAtOrBefore(streams.pnl, selectedTsMs) : null;
  const riskAgeMs = selectedTsMs && selectedRisk ? selectedTsMs - selectedRisk.ts_ms : null;
  const pnlAgeMs = selectedTsMs && selectedPnl ? selectedTsMs - selectedPnl.ts_ms : null;

  const counts = {
    candles: streams.candles.length,
    decisions: streams.decisions.length,
    orders: streams.orders.length,
    fills: streams.fills.length,
    risk: streams.risk.length,
    pnl: streams.pnl.length,
  };
  const gaps = _inferredGaps(streams, payload.gaps);
  const ready = Object.values(counts).some((count) => count > 0);

  return {
    ok: payload.ok !== false,
    readOnly: payload.read_only !== false,
    ready,
    partial: !!(payload.meta && payload.meta.partial) || gaps.length > 0,
    noData: !ready,
    date: String(payload.date || ""),
    symbol: String(payload.symbol || payload.filters?.symbol || ""),
    modelId: String(payload.model_id || payload.filters?.model_id || ""),
    range: payload.range || {},
    counts,
    streams,
    gaps,
    timeline,
    selected: {
      index: selectedIndex,
      ts_ms: selectedTsMs,
      timeLabel: selectedTsMs ? _fmtTime(selectedTsMs) : "-",
      price: selectedCandle ? selectedCandle.close : null,
      candle: selectedCandle,
      risk: selectedRisk,
      pnl: selectedPnl,
      riskStale: Number.isFinite(riskAgeMs) && riskAgeMs > 30 * 60 * 1000,
      pnlStale: Number.isFinite(pnlAgeMs) && pnlAgeMs > 30 * 60 * 1000,
    },
    nearby: {
      decisions: selectedTsMs ? _nearby(streams.decisions, selectedTsMs) : [],
      orders: selectedTsMs ? _nearby(streams.orders, selectedTsMs) : [],
      fills: selectedTsMs ? _nearby(streams.fills, selectedTsMs) : [],
    },
    markers: _buildMarkerEvents(streams),
  };
}

function _root(root) {
  return root || (typeof document !== "undefined" ? document : null);
}

function _el(root, id) {
  const r = _root(root);
  return r && typeof r.getElementById === "function" ? r.getElementById(id) : null;
}

function _setText(root, id, text) {
  const el = _el(root, id);
  if (el) el.textContent = String(text ?? "");
}

function _setHTML(root, id, html) {
  const el = _el(root, id);
  if (el) el.innerHTML = String(html ?? "");
}

function _statusClass(vm) {
  if (!vm.ready) return "pill warn";
  return vm.partial ? "pill warn" : "pill ok";
}

function _eventLabel(row) {
  if (!row) return "";
  if (row.kind === "decision") {
    const conf = _num(row.confidence, null);
    return `${row.label || "decision"} ${conf == null ? "" : `conf ${_fmtNum(conf, 2)}`}`.trim();
  }
  if (row.kind === "order") {
    return `${row.action || row.state || "order"} ${row.delta_weight == null ? "" : _fmtPct(row.delta_weight)}`.trim();
  }
  if (row.kind === "fill") {
    return `${row.side || "fill"} ${_fmtNum(row.qty, 2)} @ ${_fmtNum(row.price, 2)}`;
  }
  return String(row.kind || "");
}

function _renderEvents(vm) {
  const events = [
    ...vm.nearby.decisions,
    ...vm.nearby.orders,
    ...vm.nearby.fills,
  ].sort((a, b) => Math.abs(a.ts_ms - vm.selected.ts_ms) - Math.abs(b.ts_ms - vm.selected.ts_ms));
  if (!events.length) return '<div class="small dim">(no events near selected time)</div>';
  return `
    <table>
      <thead><tr><th>Time</th><th>Stream</th><th>Symbol</th><th>Detail</th><th>Source</th></tr></thead>
      <tbody>
        ${events.map((row) => `
          <tr>
            <td class="mono">${_esc(_fmtTime(row.ts_ms))}</td>
            <td>${_esc(row.kind)}</td>
            <td>${_esc(row.symbol || "")}</td>
            <td>${_esc(_eventLabel(row))}</td>
            <td>${_esc(row.source_table || "")}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function _renderGaps(vm) {
  if (!vm.gaps.length) return '<span class="pill ok">complete</span>';
  return vm.gaps.map((gap) => `
    <div class="replayGap ${_esc(gap.severity || "info")}">
      <span class="mono">${_esc(gap.stream || "replay")}</span>
      <span>${_esc(gap.message || gap.code || "gap")}</span>
    </div>
  `).join("");
}

function _renderSelected(vm) {
  const risk = vm.selected.risk || {};
  const pnl = vm.selected.pnl || {};
  const candle = vm.selected.candle || null;
  const riskSuffix = vm.selected.riskStale ? " (stale)" : "";
  const pnlSuffix = vm.selected.pnlStale ? " (stale)" : "";
  return `
    <div class="replayStat"><span>Time</span><b class="mono">${_esc(vm.selected.timeLabel)}</b></div>
    <div class="replayStat"><span>OHLC</span><b class="mono">${_esc(_fmtOhlc(candle))}</b></div>
    <div class="replayStat"><span>Risk gross/net${_esc(riskSuffix)}</span><b>${_fmtPct(risk.gross)} / ${_fmtPct(risk.net)}</b></div>
    <div class="replayStat"><span>Drawdown</span><b>${_fmtPct(risk.drawdown)}</b></div>
    <div class="replayStat"><span>Equity${_esc(pnlSuffix)}</span><b>${_fmtNum(pnl.equity, 2)}</b></div>
    <div class="replayStat"><span>Day PnL</span><b>${_fmtNum(pnl.day_pnl, 2)}</b></div>
  `;
}

export function replayMarkerStyle(kind, row = {}) {
  if (kind === "decision") return { color: statusToken("info").color, shape: "circle", label: "Decision" };
  if (kind === "order") return { color: statusToken("warn").color, shape: "square", label: "Order" };
  if (kind === "fill") {
    const marker = chartMarkerStyle(row.side || row.label, Number(row.qty || 0));
    return { color: marker.color, shape: marker.isBuy ? "triangle-up" : "triangle-down", label: marker.label };
  }
  return { color: statusToken("neutral").color, shape: "circle", label: "Marker" };
}

function _markerAnchor(candles, marker) {
  const own = _num(marker.price, null);
  if (Number.isFinite(own)) {
    return { price: own, source: "event_price" };
  }
  const candle = _nearestCandle(candles, marker.ts_ms);
  return candle ? { price: candle.close, source: "nearest_close", candleTsMs: candle.ts_ms } : { price: null, source: "unavailable" };
}

function _chartPriceValues(candles, markers) {
  const prices = [];
  for (const candle of candles || []) {
    prices.push(candle.open, candle.high, candle.low, candle.close);
  }
  for (const marker of markers || []) {
    const own = _num(marker.price, null);
    if (Number.isFinite(own)) prices.push(own);
  }
  return prices.filter(Number.isFinite);
}

function _timeTicks(minTs, maxTs) {
  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs)) return [];
  if (minTs === maxTs) return [{ value: minTs, label: _fmtTime(minTs) }];
  const mid = Math.round(minTs + ((maxTs - minTs) / 2));
  return _uniqueSorted([minTs, mid, maxTs]).map((value) => ({ value, label: _fmtTime(value) }));
}

function _priceTicks(minP, maxP, yFor) {
  if (!Number.isFinite(minP) || !Number.isFinite(maxP)) return [];
  const mid = minP + ((maxP - minP) / 2);
  return [maxP, mid, minP].map((value) => ({ value, y: yFor(value), label: _fmtNum(value, 2) }));
}

export function buildReplayChartModel(vm, options = {}) {
  const cssW = Math.max(320, Math.floor(options.width || 960));
  const cssH = Math.max(220, Math.floor(options.height || 280));
  const candles = vm && vm.streams && Array.isArray(vm.streams.candles) ? vm.streams.candles : [];
  const noDataText = vm && vm.noData ? "(no replay data)" : "(no price series)";
  if (!candles.length) {
    return {
      ok: false,
      state: vm && vm.noData ? "empty" : "no_price_series",
      width: cssW,
      height: cssH,
      noDataText,
      candles: [],
      markers: [],
      xTicks: [],
      yTicks: [],
      legend: [],
    };
  }

  const padL = 52;
  const padR = 18;
  const padT = 42;
  const padB = 34;
  const minTs = candles[0].ts_ms;
  const maxTs = candles[candles.length - 1].ts_ms;
  const visibleMarkers = (vm.markers || [])
    .filter((marker) => marker && marker.ts_ms >= minTs && marker.ts_ms <= maxTs);
  const prices = _chartPriceValues(candles, visibleMarkers);
  if (!prices.length) {
    return {
      ok: false,
      state: "no_numeric_prices",
      width: cssW,
      height: cssH,
      noDataText,
      candles: [],
      markers: [],
      xTicks: [],
      yTicks: [],
      legend: [],
    };
  }

  let minP = Math.min(...prices);
  let maxP = Math.max(...prices);
  if (minP === maxP) {
    minP -= 1;
    maxP += 1;
  }
  const padP = (maxP - minP) * 0.08;
  minP -= padP;
  maxP += padP;
  const plotW = Math.max(1, cssW - padL - padR);
  const plotH = Math.max(1, cssH - padT - padB);
  const xFor = (ts) => {
    if (minTs === maxTs) return padL + (plotW / 2);
    return padL + plotW * ((ts - minTs) / Math.max(1, maxTs - minTs));
  };
  const yFor = (price) => padT + plotH * (1 - ((price - minP) / Math.max(1e-9, maxP - minP)));
  const spacing = candles.length > 1 ? plotW / Math.max(1, candles.length - 1) : Math.min(plotW, 12);
  const bodyWidth = _clamp(spacing * 0.55, 2, 12);

  const candleModels = candles.map((candle) => {
    const x = xFor(candle.ts_ms);
    const openY = yFor(candle.open);
    const highY = yFor(candle.high);
    const lowY = yFor(candle.low);
    const closeY = yFor(candle.close);
    const up = candle.close >= candle.open;
    const color = up ? CHART_COLORS.up : CHART_COLORS.down;
    const bodyTop = Math.min(openY, closeY);
    const rawBodyHeight = Math.abs(closeY - openY);
    const bodyHeight = Math.max(1, rawBodyHeight);
    return {
      ...candle,
      x,
      openY,
      highY,
      lowY,
      closeY,
      bodyX: x - (bodyWidth / 2),
      bodyY: rawBodyHeight < 1 ? bodyTop - 0.5 : bodyTop,
      bodyWidth,
      bodyHeight,
      up,
      color,
      fillColor: _hexToRgba(color, 0.22),
    };
  });

  const markerModels = visibleMarkers.map((marker) => {
    const anchor = _markerAnchor(candles, marker);
    if (!Number.isFinite(anchor.price)) return null;
    const style = replayMarkerStyle(marker.markerKind, marker);
    return {
      ...marker,
      anchor,
      price: anchor.price,
      x: xFor(marker.ts_ms),
      y: yFor(anchor.price),
      style,
    };
  }).filter(Boolean);

  const selectedTs = vm && vm.selected ? _tsMs(vm.selected.ts_ms) : null;
  const selected = selectedTs && selectedTs >= minTs && selectedTs <= maxTs
    ? { ts_ms: selectedTs, x: xFor(selectedTs), label: _fmtTime(selectedTs) }
    : null;

  return {
    ok: true,
    state: "ready",
    width: cssW,
    height: cssH,
    plot: { left: padL, right: cssW - padR, top: padT, bottom: cssH - padB, width: plotW, height: plotH },
    domain: { minTs, maxTs, minP, maxP },
    noDataText,
    candles: candleModels,
    markers: markerModels,
    selected,
    xTicks: _timeTicks(minTs, maxTs).map((tick) => ({ ...tick, x: xFor(tick.value) })),
    yTicks: _priceTicks(minP, maxP, yFor),
    legend: [
      { kind: "ohlc", label: "OHLC", color: CHART_COLORS.up },
      { kind: "decision", label: "D decision", color: replayMarkerStyle("decision").color },
      { kind: "order", label: "O order", color: replayMarkerStyle("order").color },
      { kind: "fill", label: "F fill", color: replayMarkerStyle("fill", { side: "BUY", qty: 1 }).color },
      { kind: "selected", label: "selected time", color: CHART_COLORS.selected },
    ],
  };
}

function _renderReplayChartA11y(canvas, vm, errorMessage = "") {
  if (!canvas || !vm) return;
  const candles = vm.streams && Array.isArray(vm.streams.candles) ? vm.streams.candles : [];
  const symbol = vm.symbol || "selected symbol";
  const series = candles.map((c) => ({
    time: c.ts_ms,
    value: c.close,
    open: c.open,
    high: c.high,
    low: c.low,
    close: c.close,
    volume: c.volume,
  }));
  const summary = series.length
    ? `Historical replay: ${symbol} OHLC candles latest ${_fmtOhlc(series[series.length - 1])} across ${series.length} candles; decisions ${vm.counts.decisions}, orders ${vm.counts.orders}, fills ${vm.counts.fills}.`
    : "";
  const gapMessage = errorMessage || (!vm.ok && vm.gaps.length ? String(vm.gaps[0].message || vm.gaps[0].code || "Replay data is unavailable.") : "");
  renderChartAccessibility(canvas, {
    title: "Historical replay",
    series,
    timeKey: "time",
    valueKey: "value",
    valueLabel: "close",
    seriesFields: [
      { key: "open", label: "Open", formatter: (v) => _fmtNum(v, 2) },
      { key: "high", label: "High", formatter: (v) => _fmtNum(v, 2) },
      { key: "low", label: "Low", formatter: (v) => _fmtNum(v, 2) },
      { key: "close", label: "Close", formatter: (v) => _fmtNum(v, 2) },
    ],
    valueFormatter: (v) => _fmtNum(v, 2),
    summary,
    emptyMessage: vm.noData ? "No replay data is available for the selected filters." : "No replay price series is available.",
    errorMessage: gapMessage,
    chartType: "canvas-replay",
    columns: [
      { label: "Time", value: (row) => _fmtTime(row.raw && row.raw.time) },
      { label: "Open", value: (row) => _fmtNum(row.raw && row.raw.open, 2) },
      { label: "High", value: (row) => _fmtNum(row.raw && row.raw.high, 2) },
      { label: "Low", value: (row) => _fmtNum(row.raw && row.raw.low, 2) },
      { label: "Close", value: (row) => _fmtNum(row.raw && row.raw.close, 2) },
      { label: "Volume", value: (row) => _fmtNum(row.raw && row.raw.volume, 0) },
    ],
  });
}

function _drawReplayLegend(ctx, model) {
  ctx.font = "11px Consolas, monospace";
  let x = model.plot.left;
  let y = 15;
  const maxX = model.width - 12;
  for (const item of model.legend || []) {
    const label = String(item.label || "");
    const itemW = _textWidth(ctx, label) + 24;
    if (x > model.plot.left && x + itemW > maxX) {
      x = model.plot.left;
      y += 14;
    }
    ctx.fillStyle = item.color || CHART_COLORS.axis;
    ctx.strokeStyle = item.color || CHART_COLORS.axis;
    ctx.lineWidth = 1.4;
    if (item.kind === "ohlc") {
      ctx.beginPath();
      ctx.moveTo(x + 5, y - 6);
      ctx.lineTo(x + 5, y + 3);
      ctx.stroke();
      ctx.fillRect(x + 2, y - 3, 7, 5);
    } else if (item.kind === "order") {
      ctx.fillRect(x + 1, y - 7, 8, 8);
    } else if (item.kind === "fill") {
      ctx.beginPath();
      ctx.moveTo(x + 5, y - 8);
      ctx.lineTo(x + 10, y + 2);
      ctx.lineTo(x, y + 2);
      ctx.closePath();
      ctx.fill();
    } else if (item.kind === "selected") {
      ctx.beginPath();
      ctx.moveTo(x + 5, y - 8);
      ctx.lineTo(x + 5, y + 3);
      ctx.stroke();
    } else {
      ctx.beginPath();
      ctx.arc(x + 5, y - 3, 4, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.fillStyle = CHART_COLORS.axis;
    ctx.fillText(label, x + 14, y);
    x += itemW;
  }
}

export function renderReplayChart(canvas, vm) {
  if (!canvas || !vm) return;
  const ctx = canvas.getContext && canvas.getContext("2d");
  if (!ctx) {
    _renderReplayChartA11y(canvas, vm, "Replay chart could not get a canvas context.");
    return;
  }
  const dpr = typeof window !== "undefined" ? window.devicePixelRatio : 1;
  const ratio = Math.max(1, Math.min(2, Number(dpr || 1)));
  const cssW = Math.max(320, Math.floor(canvas.clientWidth || canvas.width || 960));
  const cssH = Math.max(220, Math.floor(canvas.clientHeight || canvas.height || 280));
  if (canvas.width !== Math.floor(cssW * ratio)) canvas.width = Math.floor(cssW * ratio);
  if (canvas.height !== Math.floor(cssH * ratio)) canvas.height = Math.floor(cssH * ratio);
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);
  ctx.fillStyle = CHART_COLORS.bg;
  ctx.fillRect(0, 0, cssW, cssH);
  ctx.strokeStyle = CHART_COLORS.border;
  ctx.strokeRect(0.5, 0.5, cssW - 1, cssH - 1);

  const model = buildReplayChartModel(vm, { width: cssW, height: cssH });
  if (!model.ok) {
    ctx.fillStyle = CHART_COLORS.axis;
    ctx.font = "12px Consolas, monospace";
    ctx.fillText(model.noDataText, 12, 24);
    _renderReplayChartA11y(canvas, vm);
    installChartPointInspector(canvas, [], {
      title: "Historical replay",
      kind: "canvas-replay",
      emptyMessage: model.noDataText,
    });
    return;
  }

  _drawReplayLegend(ctx, model);

  ctx.strokeStyle = CHART_COLORS.grid;
  ctx.lineWidth = 1;
  for (const tick of model.yTicks || []) {
    ctx.beginPath();
    ctx.moveTo(model.plot.left, tick.y);
    ctx.lineTo(model.plot.right, tick.y);
    ctx.stroke();
  }

  ctx.fillStyle = CHART_COLORS.axis;
  ctx.font = "12px Consolas, monospace";
  for (const tick of model.yTicks || []) {
    ctx.fillText(tick.label, 8, tick.y + 4);
  }

  ctx.fillStyle = CHART_COLORS.axis;
  for (const tick of model.xTicks || []) {
    const width = _textWidth(ctx, tick.label);
    const x = _clamp(tick.x - (width / 2), model.plot.left, model.plot.right - width);
    ctx.fillText(tick.label, x, cssH - 10);
  }

  ctx.lineWidth = 1.2;
  for (const candle of model.candles || []) {
    ctx.strokeStyle = candle.color || CHART_COLORS.wick;
    ctx.fillStyle = candle.fillColor || candle.color || CHART_COLORS.neutral;
    ctx.beginPath();
    ctx.moveTo(candle.x, candle.highY);
    ctx.lineTo(candle.x, candle.lowY);
    ctx.stroke();
    ctx.fillRect(candle.bodyX, candle.bodyY, candle.bodyWidth, candle.bodyHeight);
    ctx.strokeRect(candle.bodyX, candle.bodyY, candle.bodyWidth, candle.bodyHeight);
  }

  if (model.selected) {
    ctx.strokeStyle = CHART_COLORS.selected;
    ctx.lineWidth = 1;
    if (typeof ctx.setLineDash === "function") ctx.setLineDash([4, 3]);
    ctx.beginPath();
    ctx.moveTo(model.selected.x, model.plot.top);
    ctx.lineTo(model.selected.x, model.plot.bottom);
    ctx.stroke();
    if (typeof ctx.setLineDash === "function") ctx.setLineDash([]);
  }

  for (const marker of model.markers || []) {
    ctx.fillStyle = marker.style.color;
    ctx.beginPath();
    if (marker.markerKind === "order") {
      ctx.rect(marker.x - 4, marker.y - 4, 8, 8);
    } else if (marker.markerKind === "fill") {
      ctx.moveTo(marker.x, marker.y - 6);
      ctx.lineTo(marker.x + 6, marker.y + 5);
      ctx.lineTo(marker.x - 6, marker.y + 5);
      ctx.closePath();
    } else {
      ctx.arc(marker.x, marker.y, 4, 0, Math.PI * 2);
    }
    ctx.fill();
  }

  _renderReplayChartA11y(canvas, vm);
  installChartPointInspector(
    canvas,
    (model.candles || []).map((candle) => ({
      label: _fmtTime(candle.ts_ms),
      x: candle.x,
      values: [
        { label: "Open", value: _num(candle.open, null), valueText: _fmtNum(candle.open, 2) },
        { label: "High", value: _num(candle.high, null), valueText: _fmtNum(candle.high, 2) },
        { label: "Low", value: _num(candle.low, null), valueText: _fmtNum(candle.low, 2) },
        { label: "Close", value: _num(candle.close, null), valueText: _fmtNum(candle.close, 2) },
      ],
      note: candle.up ? "up candle" : "down candle",
    })),
    {
      title: "Historical replay",
      kind: "canvas-replay",
      emptyMessage: "No replay price series is available.",
    },
  );
}

export function renderReplayPanel(payload = {}, root = null, options = {}) {
  const vm = buildReplayViewModel(payload, options);
  const r = _root(root);
  if (!r) return vm;

  const meta = vm.symbol ? `${vm.symbol} ${vm.date || ""}`.trim() : (vm.date || "replay");
  _setText(r, "replayMeta", vm.readOnly ? `read-only | ${meta}` : meta);
  const status = _el(r, "replayStatus");
  if (status) {
    status.className = _statusClass(vm);
    status.textContent = vm.noData ? "no data" : (vm.partial ? "gaps" : "ready");
  }
  _setHTML(r, "replayStats", `
    <span class="pill dim">candles ${vm.counts.candles}</span>
    <span class="pill dim">decisions ${vm.counts.decisions}</span>
    <span class="pill dim">orders ${vm.counts.orders}</span>
    <span class="pill dim">fills ${vm.counts.fills}</span>
    <span class="pill dim">risk ${vm.counts.risk}</span>
  `);
  _setHTML(r, "replaySelected", _renderSelected(vm));
  _setHTML(r, "replayEvents", _renderEvents(vm));
  _setHTML(r, "replayGaps", _renderGaps(vm));

  const slider = _el(r, "replayTimeline");
  if (slider) {
    slider.min = "0";
    slider.max = String(Math.max(0, vm.timeline.length - 1));
    slider.disabled = vm.timeline.length < 1;
    const idx = Number.isFinite(options.selectedIndex) ? options.selectedIndex : vm.selected.index;
    slider.value = String(Math.max(0, idx));
  }
  renderReplayChart(_el(r, "replayChart"), vm);
  return vm;
}

function _readReplayControls(root, getSymbol) {
  const dateEl = _el(root, "replayDate");
  const symbolEl = _el(root, "replaySymbol");
  const modelEl = _el(root, "replayModel");
  const tfEl = _el(root, "replayTf");
  if (dateEl && !dateEl.value) dateEl.value = _todayLocalISO();
  if (symbolEl && !symbolEl.value) {
    const globalSymbol = typeof getSymbol === "function" ? getSymbol() : "";
    symbolEl.value = String(globalSymbol || "SPY").toUpperCase();
  }
  return {
    date: String(dateEl && dateEl.value ? dateEl.value : _todayLocalISO()),
    symbol: String(symbolEl && symbolEl.value ? symbolEl.value : "SPY").trim().toUpperCase(),
    model: String(modelEl && modelEl.value ? modelEl.value : "").trim(),
    tf: String(tfEl && tfEl.value ? tfEl.value : "1m").trim(),
  };
}

function _replayUrl(filters) {
  const params = new URLSearchParams();
  params.set("date", filters.date);
  params.set("symbol", filters.symbol || "SPY");
  params.set("tf", filters.tf || "1m");
  params.set("max_points", "1500");
  params.set("event_limit", "1000");
  if (filters.model) params.set("model_id", filters.model);
  return `/api/replay/day?${params.toString()}`;
}

function _renderState(root) {
  const payload = STATE.payload || {};
  const baseVm = buildReplayViewModel(payload);
  const index = Math.max(0, Math.min(STATE.selectedIndex || 0, Math.max(0, baseVm.timeline.length - 1)));
  const selectedTsMs = baseVm.timeline[index] || null;
  return renderReplayPanel(payload, root, { selectedTsMs, selectedIndex: index });
}

export async function loadReplayPanel(fetchJSON, options = {}) {
  const root = _root(options.root || STATE.root);
  const getSymbol = options.getSymbol || STATE.getSymbol;
  if (!root || typeof fetchJSON !== "function") return null;
  const filters = _readReplayControls(root, getSymbol);
  const status = _el(root, "replayStatus");
  if (status) {
    status.className = "pill dim";
    status.textContent = "loading";
  }
  try {
    const payload = await fetchJSON(_replayUrl(filters), { allowBusinessFalse: true });
    STATE.payload = payload;
    STATE.selectedIndex = 0;
    return _renderState(root);
  } catch (error) {
    STATE.payload = {
      ok: false,
      read_only: true,
      date: filters.date,
      symbol: filters.symbol,
      gaps: [{
        stream: "replay",
        code: "load_failed",
        message: error && error.message ? error.message : "Replay load failed.",
        severity: "warn",
      }],
    };
    STATE.selectedIndex = 0;
    return _renderState(root);
  }
}

export function initReplayPanel(options = {}) {
  const root = _root(options.root);
  if (!root) return;
  STATE.fetchJSON = options.fetchJSON || STATE.fetchJSON;
  STATE.root = root;
  STATE.getSymbol = options.getSymbol || STATE.getSymbol;

  _readReplayControls(root, STATE.getSymbol);

  const loadBtn = _el(root, "replayLoad");
  if (loadBtn && !loadBtn._replayBound) {
    loadBtn._replayBound = true;
    loadBtn.addEventListener("click", () => {
      void loadReplayPanel(STATE.fetchJSON, { root, getSymbol: STATE.getSymbol });
    });
  }

  const slider = _el(root, "replayTimeline");
  if (slider && !slider._replayBound) {
    slider._replayBound = true;
    slider.addEventListener("input", () => {
      STATE.selectedIndex = Math.max(0, Number(slider.value || 0));
      _renderState(root);
    });
  }

  for (const id of ["replayDate", "replaySymbol", "replayModel", "replayTf"]) {
    const el = _el(root, id);
    if (el && !el._replayBound) {
      el._replayBound = true;
      el.addEventListener("change", () => {
        if (id === "replaySymbol") el.value = String(el.value || "").trim().toUpperCase();
        void loadReplayPanel(STATE.fetchJSON, { root, getSymbol: STATE.getSymbol });
      });
    }
  }

  void loadReplayPanel(STATE.fetchJSON, { root, getSymbol: STATE.getSymbol });
}
