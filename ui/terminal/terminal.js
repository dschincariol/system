/*
  FILE: ui/terminal/terminal.js

  Browser-terminal controller for the trading system UI. This module manages
  terminal-specific fetches, rendering, and interactions for the dedicated
  `ui/terminal/` surface.
*/

import {
  startLiveMarketChart,
  stopLiveMarketChart,
  applyTerminalOverlays
} from "./pro_charting.js";
import { setProChartsState } from "../pro_chart_prefs.js";
import {
  initSelectedSymbolContextFromUrl,
  updateSelectedSymbolContext
} from "../symbol_context.mjs";
import {
  formatFxPrice,
  formatLotQty,
  isFxSymbol,
} from "../fx_format.js";
import { fxSessionStatus } from "../fx_session.js";
import { renderChartAccessibility } from "../chart_a11y.js";
import {
  buildIndicatorAccessibilitySummary,
  buildOverlayAccessibilitySummary,
  decisionOverlayLegendItems,
  decisionWindowLegendItems,
  indicatorOverlayLegendItems,
  normalizeDecisionOverlayPayload
} from "../decision_overlays.js";
import { buildTableView } from "../utils.js";
import { requestConfirmation } from "../confirmation_modal.mjs";

const LS_KEY = "terminal.state.v1";
const FETCH_TIMEOUT_MS = 15000;
const FLATTEN_HOLD_MS = 1500;

function requestId(prefix = "terminal") {
  try {
    if (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
      return globalThis.crypto.randomUUID();
    }
  } catch {}
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

async function _fetchWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(new Error(`fetch_timeout:${url}`)), FETCH_TIMEOUT_MS);
  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeoutId);
  }
}

async function _readJsonResponse(r, url) {
  const raw = await r.text();
  let j = null;
  try {
    j = raw ? JSON.parse(raw) : null;
  } catch {}
  if (!r.ok) {
    const reason = j && (j.reason_code || j.error || j.detail);
    const message = j && (j.message || j.reason || j.error);
    const text = message && reason && String(reason) !== String(message)
      ? `${message} (${reason})`
      : (message || reason || r.statusText || "request_failed");
    const error = new Error(String(text));
    error.payload = j;
    error.status = r.status;
    error.reasonCode = reason;
    throw error;
  }
  if (!j || typeof j !== "object") throw new Error(`invalid_json_response: ${url}`);
  if (j.ok === false) {
    const reason = j.reason_code || j.error || `api_error: ${url}`;
    const message = j.message || j.reason || j.error;
    const error = new Error(String(message && reason && String(reason) !== String(message) ? `${message} (${reason})` : (message || reason)));
    error.payload = j;
    error.status = Number(j.http_status || (j.meta && j.meta.status) || 0);
    error.reasonCode = reason;
    throw error;
  }
  return j;
}

async function postJson(url, body) {
  const r = await _fetchWithTimeout(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {})
  });
  return _readJsonResponse(r, url);
}

function lsGet() {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function lsSet(st) {
  try { localStorage.setItem(LS_KEY, JSON.stringify(st)); } catch {}
}

function esc(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmtNum(x, d = 2) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(d);
}

function fmtSymbolPrice(symbol, value, fallbackDigits = 4) {
  return isFxSymbol(symbol) ? formatFxPrice(symbol, value) : fmtNum(value, fallbackDigits);
}

function fmtSymbolQty(symbol, value, lotSize = 100000) {
  return isFxSymbol(symbol) ? formatLotQty(symbol, value, lotSize) : fmtNum(value, 4);
}

function fxSessionLabel(symbol, nowMs = Date.now()) {
  if (!isFxSymbol(symbol)) return "";
  return fxSessionStatus(nowMs).label;
}

function fmtTs(tsMs) {
  const n = Number(tsMs);
  if (!Number.isFinite(n) || n <= 0) return "—";
  try { return new Date(n).toLocaleString(); } catch { return String(n); }
}

function fmtAgeMs(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n < 0) return "—";
  if (n < 1000) return `${Math.round(n)}ms`;
  if (n < 60_000) return `${Math.round(n / 1000)}s`;
  if (n < 3_600_000) return `${Math.round(n / 60_000)}m`;
  return `${(n / 3_600_000).toFixed(n < 36_000_000 ? 1 : 0)}h`;
}

function fmtBps(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(Math.abs(n) >= 100 ? 0 : 1)} bps`;
}

function reasonText(row) {
  if (!row || typeof row !== "object") return "";
  return String(
    row.reason
    || row.rejection_reason
    || row.suppression_reason
    || row.reason_code
    || row.rejection_reason_code
    || row.suppression_reason_code
    || ""
  ).trim();
}

function statusLabel(row) {
  if (!row || typeof row !== "object") return "—";
  const bucket = String(row.status_bucket || "").trim().toUpperCase();
  const label = String(row.status_label || row.state || row.action || bucket || "—").trim();
  return bucket && !label.toUpperCase().includes(bucket) ? `${bucket}: ${label}` : label;
}

function statusClass(row) {
  const bucket = String(row && row.status_bucket || "").trim().toLowerCase();
  if (bucket === "rejected" || bucket === "canceled" || bucket === "stale") return "crit";
  if (bucket === "suppressed" || bucket === "partial") return "warn";
  if (bucket === "filled") return "ok";
  return "info";
}

function fillDetailText(row) {
  const count = Number(row && (row.child_fill_count || row.fill_count || 1));
  const id = String(row && (row.client_order_id || row.source_order_id || "") || "").trim();
  const label = count > 1 ? `${count} child fills` : "1 fill";
  return id ? `${label} | ${id}` : label;
}

async function fetchJson(url) {
  const r = await _fetchWithTimeout(url, { cache: "no-store" });
  return _readJsonResponse(r, url);
}

const el = {};

function initEls() {
  el.symInput = document.getElementById("symInput");
  el.tfSel = document.getElementById("tfSel");
  el.typeSel = document.getElementById("typeSel");
  el.ovVwap = document.getElementById("ovVwap");
  el.ovEma = document.getElementById("ovEma");
  el.ovMarkers = document.getElementById("ovMarkers");
  el.ovEquity = document.getElementById("ovEquity");
  el.watchFilter = document.getElementById("watchFilter");
  el.watchList = document.getElementById("watchList");
  el.watchMeta = document.getElementById("watchMeta");
  el.chartTitle = document.getElementById("chartTitle");
  el.chartHealth = document.getElementById("chartHealth");
  el.overlayLegend = document.getElementById("terminalOverlayLegend");
  el.xhairBox = document.getElementById("xhairBox");
  el.acctCash = document.getElementById("acctCash");
  el.acctEquity = document.getElementById("acctEquity");
  el.acctUpdated = document.getElementById("acctUpdated");
  el.acctMeta = document.getElementById("acctMeta");
  el.posFilter = document.getElementById("posFilter");
  el.posTbl = document.getElementById("posTbl");
  el.ordFilter = document.getElementById("ordFilter");
  el.ordStatusFilter = document.getElementById("ordStatusFilter");
  el.ordTbl = document.getElementById("ordTbl");
  el.ordersMeta = document.getElementById("ordersMeta");
  el.fillsFilter = document.getElementById("fillsFilter");
  el.fillsStatusFilter = document.getElementById("fillsStatusFilter");
  el.fillsTbl = document.getElementById("fillsTbl");
  el.fillsMeta = document.getElementById("fillsMeta");
  el.terminalTcaSummary = document.getElementById("terminalTcaSummary");
  el.statusBanner = document.getElementById("terminalStatusBanner");
  el.safetyStatus = document.getElementById("tradingSafetyStatus");
  el.terminalArmChk = document.getElementById("terminalArmChk");
  el.ordQty = document.getElementById("ordQty");
  el.orderPreview = document.getElementById("orderPreview");
  el.btnBuy = document.getElementById("btnBuy");
  el.btnSell = document.getElementById("btnSell");
  el.btnFlat = document.getElementById("btnFlat");
}

let STATE = {
  symbol: "SPY",
  tf: "1m",
  type: "candle",
  ov: { vwap: true, ema: true, markers: true, equity: true },
  watchFilter: "",
  tableFilters: { positions: "", orders: "", fills: "" },
  tableStatusFilters: { orders: "all", fills: "all" },
};

const saved = lsGet();
if (saved && typeof saved === "object") {
  STATE = {
    ...STATE,
    ...saved,
    ov: { ...STATE.ov, ...(saved.ov || {}) },
    tableFilters: { ...STATE.tableFilters, ...(saved.tableFilters || {}) },
    tableStatusFilters: { ...STATE.tableStatusFilters, ...(saved.tableStatusFilters || {}) },
  };
}

function applyLaunchParams() {
  try {
    const params = new URLSearchParams(window.location.search || "");
    const urlSymbolContext = initSelectedSymbolContextFromUrl({
      source: "terminal_url",
      persistUrl: false,
    });
    const symbol = String(params.get("symbol") || "").trim().toUpperCase();
    const tf = String(params.get("tf") || "").trim();
    const type = String(params.get("type") || "").trim();
    const returnScreen = String(params.get("screen") || "").trim().toLowerCase();
    const decisionId = String(params.get("decision_id") || "").trim();
    const advisoryId = String(params.get("advisory_id") || "").trim();

    if (urlSymbolContext.symbol || symbol) STATE.symbol = urlSymbolContext.symbol || symbol;
    if (tf) STATE.tf = tf;
    if (type) STATE.type = type;
    updateSelectedSymbolContext({
      symbol: STATE.symbol,
      source: symbol ? "terminal_url" : "terminal_state",
      persistUrl: false,
    });

    const dashLink = document.getElementById("dashboardReturnLink");
    if (dashLink) {
      const url = new URL("/ui/dashboard.html", window.location.origin);
      if (symbol) url.searchParams.set("symbol", symbol);
      if (returnScreen) url.searchParams.set("screen", returnScreen);
      if (decisionId) url.searchParams.set("decision_id", decisionId);
      if (advisoryId) url.searchParams.set("advisory_id", advisoryId);
      dashLink.href = url.toString();
    }
  } catch {}
}

function syncStateToDom() {
  if (el.symInput) el.symInput.value = STATE.symbol;
  if (el.tfSel) el.tfSel.value = STATE.tf;
  if (el.typeSel) el.typeSel.value = STATE.type;
  if (el.ovVwap) el.ovVwap.checked = !!STATE.ov.vwap;
  if (el.ovEma) el.ovEma.checked = !!STATE.ov.ema;
  if (el.ovMarkers) el.ovMarkers.checked = !!STATE.ov.markers;
  if (el.ovEquity) el.ovEquity.checked = !!STATE.ov.equity;
  if (el.watchFilter) el.watchFilter.value = STATE.watchFilter || "";
  if (el.posFilter) el.posFilter.value = STATE.tableFilters.positions || "";
  if (el.ordFilter) el.ordFilter.value = STATE.tableFilters.orders || "";
  if (el.ordStatusFilter) el.ordStatusFilter.value = STATE.tableStatusFilters.orders || "all";
  if (el.fillsFilter) el.fillsFilter.value = STATE.tableFilters.fills || "";
  if (el.fillsStatusFilter) el.fillsStatusFilter.value = STATE.tableStatusFilters.fills || "all";
}

function persist() { lsSet(STATE); }

function setSymbol(sym, source = "terminal_input") {
  const s = String(sym || "").trim().toUpperCase();
  if (!s) return;
  STATE.symbol = s;
  if (el.symInput) el.symInput.value = s;
  updateSelectedSymbolContext({
    symbol: s,
    source,
    persistUrl: true,
  });
  persist();
  void bootChart();
}

function setTf(tf) {
  STATE.tf = String(tf || "1m").trim();
  persist();
  void bootChart();
}

function setType(tp) {
  STATE.type = String(tp || "candle").trim();
  persist();
  void bootChart();
}

function setOv(key, v) {
  STATE.ov[key] = !!v;
  persist();
  void bootChart();
}

const TERMINAL_TABLE_DEFAULTS = Object.freeze({
  positions: { sortKey: "symbol", sortDir: "asc", maxRows: 500 },
  orders: { sortKey: "updatedTs", sortDir: "desc", maxRows: 300 },
  fills: { sortKey: "ts_ms", sortDir: "desc", maxRows: 2000 },
});
const TERMINAL_TABLE_STATE = {
  positions: { sortKey: "symbol", sortDir: "asc" },
  orders: { sortKey: "updatedTs", sortDir: "desc" },
  fills: { sortKey: "ts_ms", sortDir: "desc" },
};
const TERMINAL_POSITION_COLUMNS = Object.freeze([
  { key: "symbol", label: "Symbol", width: "1fr", accessor: (row) => row && String(row.symbol || "").toUpperCase() },
  { key: "qty", label: "Qty", width: "1fr", accessor: (row) => row && row.qty },
  { key: "avg_px", label: "AvgPx", width: "1fr", accessor: (row) => row && row.avg_px },
  { key: "updated_ts_ms", label: "Updated", width: "1.2fr", accessor: (row) => row && row.updated_ts_ms, searchable: false },
]);
const TERMINAL_ORDER_COLUMNS = Object.freeze([
  { key: "kind", label: "Kind", width: "0.8fr", accessor: (row) => row && row.kind },
  { key: "symbol", label: "Symbol", width: "0.9fr", accessor: (row) => row && String(row.symbol || "").toUpperCase() },
  { key: "status_bucket", label: "Status", width: "1.1fr", accessor: (row) => row && `${row.status_bucket || ""} ${row.status_label || ""} ${row.state || row.action || ""}` },
  { key: "qty", label: "Qty", width: "0.8fr", accessor: (row) => row && row.qty },
  { key: "reasonText", label: "Reason", width: "1.6fr", accessor: (row) => row && reasonText(row) },
  { key: "expected_price", label: "Expected", width: "0.9fr", accessor: (row) => row && (row.expected_price ?? row.expected_px), searchable: false },
  { key: "slippage_bps", label: "Slip", width: "0.8fr", accessor: (row) => row && row.slippage_bps, searchable: false },
  { key: "updatedTs", label: "Updated", width: "1.2fr", accessor: (row) => row && (row.updated_ts_ms || row.ts_ms), searchable: false },
]);
const TERMINAL_FILL_COLUMNS = Object.freeze([
  { key: "ts_ms", label: "Time", width: "1.2fr", accessor: (row) => row && row.ts_ms, searchable: false },
  { key: "symbol", label: "Symbol", width: "0.9fr", accessor: (row) => row && String(row.symbol || "").toUpperCase() },
  { key: "status_bucket", label: "Status", width: "1fr", accessor: (row) => row && `${row.status_bucket || ""} ${row.status_label || ""} ${row.state || ""}` },
  { key: "qty", label: "Qty", width: "0.9fr", accessor: (row) => row && row.qty },
  { key: "fill_vwap", label: "VWAP", width: "0.9fr", accessor: (row) => row && (row.fill_vwap ?? row.px), searchable: false },
  { key: "expected_price", label: "Expected", width: "0.9fr", accessor: (row) => row && (row.expected_price ?? row.expected_px), searchable: false },
  { key: "slippage_bps", label: "Slip bps", width: "0.8fr", accessor: (row) => row && row.slippage_bps, searchable: false },
  { key: "implementation_shortfall_bps", label: "IS bps", width: "0.8fr", accessor: (row) => row && row.implementation_shortfall_bps, searchable: false },
  { key: "child_fill_count", label: "Detail", width: "1fr", accessor: (row) => row && `${row.child_fill_count || row.fill_count || 1} ${row.client_order_id || ""}` },
]);

let _terminalTableRows = {
  positions: [],
  orders: [],
  fills: [],
};
let _terminalOrdersSummary = {};
let _terminalFillsSummary = {};

function getTerminalTableState(tableId) {
  const key = String(tableId || "").trim();
  const defaults = TERMINAL_TABLE_DEFAULTS[key] || {};
  if (!TERMINAL_TABLE_STATE[key]) {
    TERMINAL_TABLE_STATE[key] = {
      sortKey: defaults.sortKey || "",
      sortDir: defaults.sortDir || "asc",
    };
  }
  return TERMINAL_TABLE_STATE[key];
}

function setTerminalTableQuery(tableId, query) {
  STATE.tableFilters = STATE.tableFilters || {};
  STATE.tableFilters[tableId] = String(query || "");
  persist();
  renderTerminalTables();
}

function setTerminalStatusFilter(tableId, value) {
  STATE.tableStatusFilters = STATE.tableStatusFilters || {};
  const normalized = String(value || "all").trim().toLowerCase() || "all";
  STATE.tableStatusFilters[tableId] = normalized;
  persist();
  renderTerminalTables();
}

function setTerminalTableSort(tableId, sortKey, explicitDir = "") {
  const state = getTerminalTableState(tableId);
  const key = String(sortKey || "").trim();
  if (!key) return;
  if (state.sortKey === key) {
    state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
  } else {
    state.sortKey = key;
    state.sortDir = String(explicitDir || "asc").toLowerCase() === "desc" ? "desc" : "asc";
  }
  renderTerminalTables();
}

function renderTerminalTable(container, tableId, columns, rows, rowColsFn, emptyText, filteredEmptyText) {
  if (!container) return null;
  const state = getTerminalTableState(tableId);
  const defaults = TERMINAL_TABLE_DEFAULTS[tableId] || {};
  const query = STATE.tableFilters && STATE.tableFilters[tableId] || "";
  const statusFilter = String(STATE.tableStatusFilters && STATE.tableStatusFilters[tableId] || "all").toLowerCase();
  const sourceRows = statusFilter && statusFilter !== "all"
    ? (Array.isArray(rows) ? rows : []).filter((row) => String(row && row.status_bucket || "").toLowerCase() === statusFilter)
    : rows;
  const view = buildTableView(sourceRows, columns, {
    query,
    sortKey: state.sortKey || defaults.sortKey,
    sortDir: state.sortDir || defaults.sortDir,
    maxRows: defaults.maxRows,
  });
  const template = columns.map((column) => column.width || "minmax(90px, 1fr)").join(" ");
  const rowStyle = ` style="grid-template-columns:${esc(template)}"`;
  const header = `<div class="row h" role="row"${rowStyle}>${columns.map((column) => {
    const active = column.key === state.sortKey;
    const sortDir = active ? (state.sortDir === "desc" ? "descending" : "ascending") : "none";
    const indicator = active ? (state.sortDir === "desc" ? "v" : "^") : "";
    return `<div class="c" role="columnheader" aria-sort="${esc(sortDir)}"><button class="sortBtn ${active ? "is-active" : ""}" type="button" data-terminal-table-sort="${esc(tableId)}" data-sort-key="${esc(column.key)}">${esc(column.label)}<span class="sortIndicator" aria-hidden="true">${esc(indicator)}</span></button></div>`;
  }).join("")}</div>`;
  const body = view.visibleRows.length
    ? view.visibleRows.map((row) => {
      const cols = rowColsFn(row);
      return `<div class="row" role="row"${rowStyle}>${cols.map((col) => `<div class="c" role="cell">${col}</div>`).join("")}</div>`;
    }).join("")
    : `<div class="tableEmpty">${esc(view.totalRows > 0 ? (filteredEmptyText || "No rows match the current filter.") : emptyText)}</div>`;
  container.innerHTML = header + body;
  return view;
}

function renderTerminalTcaSummary(fills, summary = {}) {
  if (!el.terminalTcaSummary) return;
  const rows = Array.isArray(fills) ? fills : [];
  const partial = Number(summary.partial_orders ?? rows.filter((row) => String(row && row.status_bucket) === "partial").length);
  const orders = Number(summary.orders ?? rows.length);
  const slip = Number(summary.avg_slippage_bps);
  const shortfall = Number(summary.avg_implementation_shortfall_bps);
  const fees = Number(summary.total_fees);
  el.terminalTcaSummary.innerHTML = `
    <div class="tcaCell"><span class="k">Orders</span><span class="v mono">${esc(Number.isFinite(orders) ? String(orders) : "0")}</span></div>
    <div class="tcaCell"><span class="k">Partial</span><span class="v mono">${esc(Number.isFinite(partial) ? String(partial) : "0")}</span></div>
    <div class="tcaCell"><span class="k">Avg slip</span><span class="v mono">${esc(Number.isFinite(slip) ? fmtBps(slip) : "—")}</span></div>
    <div class="tcaCell"><span class="k">Avg IS</span><span class="v mono">${esc(Number.isFinite(shortfall) ? fmtBps(shortfall) : "—")}</span></div>
    <div class="tcaCell"><span class="k">Fees</span><span class="v mono">${esc(Number.isFinite(fees) ? fmtNum(fees, 2) : "—")}</span></div>
  `;
  el.terminalTcaSummary.setAttribute(
    "aria-label",
    `Execution cost summary. ${orders || 0} aggregated orders. ${partial || 0} partial orders. Average slippage ${Number.isFinite(slip) ? fmtBps(slip) : "unavailable"}. Average implementation shortfall ${Number.isFinite(shortfall) ? fmtBps(shortfall) : "unavailable"}.`
  );
}

function renderTerminalTables(emptyMessages = {}) {
  const positionsView = renderTerminalTable(
    el.posTbl,
    "positions",
    TERMINAL_POSITION_COLUMNS,
    _terminalTableRows.positions,
    (r) => ([
      esc((r && r.symbol || "").toUpperCase()),
      `<span class="mono">${esc(fmtSymbolQty(r && r.symbol, r && r.qty, r && r.fx && (r.fx.lot_size || r.fx.contract_size)))}</span>`,
      `<span class="mono">${esc(fmtSymbolPrice(r && r.symbol, r && r.avg_px, 4))}</span>`,
      `<span class="mono">${esc(fmtTs(r && r.updated_ts_ms))}</span>`,
    ]),
    emptyMessages.positions || "No live broker positions are currently available.",
    "No positions match the current filter."
  );
  renderTerminalTable(
    el.ordTbl,
    "orders",
    TERMINAL_ORDER_COLUMNS,
    _terminalTableRows.orders,
    (r) => ([
      esc(r && r.kind || "—"),
      esc((r && r.symbol || "").toUpperCase()),
      `<span class="statusMini statusMini-${esc(statusClass(r))}">${esc(statusLabel(r))}</span>`,
      `<span class="mono">${esc(r && r.qty != null ? fmtSymbolQty(r && r.symbol, r && r.qty, r && r.fx && (r.fx.lot_size || r.fx.contract_size)) : "—")}</span>`,
      esc(reasonText(r) || "—"),
      `<span class="mono">${esc(fmtSymbolPrice(r && r.symbol, r && (r.expected_price ?? r.expected_px), 4))}</span>`,
      `<span class="mono">${esc(fmtBps(r && r.slippage_bps))}</span>`,
      `<span class="mono">${esc(fmtTs(r && (r.updated_ts_ms || r.ts_ms)))}</span>`,
    ]),
    emptyMessages.orders || "No live broker or portfolio orders are currently available.",
    "No orders match the current filter."
  );
  if (el.ordersMeta) {
    const summary = _terminalOrdersSummary || {};
    const parts = [
      `${Number(summary.total ?? _terminalTableRows.orders.length)} rows`,
      `${Number(summary.rejected || 0)} rejected`,
      `${Number(summary.suppressed || 0)} suppressed`,
      `${Number(summary.partial || 0)} partial`,
      `${Number(summary.stale || 0)} stale`,
    ];
    el.ordersMeta.textContent = parts.join(" | ");
  }
  const fillsView = renderTerminalTable(
    el.fillsTbl,
    "fills",
    TERMINAL_FILL_COLUMNS,
    _terminalTableRows.fills,
    (r) => ([
      `<span class="mono">${esc(fmtTs(r && r.ts_ms))}</span>`,
      esc((r && r.symbol || "").toUpperCase()),
      `<span class="statusMini statusMini-${esc(statusClass(r))}">${esc(statusLabel(r))}</span>`,
      `<span class="mono">${esc(fmtSymbolQty(r && r.symbol, r && r.qty, r && r.fx && (r.fx.lot_size || r.fx.contract_size)))}</span>`,
      `<span class="mono">${esc(fmtSymbolPrice(r && r.symbol, r && (r.fill_vwap ?? r.px), 4))}</span>`,
      `<span class="mono">${esc(fmtSymbolPrice(r && r.symbol, r && (r.expected_price ?? r.expected_px), 4))}</span>`,
      `<span class="mono">${esc(fmtBps(r && r.slippage_bps))}</span>`,
      `<span class="mono">${esc(fmtBps(r && r.implementation_shortfall_bps))}</span>`,
      `<span class="mono" title="${esc(JSON.stringify((r && r.child_fills) || []))}">${esc(fillDetailText(r))}</span>`,
    ]),
    emptyMessages.fills || "No live fills are currently available.",
    "No fills match the current filter."
  );
  if (el.fillsMeta && fillsView) {
    el.fillsMeta.textContent = fillsView.query
      ? `${fillsView.visibleRowsCount}/${fillsView.totalRows} rows`
      : `${fillsView.totalRows} rows`;
  }
  renderTerminalTcaSummary(_terminalTableRows.fills, _terminalFillsSummary);
  return { positionsView, fillsView };
}

let _watchSymbols = [];
let _snapshotTimer = null;
let _wired = false;
let _snapshotHealth = {
  lastSuccessTs: 0,
  lastError: "",
};
let _terminalArmed = false;
let _flattenHoldTimer = null;
let _flattenHoldCompletedAt = 0;
let _executionBarrier = normalizeExecutionBarrier(null);
let _accountSnapshotAvailable = false;
let _latestPriceReference = null;

function setTerminalBanner(level, message) {
  if (!el.statusBanner) return;
  const tone = String(level || "warn").trim().toLowerCase();
  const nextTone = tone === "ok" ? "ok" : tone === "crit" ? "crit" : "warn";
  el.statusBanner.className = `termBanner termBanner-${nextTone}`;
  el.statusBanner.textContent = String(message || "");
}

function asStringList(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  const text = String(value || "").trim();
  return text ? [text] : [];
}

function normalizeExecutionBarrier(raw) {
  const gate = (raw && typeof raw === "object") ? raw : {};
  const realTradingAllowed = gate.real_trading_allowed === true;
  const mode = String(gate.execution_mode || gate.mode || "unknown").trim() || "unknown";
  const reason = String(gate.reason || gate.gate_status || (realTradingAllowed ? "real_trading_allowed" : "execution_barrier_unavailable")).trim();
  const reasonValues = [
    ...(realTradingAllowed ? [] : [reason]),
    ...asStringList(gate.blocking_reasons),
    ...asStringList(gate.severity_reasons),
    ...asStringList(gate.reason_codes),
  ];
  const blockingReasons = [...new Set(reasonValues.filter(Boolean))];
  const updatedTsMs = Number(gate.updated_ts_ms || gate.ts_ms || 0);

  return {
    raw: gate,
    realTradingAllowed,
    blocked: !realTradingAllowed,
    blockingReasons,
    reason,
    mode,
    gateStatus: String(gate.gate_status || reason || "").trim(),
    severity: String(gate.severity || (realTradingAllowed ? "OK" : "CRITICAL")).trim().toUpperCase(),
    allowSimulation: gate.allow_simulation === true,
    allowExecutionPipeline: gate.allow_execution_pipeline === true,
    armed: gate.armed,
    updatedTsMs: Number.isFinite(updatedTsMs) ? updatedTsMs : 0,
  };
}

function barrierBlockTitle() {
  if (_executionBarrier.realTradingAllowed) return "";
  const reasons = _executionBarrier.blockingReasons.length
    ? _executionBarrier.blockingReasons.join(", ")
    : _executionBarrier.reason;
  return `Real trading is blocked by the execution barrier: ${reasons || "unknown"}`;
}

function renderTradingSafetyStatus() {
  if (!el.safetyStatus) return;
  const barrier = _executionBarrier;
  const tone = barrier.realTradingAllowed
    ? "ok"
    : barrier.severity === "CRITICAL"
      ? "crit"
      : "warn";
  const prefix = barrier.realTradingAllowed
    ? "LIVE gate open"
    : barrier.allowSimulation
      ? "SIM/PAPER only"
      : "LIVE blocked";
  const shortcutState = _terminalArmed ? "shortcuts armed" : "shortcuts off";
  const reason = barrier.realTradingAllowed
    ? shortcutState
    : (barrier.blockingReasons[0] || barrier.reason || "execution blocked");
  el.safetyStatus.className = `safetyStatus safetyStatus-${tone}`;
  el.safetyStatus.textContent = `${prefix} | ${barrier.mode} | ${reason}`;
  el.safetyStatus.title = barrier.realTradingAllowed
    ? `Real trading allowed. Keyboard ${shortcutState}.`
    : barrierBlockTitle();
}

function cancelFlattenHold(message = "") {
  if (_flattenHoldTimer) {
    clearTimeout(_flattenHoldTimer);
    _flattenHoldTimer = null;
  }
  if (el.btnFlat) el.btnFlat.textContent = "FLATTEN";
  if (message) setTerminalBanner("warn", message);
}

function setOrderEntryEnabled(enabled, title = "") {
  [el.ordQty, el.btnBuy, el.btnSell].forEach((node) => {
    if (!node) return;
    node.disabled = !enabled;
    if (title) node.title = title;
    else node.removeAttribute("title");
  });
}

function setFlattenEnabled(enabled, title = "") {
  if (!el.btnFlat) return;
  if (!enabled) cancelFlattenHold();
  el.btnFlat.disabled = !enabled;
  if (title) el.btnFlat.title = title;
  else el.btnFlat.removeAttribute("title");
}

function applyOrderBarrierState(accountAvailable) {
  renderTradingSafetyStatus();
  if (!_executionBarrier.realTradingAllowed) {
    const title = barrierBlockTitle();
    setOrderEntryEnabled(false, title);
    setFlattenEnabled(false, title);
    renderOrderPreview();
    return;
  }

  setOrderEntryEnabled(
    !!accountAvailable,
    accountAvailable ? "" : "Order entry is disabled until the account snapshot is available."
  );
  setFlattenEnabled(true, "Hold to confirm flatten. Backend execution gates still apply.");
  renderOrderPreview();
}

function currentOrderQty() {
  const raw = el.ordQty ? el.ordQty.value : "";
  const qty = Number(raw || 0);
  return Number.isFinite(qty) ? qty : 0;
}

function latestPriceReferenceForSymbol(symbol) {
  const sym = String(symbol || "").trim().toUpperCase();
  const ref = (_latestPriceReference && typeof _latestPriceReference === "object") ? _latestPriceReference : null;
  if (!ref) return null;
  const refSymbol = String(ref.symbol || sym).trim().toUpperCase();
  return refSymbol === sym ? ref : null;
}

function orderConfirmationPayload(token, holdMs = 0) {
  const normalizedToken = String(token || "").trim();
  const normalizedHoldMs = Math.max(0, Number(holdMs || 0));
  const actionId = normalizedToken === "FLATTEN" ? "terminal.flatten" : "terminal.order";
  return {
    confirm: normalizedToken,
    confirmation: normalizedToken,
    confirmation_token: normalizedToken,
    confirmation_method: normalizedHoldMs > 0 ? "typed_phrase_hold" : "typed_phrase",
    confirmation_hold_ms: normalizedHoldMs,
    consequence_ack: true,
    action_id: actionId,
    actor: "terminal_operator",
    source: "terminal",
    source_surface: "terminal",
    request_id: requestId(actionId.replace(".", "-")),
    target: STATE.symbol,
  };
}

function renderOrderPreview(side = "") {
  if (!el.orderPreview) return;
  const qty = currentOrderQty();
  const priceRef = latestPriceReferenceForSymbol(STATE.symbol);
  const px = Number(priceRef && priceRef.ok !== false && priceRef.price);
  const pxAgeMs = Number(priceRef && priceRef.age_ms);
  const notional = Number.isFinite(px) && px > 0 && qty > 0 ? qty * px : null;
  const gate = _executionBarrier.realTradingAllowed
    ? `gate open (${_executionBarrier.mode})`
    : `blocked: ${_executionBarrier.blockingReasons[0] || _executionBarrier.reason || "execution barrier"}`;
  const priceSource = String(priceRef && priceRef.source || "prices").trim() || "prices";
  const priceError = String(priceRef && (priceRef.error || priceRef.reason) || "").trim();
  const priceText = Number.isFinite(px) && px > 0
    ? `price ref ${fmtSymbolPrice(STATE.symbol, px, 2)} (${priceSource})`
    : `price ref unavailable${priceError ? `: ${priceError}` : ""}`;
  const priceAgeText = Number.isFinite(pxAgeMs) && pxAgeMs >= 0 ? `price age ${fmtAgeMs(pxAgeMs)}` : "price age unavailable";
  const notionalText = notional == null ? "notional unavailable" : `est notional ${fmtNum(notional, 2)}`;
  const sessionText = fxSessionLabel(STATE.symbol);
  el.orderPreview.textContent = `${side || "Order"} ${STATE.symbol} qty ${fmtSymbolQty(STATE.symbol, qty)} | ${priceText} | ${priceAgeText} | ${notionalText} | ${gate}${sessionText ? ` | ${sessionText}` : ""}`;
}

function canSubmitRealTrade(label) {
  if (_executionBarrier.realTradingAllowed) return true;
  setTerminalBanner("crit", `${label} blocked. ${barrierBlockTitle()}`);
  return false;
}

function canSubmitDirectionalOrder(label) {
  if (!canSubmitRealTrade(label)) return false;
  if (_accountSnapshotAvailable) return true;
  setTerminalBanner("warn", `${label} blocked. Account snapshot is unavailable.`);
  return false;
}

function latestRowTs(rows, keys) {
  let latest = 0;
  for (const row of Array.isArray(rows) ? rows : []) {
    for (const key of keys) {
      const ts = Number(row && row[key]);
      if (Number.isFinite(ts) && ts > latest) {
        latest = ts;
      }
    }
  }
  return latest;
}

function renderSnapshotFailure(error) {
  const reason = String(error && error.message ? error.message : error || "snapshot unavailable");
  const lastGoodAge = _snapshotHealth.lastSuccessTs > 0
    ? fmtAgeMs(Date.now() - _snapshotHealth.lastSuccessTs)
    : null;
  _snapshotHealth.lastError = reason;

  if (el.acctCash) el.acctCash.textContent = "—";
  if (el.acctEquity) el.acctEquity.textContent = "—";
  if (el.acctUpdated) el.acctUpdated.textContent = "—";
  if (el.acctMeta) el.acctMeta.textContent = "snapshot unavailable";
  if (el.watchMeta) {
    el.watchMeta.textContent = lastGoodAge
      ? `stale • last good ${lastGoodAge} ago`
      : "snapshot unavailable";
  }
  if (el.fillsMeta) {
    el.fillsMeta.textContent = lastGoodAge
      ? `stale • last good ${lastGoodAge} ago`
      : "snapshot unavailable";
  }

  _terminalTableRows = { positions: [], orders: [], fills: [] };
  _terminalOrdersSummary = {};
  _terminalFillsSummary = {};
  renderTerminalTables({
    positions: "Terminal snapshot unavailable. Live positions cleared until a fresh snapshot succeeds.",
    orders: "Terminal snapshot unavailable. Open-order state cleared until a fresh snapshot succeeds.",
    fills: "Terminal snapshot unavailable. Recent fills cleared until a fresh snapshot succeeds.",
  });
  if (el.fillsMeta) {
    el.fillsMeta.textContent = lastGoodAge
      ? `stale • last good ${lastGoodAge} ago`
      : "snapshot unavailable";
  }

  _accountSnapshotAvailable = false;
  _executionBarrier = normalizeExecutionBarrier(null);
  _latestPriceReference = null;
  renderTradingSafetyStatus();
  setOrderEntryEnabled(false, "Order entry is disabled while the terminal snapshot is unavailable.");
  setFlattenEnabled(false, "Flatten is disabled while the terminal snapshot and execution barrier are unavailable.");
  setTerminalBanner(
    "crit",
    lastGoodAge
      ? `Terminal snapshot unavailable: ${reason}. Account, orders, positions, fills, and trading controls are stale. Last good snapshot ${lastGoodAge} ago.`
      : `Terminal snapshot unavailable: ${reason}. Account, orders, positions, fills, and trading controls are unavailable until a fresh snapshot succeeds.`
  );
}

async function refreshSnapshot() {
  if (document.hidden) return;

  try {
    const j = await fetchJson(`/api/terminal/snapshot?symbol=${encodeURIComponent(STATE.symbol)}`);
    if (!j || !j.ok) return;
    _snapshotHealth.lastSuccessTs = Number(j.ts_ms || Date.now());
    _snapshotHealth.lastError = "";
    _executionBarrier = normalizeExecutionBarrier(j.execution_barrier);
    renderTradingSafetyStatus();

    _watchSymbols = Array.isArray(j.watchlist) ? j.watchlist : _watchSymbols;
    if (el.watchMeta) el.watchMeta.textContent = `${_watchSymbols.length} symbols`;

    const acct = (j.equity && j.equity.account) ? j.equity.account : null;
    _accountSnapshotAvailable = !!acct;
    if (el.acctCash) el.acctCash.textContent = acct ? fmtNum(acct.cash, 2) : "—";
    if (el.acctEquity) el.acctEquity.textContent = acct ? fmtNum(acct.equity, 2) : "—";
    if (el.acctUpdated) el.acctUpdated.textContent = acct ? fmtTs(acct.updated_ts_ms) : "—";
    if (el.acctMeta) el.acctMeta.textContent = `snapshot latency ${Number(j.latency_ms || 0)}ms`;

    const pos = Array.isArray(j.positions) ? j.positions : [];
    _latestPriceReference = (j.price_reference && typeof j.price_reference === "object") ? j.price_reference : null;
    _terminalOrdersSummary = (j.orders_summary && typeof j.orders_summary === "object") ? j.orders_summary : {};
    _terminalFillsSummary = (j.fills_summary && typeof j.fills_summary === "object") ? j.fills_summary : {};

    const ords = (j.orders && typeof j.orders === "object") ? j.orders : { broker: [], portfolio: [] };
    const bro = Array.isArray(ords.broker) ? ords.broker : [];
    const por = Array.isArray(ords.portfolio) ? ords.portfolio : [];
    const rej = Array.isArray(ords.rejected) ? ords.rejected : [];
    const sup = Array.isArray(ords.suppressed) ? ords.suppressed : [];
    const merged = (Array.isArray(ords.all) && ords.all.length ? ords.all : [
      ...bro.slice(0, 150).map(r => ({ kind: "broker", ...r })),
      ...por.slice(0, 150).map(r => ({ kind: "portfolio", ...r })),
      ...rej.slice(0, 150).map(r => ({
        ...r,
        kind: "rejected",
        status_bucket: r && r.status_bucket || "rejected",
        status_label: r && r.status_label || "Rejected",
        state: r && r.state || `REJECTED ${String(r && (r.reason_code || r.reason) || "rejected")}`,
        action: r && (r.action || r.reason_code || r.reason || "rejected"),
      })),
      ...sup.slice(0, 150).map(r => ({
        ...r,
        kind: "suppressed",
        status_bucket: r && r.status_bucket || "suppressed",
        status_label: r && r.status_label || "Suppressed",
        state: r && r.state || `SUPPRESSED ${String(r && (r.reason_code || r.reason) || "suppressed")}`,
        action: r && (r.action || r.reason_code || r.reason || "suppressed"),
      })),
    ]).map((row) => ({
      ...row,
      kind: row && row.kind || "order",
      status_bucket: row && row.status_bucket || "active",
      status_label: row && row.status_label || row && (row.state || row.action) || "Active",
    })).sort((a, b) => Number(b.updated_ts_ms || b.ts_ms || 0) - Number(a.updated_ts_ms || a.ts_ms || 0));

    const fills = Array.isArray(j.fills) ? j.fills : [];
    _terminalTableRows = {
      positions: pos,
      orders: merged,
      fills,
    };
    renderTerminalTables();

    const liveDataTs = Math.max(
      Number(acct && (acct.updated_ts_ms || acct.ts_ms) || 0),
      latestRowTs(pos, ["updated_ts_ms", "ts_ms"]),
      latestRowTs(merged, ["updated_ts_ms", "ts_ms", "created_ts_ms"]),
      latestRowTs(fills, ["ts_ms"])
    );
    const liveDataAgeMs = liveDataTs > 0 ? Math.max(0, Date.now() - liveDataTs) : null;
    const tone = liveDataAgeMs != null && liveDataAgeMs >= 300_000
      ? "warn"
      : "ok";
    const detailParts = [
      _executionBarrier.realTradingAllowed
        ? `real trading allowed (${_executionBarrier.mode})`
        : `real trading blocked (${_executionBarrier.blockingReasons[0] || _executionBarrier.reason || _executionBarrier.mode})`,
      fxSessionLabel(STATE.symbol),
      liveDataAgeMs == null ? "no live data timestamps" : `live data ${fmtAgeMs(liveDataAgeMs)} old`,
      Number.isFinite(Number(j.latency_ms)) ? `latency ${Math.round(Number(j.latency_ms))}ms` : "",
      acct ? "" : "account snapshot unavailable",
      Array.isArray(j.positions) && j.positions.length ? "" : "no live positions returned",
    ].filter(Boolean);
    applyOrderBarrierState(_accountSnapshotAvailable);
    const barrierTone = _executionBarrier.realTradingAllowed
      ? tone
      : _executionBarrier.severity === "CRITICAL" ? "crit" : "warn";
    setTerminalBanner(barrierTone, detailParts.join(" | "));
  } catch (error) {
    renderSnapshotFailure(error);
  }
}

function renderWatch() {
  if (!el.watchList) return;

  const q = String(STATE.watchFilter || "").trim().toUpperCase();
  const list = (q ? _watchSymbols.filter(s => String(s).toUpperCase().includes(q)) : _watchSymbols).slice(0, 500);

  if (!list.length) {
    el.watchList.innerHTML = `<div class="tableEmpty">${esc(q ? "No watchlist symbols match the current filter." : "No watchlist symbols are currently available.")}</div>`;
    return;
  }

  el.watchList.innerHTML = list.map(s => {
    const active = (String(s).toUpperCase() === String(STATE.symbol).toUpperCase());
    const fxBadge = isFxSymbol(s) ? "FX" : "chart";
    return `<div class="item ${active ? "active" : ""}" data-sym="${esc(s)}">
      <div class="mono">${esc(s)}</div>
      <div class="badge">${esc(fxBadge)}</div>
    </div>`;
  }).join("");

  el.watchList.querySelectorAll(".item").forEach(n => {
    n.addEventListener("click", () => setSymbol(n.getAttribute("data-sym"), "terminal_watchlist"));
  });
}

function markerShapeGlyph(shape) {
  const raw = String(shape || "").trim();
  if (raw === "line") return "---";
  if (raw === "arrowUp") return "^";
  if (raw === "arrowDown") return "v";
  if (raw === "square") return "[]";
  if (raw === "circle") return "o";
  if (raw === "band") return "band";
  return "-";
}

function renderOverlayLegend(payload, overlays = STATE.ov) {
  if (!el.overlayLegend) return;
  const normalized = normalizeDecisionOverlayPayload(payload || {});
  const indicatorOverlays = {
    vwap: !!(overlays && overlays.vwap),
    ema: !!(overlays && overlays.ema),
    equity: !!(overlays && overlays.equity),
  };
  const items = [
    ...indicatorOverlayLegendItems(indicatorOverlays),
    ...decisionOverlayLegendItems(normalized),
    ...decisionWindowLegendItems(normalized),
  ];
  const indicatorSummary = buildIndicatorAccessibilitySummary(indicatorOverlays);
  const decisionSummary = buildOverlayAccessibilitySummary(normalized);
  const summary = `${indicatorSummary} ${decisionSummary}`;
  const windows = Array.isArray(normalized.windows) ? normalized.windows : [];
  const levels = Array.isArray(normalized.price_lines) ? normalized.price_lines : [];
  el.overlayLegend.setAttribute("aria-label", summary);
  el.overlayLegend.innerHTML = `
    <div class="overlayLegendItems">
      ${items.map((item) => `
        <span class="overlayLegendItem">
          <span class="overlayLegendGlyph" style="border-color:${esc(item.color)}; color:${esc(item.color)}; background:${esc(item.fillColor || "transparent")}">${esc(markerShapeGlyph(item.shape))}</span>
          <span>${esc(item.label)}</span>
          <span class="mono muted">${esc(item.text)}</span>
          <span class="mono">${esc(String(item.count))}</span>
        </span>
      `).join("")}
    </div>
    <div class="overlayLegendSummary">${esc(summary)} ${windows.length ? `Windows ${windows.length}.` : ""} ${levels.length ? `Levels ${levels.length}.` : ""}</div>
  `;
}

async function bootChart() {
  const sym = String(STATE.symbol || "").trim().toUpperCase();
  if (!sym) return;

  const sessionText = fxSessionLabel(sym);
  if (el.chartTitle) el.chartTitle.textContent = `${sym} • ${STATE.tf} • ${STATE.type}${sessionText ? " • FX" : ""}`;
  if (el.chartHealth) el.chartHealth.textContent = sessionText ? `boot… • ${sessionText}` : "boot…";

  setProChartsState({ enabled: true, tf: STATE.tf, type: STATE.type });

  try {
    await startLiveMarketChart({
      containerId: "terminalChart",
      symbol: sym,
      tf: STATE.tf,
      type: STATE.type,
      crosshairElId: "xhairBox",
      healthElId: "chartHealth",
    });
  } catch (e) {
    if (el.chartHealth) {
      el.chartHealth.textContent = e && e.message ? e.message : "chart unavailable";
    }
    renderChartAccessibility("terminalChart", {
      title: `Terminal market chart ${sym}`,
      series: [],
      emptyMessage: "Terminal chart is unavailable.",
      errorMessage: e && e.message ? e.message : "chart unavailable",
      valueLabel: "close",
      chartType: "lightweight-chart",
    });
    return;
  }

  const overlays = {
    vwap: !!STATE.ov.vwap,
    ema: !!STATE.ov.ema,
    markers: !!STATE.ov.markers,
    equity: !!STATE.ov.equity,
  };

  let markers = [];
  let equitySeries = [];
  let decisionOverlay = null;

  try {
    if (overlays.markers) {
      const mj = await fetchJson(`/api/terminal/decision_overlays?symbol=${encodeURIComponent(sym)}`);
      if (mj && mj.ok) {
        decisionOverlay = mj;
        if (Array.isArray(mj.markers)) markers = mj.markers;
      }
    }
  } catch {
    try {
      const fallback = await fetchJson(`/api/terminal/markers?symbol=${encodeURIComponent(sym)}`);
      if (fallback && fallback.ok) {
        decisionOverlay = fallback;
        if (Array.isArray(fallback.markers)) markers = fallback.markers;
      }
    } catch {}
  }

  try {
    if (overlays.equity) {
      const ej = await fetchJson("/api/terminal/equity?limit=3000");
      if (ej && ej.ok && Array.isArray(ej.series)) equitySeries = ej.series;
    }
  } catch {}

  applyTerminalOverlays({
    symbol: sym,
    overlays,
    markers,
    equitySeries,
    decisionOverlay,
  });
  renderOverlayLegend(overlays.markers ? (decisionOverlay || { markers }) : { markers: [] }, overlays);

  if (el.chartHealth) el.chartHealth.textContent = sessionText ? `live • ${sessionText}` : "live";
}

function stopSnapshotTimer() {
  if (_snapshotTimer) {
    clearInterval(_snapshotTimer);
    _snapshotTimer = null;
  }
}

function startSnapshotTimer() {
  stopSnapshotTimer();
  if (document.hidden) return;
  _snapshotTimer = setInterval(async () => {
    await refreshSnapshot();
    renderWatch();
  }, 2500);
}

function isTextEntryTarget(target) {
  if (!target) return false;
  const tag = String(target.tagName || "").toUpperCase();
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || target.isContentEditable === true;
}

function canUseKeyboardTradingShortcut(label) {
  if (!_terminalArmed) {
    setTerminalBanner("warn", `${label} shortcut ignored. Enable Arm shortcuts first.`);
    return false;
  }
  return canSubmitRealTrade(label);
}

function isThresholdConfirmationError(error) {
  const payload = error && error.payload;
  const reason = String(payload && payload.reason_code || "");
  return !!(
    payload
    && payload.error === "confirmation_required"
    && reason.startsWith("threshold_")
    && (payload.required_confirm || payload.required_confirmation)
  );
}

async function requestTerminalThresholdPayload(error, { side = "", qty = 0, intent = "order" } = {}) {
  const payload = error && error.payload ? error.payload : {};
  const token = String(payload.required_confirm || payload.required_confirmation || "").trim();
  if (!token) return null;
  const actionId = intent === "flatten" ? "terminal.flatten.threshold" : "terminal.order.threshold";
  const target = intent === "flatten"
    ? `${STATE.symbol} flatten`
    : `${String(side || "").toUpperCase()} ${STATE.symbol} qty ${fmtSymbolQty(STATE.symbol, qty)}`;
  const confirmation = await requestConfirmation({
    title: "Confirm terminal threshold",
    action: "Terminal pre-trade threshold",
    actionId,
    target,
    consequence: String(payload.message || payload.reason || "This request exceeds a configured terminal review threshold."),
    confirmText: token,
    submitLabel: "Confirm Threshold",
    actor: "terminal_operator",
    sourceSurface: "terminal",
  });
  if (!confirmation || !confirmation.ok) return null;
  const confirmedPayload = confirmation.payload || {};
  return {
    threshold_confirm: token,
    threshold_confirmation: token,
    threshold_confirmation_method: confirmedPayload.confirmation_method || "typed_phrase",
    threshold_confirmation_hold_ms: Number(confirmedPayload.confirmation_hold_ms || 0),
    threshold_consequence_ack: true,
    threshold_actor: confirmedPayload.actor || "terminal_operator",
    threshold_source: "terminal",
    threshold_source_surface: "terminal",
    threshold_request_id: confirmedPayload.request_id || requestId("terminal-threshold"),
    threshold_reason: confirmedPayload.reason || "",
  };
}

async function submitTerminalOrder(side, qty, label) {
  if (!canSubmitDirectionalOrder(label || side)) return;
  const cleanQty = Number(qty || 0);
  if (!Number.isFinite(cleanQty) || cleanQty <= 0) throw new Error("enter a positive quantity");
  renderOrderPreview(side);
  const body = {
    symbol: STATE.symbol,
    side,
    qty: cleanQty,
    ...orderConfirmationPayload("TRADE"),
  };
  let j;
  try {
    j = await postJson("/api/terminal/order", body);
  } catch (error) {
    if (!isThresholdConfirmationError(error)) throw error;
    const thresholdPayload = await requestTerminalThresholdPayload(error, { side, qty: cleanQty, intent: "order" });
    if (!thresholdPayload) {
      setTerminalBanner("warn", `${label || side} threshold confirmation cancelled.`);
      return;
    }
    j = await postJson("/api/terminal/order", { ...body, ...thresholdPayload });
  }
  if (j.ok) await refreshSnapshot();
}

async function submitTerminalFlatten() {
  if (!canSubmitRealTrade("Flatten")) return;
  const body = {
    symbol: STATE.symbol,
    ...orderConfirmationPayload("FLATTEN", FLATTEN_HOLD_MS),
  };
  let j;
  try {
    j = await postJson("/api/terminal/flatten", body);
  } catch (error) {
    if (!isThresholdConfirmationError(error)) throw error;
    const thresholdPayload = await requestTerminalThresholdPayload(error, { intent: "flatten" });
    if (!thresholdPayload) {
      setTerminalBanner("warn", "Flatten threshold confirmation cancelled.");
      return;
    }
    j = await postJson("/api/terminal/flatten", { ...body, ...thresholdPayload });
  }
  if (j.ok) await refreshSnapshot();
}

function startFlattenHold(e) {
  if (e) e.preventDefault();
  if (!canSubmitRealTrade("Flatten")) return;
  cancelFlattenHold();
  if (el.btnFlat) el.btnFlat.textContent = "HOLD...";
  setTerminalBanner("warn", `Hold FLATTEN for ${(FLATTEN_HOLD_MS / 1000).toFixed(1)}s to send a backend-gated flatten intent.`);
  _flattenHoldTimer = setTimeout(async () => {
    _flattenHoldTimer = null;
    _flattenHoldCompletedAt = Date.now();
    if (el.btnFlat) el.btnFlat.textContent = "FLATTEN";
    try {
      await submitTerminalFlatten();
    } catch (error) {
      setTerminalBanner("crit", `Flatten request failed: ${String(error && error.message ? error.message : error)}`);
    }
  }, FLATTEN_HOLD_MS);
}

function wire() {
  if (_wired) return;
  _wired = true;

  if (!el.symInput || !el.tfSel || !el.typeSel || !el.ovVwap || !el.ovEma || !el.ovMarkers || !el.ovEquity || !el.watchFilter || !el.posFilter || !el.ordFilter || !el.fillsFilter || !el.safetyStatus || !el.terminalArmChk) {
    throw new Error("terminal_dom_incomplete");
  }

  el.symInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") setSymbol(el.symInput.value, "terminal_input");
  });

  el.tfSel.addEventListener("change", () => setTf(el.tfSel.value));
  el.typeSel.addEventListener("change", () => setType(el.typeSel.value));

  el.ovVwap.addEventListener("change", () => setOv("vwap", el.ovVwap.checked));
  el.ovEma.addEventListener("change", () => setOv("ema", el.ovEma.checked));
  el.ovMarkers.addEventListener("change", () => setOv("markers", el.ovMarkers.checked));
  el.ovEquity.addEventListener("change", () => setOv("equity", el.ovEquity.checked));

  el.watchFilter.addEventListener("input", () => {
    STATE.watchFilter = el.watchFilter.value || "";
    persist();
    renderWatch();
  });

  el.posFilter.addEventListener("input", () => setTerminalTableQuery("positions", el.posFilter.value));
  el.ordFilter.addEventListener("input", () => setTerminalTableQuery("orders", el.ordFilter.value));
  el.fillsFilter.addEventListener("input", () => setTerminalTableQuery("fills", el.fillsFilter.value));
  if (el.ordStatusFilter) el.ordStatusFilter.addEventListener("change", () => setTerminalStatusFilter("orders", el.ordStatusFilter.value));
  if (el.fillsStatusFilter) el.fillsStatusFilter.addEventListener("change", () => setTerminalStatusFilter("fills", el.fillsStatusFilter.value));

  document.addEventListener("click", (event) => {
    const target = event && event.target;
    const button = target && typeof target.closest === "function"
      ? target.closest("[data-terminal-table-sort]")
      : null;
    if (!button) return;
    event.preventDefault();
    setTerminalTableSort(
      button.getAttribute("data-terminal-table-sort"),
      button.getAttribute("data-sort-key"),
      button.getAttribute("data-sort-default")
    );
  });

  el.terminalArmChk.addEventListener("change", () => {
    _terminalArmed = !!el.terminalArmChk.checked;
    renderTradingSafetyStatus();
    setTerminalBanner(
      _terminalArmed ? "warn" : "ok",
      _terminalArmed
        ? "Keyboard BUY/SELL shortcuts are armed. FLATTEN still requires hold-to-confirm."
        : "Keyboard trading shortcuts are off."
    );
  });

  document.addEventListener("keydown", async (e) => {
    if (isTextEntryTarget(e.target)) return;
    const key = String(e.key || "").toLowerCase();

    if (key === "b") {
      e.preventDefault();
      if (!canUseKeyboardTradingShortcut("Buy")) return;
      try {
        await submitTerminalOrder("BUY", currentOrderQty(), "Buy");
      } catch (error) {
        setTerminalBanner("crit", `Buy shortcut failed: ${String(error && error.message ? error.message : error)}`);
      }
    }

    if (key === "s") {
      e.preventDefault();
      if (!canUseKeyboardTradingShortcut("Sell")) return;
      try {
        await submitTerminalOrder("SELL", currentOrderQty(), "Sell");
      } catch (error) {
        setTerminalBanner("crit", `Sell shortcut failed: ${String(error && error.message ? error.message : error)}`);
      }
    }

    if (key === "f") {
      e.preventDefault();
      if (!_terminalArmed) {
        setTerminalBanner("warn", "Flatten shortcut ignored. Enable Arm shortcuts first.");
        return;
      }
      setTerminalBanner("warn", "Flatten cannot be sent by keypress. Hold the FLATTEN button to confirm.");
    }
  });

  const ordQty = el.ordQty;
  const btnBuy = el.btnBuy;
  const btnSell = el.btnSell;
  const btnFlat = el.btnFlat;

  if (ordQty) ordQty.addEventListener("input", () => renderOrderPreview());

  if (btnBuy && ordQty) btnBuy.addEventListener("click", async () => {
    try {
      await submitTerminalOrder("BUY", Number(ordQty.value || 0), "Buy");
    } catch (error) {
      setTerminalBanner("crit", `Buy request failed: ${String(error && error.message ? error.message : error)}`);
    }
  });

  if (btnSell && ordQty) btnSell.addEventListener("click", async () => {
    try {
      await submitTerminalOrder("SELL", Number(ordQty.value || 0), "Sell");
    } catch (error) {
      setTerminalBanner("crit", `Sell request failed: ${String(error && error.message ? error.message : error)}`);
    }
  });

  if (btnFlat) {
    btnFlat.addEventListener("click", (e) => {
      e.preventDefault();
      if (Date.now() - _flattenHoldCompletedAt < 1000) return;
      setTerminalBanner("warn", "Hold FLATTEN to confirm.");
    });
    btnFlat.addEventListener("pointerdown", startFlattenHold);
    btnFlat.addEventListener("pointerup", () => cancelFlattenHold());
    btnFlat.addEventListener("pointerleave", () => cancelFlattenHold());
    btnFlat.addEventListener("pointercancel", () => cancelFlattenHold());
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      stopSnapshotTimer();
      return;
    }
    void refreshSnapshot();
    renderWatch();
    startSnapshotTimer();
  });

  window.addEventListener("pagehide", () => {
    stopSnapshotTimer();
    stopLiveMarketChart();
  });
}

async function main() {
  initEls();
  applyLaunchParams();
  syncStateToDom();
  wire();
  setOrderEntryEnabled(false, "Waiting for the first terminal snapshot.");
  setFlattenEnabled(false, "Waiting for the first terminal snapshot and execution barrier.");
  renderTradingSafetyStatus();
  setTerminalBanner("warn", "Loading terminal snapshot…");
  await refreshSnapshot();
  renderWatch();
  await bootChart();
  startSnapshotTimer();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    void main();
  }, { once: true });
} else {
  void main();
}
