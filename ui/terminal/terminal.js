/*
  FILE: ui/terminal/terminal.js

  Browser-terminal controller for the trading system UI. This module manages
  terminal-specific fetches, rendering, and interactions for the dedicated
  `ui/terminal/` surface.
*/

import {
  setProChartsState,
  startLiveMarketChart,
  stopLiveMarketChart,
  applyTerminalOverlays
} from "./pro_charting.js";
import {
  initSelectedSymbolContextFromUrl,
  updateSelectedSymbolContext
} from "../symbol_context.mjs";
import { buildTableView } from "../utils.js";

const LS_KEY = "terminal.state.v1";
const FETCH_TIMEOUT_MS = 15000;
const FLATTEN_HOLD_MS = 1400;

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
  if (!r.ok) throw new Error(String((j && j.error) || r.statusText || "request_failed"));
  if (!j || typeof j !== "object") throw new Error(`invalid_json_response: ${url}`);
  if (j.ok === false) throw new Error(String(j.error || `api_error: ${url}`));
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
  el.xhairBox = document.getElementById("xhairBox");
  el.acctCash = document.getElementById("acctCash");
  el.acctEquity = document.getElementById("acctEquity");
  el.acctUpdated = document.getElementById("acctUpdated");
  el.acctMeta = document.getElementById("acctMeta");
  el.posFilter = document.getElementById("posFilter");
  el.posTbl = document.getElementById("posTbl");
  el.ordFilter = document.getElementById("ordFilter");
  el.ordTbl = document.getElementById("ordTbl");
  el.fillsFilter = document.getElementById("fillsFilter");
  el.fillsTbl = document.getElementById("fillsTbl");
  el.fillsMeta = document.getElementById("fillsMeta");
  el.statusBanner = document.getElementById("terminalStatusBanner");
  el.safetyStatus = document.getElementById("tradingSafetyStatus");
  el.terminalArmChk = document.getElementById("terminalArmChk");
  el.ordQty = document.getElementById("ordQty");
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
};

const saved = lsGet();
if (saved && typeof saved === "object") {
  STATE = {
    ...STATE,
    ...saved,
    ov: { ...STATE.ov, ...(saved.ov || {}) },
    tableFilters: { ...STATE.tableFilters, ...(saved.tableFilters || {}) },
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
  if (el.fillsFilter) el.fillsFilter.value = STATE.tableFilters.fills || "";
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
  { key: "symbol", label: "Symbol", accessor: (row) => row && String(row.symbol || "").toUpperCase() },
  { key: "qty", label: "Qty", accessor: (row) => row && row.qty },
  { key: "avg_px", label: "AvgPx", accessor: (row) => row && row.avg_px },
  { key: "updated_ts_ms", label: "Updated", accessor: (row) => row && row.updated_ts_ms, searchable: false },
]);
const TERMINAL_ORDER_COLUMNS = Object.freeze([
  { key: "kind", label: "Kind", accessor: (row) => row && row.kind },
  { key: "symbol", label: "Symbol", accessor: (row) => row && String(row.symbol || "").toUpperCase() },
  { key: "state", label: "State/Action", accessor: (row) => row && (row.state || row.action) },
  { key: "updatedTs", label: "Updated", accessor: (row) => row && (row.updated_ts_ms || row.ts_ms), searchable: false },
]);
const TERMINAL_FILL_COLUMNS = Object.freeze([
  { key: "ts_ms", label: "Time", accessor: (row) => row && row.ts_ms, searchable: false },
  { key: "symbol", label: "Symbol", accessor: (row) => row && String(row.symbol || "").toUpperCase() },
  { key: "qty", label: "Qty", accessor: (row) => row && row.qty },
  { key: "px", label: "Px", accessor: (row) => row && row.px },
]);

let _terminalTableRows = {
  positions: [],
  orders: [],
  fills: [],
};

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
  const view = buildTableView(rows, columns, {
    query,
    sortKey: state.sortKey || defaults.sortKey,
    sortDir: state.sortDir || defaults.sortDir,
    maxRows: defaults.maxRows,
  });
  const header = `<div class="row h" role="row">${columns.map((column) => {
    const active = column.key === state.sortKey;
    const sortDir = active ? (state.sortDir === "desc" ? "descending" : "ascending") : "none";
    const indicator = active ? (state.sortDir === "desc" ? "v" : "^") : "";
    return `<div class="c" role="columnheader" aria-sort="${esc(sortDir)}"><button class="sortBtn ${active ? "is-active" : ""}" type="button" data-terminal-table-sort="${esc(tableId)}" data-sort-key="${esc(column.key)}">${esc(column.label)}<span class="sortIndicator" aria-hidden="true">${esc(indicator)}</span></button></div>`;
  }).join("")}</div>`;
  const body = view.visibleRows.length
    ? view.visibleRows.map((row) => {
      const cols = rowColsFn(row);
      return `<div class="row" role="row">${cols.map((col) => `<div class="c" role="cell">${col}</div>`).join("")}</div>`;
    }).join("")
    : `<div class="tableEmpty">${esc(view.totalRows > 0 ? (filteredEmptyText || "No rows match the current filter.") : emptyText)}</div>`;
  container.innerHTML = header + body;
  return view;
}

function renderTerminalTables(emptyMessages = {}) {
  const positionsView = renderTerminalTable(
    el.posTbl,
    "positions",
    TERMINAL_POSITION_COLUMNS,
    _terminalTableRows.positions,
    (r) => ([
      esc((r && r.symbol || "").toUpperCase()),
      `<span class="mono">${esc(fmtNum(r && r.qty, 4))}</span>`,
      `<span class="mono">${esc(fmtNum(r && r.avg_px, 4))}</span>`,
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
      esc(r && (r.state || r.action) || "—"),
      `<span class="mono">${esc(fmtTs(r && (r.updated_ts_ms || r.ts_ms)))}</span>`,
    ]),
    emptyMessages.orders || "No live broker or portfolio orders are currently available.",
    "No orders match the current filter."
  );
  const fillsView = renderTerminalTable(
    el.fillsTbl,
    "fills",
    TERMINAL_FILL_COLUMNS,
    _terminalTableRows.fills,
    (r) => ([
      `<span class="mono">${esc(fmtTs(r && r.ts_ms))}</span>`,
      esc((r && r.symbol || "").toUpperCase()),
      `<span class="mono">${esc(fmtNum(r && r.qty, 4))}</span>`,
      `<span class="mono">${esc(fmtNum(r && r.px, 4))}</span>`,
    ]),
    emptyMessages.fills || "No live fills are currently available.",
    "No fills match the current filter."
  );
  if (el.fillsMeta && fillsView) {
    el.fillsMeta.textContent = fillsView.query
      ? `${fillsView.visibleRowsCount}/${fillsView.totalRows} rows`
      : `${fillsView.totalRows} rows`;
  }
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
    return;
  }

  setOrderEntryEnabled(
    !!accountAvailable,
    accountAvailable ? "" : "Order entry is disabled until the account snapshot is available."
  );
  setFlattenEnabled(true, "Hold to confirm flatten. Backend execution gates still apply.");
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
    const j = await fetchJson("/api/terminal/snapshot");
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

    const ords = (j.orders && typeof j.orders === "object") ? j.orders : { broker: [], portfolio: [] };
    const bro = Array.isArray(ords.broker) ? ords.broker : [];
    const por = Array.isArray(ords.portfolio) ? ords.portfolio : [];
    const merged = [
      ...bro.slice(0, 150).map(r => ({ kind: "broker", ...r })),
      ...por.slice(0, 150).map(r => ({ kind: "portfolio", ...r })),
    ].sort((a, b) => Number(b.updated_ts_ms || b.ts_ms || 0) - Number(a.updated_ts_ms || a.ts_ms || 0));

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
    return `<div class="item ${active ? "active" : ""}" data-sym="${esc(s)}">
      <div class="mono">${esc(s)}</div>
      <div class="badge">chart</div>
    </div>`;
  }).join("");

  el.watchList.querySelectorAll(".item").forEach(n => {
    n.addEventListener("click", () => setSymbol(n.getAttribute("data-sym"), "terminal_watchlist"));
  });
}

async function bootChart() {
  const sym = String(STATE.symbol || "").trim().toUpperCase();
  if (!sym) return;

  if (el.chartTitle) el.chartTitle.textContent = `${sym} • ${STATE.tf} • ${STATE.type}`;
  if (el.chartHealth) el.chartHealth.textContent = "boot…";

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

  try {
    if (overlays.markers) {
      const mj = await fetchJson(`/api/terminal/markers?symbol=${encodeURIComponent(sym)}`);
      if (mj && mj.ok && Array.isArray(mj.markers)) markers = mj.markers;
    }
  } catch {}

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
  });

  if (el.chartHealth) el.chartHealth.textContent = "live";
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

async function submitTerminalOrder(side, qty, label) {
  if (!canSubmitDirectionalOrder(label || side)) return;
  const cleanQty = Number(qty || 0);
  if (!Number.isFinite(cleanQty) || cleanQty <= 0) throw new Error("enter a positive quantity");
  const j = await postJson("/api/terminal/order", {
    symbol: STATE.symbol,
    side,
    qty: cleanQty
  });
  if (j.ok) await refreshSnapshot();
}

async function submitTerminalFlatten() {
  if (!canSubmitRealTrade("Flatten")) return;
  const j = await postJson("/api/terminal/flatten", {
    symbol: STATE.symbol
  });
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
        await submitTerminalOrder("BUY", 100, "Buy");
      } catch (error) {
        setTerminalBanner("crit", `Buy shortcut failed: ${String(error && error.message ? error.message : error)}`);
      }
    }

    if (key === "s") {
      e.preventDefault();
      if (!canUseKeyboardTradingShortcut("Sell")) return;
      try {
        await submitTerminalOrder("SELL", 100, "Sell");
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
