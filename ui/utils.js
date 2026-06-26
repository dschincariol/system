"use strict";

/* ui/utils.js — pure UI helpers (no DOM, no side effects) */

export function esc(x) {
  return String(x ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

export function escapeHTML(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function fmtTime(ms) {
  try {
    const direct = Number(ms);
    if (Number.isFinite(direct) && direct > 0) {
      return new Date(direct).toLocaleTimeString();
    }
    if (typeof ms === "string") {
      const trimmed = ms.trim();
      if (!trimmed) return "—";
      const parsed = Date.parse(trimmed);
      if (Number.isFinite(parsed) && parsed > 0) {
        return new Date(parsed).toLocaleTimeString();
      }
    }
    return "—";
  } catch {
    return "—";
  }
}

export function fmtNum(x) {
  if (x === null || x === undefined) return "";
  const v = Number(x);
  if (!isFinite(v)) return "";
  return v.toFixed(4);
}

export function _fmtPct(x) {
  if (!Number.isFinite(x)) return "?";
  return `${(x * 100).toFixed(2)}%`;
}

export function _clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

export function barWidth(pct) {
  const v = Math.max(0, Math.min(100, pct));
  return `${v.toFixed(1)}%`;
}

export function _debounce(fn, ms = 120) {
  let t;
  return (...a) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...a), ms);
  };
}

export function numOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function pickTimestamp(...values) {
  for (const value of values) {
    const n = numOrNull(value);
    if (n != null && n > 0) return n;
  }
  return null;
}

export function ageMsFromTimestamp(tsMs) {
  const ts = numOrNull(tsMs);
  if (ts == null || ts <= 0) return null;
  return Math.max(0, Date.now() - ts);
}

export function formatAgeMs(ageMs) {
  const age = numOrNull(ageMs);
  if (age == null) return "—";
  if (age < 1000) return `${Math.round(age)}ms`;
  if (age < 60_000) return `${Math.round(age / 1000)}s`;
  if (age < 3_600_000) return `${Math.round(age / 60_000)}m`;
  return `${(age / 3_600_000).toFixed(age < 36_000_000 ? 1 : 0)}h`;
}

export function freshnessTone(ageMs, warnMs = 60_000, critMs = 300_000) {
  const age = numOrNull(ageMs);
  if (age == null) return "dim";
  if (age >= critMs) return "bad";
  if (age >= warnMs) return "warn";
  return "ok";
}

export const STATUS_TOKENS = Object.freeze({
  neutral: Object.freeze({
    key: "neutral",
    className: "neutral",
    label: "Neutral",
    glyph: "-",
    color: "#A7B0BC",
    fill: "rgba(167,176,188,0.13)",
  }),
  info: Object.freeze({
    key: "info",
    className: "info",
    label: "Info",
    glyph: "i",
    color: "#56B4E9",
    fill: "rgba(86,180,233,0.14)",
  }),
  ok: Object.freeze({
    key: "ok",
    className: "ok",
    label: "OK",
    glyph: "OK",
    color: "#009E73",
    fill: "rgba(0,158,115,0.14)",
  }),
  warn: Object.freeze({
    key: "warn",
    className: "warn",
    label: "Warning",
    glyph: "!",
    color: "#E69F00",
    fill: "rgba(230,159,0,0.15)",
  }),
  high: Object.freeze({
    key: "high",
    className: "high",
    label: "High",
    glyph: "!!",
    color: "#CC79A7",
    fill: "rgba(204,121,167,0.15)",
  }),
  crit: Object.freeze({
    key: "crit",
    className: "crit",
    label: "Critical",
    glyph: "X",
    color: "#D55E00",
    fill: "rgba(213,94,0,0.16)",
  }),
  blocked: Object.freeze({
    key: "blocked",
    className: "blocked",
    label: "Blocked",
    glyph: "LOCK",
    color: "#73B7E6",
    fill: "rgba(115,183,230,0.16)",
  }),
  unavailable: Object.freeze({
    key: "unavailable",
    className: "unavailable",
    label: "Unavailable",
    glyph: "?",
    color: "#8B949E",
    fill: "rgba(139,148,158,0.13)",
  }),
});

const STATUS_ALIASES = Object.freeze({
  acked: "info",
  active: "info",
  allowed: "ok",
  available: "ok",
  bad: "crit",
  critical: "crit",
  danger: "crit",
  degraded: "warn",
  dim: "neutral",
  disabled: "blocked",
  disconnected: "crit",
  err: "crit",
  error: "crit",
  fail: "crit",
  failed: "crit",
  halted: "blocked",
  healthy: "ok",
  kill: "crit",
  kill_switch: "blocked",
  missing: "unavailable",
  muted: "neutral",
  normal: "ok",
  partial: "warn",
  pending: "warn",
  ready: "ok",
  resolved: "ok",
  stable: "ok",
  stale: "warn",
  success: "ok",
  unknown: "unavailable",
  waiting: "unavailable",
  watch: "warn",
});

export function normalizeStatusTone(tone) {
  const raw = String(tone ?? "neutral").trim().toLowerCase().replace(/[\s-]+/g, "_");
  if (!raw) return "neutral";
  if (Object.prototype.hasOwnProperty.call(STATUS_TOKENS, raw)) return raw;
  return STATUS_ALIASES[raw] || "neutral";
}

export function statusToken(tone) {
  return STATUS_TOKENS[normalizeStatusTone(tone)] || STATUS_TOKENS.neutral;
}

export function statusClassName(tone) {
  return statusToken(tone).className;
}

export function statusGlyph(tone) {
  return statusToken(tone).glyph;
}

export function statusLabel(tone) {
  return statusToken(tone).label;
}

export function statusPillClasses(tone) {
  const token = statusToken(tone);
  const classes = ["pill", token.className, `status-${token.key}`];
  if (token.key === "neutral") classes.push("dim");
  if (token.key === "crit") classes.push("bad");
  return Array.from(new Set(classes)).join(" ");
}

export function statusAriaLabel(tone, text = "") {
  const token = statusToken(tone);
  const detail = String(text || "").trim();
  return detail ? `${token.label}: ${detail}` : token.label;
}

export function withStatusGlyph(text, tone) {
  const token = statusToken(tone);
  const label = String(text || "").trim();
  if (!label) return token.glyph;
  if (label.toUpperCase().startsWith(`${token.glyph} `)) return label;
  return `${token.glyph} ${label}`;
}

function cleanPrimitiveText(value, fallback = "-") {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  return text || fallback;
}

function primitiveClassList(...parts) {
  return parts
    .flatMap((part) => String(part || "").split(/\s+/))
    .map((part) => part.trim())
    .filter(Boolean)
    .join(" ");
}

export function statusPillHtml(label, tone = "neutral", {
  className = "",
  id = "",
  mono = false,
  title = "",
} = {}) {
  const token = statusToken(tone);
  const text = cleanPrimitiveText(label);
  const classes = primitiveClassList("pill", token.className, mono ? "mono" : "", className);
  return `<span${id ? ` id="${escapeHTML(id)}"` : ""} class="${escapeHTML(classes)}" data-status="${escapeHTML(token.key)}" aria-label="${escapeHTML(statusAriaLabel(token.key, text))}"${title ? ` title="${escapeHTML(title)}"` : ""}>${escapeHTML(text)}</span>`;
}

export function kpiTileHtml({
  label,
  value,
  meta,
  tone = "neutral",
  className = "",
  valueClassName = "",
} = {}) {
  const token = statusToken(tone);
  const safeLabel = cleanPrimitiveText(label);
  const safeValue = cleanPrimitiveText(value);
  const safeMeta = cleanPrimitiveText(meta, "");
  const classes = primitiveClassList("opsKpiTile", token.className, className);
  return `
    <div class="${escapeHTML(classes)}" data-status="${escapeHTML(token.key)}" role="group" aria-label="${escapeHTML(statusAriaLabel(token.key, `${safeLabel}: ${safeValue}${safeMeta ? `. ${safeMeta}` : ""}`))}">
      <span class="opsKpiLabel">${escapeHTML(safeLabel)}</span>
      <strong class="${escapeHTML(primitiveClassList("opsKpiValue", valueClassName))}">${escapeHTML(safeValue)}</strong>
      <span class="opsKpiMeta">${escapeHTML(safeMeta || "-")}</span>
    </div>
  `;
}

export function guidanceListHtml(items, {
  className = "",
  emptyText = "Hold position and wait for the next runtime update.",
  id = "",
} = {}) {
  const steps = (Array.isArray(items) ? items : [])
    .map((item) => cleanPrimitiveText(item, ""))
    .filter(Boolean);
  const safeSteps = steps.length ? steps : [emptyText];
  return `<ol${id ? ` id="${escapeHTML(id)}"` : ""} class="${escapeHTML(primitiveClassList("opsGuidanceList", className))}">${safeSteps
    .map((item) => `<li>${escapeHTML(item)}</li>`)
    .join("")}</ol>`;
}

export function tradeMarkerStatus(side, qty = 0) {
  const rawSide = String(side || "").toUpperCase();
  const nQty = Number(qty || 0);
  if (rawSide.includes("SELL") || rawSide.includes("SHORT") || nQty < 0) return "crit";
  if (rawSide.includes("BUY") || rawSide.includes("LONG") || nQty > 0) return "info";
  return "neutral";
}

export function chartMarkerStyle(side, qty = 0) {
  const token = statusToken(tradeMarkerStatus(side, qty));
  const isSell = token.key === "crit";
  return {
    token,
    isBuy: !isSell,
    color: token.color,
    position: isSell ? "aboveBar" : "belowBar",
    shape: isSell ? "arrowDown" : "arrowUp",
    label: isSell ? "SELL" : "BUY",
    glyph: isSell ? "X" : "i",
  };
}

export function chartVolumeColor(close, open, alpha = 0.45) {
  const up = Number(close) >= Number(open);
  const token = up ? STATUS_TOKENS.info : STATUS_TOKENS.crit;
  const fallback = up ? "86,180,233" : "213,94,0";
  const match = token.color.match(/^#([0-9a-f]{6})$/i);
  if (!match) return `rgba(${fallback},${alpha})`;
  const hex = match[1];
  const r = Number.parseInt(hex.slice(0, 2), 16);
  const g = Number.parseInt(hex.slice(2, 4), 16);
  const b = Number.parseInt(hex.slice(4, 6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

export function formatSigned(value, digits = 2, suffix = "") {
  const n = numOrNull(value);
  if (n == null) return "—";
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(digits)}${suffix}`;
}

export function formatDecimal(value, digits = 2) {
  const n = numOrNull(value);
  return n == null ? "—" : n.toFixed(digits);
}

export function formatPercent(value, digits = 2) {
  const n = numOrNull(value);
  return n == null ? "—" : `${(n * 100).toFixed(digits)}%`;
}

export function safeJoin(parts, sep = " · ") {
  return (parts || []).filter(Boolean).join(sep);
}

export function normalizeTableQuery(value) {
  return String(value ?? "").trim().toLowerCase();
}

export function tableValueText(value) {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.map((item) => tableValueText(item)).filter(Boolean).join(" ");
  if (value instanceof Date) return Number.isFinite(value.getTime()) ? value.toISOString() : "";
  if (typeof value === "object") return Object.values(value).map((item) => tableValueText(item)).filter(Boolean).join(" ");
  return String(value);
}

function _isBlankTableValue(value) {
  return tableValueText(value).trim() === "";
}

function _parseTableNumber(text) {
  const cleaned = String(text || "")
    .trim()
    .replace(/[$,%]/g, "")
    .replace(/,/g, "");
  if (!cleaned) return NaN;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : NaN;
}

function _parseTableTimestamp(text) {
  const value = String(text || "").trim();
  if (!value) return NaN;
  if (!/[T:\-\/]|\b(?:AM|PM|UTC|GMT)\b/i.test(value)) return NaN;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : NaN;
}

function _tableColumnValue(column, row, accessorKey) {
  if (!column || !row) return "";
  const fn = column[accessorKey];
  if (typeof fn === "function") return fn(row);
  if (accessorKey !== "accessor" && typeof column.accessor === "function") return column.accessor(row);
  const key = column.key;
  return key ? row[key] : "";
}

export function compareTableValues(left, right) {
  const leftText = tableValueText(left).trim();
  const rightText = tableValueText(right).trim();
  const leftNumber = _parseTableNumber(leftText);
  const rightNumber = _parseTableNumber(rightText);
  const leftIsNumber = Number.isFinite(leftNumber);
  const rightIsNumber = Number.isFinite(rightNumber);

  if (leftIsNumber && rightIsNumber && leftNumber !== rightNumber) {
    return leftNumber < rightNumber ? -1 : 1;
  }
  const leftTimestamp = leftIsNumber ? NaN : _parseTableTimestamp(leftText);
  const rightTimestamp = rightIsNumber ? NaN : _parseTableTimestamp(rightText);
  if (Number.isFinite(leftTimestamp) && Number.isFinite(rightTimestamp) && leftTimestamp !== rightTimestamp) {
    return leftTimestamp < rightTimestamp ? -1 : 1;
  }
  if (leftText === "" && rightText !== "") return 1;
  if (leftText !== "" && rightText === "") return -1;
  return leftText.localeCompare(rightText, undefined, { numeric: true, sensitivity: "base" });
}

export function filterTableRows(rows, columns, query) {
  const sourceRows = Array.isArray(rows) ? rows.slice() : [];
  const normalizedQuery = normalizeTableQuery(query);
  if (!normalizedQuery) return sourceRows;

  const searchableColumns = (Array.isArray(columns) ? columns : [])
    .filter((column) => column && column.searchable !== false);
  if (!searchableColumns.length) return [];

  return sourceRows.filter((row) => searchableColumns.some((column) => {
    const value = _tableColumnValue(column, row, "searchAccessor");
    return tableValueText(value).toLowerCase().includes(normalizedQuery);
  }));
}

export function sortTableRows(rows, columns, sortKey, sortDir = "asc") {
  const sourceRows = Array.isArray(rows) ? rows.slice() : [];
  const key = String(sortKey || "").trim();
  if (!key) return sourceRows;

  const column = (Array.isArray(columns) ? columns : []).find((item) => item && item.key === key);
  if (!column || column.sortable === false) return sourceRows;

  const direction = String(sortDir || "asc").toLowerCase() === "desc" ? -1 : 1;
  return sourceRows
    .map((row, index) => ({
      row,
      index,
      value: _tableColumnValue(column, row, "sortAccessor"),
    }))
    .sort((left, right) => {
      const leftBlank = _isBlankTableValue(left.value);
      const rightBlank = _isBlankTableValue(right.value);
      if (leftBlank && !rightBlank) return 1;
      if (!leftBlank && rightBlank) return -1;
      if (leftBlank && rightBlank) return left.index - right.index;
      const result = typeof column.compare === "function"
        ? column.compare(left.value, right.value, left.row, right.row)
        : compareTableValues(left.value, right.value);
      const comparableResult = Number(result);
      if (Number.isFinite(comparableResult) && comparableResult !== 0) {
        return comparableResult * direction;
      }
      return left.index - right.index;
    })
    .map((item) => item.row);
}

export function buildTableView(rows, columns, options = {}) {
  const sourceRows = Array.isArray(rows) ? rows.slice() : [];
  const query = normalizeTableQuery(options.query);
  const filteredRows = filterTableRows(sourceRows, columns, query);
  const sortedRows = sortTableRows(filteredRows, columns, options.sortKey, options.sortDir);
  const parsedMax = Number(options.maxRows);
  const maxRows = Number.isFinite(parsedMax) && parsedMax > 0 ? Math.floor(parsedMax) : sortedRows.length;
  const visibleRows = sortedRows.slice(0, maxRows);

  return {
    query,
    sortKey: String(options.sortKey || ""),
    sortDir: String(options.sortDir || "asc").toLowerCase() === "desc" ? "desc" : "asc",
    allRows: sourceRows,
    filteredRows,
    sortedRows,
    visibleRows,
    totalRows: sourceRows.length,
    filteredRowsCount: filteredRows.length,
    visibleRowsCount: visibleRows.length,
    hiddenRowsCount: Math.max(0, sortedRows.length - visibleRows.length),
  };
}
