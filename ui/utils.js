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
