"use strict";

/*
  FILE: ui/chart_a11y.js

  Shared production helpers for chart accessibility. Renderers pass the same
  normalized rows used for drawing; this module assigns programmatic chart
  labels, one-line takeaways, keyboard focus metadata, and a toggleable table
  fallback next to the visual chart.
*/

const DEFAULT_MAX_TABLE_ROWS = 80;

function _docFor(el) {
  if (el && el.ownerDocument) return el.ownerDocument;
  if (typeof document !== "undefined") return document;
  return null;
}

function _byId(id) {
  const doc = _docFor(null);
  return doc && id ? doc.getElementById(id) : null;
}

function _asElement(target) {
  if (!target) return null;
  if (typeof target === "string") return _byId(target);
  return target;
}

function _esc(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function _safeId(value) {
  return String(value || "chart")
    .trim()
    .replace(/[^A-Za-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "") || "chart";
}

function _num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function formatChartValue(value, digits = 4) {
  const n = _num(value);
  if (n == null) return "unavailable";
  const d = Number.isFinite(Number(digits)) ? Number(digits) : 4;
  if (Math.abs(n) >= 1000) {
    return n.toLocaleString(undefined, { maximumFractionDigits: Math.min(2, d) });
  }
  return n.toFixed(Math.max(0, d));
}

export function formatChartTime(value) {
  if (value == null || value === "") return "";
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) {
    const ms = n < 10000000000 ? n * 1000 : n;
    if (ms > 100000000000) {
      try { return new Date(ms).toLocaleString(); } catch {}
    }
    return String(value);
  }
  const text = String(value);
  const parsed = Date.parse(text);
  if (Number.isFinite(parsed) && parsed > 0) {
    try { return new Date(parsed).toLocaleString(); } catch {}
  }
  return text;
}

function _pick(row, keys) {
  if (!row || typeof row !== "object") return undefined;
  for (const key of keys) {
    if (key && row[key] !== undefined && row[key] !== null) return row[key];
  }
  return undefined;
}

export function normalizeChartSeries(series = [], options = {}) {
  const rows = Array.isArray(series) ? series : [];
  const valueKeys = [
    options.valueKey,
    "value",
    "close",
    "equity",
    "drawdown",
    "sentiment",
    "stress_score",
    "diff_equity_pct",
    "acc",
  ];
  const timeKeys = [options.timeKey, "time", "ts_ms", "t", "date", "label"];
  const labelKeys = [options.labelKey, "label", "name", "source"];

  return rows.map((row, index) => {
    if (row && typeof row === "object") {
      const rawValue = _pick(row, valueKeys);
      const value = _num(rawValue);
      const time = _pick(row, timeKeys);
      const label = _pick(row, labelKeys);
      return {
        index: index + 1,
        raw: row,
        time,
        label,
        value,
        timeText: label != null && label !== "" ? String(label) : (time != null && time !== "" ? formatChartTime(time) : String(index + 1)),
      };
    }
    return {
      index: index + 1,
      raw: row,
      time: index + 1,
      label: "",
      value: _num(row),
      timeText: String(index + 1),
    };
  }).filter((row) => row.value != null);
}

function _formatWith(formatter, value) {
  if (typeof formatter === "function") {
    try {
      const out = formatter(value);
      if (out !== undefined && out !== null && out !== "") return String(out);
    } catch {}
  }
  return formatChartValue(value);
}

export function buildChartTakeaway(options = {}) {
  const title = String(options.title || "Chart").trim() || "Chart";
  const error = String(options.errorMessage || "").trim();
  if (error) return `${title}: ${error}`;

  const rows = Array.isArray(options.normalizedRows)
    ? options.normalizedRows
    : normalizeChartSeries(options.series || [], options);
  if (!rows.length) {
    return `${title}: ${String(options.emptyMessage || "No chart data is available.")}`;
  }

  const values = rows.map((row) => row.value).filter((value) => Number.isFinite(value));
  if (!values.length) {
    return `${title}: ${String(options.emptyMessage || "No numeric chart data is available.")}`;
  }

  const first = values[0];
  const last = values[values.length - 1];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const delta = last - first;
  const fmt = (value) => _formatWith(options.valueFormatter, value);
  const valueLabel = String(options.valueLabel || "value").trim() || "value";
  let movement = "flat versus the first point";
  if (Math.abs(delta) > 1e-12) {
    movement = `${delta > 0 ? "up" : "down"} ${fmt(Math.abs(delta))} versus the first point`;
  }

  return `${title}: latest ${valueLabel} ${fmt(last)}; ${movement} across ${values.length} points; range ${fmt(min)} to ${fmt(max)}.`;
}

function _resolveContainer(chartEl, containerId) {
  const doc = _docFor(chartEl);
  if (!doc || !chartEl) return null;
  const id = containerId || `${chartEl.id || "chart"}A11y`;
  let container = id ? doc.getElementById(id) : null;
  if (!container && typeof doc.createElement === "function") {
    container = doc.createElement("div");
    container.id = id;
    if (chartEl.parentNode && typeof chartEl.parentNode.insertBefore === "function") {
      chartEl.parentNode.insertBefore(container, chartEl.nextSibling || null);
    }
  }
  if (container && container.classList && typeof container.classList.add === "function") {
    container.classList.add("chartA11y");
  } else if (container) {
    const classes = new Set(String(container.className || "").split(/\s+/).filter(Boolean));
    classes.add("chartA11y");
    container.className = Array.from(classes).join(" ");
  }
  return container;
}

function _columnValue(column, row) {
  if (!column) return "";
  if (typeof column.value === "function") {
    try { return column.value(row); } catch { return ""; }
  }
  if (column.key && row.raw && typeof row.raw === "object") return row.raw[column.key];
  if (column.key && row[column.key] !== undefined) return row[column.key];
  return "";
}

function _defaultColumns(valueLabel, valueFormatter) {
  return [
    { label: "Point", value: (row) => row.timeText || row.index },
    { label: valueLabel || "Value", value: (row) => _formatWith(valueFormatter, row.value) },
  ];
}

function _renderTableRows(rows, columns, emptyText, maxRows) {
  if (!rows.length) {
    return `<tr><td colspan="${Math.max(1, columns.length)}" class="metric-meta">${_esc(emptyText)}</td></tr>`;
  }
  const visible = rows.slice(Math.max(0, rows.length - maxRows));
  return visible.map((row) => `
    <tr>
      ${columns.map((column) => `<td>${_esc(_columnValue(column, row))}</td>`).join("")}
    </tr>
  `).join("");
}

export function applyChartFocusMetadata(target, options = {}) {
  const chartEl = _asElement(target);
  if (!chartEl || typeof chartEl.setAttribute !== "function") return null;
  const title = String(options.title || "Chart").trim() || "Chart";
  const summary = String(options.summary || title).trim();
  const pointCount = Number(options.pointCount || 0);
  const state = String(options.state || (pointCount > 0 ? "ready" : "empty"));

  chartEl.setAttribute("role", options.role || "img");
  chartEl.setAttribute("aria-label", summary);
  if (options.describedBy) chartEl.setAttribute("aria-describedby", options.describedBy);
  if (options.focusable !== false && !chartEl.hasAttribute("tabindex")) {
    chartEl.setAttribute("tabindex", "0");
  }
  chartEl.setAttribute("data-chart-a11y-title", title);
  chartEl.setAttribute("data-chart-a11y-state", state);
  chartEl.setAttribute("data-chart-a11y-points", String(Math.max(0, pointCount)));
  if (options.kind) chartEl.setAttribute("data-chart-a11y-kind", String(options.kind));
  return chartEl;
}

export function renderChartAccessibility(target, options = {}) {
  const chartEl = _asElement(target);
  if (!chartEl) return null;

  const title = String(options.title || chartEl.getAttribute?.("data-chart-a11y-title") || "Chart").trim() || "Chart";
  const idBase = _safeId(options.id || chartEl.id || title);
  const rows = normalizeChartSeries(options.series || options.rows || [], options);
  const error = String(options.errorMessage || "").trim();
  const emptyText = error || String(options.emptyMessage || "No chart data is available.");
  const state = error ? "error" : (rows.length ? "ready" : "empty");
  const summaryId = `${idBase}A11ySummary`;
  const tableId = `${idBase}A11yTable`;
  const summary = String(options.summary || buildChartTakeaway({
    ...options,
    title,
    normalizedRows: rows,
    errorMessage: error,
    emptyMessage: emptyText,
  }));
  const valueLabel = String(options.valueLabel || "Value");
  const columns = Array.isArray(options.columns) && options.columns.length
    ? options.columns
    : _defaultColumns(valueLabel, options.valueFormatter);
  const maxRows = Math.max(1, Number(options.maxRows || DEFAULT_MAX_TABLE_ROWS));
  const hiddenPrefix = rows.length > maxRows
    ? `<div class="chartA11yMeta small">Showing latest ${maxRows} of ${rows.length} points.</div>`
    : "";

  applyChartFocusMetadata(chartEl, {
    title,
    summary,
    pointCount: rows.length,
    state,
    kind: options.kind || options.chartType,
    describedBy: summaryId,
    focusable: options.focusable,
  });

  const container = _resolveContainer(chartEl, options.containerId);
  if (!container) return { chartEl, container: null, rows, summary, state };

  const tableRows = _renderTableRows(rows, columns, emptyText, maxRows);
  container.innerHTML = `
    <div id="${_esc(summaryId)}" class="chartA11ySummary small">${_esc(summary)}</div>
    <details class="chartA11yDetails">
      <summary>View chart data table</summary>
      ${hiddenPrefix}
      <div class="table-wrap chartA11yTableWrap">
        <table id="${_esc(tableId)}" class="chartA11yTable">
          <caption class="sr-only srOnly">${_esc(title)} data table</caption>
          <thead>
            <tr>${columns.map((column) => `<th>${_esc(column.label || column.key || "")}</th>`).join("")}</tr>
          </thead>
          <tbody>${tableRows}</tbody>
        </table>
      </div>
    </details>
  `;
  return { chartEl, container, rows, summary, state };
}
