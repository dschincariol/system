"use strict";

/*
  FILE: ui/chart_a11y.js

  Shared production helpers for chart accessibility. Renderers pass the same
  normalized rows used for drawing; this module assigns programmatic chart
  labels, one-line takeaways, keyboard focus metadata, and a toggleable table
  fallback next to the visual chart.
*/

const DEFAULT_MAX_TABLE_ROWS = 80;
const INSPECTOR_CLEANUPS = typeof WeakMap !== "undefined" ? new WeakMap() : null;

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

function _hasAttribute(el, name) {
  if (!el) return false;
  if (typeof el.hasAttribute === "function") return el.hasAttribute(name);
  if (typeof el.getAttribute === "function") return el.getAttribute(name) != null;
  return false;
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
  const n = value === null || value === undefined || value === "" ? null : _num(value);
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

function _normalizeSeriesFields(options = {}) {
  const fields = Array.isArray(options.seriesFields)
    ? options.seriesFields
    : (Array.isArray(options.seriesDefinitions)
        ? options.seriesDefinitions
        : (Array.isArray(options.valueSeries) ? options.valueSeries : []));
  return fields
    .map((field, index) => {
      if (typeof field === "string") {
        return { key: field, label: field, formatter: options.valueFormatter };
      }
      if (!field || typeof field !== "object") return null;
      const key = String(field.key || field.valueKey || field.field || `series_${index + 1}`).trim();
      if (!key && typeof field.value !== "function") return null;
      return {
        ...field,
        key,
        label: String(field.label || field.valueLabel || key || `Series ${index + 1}`).trim() || `Series ${index + 1}`,
        formatter: field.formatter || field.valueFormatter || options.valueFormatter,
      };
    })
    .filter(Boolean);
}

function _seriesFieldValue(field, raw, index) {
  if (!field) return null;
  if (typeof field.value === "function") {
    try {
      return _num(field.value(raw, index));
    } catch {
      return null;
    }
  }
  if (field.key && raw && typeof raw === "object") return _num(raw[field.key]);
  return null;
}

export function normalizeChartSeries(series = [], options = {}) {
  const rows = Array.isArray(series) ? series : [];
  const seriesFields = _normalizeSeriesFields(options);
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
      const fieldValues = seriesFields.map((field) => {
        const value = _seriesFieldValue(field, row, index);
        return {
          key: field.key,
          label: field.label,
          value,
          valueText: value == null ? "unavailable" : _formatWith(field.formatter, value),
          formatter: field.formatter,
        };
      });
      const firstFieldValue = fieldValues.find((field) => field.value != null);
      const value = _num(rawValue) ?? (firstFieldValue ? firstFieldValue.value : null);
      const time = _pick(row, timeKeys);
      const label = _pick(row, labelKeys);
      return {
        index: index + 1,
        raw: row,
        time,
        label,
        value,
        seriesValues: fieldValues,
        timeText: label != null && label !== "" ? String(label) : (time != null && time !== "" ? formatChartTime(time) : String(index + 1)),
      };
    }
    if (seriesFields.length) {
      return {
        index: index + 1,
        raw: row,
        time: index + 1,
        label: "",
        value: null,
        seriesValues: seriesFields.map((field) => ({
          key: field.key,
          label: field.label,
          value: null,
          valueText: "unavailable",
          formatter: field.formatter,
        })),
        timeText: String(index + 1),
      };
    }
    return {
      index: index + 1,
      raw: row,
      time: index + 1,
      label: "",
      value: _num(row),
      seriesValues: [],
      timeText: String(index + 1),
    };
  }).filter((row) => (
    seriesFields.length
      ? row.seriesValues.some((field) => field.value != null)
      : row.value != null
  ));
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

  const seriesFields = _normalizeSeriesFields(options);
  const rowSeriesValues = rows.flatMap((row) => Array.isArray(row.seriesValues) ? row.seriesValues : []);
  const summaryFields = seriesFields.length
    ? seriesFields
    : Array.from(new Map(rowSeriesValues.map((field) => [field.key, field])).values());
  if (summaryFields.length > 1) {
    const latestParts = [];
    const rangeParts = [];
    for (const field of summaryFields) {
      const values = rows
        .map((row) => (Array.isArray(row.seriesValues) ? row.seriesValues.find((item) => item.key === field.key) : null))
        .filter((item) => item && Number.isFinite(item.value));
      if (!values.length) continue;
      const latest = values[values.length - 1];
      const nums = values.map((item) => item.value);
      const min = Math.min(...nums);
      const max = Math.max(...nums);
      const fmt = (value) => _formatWith(field.formatter || options.valueFormatter, value);
      latestParts.push(`${field.label} ${latest.valueText || fmt(latest.value)}`);
      rangeParts.push(`${field.label} ${fmt(min)} to ${fmt(max)}`);
    }
    if (latestParts.length) {
      const context = String(options.contextSummary || options.annotationSummary || "").trim();
      return `${title}: latest ${latestParts.join(", ")}; ranges ${rangeParts.join(", ")}; across ${rows.length} rows.${context ? ` ${context}` : ""}`;
    }
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

function _defaultMultiSeriesColumns(seriesFields, valueFormatter) {
  return [
    { label: "Point", value: (row) => row.timeText || row.index },
    ...seriesFields.map((field) => ({
      label: field.label,
      value: (row) => {
        const match = Array.isArray(row.seriesValues)
          ? row.seriesValues.find((item) => item.key === field.key)
          : null;
        if (!match || match.value == null) return "unavailable";
        return match.valueText || _formatWith(field.formatter || valueFormatter, match.value);
      },
    })),
  ];
}

function _addClass(el, className) {
  if (!el || !className) return;
  if (el.classList && typeof el.classList.add === "function") {
    el.classList.add(className);
    return;
  }
  const classes = new Set(String(el.className || "").split(/\s+/).filter(Boolean));
  classes.add(className);
  el.className = Array.from(classes).join(" ");
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

function _resolveDataWindow(chartEl, containerId) {
  const doc = _docFor(chartEl);
  if (!doc || !chartEl) return null;
  const id = containerId || `${_safeId(chartEl.id || chartEl.getAttribute?.("data-chart-a11y-title") || "chart")}DataWindow`;
  let node = id ? doc.getElementById(id) : null;
  if (!node && typeof doc.createElement === "function") {
    node = doc.createElement("div");
    node.id = id;
    if (chartEl.parentNode && typeof chartEl.parentNode.insertBefore === "function") {
      chartEl.parentNode.insertBefore(node, chartEl.nextSibling || null);
    }
  }
  if (!node) return null;
  _addClass(node, "chartDataWindow");
  _addClass(node, "small");
  _addClass(node, "mono");
  if (typeof node.setAttribute === "function") {
    node.setAttribute("role", "status");
    node.setAttribute("aria-live", "polite");
  }
  return node;
}

function _setNodeText(node, text) {
  if (!node) return;
  if ("textContent" in node) {
    node.textContent = String(text || "");
  } else {
    node.innerHTML = _esc(text || "");
  }
}

function _normalizeInspectorValue(field, raw, index, options = {}) {
  const formatter = field && (field.formatter || field.valueFormatter) || options.valueFormatter;
  let value;
  if (field && typeof field.value === "function") {
    try { value = field.value(raw, index); } catch { value = null; }
  } else if (field && field.key && raw && typeof raw === "object") {
    value = raw[field.key];
  } else if (field && Object.prototype.hasOwnProperty.call(field, "value")) {
    value = field.value;
  }
  const n = _num(value);
  return {
    key: field && field.key ? String(field.key) : "",
    label: String(field && (field.label || field.valueLabel || field.key) || options.valueLabel || "Value"),
    value: n,
    valueText: field && field.valueText
      ? String(field.valueText)
      : (n == null ? "unavailable" : _formatWith(formatter, n)),
  };
}

function _normalizeInspectorPoint(row, index, options = {}) {
  const raw = row && typeof row === "object" ? (row.raw && typeof row.raw === "object" ? row.raw : row) : row;
  const base = row && typeof row === "object" ? row : {};
  const explicitLabel = _pick(base, ["label", "timeText", "bucket", "step"])
    ?? _pick(raw, ["label", "timeText", "bucket", "step"]);
  const timeLabel = explicitLabel == null
    ? (_pick(base, ["time", "ts_ms"]) ?? _pick(raw, ["time", "ts_ms"]))
    : null;
  let values = [];
  if (Array.isArray(base.values)) {
    values = base.values.map((field) => _normalizeInspectorValue(field, raw, index, options));
  } else if (Array.isArray(base.seriesValues)) {
    values = base.seriesValues.map((field) => _normalizeInspectorValue(field, raw, index, options));
  } else if (Array.isArray(options.fields)) {
    values = options.fields.map((field) => _normalizeInspectorValue(field, raw, index, options));
  } else {
    const sourceValue = base.value ?? raw;
    const value = sourceValue === null || sourceValue === undefined || sourceValue === "" ? null : _num(sourceValue);
    values = [{
      key: "value",
      label: String(options.valueLabel || "Value"),
      value,
      valueText: value == null ? "unavailable" : _formatWith(options.valueFormatter, value),
    }];
  }
  return {
    ...base,
    raw,
    index,
    label: explicitLabel == null
      ? (formatChartTime(timeLabel) || String(timeLabel ?? index + 1))
      : String(explicitLabel),
    x: _num(base.x ?? base.xCoord ?? base.plotX),
    y: _num(base.y ?? base.yCoord ?? base.plotY),
    values,
    note: base.note || "",
  };
}

export function buildChartPointSummary(point, options = {}) {
  if (!point) {
    return `${String(options.title || "Chart").trim() || "Chart"}: no point selected.`;
  }
  const title = String(options.title || "Chart").trim() || "Chart";
  const label = String(point.label || point.timeText || point.index + 1 || "point").trim();
  const values = Array.isArray(point.values) ? point.values : [];
  const valueText = values.length
    ? values.map((field) => `${field.label} ${field.valueText || "unavailable"}`).join(", ")
    : "value unavailable";
  const note = String(point.note || "").trim();
  return `${title}: ${label}; ${valueText}${note ? `; ${note}` : ""}.`;
}

function _normalizedInspectorPoints(points, options = {}) {
  return (Array.isArray(points) ? points : [])
    .map((point, index) => _normalizeInspectorPoint(point, index, options))
    .filter((point) => point.values.some((field) => field.value != null) || point.label);
}

function _nearestInspectorIndex(rows, x, fallbackIndex) {
  if (!rows.length) return 0;
  const withX = rows.filter((row) => Number.isFinite(row.x));
  if (!withX.length || !Number.isFinite(Number(x))) return Math.max(0, Math.min(rows.length - 1, fallbackIndex));
  let bestIndex = 0;
  let bestDistance = Infinity;
  rows.forEach((row, index) => {
    if (!Number.isFinite(row.x)) return;
    const distance = Math.abs(row.x - Number(x));
    if (distance < bestDistance) {
      bestDistance = distance;
      bestIndex = index;
    }
  });
  return bestIndex;
}

function _eventCanvasX(chartEl, event) {
  if (!event) return null;
  const clientX = _num(event.clientX);
  if (clientX == null) return null;
  const rect = typeof chartEl.getBoundingClientRect === "function"
    ? chartEl.getBoundingClientRect()
    : null;
  const width = _num(chartEl.width) || (rect && _num(rect.width)) || null;
  if (!rect || !Number.isFinite(Number(rect.left)) || !Number.isFinite(Number(rect.width)) || Number(rect.width) <= 0 || width == null) {
    return clientX;
  }
  return (clientX - Number(rect.left)) * (width / Number(rect.width));
}

export function installChartPointInspector(target, points = [], options = {}) {
  const chartEl = _asElement(target);
  if (!chartEl) return null;
  const rows = _normalizedInspectorPoints(points, options);
  const title = String(options.title || chartEl.getAttribute?.("data-chart-a11y-title") || "Chart").trim() || "Chart";
  const dataWindow = _resolveDataWindow(chartEl, options.containerId || options.dataWindowId);
  const existingAriaLabel = String(chartEl.getAttribute?.("aria-label") || "").trim();
  const existingState = String(chartEl.getAttribute?.("data-chart-a11y-state") || "").trim();
  if (INSPECTOR_CLEANUPS && INSPECTOR_CLEANUPS.has(chartEl)) {
    try { INSPECTOR_CLEANUPS.get(chartEl)(); } catch {}
    INSPECTOR_CLEANUPS.delete(chartEl);
  }
  const initialCandidate = Number(options.initialIndex ?? rows.length - 1);
  const initialIndex = Number.isFinite(initialCandidate)
    ? Math.max(0, Math.min(rows.length - 1, initialCandidate))
    : Math.max(0, rows.length - 1);
  const existingDescribedBy = String(chartEl.getAttribute?.("aria-describedby") || "").trim();
  const describedBy = [
    existingDescribedBy,
    dataWindow && dataWindow.id ? dataWindow.id : "",
  ].filter(Boolean).join(" ");
  applyChartFocusMetadata(chartEl, {
    title,
    summary: existingAriaLabel || (rows.length
      ? buildChartPointSummary(rows[initialIndex], { title })
      : `${title}: ${String(options.emptyMessage || "No point data is available.")}`),
    pointCount: rows.length,
    state: rows.length ? (existingState || "ready") : (existingState || "empty"),
    kind: options.kind || options.chartType || chartEl.getAttribute?.("data-chart-a11y-kind"),
    describedBy: describedBy || chartEl.getAttribute?.("aria-describedby"),
    focusable: options.focusable,
  });

  if (!rows.length) {
    const message = `${title}: ${String(options.emptyMessage || "No point data is available.")}`;
    _setNodeText(dataWindow, message);
    if (typeof chartEl.setAttribute === "function") chartEl.setAttribute("data-chart-inspector-state", "empty");
    return { rows, dataWindow, selectedIndex: -1, selectPoint: () => null };
  }

  let selectedIndex = initialIndex;
  const show = (index) => {
    selectedIndex = Math.max(0, Math.min(rows.length - 1, Number(index)));
    const summary = buildChartPointSummary(rows[selectedIndex], { ...options, title });
    _setNodeText(dataWindow, summary);
    if (typeof chartEl.setAttribute === "function") {
      chartEl.setAttribute("data-chart-inspector-state", "ready");
      chartEl.setAttribute("data-chart-selected-index", String(selectedIndex));
      chartEl.setAttribute("title", summary);
      if (!existingAriaLabel || options.updateAriaLabel === true) {
        chartEl.setAttribute("aria-label", summary);
      }
    }
    return rows[selectedIndex];
  };
  const onMove = (event) => {
    const x = _eventCanvasX(chartEl, event);
    show(_nearestInspectorIndex(rows, x, selectedIndex));
  };
  const onFocus = () => show(selectedIndex);
  const onKeyDown = (event) => {
    const key = String(event && event.key || "");
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(key)) return;
    if (typeof event.preventDefault === "function") event.preventDefault();
    if (key === "Home") show(0);
    else if (key === "End") show(rows.length - 1);
    else if (key === "ArrowLeft") show(selectedIndex - 1);
    else if (key === "ArrowRight") show(selectedIndex + 1);
  };
  const listeners = [
    ["mousemove", onMove],
    ["focus", onFocus],
    ["keydown", onKeyDown],
  ];
  if (typeof chartEl.addEventListener === "function") {
    listeners.forEach(([eventName, handler]) => chartEl.addEventListener(eventName, handler));
    if (INSPECTOR_CLEANUPS) {
      INSPECTOR_CLEANUPS.set(chartEl, () => {
        if (typeof chartEl.removeEventListener === "function") {
          listeners.forEach(([eventName, handler]) => chartEl.removeEventListener(eventName, handler));
        }
      });
    }
  }
  show(selectedIndex);
  return { rows, dataWindow, selectedIndex, selectPoint: show };
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
  if (options.focusable !== false && !_hasAttribute(chartEl, "tabindex")) {
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
  const seriesFields = _normalizeSeriesFields(options);
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
    : (seriesFields.length > 1
        ? _defaultMultiSeriesColumns(seriesFields, options.valueFormatter)
        : _defaultColumns(valueLabel, options.valueFormatter));
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
