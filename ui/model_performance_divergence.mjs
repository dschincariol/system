"use strict";

function _esc(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function _num(value) {
  const out = Number(value);
  return Number.isFinite(out) ? out : null;
}

function _fmtValue(point, fallbackUnit = "") {
  if (!point || typeof point !== "object") return "-";
  const value = _num(point.value);
  if (value == null) return "-";
  const unit = String(point.unit || fallbackUnit || "");
  if (unit === "pct") return `${(value * 100).toFixed(2)}%`;
  if (unit === "bps") return `${value.toFixed(2)} bps`;
  return value.toFixed(3);
}

function _fmtDelta(value, unit) {
  const delta = _num(value);
  if (delta == null) return "-";
  if (unit === "pct") {
    const scaled = delta * 100;
    return `${scaled >= 0 ? "+" : ""}${scaled.toFixed(2)} pp`;
  }
  if (unit === "bps") {
    return `${delta >= 0 ? "+" : ""}${delta.toFixed(2)} bps`;
  }
  return `${delta >= 0 ? "+" : ""}${delta.toFixed(3)}`;
}

function _fmtTime(tsMs) {
  const ts = Number(tsMs);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return "-";
  }
}

function _statusTone(status) {
  const state = String(status || "incomplete").toLowerCase();
  if (state === "ok") return "ok";
  if (state === "watch" || state === "partial") return "warn";
  if (state === "diverged") return "bad";
  return "dim";
}

function _sourceText(point) {
  if (!point || typeof point !== "object") return "-";
  const source = String(point.source || "").replace(/_/g, " ");
  const ts = _fmtTime(point.ts_ms);
  return ts === "-" ? source || "-" : `${source || "source"} @ ${ts}`;
}

function _pointTime(point) {
  if (!point || typeof point !== "object") return null;
  const ts = _num(point.ts_ms ?? point.time ?? point.t);
  return ts !== null && ts > 0 ? ts : null;
}

function _missingDetails(payload, rows) {
  const missing = Array.isArray(payload && payload.missing_sources)
    ? payload.missing_sources.map(String).filter(Boolean)
    : [];
  const metricMissing = rows
    .filter((row) => row.status === "incomplete" && row.explanation)
    .map((row) => `${row.label}: ${row.explanation}`);
  return [...missing.map((name) => `Missing source: ${name}`), ...metricMissing];
}

function _chartSeries(rows) {
  for (const row of rows) {
    const points = [
      { label: "Expected/backtest", value: row.expectedValue, source: row.expectedSource, time: row.expectedTime },
      { label: "Shadow", value: row.shadowValue, source: row.shadowSource, time: row.shadowTime },
      { label: "Live realized", value: row.realizedValue, source: row.realizedSource, time: row.realizedTime },
    ].filter((point) => Number.isFinite(point.value));
    if (points.length >= 2) {
      return {
        label: row.label,
        unit: row.unit,
        values: points.map((point) => point.value),
        points,
      };
    }
  }
  return { label: "Performance", unit: "", values: [], points: [] };
}

export function buildPerformanceDivergenceViewModel(payload = {}) {
  const safe = payload && typeof payload === "object" ? payload : {};
  const selection = safe.selection && typeof safe.selection === "object" ? safe.selection : {};
  const status = safe.status && typeof safe.status === "object" ? safe.status : {};
  const comparisons = Array.isArray(safe.comparisons) ? safe.comparisons : [];

  const rows = comparisons.map((item) => {
    const row = item && typeof item === "object" ? item : {};
    const expected = row.expected && typeof row.expected === "object" ? row.expected : null;
    const shadow = row.shadow && typeof row.shadow === "object" ? row.shadow : null;
    const realized = row.realized && typeof row.realized === "object" ? row.realized : null;
    const unit = String(row.unit || "");
    return {
      key: String(row.key || ""),
      label: String(row.label || row.key || "Metric"),
      unit,
      expectedText: _fmtValue(expected, unit),
      shadowText: _fmtValue(shadow, unit),
      realizedText: _fmtValue(realized, unit),
      deltaText: _fmtDelta(row.delta, unit),
      expectedSource: _sourceText(expected),
      shadowSource: _sourceText(shadow),
      realizedSource: _sourceText(realized),
      expectedTime: _pointTime(expected),
      shadowTime: _pointTime(shadow),
      realizedTime: _pointTime(realized),
      expectedValue: expected ? _num(expected.value) : null,
      shadowValue: shadow ? _num(shadow.value) : null,
      realizedValue: realized ? _num(realized.value) : null,
      status: String(row.status || "incomplete").toLowerCase(),
      tone: _statusTone(row.status),
      explanation: String(row.explanation || ""),
    };
  });

  const model = String(selection.model_id || "").trim();
  const strategy = String(selection.strategy || "").trim();
  const selected = model && strategy
    ? `${model} / ${strategy}`
    : model || strategy || "No model or strategy selected";
  const state = String(status.state || (safe.ok === false ? "error" : "incomplete")).toLowerCase();
  const missing = _missingDetails(safe, rows);

  return {
    ok: safe.ok !== false,
    selected,
    state,
    tone: _statusTone(state),
    reason: String(status.reason || safe.error || "Performance divergence data is incomplete."),
    updatedText: _fmtTime(status.ts_ms || safe.updated_ts_ms),
    rows,
    missing,
    chart: _chartSeries(rows),
  };
}

function _setText(root, id, text) {
  const el = root && typeof root.getElementById === "function" ? root.getElementById(id) : null;
  if (el) el.textContent = text;
  return el;
}

function _setHtml(root, id, html) {
  const el = root && typeof root.getElementById === "function" ? root.getElementById(id) : null;
  if (el) el.innerHTML = html;
  return el;
}

function _renderChart(root, vm, renderLineChart) {
  const canvas = root && typeof root.getElementById === "function"
    ? root.getElementById("performanceDivergenceChart")
    : null;
  if (!canvas || typeof renderLineChart !== "function") return;

  const values = Array.isArray(vm.chart.values) ? vm.chart.values : [];
  const opts = {
    xValues: Array.isArray(vm.chart.points) ? vm.chart.points.map((point) => point.time ?? null) : [],
    fmtX: (value, index) => {
      const point = Array.isArray(vm.chart.points) ? vm.chart.points[index] : null;
      return point && point.time ? _fmtTime(point.time) : String(point && point.label || index + 1);
    },
    topLabel: `${vm.chart.label} expected to live`,
    a11yTitle: "Model performance divergence",
    a11ySeries: vm.chart.points,
    a11yLabelKey: "label",
    a11yTimeKey: "time",
    valueLabel: vm.chart.label,
    a11yValueFormatter: (value) => vm.chart.unit === "pct"
      ? `${(Number(value) * 100).toFixed(1)}%`
      : vm.chart.unit === "bps"
        ? `${Number(value).toFixed(1)}bp`
        : Number(value).toFixed(2),
    a11yColumns: [
      { label: "Series", value: (row) => row.raw && row.raw.label },
      {
        label: vm.chart.label,
        value: (row) => vm.chart.unit === "pct"
          ? `${(Number(row.value) * 100).toFixed(1)}%`
          : vm.chart.unit === "bps"
            ? `${Number(row.value).toFixed(1)}bp`
            : Number(row.value).toFixed(2),
      },
      { label: "Source", value: (row) => row.raw && row.raw.source },
    ],
    emptyMessage: "No comparable performance data is available for the selected model.",
    stroke: vm.state === "diverged" ? "#ff6b6b" : vm.state === "watch" ? "#d29922" : "#58a6ff",
    fmtY: (value) => vm.chart.unit === "pct"
      ? `${(Number(value) * 100).toFixed(1)}%`
      : vm.chart.unit === "bps"
        ? `${Number(value).toFixed(1)}bp`
        : Number(value).toFixed(2),
  };
  renderLineChart(canvas, values, opts);
}

export function renderPerformanceDivergencePanel(payload = {}, root = document, renderLineChart = null) {
  const vm = buildPerformanceDivergenceViewModel(payload);
  const meta = _setText(root, "performanceDivergenceMeta", vm.state);
  if (meta) meta.className = `pill ${vm.tone}`;
  _setText(root, "performanceDivergenceStatus", vm.reason);
  _setText(root, "performanceDivergenceUpdated", `updated ${vm.updatedText}`);
  _setText(root, "performanceDivergenceSelected", vm.selected);

  const rowsHtml = vm.rows.length
    ? vm.rows.map((row) => `
      <tr class="table-row">
        <td>${_esc(row.label)}</td>
        <td class="mono" title="${_esc(row.expectedSource)}">${_esc(row.expectedText)}</td>
        <td class="mono" title="${_esc(row.shadowSource)}">${_esc(row.shadowText)}</td>
        <td class="mono" title="${_esc(row.realizedSource)}">${_esc(row.realizedText)}</td>
        <td class="mono">${_esc(row.deltaText)}</td>
        <td><span class="pill ${_esc(row.tone)}">${_esc(row.status)}</span></td>
        <td class="small">${_esc(row.explanation)}</td>
      </tr>
    `).join("")
    : `<tr class="table-row"><td colspan="7" class="metric-meta">No comparable performance data available.</td></tr>`;
  _setHtml(root, "performanceDivergenceRows", rowsHtml);

  const missingHtml = vm.missing.length
    ? vm.missing.map((item) => `<div class="opsNote">${_esc(item)}</div>`).join("")
    : `<div class="opsNote">All primary sources needed for the current comparison are present.</div>`;
  _setHtml(root, "performanceDivergenceMissing", missingHtml);
  _renderChart(root, vm, renderLineChart);
  return vm;
}

export async function loadPerformanceDivergence(fetchJSON, root = document, renderLineChart = null) {
  try {
    const payload = await fetchJSON("/api/model/performance_divergence", { allowBusinessFalse: true });
    return renderPerformanceDivergencePanel(payload, root, renderLineChart);
  } catch (error) {
    return renderPerformanceDivergencePanel(
      {
        ok: false,
        status: {
          state: "incomplete",
          reason: `Performance divergence endpoint unavailable: ${error && error.message ? error.message : String(error)}`,
          ts_ms: Date.now(),
        },
        comparisons: [],
        missing_sources: ["model_performance_divergence"],
      },
      root,
      renderLineChart,
    );
  }
}
