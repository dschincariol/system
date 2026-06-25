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

const STATUS_RANK = {
  diverged: 4,
  watch: 3,
  ok: 2,
  partial: 1,
  incomplete: 0,
};

const PRODUCT_IMPORTANCE_RANK = {
  return: 100,
  net_pnl_degradation: 95,
  shadow_live_disagreement: 90,
  hit_rate: 85,
  calibration_ece: 80,
  conformal_coverage: 75,
  slippage_bps: 70,
  fill_rate: 65,
  prediction_drift: 55,
  target_label_drift: 50,
  feature_drift: 45,
  missing_feature_rate: 40,
};

function _statusRank(status) {
  const state = String(status || "incomplete").toLowerCase();
  return STATUS_RANK[state] ?? 0;
}

function _productImportance(row) {
  const key = String(row && row.key || "").toLowerCase();
  return PRODUCT_IMPORTANCE_RANK[key] ?? 10;
}

function _deltaRank(row) {
  const delta = _num(row && row.deltaValue);
  if (delta == null) return 0;
  const unit = String(row && row.unit || "");
  if (unit === "pct") return Math.abs(delta) * 100;
  return Math.abs(delta);
}

function _freshnessRank(row) {
  const times = [
    row && row.expectedTime,
    row && row.shadowTime,
    row && row.realizedTime,
  ].filter((value) => Number.isFinite(value) && value > 0);
  return times.length ? Math.max(...times) : 0;
}

function _chartPointsForRow(row) {
  if (!row || typeof row !== "object") return [];
  return [
    { label: "Expected/backtest", value: row.expectedValue, source: row.expectedSource, time: row.expectedTime },
    { label: "Shadow", value: row.shadowValue, source: row.shadowSource, time: row.shadowTime },
    { label: "Live realized", value: row.realizedValue, source: row.realizedSource, time: row.realizedTime },
  ].filter((point) => Number.isFinite(point.value));
}

function _rankedChartEntries(rows) {
  return rows
    .map((row, index) => ({
      row,
      index,
      points: _chartPointsForRow(row),
      severity: _statusRank(row && row.status),
      delta: _deltaRank(row),
      freshness: _freshnessRank(row),
      importance: _productImportance(row),
    }))
    .filter((entry) => entry.points.length >= 2)
    .sort((a, b) => (
      b.severity - a.severity
      || b.delta - a.delta
      || b.freshness - a.freshness
      || b.importance - a.importance
      || a.index - b.index
    ));
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

function _chartSeries(entry) {
  if (entry && entry.row && Array.isArray(entry.points) && entry.points.length >= 2) {
    return {
      key: entry.row.key,
      label: entry.row.label,
      unit: entry.row.unit,
      status: entry.row.status,
      tone: entry.row.tone,
      values: entry.points.map((point) => point.value),
      points: entry.points,
    };
  }
  return { key: "", label: "Performance", unit: "", status: "incomplete", tone: "dim", values: [], points: [] };
}

export function buildPerformanceDivergenceViewModel(payload = {}, options = {}) {
  const safe = payload && typeof payload === "object" ? payload : {};
  const opts = options && typeof options === "object" ? options : {};
  const selection = safe.selection && typeof safe.selection === "object" ? safe.selection : {};
  const status = safe.status && typeof safe.status === "object" ? safe.status : {};
  const comparisons = Array.isArray(safe.comparisons) ? safe.comparisons : [];

  const rows = comparisons.map((item, index) => {
    const row = item && typeof item === "object" ? item : {};
    const expected = row.expected && typeof row.expected === "object" ? row.expected : null;
    const shadow = row.shadow && typeof row.shadow === "object" ? row.shadow : null;
    const realized = row.realized && typeof row.realized === "object" ? row.realized : null;
    const unit = String(row.unit || "");
    const key = String(row.key || row.label || `metric_${index}`);
    return {
      key,
      label: String(row.label || row.key || "Metric"),
      unit,
      expectedText: _fmtValue(expected, unit),
      shadowText: _fmtValue(shadow, unit),
      realizedText: _fmtValue(realized, unit),
      deltaText: _fmtDelta(row.delta, unit),
      deltaValue: _num(row.delta),
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
  const chartEntries = _rankedChartEntries(rows);
  const requestedMetricKey = String(opts.selectedMetricKey || safe.selected_metric_key || "").trim();
  const selectedChartEntry = (
    requestedMetricKey
      ? chartEntries.find((entry) => entry.row.key === requestedMetricKey)
      : null
  ) || chartEntries[0] || null;
  const selectedMetricKey = selectedChartEntry && selectedChartEntry.row ? selectedChartEntry.row.key : "";
  const chartOptions = chartEntries.map((entry) => ({
    key: entry.row.key,
    label: entry.row.label,
    status: entry.row.status,
    tone: entry.row.tone,
    deltaText: entry.row.deltaText,
    rank: {
      severity: entry.severity,
      delta: entry.delta,
      freshness: entry.freshness,
      importance: entry.importance,
    },
    selected: entry.row.key === selectedMetricKey,
  }));

  return {
    ok: safe.ok !== false,
    selected,
    state,
    tone: _statusTone(state),
    reason: String(status.reason || safe.error || "Performance divergence data is incomplete."),
    updatedText: _fmtTime(status.ts_ms || safe.updated_ts_ms),
    rows,
    missing,
    chart: _chartSeries(selectedChartEntry),
    chartOptions,
    selectedMetricKey,
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

function _currentMetricSelection(root) {
  const el = root && typeof root.getElementById === "function"
    ? root.getElementById("performanceDivergenceMetric")
    : null;
  return el && typeof el.value === "string" ? el.value : "";
}

function _chartStroke(status) {
  const state = String(status || "incomplete").toLowerCase();
  if (state === "diverged") return "#ff6b6b";
  if (state === "watch") return "#d29922";
  return "#58a6ff";
}

function _renderMetricSelector(root, payload, vm, renderLineChart) {
  const select = root && typeof root.getElementById === "function"
    ? root.getElementById("performanceDivergenceMetric")
    : null;
  const charted = _setText(
    root,
    "performanceDivergenceCharted",
    vm.selectedMetricKey ? `chart ${vm.chart.label} / ${vm.chart.status}` : "chart -",
  );
  if (charted) charted.className = `pill ${vm.chart.tone || "dim"}`;
  if (!select) return;

  const options = Array.isArray(vm.chartOptions) ? vm.chartOptions : [];
  select.innerHTML = options.length
    ? options.map((option) => `
      <option value="${_esc(option.key)}">${_esc(`${option.label} | ${option.status} | ${option.deltaText}`)}</option>
    `).join("")
    : `<option value="">No chartable metrics</option>`;
  select.value = vm.selectedMetricKey || "";
  select.disabled = options.length <= 1;
  select.onchange = () => renderPerformanceDivergencePanel(
    payload,
    root,
    renderLineChart,
    { selectedMetricKey: String(select.value || "") },
  );
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
    seriesLabel: vm.chart.label,
    xAxisLabel: "source/time",
    yAxisLabel: vm.chart.unit === "pct" ? "percent" : vm.chart.unit === "bps" ? "basis points" : "value",
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
    stroke: _chartStroke(vm.chart.status || vm.state),
    fmtY: (value) => vm.chart.unit === "pct"
      ? `${(Number(value) * 100).toFixed(1)}%`
      : vm.chart.unit === "bps"
        ? `${Number(value).toFixed(1)}bp`
        : Number(value).toFixed(2),
  };
  renderLineChart(canvas, values, opts);
}

export function renderPerformanceDivergencePanel(payload = {}, root = document, renderLineChart = null, options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const selectedMetricKey = Object.prototype.hasOwnProperty.call(opts, "selectedMetricKey")
    ? String(opts.selectedMetricKey || "")
    : _currentMetricSelection(root);
  const vm = buildPerformanceDivergenceViewModel(payload, { selectedMetricKey });
  const meta = _setText(root, "performanceDivergenceMeta", vm.state);
  if (meta) meta.className = `pill ${vm.tone}`;
  _setText(root, "performanceDivergenceStatus", vm.reason);
  _setText(root, "performanceDivergenceUpdated", `updated ${vm.updatedText}`);
  _setText(root, "performanceDivergenceSelected", vm.selected);
  _renderMetricSelector(root, payload, vm, renderLineChart);

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
