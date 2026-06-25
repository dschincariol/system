"use strict";

/*
  ui/charts.js — shared lightweight chart engine
  Extracted from ui/dashboard.js (Phase 3)
*/

import {
  formatChartTime,
  installChartPointInspector,
  renderChartAccessibility,
} from "./chart_a11y.js";

// -----------------------------
// Small utilities
// -----------------------------
function _num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function _xNumber(value, fallbackIndex) {
  const n = Number(value);
  if (Number.isFinite(n)) return n;
  if (typeof value === "string" && value.trim()) {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return Number(fallbackIndex);
}

function _xScaleValid(value) {
  const n = Number(value);
  if (Number.isFinite(n)) return true;
  if (typeof value === "string" && value.trim()) {
    return Number.isFinite(Date.parse(value));
  }
  return false;
}

function _xText(value, index, opts = {}) {
  if (typeof opts.fmtX === "function") {
    try {
      const out = opts.fmtX(value, index);
      if (out !== undefined && out !== null && out !== "") return String(out);
    } catch {}
  }
  if (opts.hasExplicitX && value !== undefined && value !== null && value !== "") {
    return formatChartTime(value) || String(value);
  }
  return String(index + 1);
}

function _buildYRange(values, opts = {}) {
  const explicitMin = Number.isFinite(Number(opts.yMin));
  const explicitMax = Number.isFinite(Number(opts.yMax));
  let yMin = explicitMin ? Number(opts.yMin) : Math.min(...values);
  let yMax = explicitMax ? Number(opts.yMax) : Math.max(...values);

  if (yMin === yMax) {
    if (!explicitMin) yMin -= 1;
    if (!explicitMax) yMax += 1;
    if (explicitMin && explicitMax) {
      yMin -= 1;
      yMax += 1;
    }
  }

  const span = yMax - yMin;
  const pad = Number.isFinite(span) && span > 0 ? span * 0.08 : 1;
  if (!explicitMin) yMin -= pad;
  if (!explicitMax) yMax += pad;
  return { yMin, yMax, explicitMin, explicitMax };
}

function _pointAt(a, b, t, yMin, yMax) {
  const xCoord = a.xCoord + (b.xCoord - a.xCoord) * t;
  let value = a.value + (b.value - a.value) * t;
  if (Math.abs(value - yMin) < 1e-10) value = yMin;
  if (Math.abs(value - yMax) < 1e-10) value = yMax;
  return {
    index: a.index + (b.index - a.index) * t,
    xCoord,
    value,
    clipped: t > 1e-10 && t < 1 - 1e-10,
  };
}

function _clipSegmentToYRange(a, b, yMin, yMax) {
  if (a.value == null || b.value == null) return null;
  if (!Number.isFinite(a.xCoord) || !Number.isFinite(b.xCoord)) return null;

  const dy = b.value - a.value;
  if (dy === 0) {
    return a.value >= yMin && a.value <= yMax
      ? { from: a, to: b, clipped: false }
      : null;
  }

  const tMin = (yMin - a.value) / dy;
  const tMax = (yMax - a.value) / dy;
  const low = Math.min(tMin, tMax);
  const high = Math.max(tMin, tMax);
  const start = Math.max(0, low);
  const end = Math.min(1, high);
  if (start > end) return null;

  const from = _pointAt(a, b, start, yMin, yMax);
  const to = _pointAt(a, b, end, yMin, yMax);
  if (!Number.isFinite(from.value) || !Number.isFinite(to.value)) return null;
  return { from, to, clipped: from.clipped || to.clipped || start > 0 || end < 1 };
}

function _defaultA11ySeries(vm) {
  return vm.points
    .filter((point) => point.value != null)
    .map((point) => ({
      label: point.xText,
      time: point.xRaw ?? point.xText,
      value: point.value,
    }));
}

function _firstFinite(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function _firstPresent(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== "") return value;
  }
  return null;
}

function _clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.min(1, n));
}

function _pickTicks(points) {
  if (points.length < 2) return [];
  const indexes = points.length === 2
    ? [0, 1]
    : [0, Math.floor((points.length - 1) / 2), points.length - 1];
  const seen = new Set();
  const out = [];
  for (const index of indexes) {
    const point = points[index];
    if (!point) continue;
    const key = `${point.xCoord}|${point.xText}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      xCoord: point.xCoord,
      label: point.xText,
      index: point.index,
      raw: point.xRaw,
    });
  }
  return out;
}

function _formatLineValue(value, opts = {}) {
  const formatter = opts.fmtY || opts.a11yValueFormatter;
  if (typeof formatter === "function") {
    try {
      const out = formatter(value);
      if (out !== undefined && out !== null && out !== "") return String(out);
    } catch {}
  }
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(3) : "unavailable";
}

function _measureText(ctx, text) {
  if (ctx && typeof ctx.measureText === "function") {
    try { return ctx.measureText(String(text)).width; } catch {}
  }
  return String(text || "").length * 6;
}

function _clamp(value, lo, hi) {
  const n = Number(value);
  if (!Number.isFinite(n)) return lo;
  return Math.max(lo, Math.min(hi, n));
}

function _lineSeriesLabel(opts = {}) {
  return String(opts.seriesLabel || opts.legendLabel || opts.valueLabel || opts.topLabel || "Value").trim() || "Value";
}

function _drawLineLegend(ctx, opts, x, y) {
  const label = _lineSeriesLabel(opts);
  const color = opts.stroke || "#2ea043";
  if (typeof ctx.fillRect === "function") {
    ctx.fillStyle = color;
    ctx.fillRect(x, y - 8, 14, 3);
  }
  ctx.fillStyle = "#9da7b1";
  ctx.font = "11px Consolas, monospace";
  ctx.fillText(label, x + 18, y - 4);
}

export function buildLineChartInspectorPoints(vm, opts = {}, geometry = {}) {
  if (!vm || !Array.isArray(vm.finitePoints)) return [];
  const xFor = typeof geometry.xFor === "function" ? geometry.xFor : null;
  const yFor = typeof geometry.yFor === "function" ? geometry.yFor : null;
  return vm.finitePoints.map((point) => ({
    label: point.xText,
    x: xFor ? xFor(point.xCoord) : point.xCoord,
    y: yFor ? yFor(point.value) : null,
    value: point.value,
    values: [{
      label: _lineSeriesLabel(opts),
      value: point.value,
      formatter: opts.fmtY || opts.a11yValueFormatter,
      valueText: _formatLineValue(point.value, opts),
    }],
  }));
}

export function buildLineChartViewModel(ys, opts = {}) {
  const source = Array.isArray(ys) ? ys : [];
  const xValues = Array.isArray(opts.xValues) ? opts.xValues : null;
  const hasExplicitX = !!xValues;

  const draft = source.map((value, index) => {
    const rawX = hasExplicitX ? xValues[index] : index + 1;
    return {
      index,
      rawValue: value,
      value: _num(value),
      xRaw: rawX,
      rawXCoord: _xNumber(rawX, index),
      rawXScaleValid: hasExplicitX ? _xScaleValid(rawX) : false,
      xText: _xText(rawX, index, { ...opts, hasExplicitX }),
    };
  });

  const finiteDraft = draft.filter((point) => point.value != null);
  const explicitScaleValues = finiteDraft.map((point) => point.rawXCoord);
  const xMinRaw = explicitScaleValues.length ? Math.min(...explicitScaleValues) : 0;
  const xMaxRaw = explicitScaleValues.length ? Math.max(...explicitScaleValues) : 1;
  const useExplicitXScale =
    hasExplicitX &&
    finiteDraft.length >= 2 &&
    finiteDraft.every((point) => point.rawXScaleValid) &&
    explicitScaleValues.every(Number.isFinite) &&
    xMaxRaw > xMinRaw;

  const points = draft.map((point) => ({
    ...point,
    xCoord: useExplicitXScale ? point.rawXCoord : point.index,
  }));
  const finitePoints = points.filter((point) => point.value != null);
  const values = finitePoints.map((point) => point.value);
  const message = opts.emptyMessage || opts.errorMessage || (
    source.length ? "(no numeric data)" : "(no data)"
  );

  if (values.length < 2) {
    return {
      ok: false,
      state: values.length ? "insufficient" : "empty",
      message,
      points,
      finitePoints,
      values,
      yMin: null,
      yMax: null,
      xMin: null,
      xMax: null,
      xTicks: [],
      segments: [],
      usesExplicitXScale: false,
    };
  }

  const { yMin, yMax } = _buildYRange(values, opts);
  const xCoords = finitePoints.map((point) => point.xCoord);
  const xMin = Math.min(...xCoords);
  const xMax = Math.max(...xCoords);
  const segments = [];

  for (let i = 1; i < points.length; i++) {
    const clipped = _clipSegmentToYRange(points[i - 1], points[i], yMin, yMax);
    if (clipped) segments.push(clipped);
  }

  return {
    ok: segments.length > 0,
    state: segments.length > 0 ? "ready" : "out_of_range",
    message: segments.length > 0
      ? message
      : (opts.emptyMessage || "No values fall inside the selected y-axis range."),
    points,
    finitePoints,
    values,
    yMin,
    yMax,
    xMin,
    xMax,
    xTicks: _pickTicks(finitePoints),
    segments,
    usesExplicitXScale: useExplicitXScale,
  };
}

function _drawXAxis(ctx, vm, xFor, y, w, padL, padR) {
  if (!vm.xTicks.length) return;
  ctx.strokeStyle = "#20252c";
  ctx.beginPath();
  ctx.moveTo(padL, y + 0.5);
  ctx.lineTo(w - padR, y + 0.5);
  for (const tick of vm.xTicks) {
    const x = xFor(tick.xCoord);
    ctx.moveTo(x, y);
    ctx.lineTo(x, y + 4);
  }
  ctx.stroke();

  ctx.fillStyle = "#9da7b1";
  ctx.font = "10px Consolas, monospace";
  for (const [index, tick] of vm.xTicks.entries()) {
    const x = xFor(tick.xCoord);
    const label = String(tick.label || "");
    const width = ctx.measureText ? ctx.measureText(label).width : label.length * 6;
    let tx = x - width / 2;
    if (index === 0) tx = Math.max(padL, tx);
    if (index === vm.xTicks.length - 1) tx = Math.min(w - padR - width, tx);
    ctx.fillText(label, tx, y + 16);
  }
}

// -----------------------------
// Generic line chart (main engine)
// -----------------------------
export function renderLineChart(canvas, ys, opts = {}) {
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const w = canvas.width;
  const h = canvas.height;
  const vm = buildLineChartViewModel(ys, opts);

  const padL = 44;
  const padR = 10;
  const padT = 12;
  const padB = vm.xTicks.length ? 34 : 20;

  ctx.clearRect(0, 0, w, h);

  // frame
  ctx.strokeStyle = "#30363d";
  ctx.lineWidth = 1;
  ctx.strokeRect(0.5, 0.5, w - 1, h - 1);

  if (!vm.ok) {
    ctx.fillStyle = "#9da7b1";
    ctx.font = "12px Consolas, monospace";
    const message = vm.message || opts.emptyMessage || opts.errorMessage || "(no data)";
    ctx.fillText(message, 12, 24);
    renderChartAccessibility(canvas, {
      title: opts.a11yTitle || opts.title || opts.topLabel || "Line chart",
      series: Array.isArray(opts.a11ySeries)
        ? opts.a11ySeries
        : _defaultA11ySeries(vm),
      emptyMessage: message,
      errorMessage: opts.errorMessage,
      valueLabel: opts.valueLabel || "value",
      valueFormatter: opts.a11yValueFormatter || opts.fmtY,
      timeKey: opts.a11yTimeKey || (opts.xValues ? "time" : undefined),
      labelKey: opts.a11yLabelKey || "label",
      columns: opts.a11yColumns,
      maxRows: opts.a11yMaxRows,
      chartType: "canvas-line",
    });
    installChartPointInspector(canvas, [], {
      title: opts.a11yTitle || opts.title || opts.topLabel || "Line chart",
      kind: "canvas-line",
      emptyMessage: message,
    });
    return;
  }

  const { yMin, yMax } = vm;

  // labels
  ctx.fillStyle = "#9da7b1";
  ctx.font = "12px Consolas, monospace";
  if (opts.topLabel) ctx.fillText(opts.topLabel, 8, padT + 10);
  if (opts.bottomLabel) ctx.fillText(opts.bottomLabel, 8, vm.xTicks.length ? h - 18 : h - 8);
  if (opts.yAxisLabel) ctx.fillText(String(opts.yAxisLabel), 8, padT + 34);

  const fmtY = opts.fmtY || ((v) => v.toFixed(3));
  ctx.fillText(fmtY(yMax), 8, padT + 22);
  ctx.fillText(fmtY(yMin), 8, h - padB - 4);

  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  const xFor = (xCoord) => {
    const span = vm.xMax - vm.xMin;
    if (!Number.isFinite(span) || span <= 0) return padL;
    return padL + plotW * ((xCoord - vm.xMin) / span);
  };

  const yFor = (v) => padT + plotH * (1 - ((v - yMin) / (yMax - yMin)));

  // mid gridline
  ctx.strokeStyle = "#20252c";
  ctx.beginPath();
  ctx.moveTo(padL, padT + plotH / 2);
  ctx.lineTo(w - padR, padT + plotH / 2);
  ctx.stroke();
  _drawXAxis(ctx, vm, xFor, padT + plotH, w, padL, padR);
  if (opts.xAxisLabel) {
    const label = String(opts.xAxisLabel);
    ctx.fillStyle = "#9da7b1";
    ctx.font = "11px Consolas, monospace";
    ctx.fillText(label, Math.max(padL, w - padR - _measureText(ctx, label)), h - 4);
  }
  _drawLineLegend(ctx, opts, padL, padT + 12);

  // line
  ctx.strokeStyle = opts.stroke || "#2ea043";
  ctx.lineWidth = 2;
  ctx.beginPath();
  let drew = false;
  for (const segment of vm.segments) {
    ctx.moveTo(xFor(segment.from.xCoord), yFor(segment.from.value));
    ctx.lineTo(xFor(segment.to.xCoord), yFor(segment.to.value));
    drew = true;
  }
  if (drew) ctx.stroke();
  const latest = vm.finitePoints[vm.finitePoints.length - 1];
  if (latest && opts.showLatestValue !== false) {
    const x = xFor(latest.xCoord);
    const y = yFor(latest.value);
    const label = _formatLineValue(latest.value, opts);
    if (typeof ctx.beginPath === "function" && typeof ctx.arc === "function" && typeof ctx.fill === "function") {
      ctx.fillStyle = opts.stroke || "#2ea043";
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.fillStyle = "#e8f0fa";
    ctx.font = "11px Consolas, monospace";
    ctx.fillText(label, _clamp(x + 6, padL, w - padR - _measureText(ctx, label)), _clamp(y - 6, padT + 10, h - padB - 2));
  }

  renderChartAccessibility(canvas, {
    title: opts.a11yTitle || opts.title || opts.topLabel || "Line chart",
    series: Array.isArray(opts.a11ySeries)
      ? opts.a11ySeries
      : _defaultA11ySeries(vm),
    valueKey: opts.a11yValueKey || "value",
    timeKey: opts.a11yTimeKey || (opts.xValues ? "time" : undefined),
    labelKey: opts.a11yLabelKey || "label",
    valueLabel: opts.valueLabel || opts.a11yValueLabel || "value",
    valueFormatter: opts.a11yValueFormatter || opts.fmtY,
    summary: opts.a11ySummary,
    emptyMessage: opts.emptyMessage,
    errorMessage: opts.errorMessage,
    columns: opts.a11yColumns,
    maxRows: opts.a11yMaxRows,
    chartType: "canvas-line",
  });
  installChartPointInspector(
    canvas,
    buildLineChartInspectorPoints(vm, opts, { xFor, yFor }),
    {
      title: opts.a11yTitle || opts.title || opts.topLabel || "Line chart",
      kind: "canvas-line",
      valueLabel: _lineSeriesLabel(opts),
      valueFormatter: opts.fmtY || opts.a11yValueFormatter,
    },
  );
}

// -----------------------------
// Calibration curve
// -----------------------------
function _calibrationInputRows(input) {
  if (Array.isArray(input)) return input;
  const source = input && typeof input === "object" ? input : {};
  for (const key of ["points", "curve", "bin_stats", "bins", "rows"]) {
    if (Array.isArray(source[key])) return source[key];
  }
  return [];
}

function _calibrationSampleCount(row) {
  const n = _firstFinite(
    row && row.count,
    row && row.n,
    row && row.sample_count,
    row && row.samples,
    row && row.bin_count,
    row && row.n_points,
  );
  if (n == null || n < 0) return null;
  return Math.round(n);
}

function _calibrationConfidence(row, index, rows, opts) {
  const direct = _firstFinite(
    row && row.confidence,
    row && row.conf,
    row && row.avg_conf,
    row && row.prob_mean,
    row && row.mean_confidence,
    row && row.x,
  );
  if (direct != null) return _clamp01(direct);

  const lo = _firstFinite(row && row.lo, row && row.conf_lo, row && row.lower);
  const hi = _firstFinite(row && row.hi, row && row.conf_hi, row && row.upper);
  if (lo != null && hi != null) return _clamp01((lo + hi) / 2);

  const edges = Array.isArray(opts.edges) ? opts.edges : [];
  if (edges.length >= rows.length + 1) {
    const edgeLo = _firstFinite(edges[index]);
    const edgeHi = _firstFinite(edges[index + 1]);
    if (edgeLo != null && edgeHi != null) return _clamp01((edgeLo + edgeHi) / 2);
  }

  if (rows.length > 1) return _clamp01((index + 0.5) / rows.length);
  return null;
}

function _calibrationAccuracy(row) {
  return _clamp01(_firstFinite(
    row && row.accuracy,
    row && row.acc,
    row && row.empirical_accuracy,
    row && row.directional_acc,
    row && row.win_rate,
    row && row.y,
    row && row.prob_true,
  ));
}

function _calibrationPointLabel(row, index) {
  const label = _firstPresent(row && row.label, row && row.bin, row && row.bucket);
  if (label != null) return String(label);
  const lo = _firstFinite(row && row.lo, row && row.conf_lo, row && row.lower);
  const hi = _firstFinite(row && row.hi, row && row.conf_hi, row && row.upper);
  if (lo != null && hi != null) {
    return `${Math.round(lo * 100)}-${Math.round(hi * 100)}%`;
  }
  return `Bin ${index + 1}`;
}

function _formatPct(value, digits = 1) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "unavailable";
  return `${(n * 100).toFixed(digits)}%`;
}

export function calibrationVerdict(ece, signedGap = 0, opts = {}) {
  const n = Number(ece);
  if (!Number.isFinite(n)) {
    return {
      key: "unavailable",
      label: "Calibration unavailable",
      tone: "dim",
      description: "ECE cannot be computed from the provided calibration payload.",
    };
  }
  const threshold = Number.isFinite(Number(opts.threshold)) ? Number(opts.threshold) : 0.05;
  if (n <= threshold) {
    return {
      key: "calibrated",
      label: "Calibrated",
      tone: "ok",
      description: `Confidence is close to realized accuracy; ECE ${_formatPct(n, 1)}.`,
    };
  }
  const gap = Number(signedGap);
  if (Number.isFinite(gap) && gap > 0) {
    return {
      key: "underconfident",
      label: "Underconfident",
      tone: "warn",
      description: `Realized accuracy is above stated confidence; ECE ${_formatPct(n, 1)}.`,
    };
  }
  return {
    key: "overconfident",
    label: "Overconfident",
    tone: "crit",
    description: `Stated confidence is above realized accuracy; ECE ${_formatPct(n, 1)}.`,
  };
}

export function buildCalibrationViewModel(input, opts = {}) {
  const source = input && typeof input === "object" && !Array.isArray(input) ? input : {};
  const rows = _calibrationInputRows(input);
  const edges = Array.isArray(source.edges) ? source.edges : (Array.isArray(opts.edges) ? opts.edges : []);
  const points = rows
    .map((row, index) => {
      const safeRow = row && typeof row === "object" ? row : {};
      const confidence = _calibrationConfidence(safeRow, index, rows, { ...opts, edges });
      const accuracy = _calibrationAccuracy(safeRow);
      const count = _calibrationSampleCount(safeRow);
      if (confidence == null || accuracy == null) return null;
      const gap = accuracy - confidence;
      return {
        label: _calibrationPointLabel(safeRow, index),
        confidence,
        accuracy,
        value: accuracy,
        count,
        gap,
        absGap: Math.abs(gap),
        raw: safeRow,
      };
    })
    .filter(Boolean);

  const countRows = points.filter((point) => Number.isFinite(Number(point.count)) && Number(point.count) > 0);
  const countAvailable = countRows.length > 0;
  const totalSampleCount = countAvailable
    ? countRows.reduce((acc, point) => acc + Number(point.count || 0), 0)
    : _firstFinite(source.n_points, source.sample_count, source.samples, opts.sampleCount, opts.n_points);
  const weightedRows = countAvailable
    ? countRows.map((point) => ({ point, weight: Number(point.count || 0) }))
    : points.map((point) => ({ point, weight: 1 }));
  const weightTotal = weightedRows.reduce((acc, row) => acc + Number(row.weight || 0), 0);
  const ece = weightTotal > 0
    ? weightedRows.reduce((acc, row) => acc + (Number(row.weight || 0) * Number(row.point.absGap || 0)), 0) / weightTotal
    : null;
  const signedGap = weightTotal > 0
    ? weightedRows.reduce((acc, row) => acc + (Number(row.weight || 0) * Number(row.point.gap || 0)), 0) / weightTotal
    : null;
  const verdict = calibrationVerdict(ece, signedGap, opts);
  const weighting = countAvailable ? "sample_weighted" : "equal_bins";
  const maxCount = countAvailable ? Math.max(...countRows.map((point) => Number(point.count || 0))) : 0;

  return {
    ok: points.length >= 2,
    points,
    pointCount: points.length,
    countAvailable,
    ece,
    signedGap,
    totalSampleCount: totalSampleCount == null ? null : Math.round(Number(totalSampleCount)),
    verdict,
    weighting,
    maxCount,
    summary: points.length
      ? `${verdict.label}: ${weighting === "sample_weighted" ? "sample-weighted" : "equal-bin"} ECE ${_formatPct(ece, 1)} across ${points.length} calibration bins${totalSampleCount == null ? "" : ` and ${Math.round(Number(totalSampleCount))} samples`}.`
      : "Calibration data is unavailable.",
  };
}

export function drawCalibration(canvas, pts, opts = {}) {
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const W = canvas.width;
  const H = canvas.height;
  ctx.clearRect(0, 0, W, H);

  const vm = buildCalibrationViewModel(pts, opts);
  const rows = vm.points;

  if (rows.length < 2) {
    const message = opts.errorMessage || opts.emptyMessage || "(no calibration data)";
    ctx.fillStyle = "#9da7b1";
    ctx.font = "12px Consolas, monospace";
    ctx.fillText(message, 12, 24);
    renderChartAccessibility(canvas, {
      title: opts.a11yTitle || "Confidence calibration",
      series: rows,
      emptyMessage: message,
      errorMessage: opts.errorMessage,
      valueLabel: "accuracy",
      valueFormatter: (v) => `${(Number(v) * 100).toFixed(1)}%`,
      chartType: "canvas-calibration",
      columns: [
        { label: "Bin", value: (row) => row.raw && row.raw.label ? row.raw.label : row.timeText || row.index },
        { label: "Confidence", value: (row) => row.raw && Number.isFinite(row.raw.confidence) ? _formatPct(row.raw.confidence, 1) : "unavailable" },
        { label: "Accuracy", value: (row) => row.raw && Number.isFinite(row.raw.accuracy) ? _formatPct(row.raw.accuracy, 1) : "unavailable" },
        { label: "Count", value: (row) => row.raw && Number.isFinite(Number(row.raw.count)) ? String(Math.round(Number(row.raw.count))) : "unavailable" },
      ],
    });
    installChartPointInspector(canvas, [], {
      title: opts.a11yTitle || "Confidence calibration",
      kind: "canvas-calibration",
      emptyMessage: message,
    });
    return;
  }

  const pad = 18;
  const x0 = pad, y0 = H - pad;
  const x1 = W - pad, y1 = pad;

  // diagonal reference
  ctx.beginPath();
  ctx.moveTo(x0, y0);
  ctx.lineTo(x1, y1);
  ctx.strokeStyle = "rgba(160,160,160,0.35)";
  ctx.lineWidth = 1;
  ctx.stroke();

  ctx.beginPath();
  const inspectorPoints = [];
  rows.forEach((p, i) => {
    const x = Math.max(0, Math.min(1, Number(p.confidence)));
    const y = Math.max(0, Math.min(1, Number(p.accuracy)));
    const px = x0 + x * (x1 - x0);
    const py = y0 - y * (y0 - y1);
    inspectorPoints.push({
      label: p.label,
      x: px,
      y: py,
      values: [
        { label: "Confidence", value: p.confidence, valueText: _formatPct(p.confidence, 1) },
        { label: "Accuracy", value: p.accuracy, valueText: _formatPct(p.accuracy, 1) },
        { label: "Gap", value: p.gap, valueText: _formatPct(p.gap, 1) },
        { label: "Count", value: p.count, valueText: p.count == null ? "unavailable" : String(Math.round(Number(p.count))) },
      ],
    });
    if (i === 0) ctx.moveTo(px, py);
    else ctx.lineTo(px, py);
  });

  ctx.strokeStyle = "rgba(220,220,220,0.85)";
  ctx.lineWidth = 2;
  ctx.stroke();

  const countBandH = 18;
  if (vm.countAvailable && vm.maxCount > 0 && typeof ctx.fillRect === "function") {
    rows.forEach((p) => {
      if (!Number.isFinite(Number(p.count)) || Number(p.count) <= 0) return;
      const px = x0 + Number(p.confidence) * (x1 - x0);
      const barH = Math.max(2, countBandH * (Number(p.count) / vm.maxCount));
      ctx.fillStyle = "rgba(230,159,0,0.36)";
      ctx.fillRect(px - 3, y0 - barH, 6, barH);
    });
  }

  ctx.fillStyle = "#9da7b1";
  ctx.font = "11px Consolas, monospace";
  ctx.fillText("confidence", Math.max(x0, W - pad - 82), H - 4);
  ctx.fillText("accuracy", 4, y1 + 12);
  ctx.fillText("0%", x0 - 4, y0 + 12);
  ctx.fillText("100%", x1 - 28, y0 + 12);
  ctx.fillText("100%", 2, y1 + 2);

  ctx.fillStyle = vm.verdict.key === "calibrated" ? "#009E73" : (vm.verdict.key === "underconfident" ? "#E69F00" : "#D55E00");
  const eceLabel = `${vm.verdict.label} • ECE ${_formatPct(vm.ece, 1)}${vm.weighting === "sample_weighted" ? "" : " equal-bin"}`;
  ctx.fillText(eceLabel, x0, 12);
  ctx.fillStyle = "#9da7b1";
  const sampleLabel = vm.totalSampleCount == null
    ? "sample count unavailable"
    : `samples ${vm.totalSampleCount}`;
  ctx.fillText(sampleLabel, Math.max(x0, W - pad - 150), 12);
  if (typeof ctx.fillRect === "function") {
    ctx.fillStyle = "rgba(220,220,220,0.85)";
    ctx.fillRect(x0, 20, 14, 3);
    ctx.fillStyle = "rgba(230,159,0,0.36)";
    ctx.fillRect(x0 + 92, 18, 10, 7);
  }
  ctx.fillStyle = "#9da7b1";
  ctx.fillText("accuracy line", x0 + 18, 23);
  ctx.fillText("bin count bars", x0 + 106, 23);

  renderChartAccessibility(canvas, {
    title: opts.a11yTitle || "Confidence calibration",
    series: rows,
    valueKey: "value",
    labelKey: "label",
    valueLabel: "accuracy",
    valueFormatter: (v) => _formatPct(v, 1),
    summary: opts.a11ySummary || vm.summary,
    chartType: "canvas-calibration",
    columns: [
      { label: "Bin", value: (row) => row.timeText || row.index },
      { label: "Confidence", value: (row) => row.raw && Number.isFinite(row.raw.confidence) ? _formatPct(row.raw.confidence, 1) : "unavailable" },
      { label: "Accuracy", value: (row) => row.raw && Number.isFinite(row.raw.accuracy) ? _formatPct(row.raw.accuracy, 1) : "unavailable" },
      { label: "Gap", value: (row) => row.raw && Number.isFinite(row.raw.gap) ? _formatPct(row.raw.gap, 1) : "unavailable" },
      { label: "Count", value: (row) => row.raw && Number.isFinite(Number(row.raw.count)) ? String(Math.round(Number(row.raw.count))) : "unavailable" },
    ],
  });
  installChartPointInspector(canvas, inspectorPoints, {
    title: opts.a11yTitle || "Confidence calibration",
    kind: "canvas-calibration",
    emptyMessage: "No calibration point data is available.",
  });
}
