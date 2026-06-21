/*
  FILE: ui/market_stress.js

  Market-stress panel loader for the dashboard. It fetches the normalized
  stress snapshot and renders a compact badge, timestamp, and structured detail
  view for operator monitoring.
*/

import { renderChartAccessibility } from "./chart_a11y.js";
import { applyInlineMetricAnnotation, setMetricValueAttribute } from "./tooltip.js";
import {
  MARKET_STRESS_THRESHOLDS,
  classifyMarketStressScore,
  normalizeMarketStressThresholds,
} from "./market_stress_thresholds.js";

export {
  MARKET_STRESS_THRESHOLDS,
  classifyMarketStressScore,
  normalizeMarketStressThresholds,
} from "./market_stress_thresholds.js";

export function syncMarketStressSparklineSize(canvas) {
  if (!canvas) return { width: 900, height: 48, ratio: 1 };
  const rect = typeof canvas.getBoundingClientRect === "function"
    ? canvas.getBoundingClientRect()
    : {};
  const attrWidth = Number(canvas.getAttribute?.("width"));
  const attrHeight = Number(canvas.getAttribute?.("height"));
  const cssWidth = Math.max(1, Math.round(
    Number(rect.width) ||
    Number(canvas.clientWidth) ||
    (Number.isFinite(attrWidth) && attrWidth > 0 ? attrWidth : 900)
  ));
  const cssHeight = Math.max(1, Math.round(
    Number(rect.height) ||
    Number(canvas.clientHeight) ||
    (Number.isFinite(attrHeight) && attrHeight > 0 ? attrHeight : 48)
  ));
  const ratio = Math.max(1, Number(typeof window !== "undefined" ? window.devicePixelRatio : 1) || 1);
  const backingWidth = Math.round(cssWidth * ratio);
  const backingHeight = Math.round(cssHeight * ratio);
  if (canvas.width !== backingWidth) canvas.width = backingWidth;
  if (canvas.height !== backingHeight) canvas.height = backingHeight;
  return { width: cssWidth, height: cssHeight, ratio };
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function numOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function stressBody(payload) {
  const root = asObject(payload);
  return asObject(root.stress && typeof root.stress === "object" ? root.stress : root);
}

const SPARKLINE_PADDING = Object.freeze({ top: 4, right: 2, bottom: 4, left: 2 });
const SPARKLINE_BANDS = Object.freeze({
  normal: "rgba(46, 160, 67, 0.10)",
  warning: "rgba(210, 153, 34, 0.13)",
  critical: "rgba(248, 81, 73, 0.13)",
});
const SPARKLINE_LINES = Object.freeze({
  warning: "rgba(210, 153, 34, 0.92)",
  critical: "rgba(248, 81, 73, 0.95)",
  series: "rgba(240, 246, 252, 0.95)",
});

function niceStressMax(value) {
  const v = Number(value);
  if (!Number.isFinite(v) || v <= 1) return 1;
  const padded = v * 1.08;
  if (padded <= 1.25) return 1.25;
  if (padded <= 1.5) return 1.5;
  if (padded <= 2) return 2;
  if (padded <= 3) return Math.ceil(padded * 4) / 4;
  const magnitude = 10 ** Math.floor(Math.log10(padded));
  return Math.ceil((padded / magnitude) * 4) * magnitude / 4;
}

function buildBandRect({ key, label, startValue, endValue, yFor, width }) {
  const yStart = yFor(startValue);
  const yEnd = yFor(endValue);
  const y = Math.min(yStart, yEnd);
  return {
    key,
    label,
    startValue,
    endValue,
    x: 0,
    y,
    width,
    height: Math.max(0, Math.abs(yStart - yEnd)),
    fillStyle: SPARKLINE_BANDS[key] || "rgba(139, 148, 158, 0.10)",
  };
}

export function buildMarketStressSparklineModel(series = [], size = {}, thresholdSource = {}) {
  const thresholds = normalizeMarketStressThresholds(thresholdSource);
  const width = Math.max(1, Number(size.width) || 900);
  const height = Math.max(1, Number(size.height) || 48);
  const padding = {
    ...SPARKLINE_PADDING,
    ...(asObject(size.padding)),
  };
  const plot = {
    left: Math.max(0, Number(padding.left) || 0),
    right: Math.max(0, Number(padding.right) || 0),
    top: Math.max(0, Number(padding.top) || 0),
    bottom: Math.max(0, Number(padding.bottom) || 0),
  };
  plot.width = Math.max(1, width - plot.left - plot.right);
  plot.height = Math.max(1, height - plot.top - plot.bottom);

  const rows = (Array.isArray(series) ? series : [])
    .map((point, index) => ({
      index,
      time: point && (point.time ?? point.ts_ms ?? point.t ?? index + 1),
      value: Number(point && point.value),
      raw: point,
    }))
    .filter((point) => Number.isFinite(point.value));
  const values = rows.map((point) => point.value);
  const observedMin = values.length ? Math.min(...values) : 0;
  const observedMax = values.length ? Math.max(...values) : 0;
  const domainMin = Math.min(0, observedMin);
  const domainMax = Math.max(
    niceStressMax(Math.max(1, thresholds.critical, thresholds.warning, observedMax)),
    domainMin + 1e-6,
  );
  const domainSpan = Math.max(1e-6, domainMax - domainMin);
  const yFor = (value) => plot.top + ((domainMax - Number(value)) / domainSpan) * plot.height;
  const xFor = (index) => (
    rows.length <= 1
      ? plot.left + (plot.width / 2)
      : plot.left + (index / (rows.length - 1)) * plot.width
  );
  const clampDomain = (value) => Math.max(domainMin, Math.min(domainMax, Number(value)));

  const bands = [
    buildBandRect({
      key: "critical",
      label: "High stress",
      startValue: clampDomain(thresholds.critical),
      endValue: domainMax,
      yFor,
      width,
    }),
    buildBandRect({
      key: "warning",
      label: "Elevated stress",
      startValue: clampDomain(thresholds.warning),
      endValue: clampDomain(thresholds.critical),
      yFor,
      width,
    }),
    buildBandRect({
      key: "normal",
      label: "Normal",
      startValue: domainMin,
      endValue: clampDomain(thresholds.warning),
      yFor,
      width,
    }),
  ].filter((band) => band.height > 0);
  const thresholdLines = [
    {
      key: "warning",
      label: "Warning",
      value: thresholds.warning,
      y: yFor(thresholds.warning),
      strokeStyle: SPARKLINE_LINES.warning,
    },
    {
      key: "critical",
      label: "Critical",
      value: thresholds.critical,
      y: yFor(thresholds.critical),
      strokeStyle: SPARKLINE_LINES.critical,
    },
  ].filter((line) => line.y >= 0 && line.y <= height);
  const points = rows.map((point, index) => {
    const classification = classifyMarketStressScore(point.value, thresholds);
    return {
      ...point,
      x: xFor(index),
      y: yFor(point.value),
      band: classification.state,
      bandLabel: classification.label,
    };
  });

  return {
    width,
    height,
    plot,
    domainMin,
    domainMax,
    observedMin,
    observedMax,
    thresholds,
    bands,
    thresholdLines,
    points,
    hasData: points.length >= 2,
  };
}

export function clearMarketStressSparklineMetadata(canvas) {
  if (!canvas || typeof canvas.removeAttribute !== "function") return;
  [
    "data-stress-scale-min",
    "data-stress-scale-max",
    "data-stress-observed-max",
    "data-stress-threshold-warning",
    "data-stress-threshold-critical",
    "data-stress-thresholds",
  ].forEach((name) => canvas.removeAttribute(name));
}

export function applyMarketStressSparklineMetadata(canvas, model) {
  if (!canvas || !model || typeof canvas.setAttribute !== "function") return;
  canvas.setAttribute("data-stress-scale-min", model.domainMin.toFixed(3));
  canvas.setAttribute("data-stress-scale-max", model.domainMax.toFixed(3));
  canvas.setAttribute("data-stress-observed-max", model.observedMax.toFixed(3));
  canvas.setAttribute("data-stress-threshold-warning", model.thresholds.warning.toFixed(3));
  canvas.setAttribute("data-stress-threshold-critical", model.thresholds.critical.toFixed(3));
  canvas.setAttribute("data-stress-thresholds", JSON.stringify(model.thresholds));
}

export function marketStressSparklineSummary(model) {
  if (!model || !Array.isArray(model.points) || !model.points.length) {
    return "Market stress history: no numeric stress history is available.";
  }
  const first = model.points[0].value;
  const last = model.points[model.points.length - 1].value;
  const delta = last - first;
  const movement = Math.abs(delta) <= 1e-12
    ? "flat versus the first point"
    : `${delta > 0 ? "up" : "down"} ${Math.abs(delta).toFixed(3)} versus the first point`;
  return (
    `Market stress history: latest stress score ${last.toFixed(3)}; ${movement}; ` +
    `range ${model.observedMin.toFixed(3)} to ${model.observedMax.toFixed(3)}; ` +
    `warning line ${model.thresholds.warning.toFixed(3)}, critical line ${model.thresholds.critical.toFixed(3)}; ` +
    `chart scale ${model.domainMin.toFixed(3)} to ${model.domainMax.toFixed(3)}.`
  );
}

export function drawMarketStressSparkline(ctx, model) {
  if (!ctx || !model) return;
  const { width, height, bands, thresholdLines, points } = model;
  ctx.clearRect(0, 0, width, height);

  if (typeof ctx.save === "function") ctx.save();
  for (const band of bands) {
    ctx.fillStyle = band.fillStyle;
    ctx.fillRect(band.x, band.y, band.width, band.height);
  }

  for (const line of thresholdLines) {
    ctx.beginPath();
    if (typeof ctx.setLineDash === "function") ctx.setLineDash([4, 3]);
    ctx.lineWidth = 1;
    ctx.strokeStyle = line.strokeStyle;
    ctx.moveTo(0, line.y);
    ctx.lineTo(width, line.y);
    ctx.stroke();
  }
  if (typeof ctx.setLineDash === "function") ctx.setLineDash([]);

  if (points.length >= 2) {
    ctx.beginPath();
    ctx.lineWidth = 1.8;
    ctx.strokeStyle = SPARKLINE_LINES.series;
    points.forEach((point, index) => {
      if (index === 0) ctx.moveTo(point.x, point.y);
      else ctx.lineTo(point.x, point.y);
    });
    ctx.stroke();
  }
  if (typeof ctx.restore === "function") ctx.restore();
}

export function buildMarketStressComponentRows(payload = {}) {
  const s = stressBody(payload);
  return [
    { label: "VIX", metricKey: "vix", value: s.vix, zMetricKey: "z_vix", zValue: s.z_vix },
    { label: "VVIX", metricKey: "vvix", value: s.vvix, zMetricKey: "z_vvix", zValue: s.z_vvix },
    { label: "MOVE", metricKey: "move", value: s.move, zMetricKey: "z_move", zValue: s.z_move },
    { label: "VIX1D/VIX", metricKey: "vix1d_over_vix", value: s.vix1d_over_vix, zMetricKey: null, zValue: null },
    { label: "VIX9D/VIX", metricKey: "vix9d_over_vix", value: s.vix9d_over_vix, zMetricKey: null, zValue: null },
    { label: "VIX3M/VIX", metricKey: "vix3m_over_vix", value: s.vix3m_over_vix, zMetricKey: null, zValue: null },
    { label: "Term Z", metricKey: "z_term", value: null, zMetricKey: "z_term", zValue: s.z_term },
    { label: "Credit LQD/HYG", metricKey: "credit_lqd_over_hyg", value: s.credit_lqd_over_hyg, zMetricKey: "z_credit", zValue: s.z_credit },
    { label: "Rates TLT/SHY", metricKey: "rates_tlt_over_shy", value: s.rates_tlt_over_shy, zMetricKey: "z_rates", zValue: s.z_rates },
  ];
}

export function buildMarketStressTopDriver(payload = {}) {
  const s = stressBody(payload);
  const score = numOrNull(s.stress_score);
  const thresholds = normalizeMarketStressThresholds(payload && payload.thresholds);
  const scoredRows = buildMarketStressComponentRows(s)
    .map((row) => ({
      ...row,
      zNumber: numOrNull(row.zValue),
    }))
    .filter((row) => row.zNumber !== null);
  scoredRows.sort((a, b) => Math.abs(b.zNumber) - Math.abs(a.zNumber));
  const driver = scoredRows[0] || null;
  const tone = classifyMarketStressScore(score, thresholds).tone;
  const scoreText = score === null ? "Stress unavailable" : `Stress ${score.toFixed(2)}`;

  if (!driver) {
    return {
      tone,
      score,
      scoreText,
      label: "unavailable",
      text: "Top driver unavailable.",
      fallbackText: `${scoreText}. Top market-stress driver unavailable.`,
    };
  }

  const direction = driver.zNumber >= 0 ? "above normal" : "below normal";
  const text = `Driven by ${driver.label} (${Math.abs(driver.zNumber).toFixed(2)}z ${direction})`;
  return {
    tone,
    score,
    scoreText,
    label: driver.label,
    zValue: driver.zNumber,
    text,
    fallbackText: `${scoreText}. ${text}.`,
  };
}

function resetMarketStressUI({
  badge,
  updated,
  body,
  raw,
  message = "(market stress unavailable)",
} = {}) {
  if (badge) {
    badge.className = "pill dim";
    badge.textContent = "—";
    setMetricValueAttribute(badge, null);
  }

  const hdr = document.getElementById("marketStressHeader");
  if (hdr) {
    hdr.className = "pill dim";
    hdr.textContent = "Stress: —";
  }

  if (updated) {
    updated.textContent = "—";
    setMetricValueAttribute(updated, null);
  }
  if (body) {
    body.innerHTML = `
      <tr class="table-row">
        <td colspan="3" class="metric-meta">(market stress unavailable)</td>
      </tr>
    `;
  }
  if (raw) raw.textContent = message;
  try {
    const setPanelState = typeof window !== "undefined" ? window.__setDashboardPanelState__ : null;
    if (typeof setPanelState === "function") {
      setPanelState("marketStressPanel", {
        state: "error",
        reason: String(message || "(market stress unavailable)"),
      });
    }
  } catch {}

  const stripStress = document.getElementById("tStress");
  if (stripStress) {
    stripStress.textContent = "Market Stress —";
    setMetricValueAttribute(stripStress, null);
  }

  renderChartAccessibility("marketStressSparkline", {
    title: "Market stress history",
    series: [],
    emptyMessage: message,
    errorMessage: message,
    valueLabel: "stress score",
    valueFormatter: (v) => Number(v).toFixed(3),
    chartType: "canvas-sparkline",
  });
}

export async function loadMarketStress(fetchJSON) {
  const badge = document.getElementById("marketStressBadge");
  const updated = document.getElementById("marketStressUpdated");
  const body = document.getElementById("marketStressBody");
  const raw = document.getElementById("marketStressRaw");

  if (!badge || !updated || !body || !raw) return;

  try {
    const j = await fetchJSON("/api/market_stress");
    if (!j || !j.ok || !j.stress) {
      window.__LAST_MARKET_STRESS__ = null;
      resetMarketStressUI({
        badge,
        updated,
        body,
        raw,
        message: j && j.error ? String(j.error) : "(market stress unavailable)",
      });
      return;
    }

    window.__LAST_MARKET_STRESS__ = j;

    const s = j.stress || {};
    const thresholds = normalizeMarketStressThresholds(j.thresholds || s.thresholds);
    const score = Number(s.stress_score ?? 0);
    const ts_ms = Number(s.ts_ms ?? 0);

    const cls = classifyMarketStressScore(score, thresholds).pillClass;

    badge.className = cls;
    badge.textContent = Number.isFinite(score) ? score.toFixed(3) : "—";
    setMetricValueAttribute(badge, Number.isFinite(score) ? score : null);
    applyInlineMetricAnnotation(
      badge,
      "market_stress_score",
      Number.isFinite(score) ? score : null
    );

    const hdr = document.getElementById("marketStressHeader");
    if (hdr) {
      hdr.className = cls;
      hdr.textContent = Number.isFinite(score)
        ? `Stress: ${score.toFixed(2)}`
        : "Stress: —";
    }

    if (Number.isFinite(ts_ms) && ts_ms > 0) {
      updated.textContent = new Date(ts_ms).toLocaleString();
      setMetricValueAttribute(updated, ts_ms);
    } else {
      updated.textContent = "—";
      setMetricValueAttribute(updated, null);
    }

    const stripStress = document.getElementById("tStress");
    if (stripStress) {
      stripStress.textContent = Number.isFinite(score)
        ? `Market Stress ${score.toFixed(2)}`
        : "Market Stress —";
      setMetricValueAttribute(stripStress, Number.isFinite(score) ? score : null);
      applyInlineMetricAnnotation(
        stripStress,
        "market_stress_score",
        Number.isFinite(score) ? score : null
      );
    }

    const rows = buildMarketStressComponentRows(s);

    body.innerHTML = "";

    for (const row of rows) {
      const tr = document.createElement("tr");
      const nameTd = document.createElement("td");
      const valueTd = document.createElement("td");
      const zTd = document.createElement("td");

      const v =
        row.value === null || row.value === undefined || !Number.isFinite(Number(row.value))
          ? "—"
          : Number(row.value).toFixed(4);

      const zz =
        row.zValue === null || row.zValue === undefined || !Number.isFinite(Number(row.zValue))
          ? "—"
          : Number(row.zValue).toFixed(3);

      nameTd.textContent = row.label;
      if (row.metricKey) {
        nameTd.setAttribute("data-metric", row.metricKey);
      }

      valueTd.className = "mono";
      valueTd.textContent = v;
      if (row.metricKey) {
        valueTd.setAttribute("data-metric", row.metricKey);
        setMetricValueAttribute(
          valueTd,
          Number.isFinite(Number(row.value)) ? Number(row.value) : null
        );
      }

      zTd.className = "mono";
      zTd.textContent = zz;
      if (row.zMetricKey) {
        zTd.setAttribute("data-metric", row.zMetricKey);
        setMetricValueAttribute(
          zTd,
          Number.isFinite(Number(row.zValue)) ? Number(row.zValue) : null
        );
        applyInlineMetricAnnotation(
          zTd,
          row.zMetricKey,
          Number.isFinite(Number(row.zValue)) ? Number(row.zValue) : null
        );
      }

      tr.appendChild(nameTd);
      tr.appendChild(valueTd);
      tr.appendChild(zTd);

      body.appendChild(tr);
    }

    raw.textContent = JSON.stringify(s, null, 2);
    try {
      const setPanelState = typeof window !== "undefined" ? window.__setDashboardPanelState__ : null;
      if (typeof setPanelState === "function") {
        const ageMs = Number.isFinite(ts_ms) && ts_ms > 0 ? Math.max(0, Date.now() - ts_ms) : null;
        setPanelState("marketStressPanel", {
          state: ageMs != null && ageMs >= 300_000 ? "stale" : "fresh",
          reason: Number.isFinite(score)
            ? `Stress score ${score.toFixed(3)} ${ageMs == null ? "without a timestamp" : `• backend ${Math.round(ageMs / 1000)}s old`}`
            : "Market stress snapshot is available without a usable score.",
        });
      }
    } catch {}

  } catch (e) {
    window.__LAST_MARKET_STRESS__ = null;
    resetMarketStressUI({
      badge,
      updated,
      body,
      raw,
      message: e && e.message ? e.message : String(e),
    });
  }
}

export async function loadMarketStressHistory(fetchJSON) {

  const canvas = document.getElementById("marketStressSparkline");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  try {
    const j = await fetchJSON("/api/market_stress_history");
    const size = syncMarketStressSparklineSize(canvas);
    const w = size.width;
    const h = size.height;
    if (typeof ctx.setTransform === "function") {
      ctx.setTransform(size.ratio, 0, 0, size.ratio, 0, 0);
    }

    ctx.clearRect(0, 0, w, h);
    if (!j || !j.ok || !Array.isArray(j.series)) {
      clearMarketStressSparklineMetadata(canvas);
      renderChartAccessibility(canvas, {
        title: "Market stress history",
        series: [],
        emptyMessage: j && j.error ? String(j.error) : "Market stress history is unavailable.",
        errorMessage: j && j.error ? String(j.error) : "",
        valueLabel: "stress score",
        valueFormatter: (v) => Number(v).toFixed(3),
        chartType: "canvas-sparkline",
      });
      return;
    }

    const series = j.series
      .map((p, index) => ({
        time: p && (p.ts_ms ?? p.time ?? p.t ?? index + 1),
        value: Number(p && p.stress_score),
      }))
      .filter((p) => Number.isFinite(p.value));
    const ys = series.map((p) => p.value);
    if (ys.length < 2) {
      clearMarketStressSparklineMetadata(canvas);
      renderChartAccessibility(canvas, {
        title: "Market stress history",
        series,
        emptyMessage: "Market stress history does not have enough points to draw.",
        valueLabel: "stress score",
        valueFormatter: (v) => Number(v).toFixed(3),
        chartType: "canvas-sparkline",
      });
      return;
    }

    const thresholds = normalizeMarketStressThresholds(j.thresholds || j.market_stress_thresholds);
    const model = buildMarketStressSparklineModel(series, { width: w, height: h }, thresholds);
    drawMarketStressSparkline(ctx, model);
    applyMarketStressSparklineMetadata(canvas, model);

    renderChartAccessibility(canvas, {
      title: "Market stress history",
      series,
      valueKey: "value",
      timeKey: "time",
      valueLabel: "stress score",
      valueFormatter: (v) => Number(v).toFixed(3),
      summary: marketStressSparklineSummary(model),
      columns: [
        { label: "Point", value: (row) => row.timeText || row.index },
        { label: "Stress score", value: (row) => Number(row.value).toFixed(3) },
        { label: "Band", value: (row) => classifyMarketStressScore(row.value, thresholds).label },
      ],
      chartType: "canvas-sparkline",
    });

  } catch (e) {
    const size = syncMarketStressSparklineSize(canvas);
    if (typeof ctx.setTransform === "function") {
      ctx.setTransform(size.ratio, 0, 0, size.ratio, 0, 0);
    }
    ctx.clearRect(0, 0, size.width, size.height);
    clearMarketStressSparklineMetadata(canvas);
    renderChartAccessibility(canvas, {
      title: "Market stress history",
      series: [],
      emptyMessage: "Market stress history failed to load.",
      errorMessage: e && e.message ? e.message : "Market stress history failed to load.",
      valueLabel: "stress score",
      valueFormatter: (v) => Number(v).toFixed(3),
      chartType: "canvas-sparkline",
    });
  }
}
