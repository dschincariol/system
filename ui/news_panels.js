/*
  FILE: ui/news_panels.js

  News and sentiment panel helpers for the dashboard. This module renders
  recent news items plus lightweight sentiment visualizations from the
  dashboard-facing news endpoints.
*/

import { renderChartAccessibility } from "./chart_a11y.js";
import { esc, fmtTime } from "./utils.js";

export const NEWS_SENTIMENT_RANGE = Object.freeze({
  min: -1,
  baseline: 0,
  max: 1,
});

const NEWS_SENTIMENT_PADDING = Object.freeze({
  top: 16,
  right: 14,
  bottom: 18,
  left: 48,
});

const NEWS_SENTIMENT_COLORS = Object.freeze({
  positiveBand: "rgba(46, 160, 67, 0.10)",
  negativeBand: "rgba(248, 81, 73, 0.10)",
  baseline: "rgba(201, 209, 217, 0.82)",
  axis: "rgba(48, 54, 61, 0.92)",
  label: "#9da7b1",
  line: "#58a6ff",
  point: "#58a6ff",
  clipped: "#d29922",
});

function _normalizeSymbol(value) {
  return String(value || "").trim().toUpperCase();
}

function _numOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "boolean") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function _sentimentValueFromPoint(point) {
  if (!point || typeof point !== "object") return point;
  if (point.sentiment !== undefined) return point.sentiment;
  if (point.value !== undefined) return point.value;
  if (point.score !== undefined) return point.score;
  return undefined;
}

function _timeFromPoint(point, index) {
  if (!point || typeof point !== "object") return index + 1;
  return point.ts_ms ?? point.time ?? point.t ?? point.label ?? index + 1;
}

function _clampSentiment(value) {
  return Math.max(NEWS_SENTIMENT_RANGE.min, Math.min(NEWS_SENTIMENT_RANGE.max, value));
}

function _sentimentContext(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "unavailable";
  if (n > NEWS_SENTIMENT_RANGE.baseline) return "positive";
  if (n < NEWS_SENTIMENT_RANGE.baseline) return "negative";
  return "neutral";
}

export function formatNewsSentimentValue(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "unavailable";
  const prefix = n > 0 ? "+" : "";
  return `${prefix}${n.toFixed(3)}`;
}

function _qualityLabel(point) {
  if (!point || !point.clipped) return "within expected range";
  return point.rawValue > NEWS_SENTIMENT_RANGE.max
    ? "clipped above expected range"
    : "clipped below expected range";
}

function _sentimentBandRect({ key, label, startValue, endValue, yFor, plot }) {
  const yStart = yFor(startValue);
  const yEnd = yFor(endValue);
  const y = Math.min(yStart, yEnd);
  return {
    key,
    label,
    startValue,
    endValue,
    x: plot.left,
    y,
    width: plot.width,
    height: Math.max(0, Math.abs(yStart - yEnd)),
    fillStyle: key === "positive"
      ? NEWS_SENTIMENT_COLORS.positiveBand
      : NEWS_SENTIMENT_COLORS.negativeBand,
  };
}

export function syncNewsSentimentSparklineSize(canvas) {
  if (!canvas) return { width: 900, height: 160, ratio: 1 };
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
    (Number.isFinite(attrHeight) && attrHeight > 0 ? attrHeight : 160)
  ));
  const ratio = Math.max(1, Number(typeof window !== "undefined" ? window.devicePixelRatio : 1) || 1);
  const backingWidth = Math.round(cssWidth * ratio);
  const backingHeight = Math.round(cssHeight * ratio);
  if (canvas.width !== backingWidth) canvas.width = backingWidth;
  if (canvas.height !== backingHeight) canvas.height = backingHeight;
  return { width: cssWidth, height: cssHeight, ratio };
}

export function buildNewsSentimentSparklineModel(series = [], size = {}) {
  const width = Math.max(1, Number(size.width) || 900);
  const height = Math.max(1, Number(size.height) || 160);
  const padding = {
    ...NEWS_SENTIMENT_PADDING,
    ...(size && typeof size.padding === "object" ? size.padding : {}),
  };
  const plot = {
    left: Math.max(0, Number(padding.left) || 0),
    right: Math.max(0, Number(padding.right) || 0),
    top: Math.max(0, Number(padding.top) || 0),
    bottom: Math.max(0, Number(padding.bottom) || 0),
  };
  plot.width = Math.max(1, width - plot.left - plot.right);
  plot.height = Math.max(1, height - plot.top - plot.bottom);

  const inputRows = Array.isArray(series) ? series : [];
  let malformedCount = 0;
  const rows = [];
  inputRows.forEach((point, index) => {
    const rawValue = _numOrNull(_sentimentValueFromPoint(point));
    if (rawValue == null) {
      malformedCount += 1;
      return;
    }
    const value = _clampSentiment(rawValue);
    const clipped = Math.abs(value - rawValue) > 1e-12;
    rows.push({
      index,
      pointNumber: index + 1,
      time: _timeFromPoint(point, index),
      rawValue,
      value,
      clipped,
      anomaly: clipped
        ? (rawValue > NEWS_SENTIMENT_RANGE.max ? "above_expected_range" : "below_expected_range")
        : null,
      context: _sentimentContext(value),
      quality: clipped
        ? (rawValue > NEWS_SENTIMENT_RANGE.max ? "clipped above expected range" : "clipped below expected range")
        : "within expected range",
      raw: point,
    });
  });

  const rawValues = rows.map((point) => point.rawValue);
  const observedMin = rawValues.length ? Math.min(...rawValues) : null;
  const observedMax = rawValues.length ? Math.max(...rawValues) : null;
  const visibleValues = rows.map((point) => point.value);
  const visibleMin = visibleValues.length ? Math.min(...visibleValues) : null;
  const visibleMax = visibleValues.length ? Math.max(...visibleValues) : null;
  const domainMin = NEWS_SENTIMENT_RANGE.min;
  const domainMax = NEWS_SENTIMENT_RANGE.max;
  const domainSpan = domainMax - domainMin;
  const yFor = (value) => plot.top + ((domainMax - Number(value)) / domainSpan) * plot.height;
  const xFor = (index) => (
    rows.length <= 1
      ? plot.left + (plot.width / 2)
      : plot.left + (index / (rows.length - 1)) * plot.width
  );

  const bands = [
    _sentimentBandRect({
      key: "positive",
      label: "Positive sentiment",
      startValue: NEWS_SENTIMENT_RANGE.baseline,
      endValue: NEWS_SENTIMENT_RANGE.max,
      yFor,
      plot,
    }),
    _sentimentBandRect({
      key: "negative",
      label: "Negative sentiment",
      startValue: NEWS_SENTIMENT_RANGE.min,
      endValue: NEWS_SENTIMENT_RANGE.baseline,
      yFor,
      plot,
    }),
  ].filter((band) => band.height > 0);

  const points = rows.map((point, plotIndex) => ({
    ...point,
    x: xFor(plotIndex),
    y: yFor(point.value),
  }));

  return {
    width,
    height,
    plot,
    domainMin,
    domainMax,
    baseline: NEWS_SENTIMENT_RANGE.baseline,
    baselineY: yFor(NEWS_SENTIMENT_RANGE.baseline),
    bands,
    points,
    hasData: points.length > 0,
    hasLine: points.length >= 2,
    malformedCount,
    clippedCount: points.filter((point) => point.clipped).length,
    observedMin,
    observedMax,
    visibleMin,
    visibleMax,
  };
}

export function clearNewsSentimentSparklineMetadata(canvas) {
  if (!canvas || typeof canvas.removeAttribute !== "function") return;
  [
    "data-sentiment-scale-min",
    "data-sentiment-scale-max",
    "data-sentiment-baseline",
    "data-sentiment-clipped-points",
    "data-sentiment-malformed-points",
    "data-sentiment-observed-min",
    "data-sentiment-observed-max",
  ].forEach((name) => canvas.removeAttribute(name));
}

export function applyNewsSentimentSparklineMetadata(canvas, model) {
  if (!canvas || !model || typeof canvas.setAttribute !== "function") return;
  canvas.setAttribute("data-sentiment-scale-min", formatNewsSentimentValue(model.domainMin));
  canvas.setAttribute("data-sentiment-scale-max", formatNewsSentimentValue(model.domainMax));
  canvas.setAttribute("data-sentiment-baseline", formatNewsSentimentValue(model.baseline));
  canvas.setAttribute("data-sentiment-clipped-points", String(model.clippedCount || 0));
  canvas.setAttribute("data-sentiment-malformed-points", String(model.malformedCount || 0));
  if (Number.isFinite(model.observedMin)) {
    canvas.setAttribute("data-sentiment-observed-min", formatNewsSentimentValue(model.observedMin));
  }
  if (Number.isFinite(model.observedMax)) {
    canvas.setAttribute("data-sentiment-observed-max", formatNewsSentimentValue(model.observedMax));
  }
}

function _fitText(ctx, text, maxWidth) {
  const raw = String(text || "");
  if (!ctx || typeof ctx.measureText !== "function" || ctx.measureText(raw).width <= maxWidth) return raw;
  let out = raw;
  while (out.length > 1 && ctx.measureText(`${out}...`).width > maxWidth) {
    out = out.slice(0, -1);
  }
  return `${out}...`;
}

function _labelY(y, height) {
  return Math.min(height - 4, Math.max(10, Number(y) + 4));
}

export function drawNewsSentimentSparkline(ctx, model) {
  if (!ctx || !model) return;
  const { width: w, height: h, plot } = model;

  ctx.clearRect(0, 0, w, h);
  ctx.strokeStyle = NEWS_SENTIMENT_COLORS.axis;
  ctx.lineWidth = 1;
  if (typeof ctx.strokeRect === "function") ctx.strokeRect(0.5, 0.5, w - 1, h - 1);

  for (const band of model.bands) {
    ctx.fillStyle = band.fillStyle;
    if (typeof ctx.fillRect === "function") {
      ctx.fillRect(band.x, band.y, band.width, band.height);
    }
  }

  ctx.strokeStyle = "rgba(48, 54, 61, 0.72)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(plot.left, plot.top);
  ctx.lineTo(plot.left + plot.width, plot.top);
  ctx.moveTo(plot.left, plot.top + plot.height);
  ctx.lineTo(plot.left + plot.width, plot.top + plot.height);
  ctx.stroke();

  if (typeof ctx.setLineDash === "function") ctx.setLineDash([4, 4]);
  ctx.strokeStyle = NEWS_SENTIMENT_COLORS.baseline;
  ctx.lineWidth = 1.25;
  ctx.beginPath();
  ctx.moveTo(plot.left, model.baselineY);
  ctx.lineTo(plot.left + plot.width, model.baselineY);
  ctx.stroke();
  if (typeof ctx.setLineDash === "function") ctx.setLineDash([]);

  ctx.fillStyle = NEWS_SENTIMENT_COLORS.label;
  ctx.font = "11px Consolas, monospace";
  ctx.fillText("+1", 8, _labelY(plot.top, h));
  ctx.fillText("0 neutral", 8, _labelY(model.baselineY, h));
  ctx.fillText("-1", 8, _labelY(plot.top + plot.height, h));
  ctx.fillText("positive", plot.left + 8, _labelY(plot.top + plot.height * 0.25, h));
  ctx.fillText("negative", plot.left + 8, _labelY(plot.top + plot.height * 0.75, h));

  if (!model.hasData) {
    return;
  }

  if (model.hasLine) {
    ctx.strokeStyle = NEWS_SENTIMENT_COLORS.line;
    ctx.lineWidth = 2;
    ctx.beginPath();
    model.points.forEach((point, index) => {
      if (index === 0) ctx.moveTo(point.x, point.y);
      else ctx.lineTo(point.x, point.y);
    });
    ctx.stroke();
  }

  const pointsToMark = model.points.filter((point, index) => (
    point.clipped ||
    !model.hasLine ||
    index === model.points.length - 1
  ));
  for (const point of pointsToMark) {
    if (typeof ctx.arc !== "function" || typeof ctx.fill !== "function") continue;
    ctx.beginPath();
    ctx.fillStyle = point.clipped ? NEWS_SENTIMENT_COLORS.clipped : NEWS_SENTIMENT_COLORS.point;
    ctx.arc(point.x, point.y, point.clipped ? 4 : 3, 0, Math.PI * 2);
    ctx.fill();
  }

  const notes = [];
  if (model.clippedCount) notes.push(`${model.clippedCount} clipped`);
  if (model.malformedCount) notes.push(`${model.malformedCount} skipped`);
  if (notes.length) {
    const text = _fitText(ctx, notes.join(", "), Math.max(80, plot.width * 0.45));
    const textWidth = typeof ctx.measureText === "function" ? ctx.measureText(text).width : text.length * 7;
    ctx.fillStyle = NEWS_SENTIMENT_COLORS.clipped;
    ctx.font = "11px Consolas, monospace";
    ctx.fillText(text, Math.max(plot.left, w - plot.right - textWidth - 4), plot.top + 12);
  }
}

export function newsSentimentSparklineSummary(model) {
  const scale = `${formatNewsSentimentValue(NEWS_SENTIMENT_RANGE.min)} negative to ${formatNewsSentimentValue(NEWS_SENTIMENT_RANGE.max)} positive`;
  const baseline = `${formatNewsSentimentValue(NEWS_SENTIMENT_RANGE.baseline)} neutral baseline`;
  if (!model || !model.hasData) {
    const skipped = model && model.malformedCount
      ? ` Skipped ${model.malformedCount} malformed point${model.malformedCount === 1 ? "" : "s"}.`
      : "";
    return `News sentiment: no valid sentiment points. Expected scale is ${scale} with a ${baseline}.${skipped}`;
  }

  const latest = model.points[model.points.length - 1];
  const parts = [
    `News sentiment: latest sentiment ${formatNewsSentimentValue(latest.value)} (${latest.context})`,
    `visible range ${formatNewsSentimentValue(model.visibleMin)} to ${formatNewsSentimentValue(model.visibleMax)} on expected scale ${scale} with a ${baseline}`,
  ];
  if (model.clippedCount) {
    parts.push(`clipped ${model.clippedCount} out-of-range point${model.clippedCount === 1 ? "" : "s"} to the expected scale; raw range ${formatNewsSentimentValue(model.observedMin)} to ${formatNewsSentimentValue(model.observedMax)}`);
  }
  if (model.malformedCount) {
    parts.push(`skipped ${model.malformedCount} malformed point${model.malformedCount === 1 ? "" : "s"}`);
  }
  return `${parts.join("; ")}.`;
}

function _newsSentimentA11yColumns() {
  return [
    { label: "Point", value: (row) => row.timeText || row.index },
    { label: "Displayed sentiment", value: (row) => formatNewsSentimentValue(row.value) },
    { label: "Raw sentiment", value: (row) => formatNewsSentimentValue(row.raw && row.raw.rawValue) },
    { label: "Context", value: (row) => row.raw && row.raw.context ? row.raw.context : _sentimentContext(row.value) },
    { label: "Data quality", value: (row) => _qualityLabel(row.raw) },
  ];
}

function _drawNewsSentimentMessage(canvas, text, a11yOptions = {}) {
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  clearNewsSentimentSparklineMetadata(canvas);

  const w = canvas.width;
  const h = canvas.height;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#9da7b1";
  ctx.font = "12px Consolas, monospace";
  ctx.fillText(String(text || "(no data)"), 12, 24);
  renderChartAccessibility(canvas, {
    title: "News sentiment",
    series: [],
    emptyMessage: String(text || "(no data)"),
    valueLabel: "sentiment",
    valueFormatter: formatNewsSentimentValue,
    chartType: "canvas-line",
    ...a11yOptions,
  });
}

export async function loadNewsPanels(fetchJSON, options = {}) {

  const body = document.getElementById("newsBody");
  if (!body) return;
  const selectedSymbol = _normalizeSymbol(options && options.symbol);

  try {

    const res = await fetchJSON("/api/news/latest");

    const rows =
      (res && res.ok && Array.isArray(res.items))
        ? res.items
        : [];

    body.innerHTML = "";

    if (!rows.length) {

      body.innerHTML = `
        <tr>
          <td colspan="4" class="small">${res && res.meta && res.meta.ready === false ? "(news not ready)" : "(no news)"}</td>
        </tr>
      `;

      return;
    }

    const visibleRows = rows.slice(0, 20);
    const hasSelectedRow = !!selectedSymbol && visibleRows.some((r) => _normalizeSymbol(r && r.symbol) === selectedSymbol);

    if (selectedSymbol && !hasSelectedRow) {
      body.insertAdjacentHTML(
        "beforeend",
        `
        <tr class="table-row">
          <td colspan="4" class="small">No latest news for ${esc(selectedSymbol)} in this feed; showing global latest news.</td>
        </tr>
        `
      );
    }

    for (const r of visibleRows) {

      const ts = Number(r.ts_ms);
      const isSelected = selectedSymbol && _normalizeSymbol(r.symbol) === selectedSymbol;

      body.insertAdjacentHTML(
        "beforeend",
        `
        <tr class="table-row${isSelected ? " symbolContextMatch" : ""}">
          <td class="mono">${fmtTime(ts)}</td>
          <td>${esc(r.symbol || "")}</td>
          <td>${esc(r.title || "")}</td>
          <td class="small">${esc(r.source || "")}</td>
        </tr>
        `
      );
    }

  } catch (e) {

    body.innerHTML = `
      <tr>
        <td colspan="4" class="small">
          ${esc(e && e.message ? e.message : "error loading news")}
        </td>
      </tr>
    `;
  }
}

export async function loadNewsSentiment(fetchJSON, _options = {}) {

  const canvas = document.getElementById("newsSentimentCanvas");
  if (!canvas) return;

  try {

    const res = await fetchJSON("/api/news/sentiment");

    if (!res || !res.ok || !Array.isArray(res.series)) {
      _drawNewsSentimentMessage(canvas, "(news sentiment unavailable)");
      return;
    }

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const size = syncNewsSentimentSparklineSize(canvas);
    if (typeof ctx.setTransform === "function") {
      ctx.setTransform(size.ratio, 0, 0, size.ratio, 0, 0);
    }

    const model = buildNewsSentimentSparklineModel(res.series, { width: size.width, height: size.height });

    if (!model.hasData) {
      const summary = newsSentimentSparklineSummary(model);
      _drawNewsSentimentMessage(
        canvas,
        res && res.meta && res.meta.ready === false ? "(news sentiment not ready)" : "(no sentiment data)",
        {
          summary,
          emptyMessage: summary,
        }
      );
      return;
    }

    drawNewsSentimentSparkline(ctx, model);
    applyNewsSentimentSparklineMetadata(canvas, model);

    renderChartAccessibility(canvas, {
      title: "News sentiment",
      series: model.points,
      valueKey: "value",
      timeKey: "time",
      valueLabel: "sentiment",
      valueFormatter: formatNewsSentimentValue,
      summary: newsSentimentSparklineSummary(model),
      columns: _newsSentimentA11yColumns(),
      chartType: "canvas-line",
    });

  } catch (e) {
    _drawNewsSentimentMessage(canvas, e && e.message ? e.message : "error loading sentiment");
    renderChartAccessibility(canvas, {
      title: "News sentiment",
      series: [],
      emptyMessage: "News sentiment failed to load.",
      errorMessage: e && e.message ? e.message : "error loading sentiment",
      valueLabel: "sentiment",
      valueFormatter: formatNewsSentimentValue,
      chartType: "canvas-line",
    });
  }
}
