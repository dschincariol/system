/*
  FILE: ui/news_panels.js

  News and sentiment panel helpers for the dashboard. This module renders
  recent news items plus lightweight sentiment visualizations from the
  dashboard-facing news endpoints.
*/

import { renderChartAccessibility } from "./chart_a11y.js";
import { esc, fmtTime } from "./utils.js";

function _normalizeSymbol(value) {
  return String(value || "").trim().toUpperCase();
}

function _drawNewsSentimentMessage(canvas, text) {
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

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
    valueFormatter: (v) => Number(v).toFixed(3),
    chartType: "canvas-line",
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

    const w = canvas.width;
    const h = canvas.height;

    ctx.clearRect(0, 0, w, h);

    const series =
      res.series
        .map((p, index) => ({
          time: p && (p.ts_ms ?? p.time ?? p.t ?? index + 1),
          value: Number(p && p.sentiment),
        }))
        .filter((p) => Number.isFinite(p.value));
    const ys = series.map((p) => p.value);

    if (!ys.length) {
      _drawNewsSentimentMessage(canvas, res && res.meta && res.meta.ready === false ? "(news sentiment not ready)" : "(no sentiment data)");
      return;
    }

    const min = -1;
    const max = 1;

    ctx.beginPath();
    ctx.strokeStyle = "#58a6ff";

    ys.forEach((v, i) => {

      const x = ys.length <= 1 ? 0 : (i / (ys.length - 1)) * w;
      const y = h - ((v - min) / (max - min)) * h;

      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);

    });

    ctx.stroke();

    renderChartAccessibility(canvas, {
      title: "News sentiment",
      series,
      valueKey: "value",
      timeKey: "time",
      valueLabel: "sentiment",
      valueFormatter: (v) => Number(v).toFixed(3),
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
      valueFormatter: (v) => Number(v).toFixed(3),
      chartType: "canvas-line",
    });
  }
}
