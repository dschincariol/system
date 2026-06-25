"use strict";

/*
  ui/risk_charts.js

  Lazy-loaded production consumers for risk history, Monte-Carlo summaries,
  alpha-decay history, and regime-stack history.
*/

import { installChartPointInspector, renderChartAccessibility } from "./chart_a11y.js";

const RISK_SERIES = Object.freeze([
  Object.freeze({ key: "gross", label: "Gross", color: "#56B4E9", formatter: formatPercent }),
  Object.freeze({ key: "net", label: "Net", color: "#0072B2", formatter: formatSignedPercent }),
  Object.freeze({ key: "drawdown", label: "Drawdown", color: "#E69F00", formatter: formatPercent }),
]);

const SEVERITY_RANK = Object.freeze({
  severe: 3,
  warn: 2,
  ok: 1,
});

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function numOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function clamp(value, lo, hi) {
  const n = Number(value);
  if (!Number.isFinite(n)) return lo;
  return Math.max(lo, Math.min(hi, n));
}

function escapeHTML(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatPercent(value, digits = 1) {
  const n = numOrNull(value);
  if (n == null) return "unavailable";
  return `${(n * 100).toFixed(digits)}%`;
}

function formatSignedPercent(value, digits = 1) {
  const n = numOrNull(value);
  if (n == null) return "unavailable";
  const pct = n * 100;
  return `${pct > 0 ? "+" : ""}${pct.toFixed(digits)}%`;
}

function formatNumber(value, digits = 2) {
  const n = numOrNull(value);
  if (n == null) return "unavailable";
  return n.toFixed(digits);
}

function fmtTime(value) {
  const n = numOrNull(value);
  if (n == null || n <= 0) return "time unavailable";
  try {
    return new Date(n).toLocaleString();
  } catch {
    return String(value);
  }
}

function setText(id, text) {
  const el = typeof document !== "undefined" ? document.getElementById(id) : null;
  if (el) el.textContent = String(text ?? "");
}

function setClass(id, className) {
  const el = typeof document !== "undefined" ? document.getElementById(id) : null;
  if (el) el.className = className;
}

function sortedByTime(rows) {
  return rows.slice().sort((a, b) => Number(a.ts_ms || 0) - Number(b.ts_ms || 0));
}

function drawEmpty(canvas, title, message) {
  if (!canvas) return;
  const ctx = canvas.getContext && canvas.getContext("2d");
  if (ctx) {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = "#9da7b1";
    ctx.font = "12px Consolas, monospace";
    ctx.fillText(String(message || "No data"), 12, 24);
  }
  renderChartAccessibility(canvas, {
    title,
    series: [],
    emptyMessage: message || "No chart data is available.",
    chartType: "canvas-line",
  });
  installChartPointInspector(canvas, [], {
    title,
    kind: "canvas-line",
    emptyMessage: message || "No chart data is available.",
  });
}

function setHidden(el, hidden) {
  if (!el) return;
  el.hidden = Boolean(hidden);
  if (el.style) el.style.display = hidden ? "none" : "";
}

function ensureUnavailableNode(canvas) {
  const doc = canvas && canvas.ownerDocument ? canvas.ownerDocument : null;
  if (!doc || !canvas || !canvas.parentNode) return null;
  const id = `${canvas.id || "chart"}Unavailable`;
  let node = doc.getElementById(id);
  if (!node && typeof doc.createElement === "function") {
    node = doc.createElement("div");
    node.id = id;
    node.className = "riskUnavailable";
    canvas.parentNode.insertBefore(node, canvas);
  }
  return node;
}

function renderCanvasUnavailable(canvas, title, message) {
  if (!canvas) return;
  const node = ensureUnavailableNode(canvas);
  if (node) {
    node.textContent = String(message || "No chart data is available.");
    setHidden(node, false);
  }
  setHidden(canvas, true);
  const ctx = canvas.getContext && canvas.getContext("2d");
  if (ctx) ctx.clearRect(0, 0, canvas.width, canvas.height);
  renderChartAccessibility(canvas, {
    title,
    series: [],
    emptyMessage: message || "No chart data is available.",
    chartType: "canvas-line",
  });
  installChartPointInspector(canvas, [], {
    title,
    kind: "canvas-line",
    emptyMessage: message || "No chart data is available.",
  });
}

function prepareCanvasForData(canvas) {
  if (!canvas) return;
  const node = ensureUnavailableNode(canvas);
  if (node) setHidden(node, true);
  setHidden(canvas, false);
}

function drawLegend(ctx, defs, x, y) {
  let offset = 0;
  ctx.font = "11px Consolas, monospace";
  for (const def of defs) {
    ctx.fillStyle = def.color;
    ctx.fillRect(x + offset, y - 7, 10, 3);
    ctx.fillStyle = "#9da7b1";
    ctx.fillText(def.label, x + offset + 14, y - 3);
    offset += Math.max(70, ctx.measureText(def.label).width + 28);
  }
}

function safeRange(values, fallbackMin = 0, fallbackMax = 1) {
  const nums = values.map(Number).filter(Number.isFinite);
  if (!nums.length) return [fallbackMin, fallbackMax];
  let min = Math.min(...nums);
  let max = Math.max(...nums);
  if (min === max) {
    min -= 1;
    max += 1;
  }
  const pad = (max - min) * 0.08;
  return [min - pad, max + pad];
}

export function normalizeRiskHistory(portfolioRisk = {}) {
  const rows = asArray(asObject(portfolioRisk).history)
    .map((row, index) => {
      const item = asObject(row);
      return {
        index: index + 1,
        ts_ms: numOrNull(item.ts_ms) || 0,
        gross: numOrNull(item.gross),
        net: numOrNull(item.net),
        drawdown: numOrNull(item.drawdown),
        vol_proxy: numOrNull(item.vol_proxy),
        blocked: item.blocked === true || item.blocked === 1 || item.blocked === "1",
      };
    })
    .filter((row) => row.ts_ms > 0 && (row.gross != null || row.net != null || row.drawdown != null));
  return sortedByTime(rows);
}

export function buildRiskHistoryViewModel(portfolioRisk = {}) {
  const root = asObject(portfolioRisk);
  const rows = normalizeRiskHistory(root);
  const latest = rows.length ? rows[rows.length - 1] : {};
  const blockedCount = rows.filter((row) => row.blocked).length;
  const unavailable = [];
  if (rows.length < 2) {
    unavailable.push({
      field: "history",
      reason: "portfolio_risk_snapshots has fewer than two timestamped rows",
    });
  }
  return {
    ok: root.ok !== false && rows.length >= 2,
    ready: rows.length >= 2,
    rows,
    pointCount: rows.length,
    latest,
    source: "/api/risk/portfolio.history",
    blockedCount,
    unavailable,
    summary: rows.length
      ? `Risk history uses ${rows.length} timestamped rows; latest gross ${formatPercent(latest.gross)}, net ${formatSignedPercent(latest.net)}, drawdown ${formatPercent(latest.drawdown)}; blocked context ${blockedCount} of ${rows.length} row${rows.length === 1 ? "" : "s"}.`
      : "Risk history is unavailable.",
  };
}

export function renderRiskHistoryChart(canvas, vm) {
  if (!canvas) return;
  const model = vm || buildRiskHistoryViewModel({});
  const rows = asArray(model.rows);
  if (rows.length < 2) {
    drawEmpty(canvas, "Portfolio risk history", "Risk history needs at least two timestamped rows.");
    return;
  }

  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const w = canvas.width;
  const h = canvas.height;
  const padL = 52;
  const padR = 14;
  const padT = 22;
  const padB = 30;
  const plotW = Math.max(10, w - padL - padR);
  const plotH = Math.max(10, h - padT - padB);
  const values = [];
  rows.forEach((row) => {
    for (const def of RISK_SERIES) {
      const value = numOrNull(row[def.key]);
      if (value != null) values.push(value);
    }
  });
  const [yMin, yMax] = safeRange(values, -0.1, 1.0);
  const xFor = (index) => padL + plotW * (index / Math.max(1, rows.length - 1));
  const yFor = (value) => padT + plotH * (1 - ((value - yMin) / Math.max(1e-9, yMax - yMin)));

  ctx.clearRect(0, 0, w, h);
  ctx.strokeStyle = "#30363d";
  ctx.lineWidth = 1;
  ctx.strokeRect(0.5, 0.5, w - 1, h - 1);

  rows.forEach((row, index) => {
    if (!row.blocked) return;
    const left = index === 0 ? padL : (xFor(index - 1) + xFor(index)) / 2;
    const right = index === rows.length - 1 ? w - padR : (xFor(index) + xFor(index + 1)) / 2;
    ctx.fillStyle = "rgba(213,94,0,0.16)";
    ctx.fillRect(left, padT, Math.max(1, right - left), plotH);
  });

  const zeroInRange = yMin <= 0 && yMax >= 0;
  const zeroY = zeroInRange ? yFor(0) : null;
  if (zeroInRange) {
    ctx.strokeStyle = "#20252c";
    ctx.beginPath();
    ctx.moveTo(padL, zeroY);
    ctx.lineTo(w - padR, zeroY);
    ctx.stroke();
  }

  ctx.fillStyle = "#9da7b1";
  ctx.font = "12px Consolas, monospace";
  ctx.fillText(formatPercent(yMax, 1), 8, padT + 6);
  if (zeroInRange) {
    ctx.fillText("0.0%", 8, clamp(zeroY + 4, padT + 10, padT + plotH - 4));
  }
  ctx.fillText(formatPercent(yMin, 1), 8, padT + plotH);
  ctx.fillText(fmtTime(rows[0].ts_ms), padL, h - 8);
  const lastLabel = fmtTime(rows[rows.length - 1].ts_ms);
  ctx.fillText(lastLabel, Math.max(padL, w - padR - ctx.measureText(lastLabel).width), h - 8);
  ctx.fillText("time", Math.max(padL, w - padR - 28), h - 20);
  ctx.fillText("exposure / drawdown", 8, padT + 20);

  for (const def of RISK_SERIES) {
    ctx.strokeStyle = def.color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    let started = false;
    rows.forEach((row, index) => {
      const value = numOrNull(row[def.key]);
      if (value == null) return;
      const x = xFor(index);
      const y = yFor(value);
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    if (started) ctx.stroke();
  }
  drawLegend(ctx, RISK_SERIES, padL, 16);

  renderChartAccessibility(canvas, {
    title: "Portfolio risk history",
    series: rows,
    seriesFields: RISK_SERIES,
    timeKey: "ts_ms",
    valueLabel: "gross exposure",
    valueFormatter: (value) => formatPercent(value, 2),
    summary: model.summary,
    chartType: "canvas-multi-line",
    columns: [
      { label: "Time", value: (row) => fmtTime(row.raw && row.raw.ts_ms) },
      { label: "Gross", value: (row) => formatPercent(row.raw && row.raw.gross, 2) },
      { label: "Net", value: (row) => formatSignedPercent(row.raw && row.raw.net, 2) },
      { label: "Drawdown", value: (row) => formatPercent(row.raw && row.raw.drawdown, 2) },
      { label: "Blocked", value: (row) => (row.raw && row.raw.blocked ? "yes" : "no") },
    ],
    maxRows: 120,
  });
  installChartPointInspector(
    canvas,
    rows.map((row, index) => ({
      label: fmtTime(row.ts_ms),
      x: xFor(index),
      values: RISK_SERIES.map((def) => ({
        label: def.label,
        value: numOrNull(row[def.key]),
        valueText: def.formatter(numOrNull(row[def.key]), 2),
      })),
      note: row.blocked ? "blocked context" : "within risk limits",
    })),
    {
      title: "Portfolio risk history",
      kind: "canvas-multi-line",
      emptyMessage: "Risk history needs at least two timestamped rows.",
    },
  );
}

function positiveLoss(value) {
  const n = numOrNull(value);
  if (n == null) return null;
  return Math.max(0, -n);
}

function drawdownValue(value) {
  const n = numOrNull(value);
  if (n == null) return null;
  return Math.max(0, n);
}

function normalizeFanRows(rawFan) {
  if (Array.isArray(rawFan)) {
    return rawFan
      .map((row, index) => {
        const item = asObject(row);
        return {
          step: numOrNull(item.step) ?? numOrNull(item.horizon) ?? index + 1,
          p05: numOrNull(item.p05) ?? numOrNull(item.p5) ?? numOrNull(item.q05),
          p50: numOrNull(item.p50) ?? numOrNull(item.median) ?? numOrNull(item.q50),
          p95: numOrNull(item.p95) ?? numOrNull(item.q95),
        };
      })
      .filter((row) => row.p50 != null || row.p05 != null || row.p95 != null);
  }
  const fan = asObject(rawFan);
  const p50 = asArray(fan.p50 || fan.median || fan.q50);
  const p05 = asArray(fan.p05 || fan.p5 || fan.q05);
  const p95 = asArray(fan.p95 || fan.q95);
  const n = Math.max(p50.length, p05.length, p95.length);
  return Array.from({ length: n }, (_unused, index) => ({
    step: index + 1,
    p05: numOrNull(p05[index]),
    p50: numOrNull(p50[index]),
    p95: numOrNull(p95[index]),
  })).filter((row) => row.p50 != null || row.p05 != null || row.p95 != null);
}

function normalizeDistribution(rawDistribution) {
  if (Array.isArray(rawDistribution)) {
    return rawDistribution
      .map((row, index) => {
        if (typeof row === "number") return { bucket: String(index + 1), value: Number(row), count: 1, probability: null };
        const item = asObject(row);
        const lower = numOrNull(item.lower ?? item.min);
        const upper = numOrNull(item.upper ?? item.max);
        const value = numOrNull(item.value ?? item.return ?? item.loss ?? item.midpoint);
        const count = numOrNull(item.count ?? item.n ?? 0) || 0;
        return {
          bucket: String(item.bucket ?? item.label ?? index + 1),
          value,
          lower,
          upper,
          count,
          probability: numOrNull(item.probability ?? item.prob ?? item.pct),
        };
      })
      .filter((row) => row.value != null || row.count > 0 || row.lower != null || row.upper != null);
  }
  const dist = asObject(rawDistribution);
  const bins = asArray(dist.bins || dist.rows);
  return normalizeDistribution(bins);
}

function monteCarloMode(hasFan, hasDistribution) {
  if (hasFan && hasDistribution) return "fan_distribution";
  if (hasFan) return "fan";
  if (hasDistribution) return "distribution";
  return "summary";
}

export function buildMonteCarloRiskViewModel(payload = {}) {
  const root = asObject(payload);
  const stress = asObject(root.stress);
  const ddPct = asObject(root.drawdown_percentiles);
  const stressDdPct = asObject(stress.drawdown_percentiles);
  const detail = asObject(root.chart_detail);
  const rawFan = root.fan || root.fan_chart || root.paths_percentiles;
  const rawDistribution = root.distribution;
  const fanRows = normalizeFanRows(rawFan)
    .filter((row) => row.p05 != null && row.p50 != null && row.p95 != null);
  const distributionRows = normalizeDistribution(rawDistribution);
  const hasFan = fanRows.length >= 2;
  const hasDistribution = distributionRows.length > 0;
  const latestFan = hasFan ? fanRows[fanRows.length - 1] : {};

  const bars = [
    { key: "var_95", label: "VaR 95", value: positiveLoss(root.var_95), source: "base" },
    { key: "var_99", label: "VaR 99", value: positiveLoss(root.var_99), source: "base" },
    { key: "cvar_95", label: "CVaR 95", value: positiveLoss(root.cvar_95), source: "base" },
    { key: "cvar_99", label: "CVaR 99", value: positiveLoss(root.cvar_99), source: "base" },
    { key: "drawdown_p95", label: "Drawdown P95", value: drawdownValue(ddPct.p95), source: "base" },
    { key: "drawdown_worst", label: "Worst Drawdown", value: drawdownValue(root.worst_simulated_drawdown), source: "base" },
    { key: "stress_var_95", label: "Stress VaR 95", value: positiveLoss(stress.var_95), source: "stress" },
    { key: "stress_cvar_95", label: "Stress CVaR 95", value: positiveLoss(stress.cvar_95), source: "stress" },
    { key: "stress_drawdown_p95", label: "Stress DD P95", value: drawdownValue(stressDdPct.p95), source: "stress" },
  ].filter((row) => row.value != null);
  const maxValue = Math.max(0.001, ...bars.map((row) => row.value || 0));
  const unavailable = asArray(detail.unavailable).map((item) => ({
    field: String(asObject(item).field || "detail"),
    reason: String(asObject(item).reason || "detail unavailable"),
  }));
  if (!hasFan) {
    unavailable.push({
      field: "fan_chart",
      reason: "Fan chart input unavailable: no simulated path fan percentiles were returned by /api/risk/monte_carlo.",
    });
  }
  if (!hasDistribution) {
    unavailable.push({
      field: "distribution",
      reason: "No simulated return distribution buckets were returned by /api/risk/monte_carlo.",
    });
  }

  return {
    ok: root.ok !== false,
    ready: root.ready === true,
    pending: root.pending === true,
    status: String(root.status || "unknown"),
    ts_ms: numOrNull(root.ts_ms) || 0,
    simulations: numOrNull(root.simulations),
    horizon: numOrNull(root.horizon),
    bars: bars.map((row) => ({ ...row, fillPct: clamp((row.value / maxValue) * 100, 0, 100) })),
    maxValue,
    mode: monteCarloMode(hasFan, hasDistribution),
    fanRows,
    distributionRows,
    hasFan,
    hasDistribution,
    unavailable,
    fanSummary: hasFan
      ? `Monte-Carlo fan: latest p05 ${formatSignedPercent(latestFan.p05, 2)}, p50 ${formatSignedPercent(latestFan.p50, 2)}, p95 ${formatSignedPercent(latestFan.p95, 2)} across ${fanRows.length} horizon steps; shaded band spans p05 to p95 around the p50 path.`
      : "Monte-Carlo fan is unavailable.",
    summary: bars.length
      ? `Monte-Carlo risk shows ${bars.length} summary tail metrics; fan data ${hasFan ? `available with latest p05 ${formatSignedPercent(latestFan.p05, 2)}, p50 ${formatSignedPercent(latestFan.p50, 2)}, p95 ${formatSignedPercent(latestFan.p95, 2)}` : "unavailable"}; distribution data ${hasDistribution ? "available" : "unavailable"}.`
      : "Monte-Carlo risk summary is unavailable.",
  };
}

function renderMonteCarloBars(mount, vm) {
  if (!mount) return;
  const model = vm || buildMonteCarloRiskViewModel({});
  if (!model.bars.length) {
    mount.innerHTML = `<div class="riskUnavailable">Monte-Carlo summary unavailable.</div>`;
    return;
  }
  mount.innerHTML = `
    <div class="mcBars" role="list" aria-label="${escapeHTML(model.summary)}">
      ${model.bars.map((bar) => `
        <div class="mcBar mcBar-${escapeHTML(bar.source)}" role="listitem" aria-label="${escapeHTML(`${bar.label}: ${formatPercent(bar.value, 2)}`)}">
          <div class="mcBarHeader">
            <span>${escapeHTML(bar.label)}</span>
            <span class="mono">${escapeHTML(formatPercent(bar.value, 2))}</span>
          </div>
          <div class="mcBarTrack"><span class="mcBarFill" style="width:${bar.fillPct.toFixed(2)}%"></span></div>
        </div>
      `).join("")}
    </div>
  `;
}

export function renderMonteCarloFanChart(canvas, vm) {
  if (!canvas) return;
  const model = vm || buildMonteCarloRiskViewModel({});
  if (!model.hasFan || model.fanRows.length < 2) {
    renderCanvasUnavailable(canvas, "Monte-Carlo fan", "Fan chart unavailable: no simulated path percentile rows were returned.");
    return;
  }
  prepareCanvasForData(canvas);
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const rows = model.fanRows;
  const values = [];
  rows.forEach((row) => {
    for (const key of ["p05", "p50", "p95"]) {
      const value = numOrNull(row[key]);
      if (value != null) values.push(value);
    }
  });
  const [yMin, yMax] = safeRange(values, -0.1, 0.1);
  const w = canvas.width;
  const h = canvas.height;
  const padL = 48;
  const padR = 14;
  const padT = 16;
  const padB = 24;
  const plotW = Math.max(10, w - padL - padR);
  const plotH = Math.max(10, h - padT - padB);
  const xFor = (index) => padL + plotW * (index / Math.max(1, rows.length - 1));
  const yFor = (value) => padT + plotH * (1 - ((value - yMin) / Math.max(1e-9, yMax - yMin)));

  ctx.clearRect(0, 0, w, h);
  ctx.strokeStyle = "#30363d";
  ctx.strokeRect(0.5, 0.5, w - 1, h - 1);
  ctx.beginPath();
  rows.forEach((row, index) => {
    const value = numOrNull(row.p95);
    if (value == null) return;
    if (index === 0) ctx.moveTo(xFor(index), yFor(value));
    else ctx.lineTo(xFor(index), yFor(value));
  });
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    const value = numOrNull(rows[index].p05);
    if (value == null) continue;
    ctx.lineTo(xFor(index), yFor(value));
  }
  ctx.closePath();
  ctx.fillStyle = "rgba(86,180,233,0.18)";
  ctx.fill();
  ctx.strokeStyle = "#56B4E9";
  ctx.lineWidth = 2;
  ctx.beginPath();
  rows.forEach((row, index) => {
    const value = numOrNull(row.p50);
    if (value == null) return;
    if (index === 0) ctx.moveTo(xFor(index), yFor(value));
    else ctx.lineTo(xFor(index), yFor(value));
  });
  ctx.stroke();
  if (typeof ctx.fillRect === "function") {
    ctx.fillStyle = "rgba(86,180,233,0.18)";
    ctx.fillRect(padL, 8, 14, 7);
  }
  ctx.fillStyle = "#9da7b1";
  ctx.font = "11px Consolas, monospace";
  ctx.fillText("P05-P95 band", padL + 18, 15);
  if (typeof ctx.fillRect === "function") {
    ctx.fillStyle = "#56B4E9";
    ctx.fillRect(padL + 118, 12, 14, 3);
  }
  ctx.fillStyle = "#9da7b1";
  ctx.fillText("P50 median", padL + 136, 15);
  ctx.fillText("step", Math.max(padL, w - padR - 28), h - 7);

  renderChartAccessibility(canvas, {
    title: "Monte-Carlo fan",
    series: rows,
    seriesFields: [
      { key: "p05", label: "P05", formatter: (value) => formatSignedPercent(value, 2) },
      { key: "p50", label: "P50", formatter: (value) => formatSignedPercent(value, 2) },
      { key: "p95", label: "P95", formatter: (value) => formatSignedPercent(value, 2) },
    ],
    labelKey: "step",
    valueLabel: "median simulated return",
    valueFormatter: (value) => formatSignedPercent(value, 2),
    summary: model.fanSummary,
    chartType: "canvas-fan",
    columns: [
      { label: "Step", value: (row) => row.raw && row.raw.step },
      { label: "P05", value: (row) => formatSignedPercent(row.raw && row.raw.p05, 2) },
      { label: "P50", value: (row) => formatSignedPercent(row.raw && row.raw.p50, 2) },
      { label: "P95", value: (row) => formatSignedPercent(row.raw && row.raw.p95, 2) },
    ],
  });
  installChartPointInspector(
    canvas,
    rows.map((row, index) => ({
      label: `Step ${row.step ?? index + 1}`,
      x: xFor(index),
      values: [
        { label: "P05", value: numOrNull(row.p05), valueText: formatSignedPercent(row.p05, 2) },
        { label: "P50", value: numOrNull(row.p50), valueText: formatSignedPercent(row.p50, 2) },
        { label: "P95", value: numOrNull(row.p95), valueText: formatSignedPercent(row.p95, 2) },
      ],
      note: "shaded band spans p05 to p95",
    })),
    {
      title: "Monte-Carlo fan",
      kind: "canvas-fan",
      emptyMessage: "Fan chart unavailable: no simulated path percentile rows were returned.",
    },
  );
}

export function renderMonteCarloDistributionChart(canvas, vm) {
  if (!canvas) return;
  const model = vm || buildMonteCarloRiskViewModel({});
  if (!model.hasDistribution || !model.distributionRows.length) {
    renderCanvasUnavailable(canvas, "Monte-Carlo return distribution", "Return distribution unavailable: no simulated final-return buckets were returned.");
    return;
  }
  prepareCanvasForData(canvas);
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const rows = model.distributionRows;
  const counts = rows.map((row) => Math.max(0, numOrNull(row.count) || 0));
  const values = rows.map((row) => numOrNull(row.value)).filter((value) => value != null);
  const maxCount = Math.max(1, ...counts);
  const [xMin, xMax] = safeRange(values.length ? values : [0], -0.1, 0.1);
  const w = canvas.width;
  const h = canvas.height;
  const padL = 48;
  const padR = 14;
  const padT = 16;
  const padB = 28;
  const plotW = Math.max(10, w - padL - padR);
  const plotH = Math.max(10, h - padT - padB);
  const barGap = 2;
  const barW = Math.max(2, (plotW / Math.max(1, rows.length)) - barGap);

  ctx.clearRect(0, 0, w, h);
  ctx.strokeStyle = "#30363d";
  ctx.strokeRect(0.5, 0.5, w - 1, h - 1);
  ctx.strokeStyle = "rgba(157,167,177,0.35)";
  ctx.beginPath();
  ctx.moveTo(padL, padT + plotH);
  ctx.lineTo(padL + plotW, padT + plotH);
  ctx.stroke();

  rows.forEach((row, index) => {
    const count = counts[index] || 0;
    const x = padL + index * (plotW / Math.max(1, rows.length)) + (barGap / 2);
    const barH = plotH * (count / maxCount);
    const y = padT + plotH - barH;
    const value = numOrNull(row.value);
    ctx.fillStyle = value != null && value < 0 ? "rgba(230,159,0,0.74)" : "rgba(86,180,233,0.72)";
    ctx.fillRect(x, y, barW, barH);
  });

  ctx.fillStyle = "#9da7b1";
  ctx.font = "11px Consolas, monospace";
  ctx.fillText(String(maxCount), 8, padT + 8);
  ctx.fillText(formatSignedPercent(xMin, 1), padL, h - 8);
  ctx.fillText(formatSignedPercent(xMax, 1), Math.max(padL + 80, w - padR - 82), h - 8);
  ctx.fillText("path count", 8, padT + 22);
  ctx.fillText("final return", Math.max(padL, w - padR - 70), h - 20);

  renderChartAccessibility(canvas, {
    title: "Monte-Carlo return distribution",
    series: rows.map((row) => ({ ...row, label: row.bucket, value: row.count })),
    valueKey: "value",
    labelKey: "label",
    valueLabel: "simulated path count",
    valueFormatter: (value) => formatNumber(value, 0),
    chartType: "canvas-histogram",
    columns: [
      { label: "Bucket", value: (row) => row.raw && row.raw.bucket },
      { label: "Return", value: (row) => formatSignedPercent(row.raw && row.raw.value, 2) },
      { label: "Count", value: (row) => formatNumber(row.raw && row.raw.count, 0) },
      { label: "Probability", value: (row) => formatPercent(row.raw && row.raw.probability, 2) },
    ],
    maxRows: 80,
  });
  installChartPointInspector(
    canvas,
    rows.map((row, index) => {
      const x = padL + index * (plotW / Math.max(1, rows.length)) + (barGap / 2) + (barW / 2);
      return {
        label: row.bucket || formatSignedPercent(row.value, 1),
        x,
        values: [
          { label: "Return", value: numOrNull(row.value), valueText: formatSignedPercent(row.value, 2) },
          { label: "Count", value: numOrNull(row.count), valueText: formatNumber(row.count, 0) },
          { label: "Probability", value: numOrNull(row.probability), valueText: formatPercent(row.probability, 2) },
        ],
      };
    }),
    {
      title: "Monte-Carlo return distribution",
      kind: "canvas-histogram",
      emptyMessage: "Return distribution unavailable: no simulated final-return buckets were returned.",
    },
  );
}

function severityRank(value) {
  return SEVERITY_RANK[String(value || "").toLowerCase()] || 0;
}

function groupStrategyHistory(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const strategy = String(row.strategy || "unknown") || "unknown";
    if (!grouped.has(strategy)) grouped.set(strategy, []);
    grouped.get(strategy).push(row);
  }
  return Array.from(grouped.entries()).map(([strategy, items]) => ({
    strategy,
    rows: sortedByTime(items),
    latest: sortedByTime(items).slice(-1)[0] || {},
  }));
}

function finiteValueCount(rows, key) {
  return asArray(rows).reduce((count, row) => count + (numOrNull(row && row[key]) != null ? 1 : 0), 0);
}

function alphaStrategyRank(group) {
  const latest = asObject(group && group.latest);
  const chartablePoints = finiteValueCount(group && group.rows, "rolling_sharpe");
  const severity = severityRank(latest.severity);
  const severityScore = numOrNull(latest.severity_score) || 0;
  const throttle = numOrNull(latest.throttle_mult);
  const throttlePressure = throttle == null ? 0 : 1 - clamp(throttle, 0, 1);
  const structuralBreak = Math.abs(numOrNull(latest.structural_break_z) || 0);
  const rollingSharpe = numOrNull(latest.rolling_sharpe);
  const weakSharpe = rollingSharpe == null ? 0 : Math.max(0, -rollingSharpe);
  return {
    chartable: chartablePoints >= 2 ? 1 : 0,
    chartablePoints,
    severity,
    severityScore,
    throttlePressure,
    structuralBreak,
    weakSharpe,
    latestTsMs: Number(latest.ts_ms || 0),
  };
}

function compareAlphaStrategies(a, b) {
  const ar = alphaStrategyRank(a);
  const br = alphaStrategyRank(b);
  const fields = [
    "chartable",
    "severity",
    "severityScore",
    "throttlePressure",
    "structuralBreak",
    "weakSharpe",
    "latestTsMs",
    "chartablePoints",
  ];
  for (const field of fields) {
    const delta = Number(br[field] || 0) - Number(ar[field] || 0);
    if (delta) return delta;
  }
  return String(a.strategy || "").localeCompare(String(b.strategy || ""));
}

function alphaStrategyLabel(option) {
  const strategy = String(option.strategy || "unknown");
  const severity = String(option.latestSeverity || "unknown").toUpperCase();
  const points = Number(option.points || 0);
  return `${strategy} (${severity}, ${points} pt${points === 1 ? "" : "s"})`;
}

export function buildAlphaDecayViewModel(payload = {}, options = {}) {
  const root = asObject(payload);
  const opts = asObject(options);
  const requestedStrategy = String(opts.selectedStrategy || root.selected_strategy || "").trim();
  const history = asArray(root.strategy_history)
    .map((row) => {
      const item = asObject(row);
      return {
        strategy: String(item.strategy || item.strategy_name || "unknown"),
        ts_ms: numOrNull(item.ts_ms) || 0,
        rolling_sharpe: numOrNull(item.rolling_sharpe),
        half_life_buckets: numOrNull(item.half_life_buckets),
        half_life_seconds: numOrNull(item.half_life_seconds),
        structural_break_z: numOrNull(item.structural_break_z),
        severity: String(item.severity || "ok").toLowerCase(),
        severity_score: numOrNull(item.severity_score) || 0,
        throttle_mult: numOrNull(item.throttle_mult),
        n_obs: numOrNull(item.n_obs) || 0,
      };
    })
    .filter((row) => row.ts_ms > 0);
  const groups = groupStrategyHistory(history);
  groups.sort(compareAlphaStrategies);
  const requested = requestedStrategy
    ? groups.find((group) => group.strategy === requestedStrategy)
    : null;
  const selected = requested || groups.find((group) => alphaStrategyRank(group).chartable) || groups[0] || { strategy: "", rows: [], latest: {} };
  const selectedChartableCount = finiteValueCount(selected.rows, "rolling_sharpe");
  const unavailable = asArray(root.unavailable).map((item) => ({
    field: String(asObject(item).field || "alpha_decay"),
    reason: String(asObject(item).reason || "alpha-decay data unavailable"),
  }));
  if (selectedChartableCount < 2) {
    unavailable.push({
      field: "strategy_history",
      reason: "Rolling-Sharpe and half-life chart needs at least two rows for one strategy.",
    });
  }
  const latest = selected.latest || {};
  return {
    ok: root.ok !== false && selectedChartableCount >= 2,
    ready: selectedChartableCount >= 2,
    status: String(asObject(root.runtime).status || latest.severity || "unknown"),
    selectedStrategy: selected.strategy,
    rows: selected.rows,
    strategies: groups.map((group) => ({
      ...alphaStrategyRank(group),
      strategy: group.strategy,
      points: finiteValueCount(group.rows, "rolling_sharpe"),
      latestSeverity: String(group.latest.severity || "unknown"),
      latestSeverityScore: numOrNull(group.latest.severity_score) || 0,
      latestTsMs: group.latest.ts_ms || 0,
      selected: group.strategy === selected.strategy,
    })),
    latest,
    unavailable,
    summary: selectedChartableCount
      ? `Alpha decay for ${selected.strategy}: ${selectedChartableCount} points; latest rolling Sharpe ${latest.rolling_sharpe == null ? "unavailable" : formatNumber(latest.rolling_sharpe, 2)}, half-life ${latest.half_life_buckets == null ? "unavailable" : formatNumber(latest.half_life_buckets, 1)} buckets.`
      : "Alpha-decay history is unavailable.",
  };
}

export function renderAlphaDecayStrategySelector(select, payload = {}, vm = null, onSelect = null) {
  if (!select) return;
  const model = vm || buildAlphaDecayViewModel(payload);
  const options = asArray(model.strategies);
  setHidden(select, options.length === 0);
  select.disabled = options.length < 2;
  select.innerHTML = options.map((option) => {
    const selected = option.strategy === model.selectedStrategy ? " selected" : "";
    const disabled = option.chartable ? "" : " data-unchartable=\"true\"";
    return `<option value="${escapeHTML(option.strategy)}"${selected}${disabled}>${escapeHTML(alphaStrategyLabel(option))}</option>`;
  }).join("");
  select.value = String(model.selectedStrategy || "");
  select.onchange = () => {
    if (typeof onSelect === "function") onSelect(String(select.value || ""));
  };
}

export function renderAlphaDecayChart(canvas, vm) {
  if (!canvas) return;
  const model = vm || buildAlphaDecayViewModel({});
  const rows = asArray(model.rows);
  if (finiteValueCount(rows, "rolling_sharpe") < 2) {
    drawEmpty(canvas, "Alpha-decay rolling Sharpe and half-life", "Alpha-decay history needs at least two points for one strategy.");
    return;
  }
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  const w = canvas.width;
  const h = canvas.height;
  const padL = 52;
  const padR = 14;
  const padT = 18;
  const padB = 28;
  const gap = 12;
  const paneH = Math.max(40, (h - padT - padB - gap) / 2);
  const plotW = Math.max(10, w - padL - padR);
  const sharpeVals = rows.map((row) => row.rolling_sharpe).filter(Number.isFinite);
  const halfVals = rows.map((row) => row.half_life_buckets).filter(Number.isFinite);
  const [shMin, shMax] = safeRange(sharpeVals, -1, 1);
  const [hlMin, hlMax] = safeRange(halfVals.length ? halfVals : [0, 1], 0, 10);
  const xFor = (index) => padL + plotW * (index / Math.max(1, rows.length - 1));
  const ySharpe = (value) => padT + paneH * (1 - ((value - shMin) / Math.max(1e-9, shMax - shMin)));
  const yHalf = (value) => padT + paneH + gap + paneH * (1 - ((value - hlMin) / Math.max(1e-9, hlMax - hlMin)));
  const strokeSegmented = (valueKey, yFor) => {
    let started = false;
    rows.forEach((row, index) => {
      const value = numOrNull(row && row[valueKey]);
      if (value == null) {
        started = false;
        return;
      }
      const x = xFor(index);
      const y = yFor(value);
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
  };

  ctx.clearRect(0, 0, w, h);
  ctx.strokeStyle = "#30363d";
  ctx.strokeRect(0.5, 0.5, w - 1, h - 1);
  ctx.fillStyle = "#9da7b1";
  ctx.font = "12px Consolas, monospace";
  ctx.fillText("rolling Sharpe", 8, padT + 10);
  ctx.fillText("half-life buckets", 8, padT + paneH + gap + 10);

  ctx.strokeStyle = "#56B4E9";
  ctx.lineWidth = 2;
  ctx.beginPath();
  strokeSegmented("rolling_sharpe", ySharpe);
  ctx.stroke();

  ctx.strokeStyle = "#E69F00";
  ctx.beginPath();
  strokeSegmented("half_life_buckets", yHalf);
  ctx.stroke();
  drawLegend(ctx, [
    { label: "Sharpe", color: "#56B4E9" },
    { label: "Half-life", color: "#E69F00" },
  ], padL, 15);

  renderChartAccessibility(canvas, {
    title: "Alpha-decay rolling Sharpe and half-life",
    series: rows,
    seriesFields: [
      { key: "rolling_sharpe", label: "Rolling Sharpe", formatter: (value) => formatNumber(value, 3) },
      { key: "half_life_buckets", label: "Half-life Buckets", formatter: (value) => formatNumber(value, 2) },
    ],
    timeKey: "ts_ms",
    valueLabel: "rolling Sharpe",
    valueFormatter: (value) => formatNumber(value, 3),
    summary: model.summary,
    chartType: "canvas-two-pane-line",
    columns: [
      { label: "Time", value: (row) => fmtTime(row.raw && row.raw.ts_ms) },
      { label: "Strategy", value: (row) => row.raw && row.raw.strategy },
      { label: "Rolling Sharpe", value: (row) => formatNumber(row.raw && row.raw.rolling_sharpe, 3) },
      { label: "Half-life Buckets", value: (row) => row.raw && row.raw.half_life_buckets == null ? "unavailable" : formatNumber(row.raw && row.raw.half_life_buckets, 2) },
      { label: "Severity", value: (row) => row.raw && row.raw.severity },
    ],
    maxRows: 120,
  });
  installChartPointInspector(
    canvas,
    rows.map((row, index) => ({
      label: fmtTime(row.ts_ms),
      x: xFor(index),
      values: [
        { label: "Rolling Sharpe", value: numOrNull(row.rolling_sharpe), valueText: formatNumber(row.rolling_sharpe, 3) },
        { label: "Half-life Buckets", value: numOrNull(row.half_life_buckets), valueText: row.half_life_buckets == null ? "unavailable" : formatNumber(row.half_life_buckets, 2) },
      ],
      note: row.severity ? `severity ${row.severity}` : "",
    })),
    {
      title: "Alpha-decay rolling Sharpe and half-life",
      kind: "canvas-two-pane-line",
      emptyMessage: "Alpha-decay history needs at least two points for one strategy.",
    },
  );
}

function layerLabel(row, layer) {
  return String(asObject(asObject(row).layers[layer]).label || "UNKNOWN").toUpperCase();
}

function layerConfidence(row, layer) {
  return numOrNull(asObject(asObject(row).layers[layer]).confidence);
}

function regimeTone(label) {
  const raw = String(label || "UNKNOWN").toUpperCase();
  if (["UNKNOWN", "UNAVAILABLE", "MISSING"].includes(raw)) return "unavailable";
  if (["RISK_OFF", "VOL_EXPANSION", "CREDIT_STRESS", "THIN", "SHIFT", "BREAK", "CRITICAL"].includes(raw)) return "warn";
  if (["RISK_ON", "CALM", "NORMAL", "STABLE", "AMPLE"].includes(raw)) return "ok";
  return "info";
}

export function buildRegimeHistoryViewModel(payload = {}) {
  const root = asObject(payload);
  const rawRows = asArray(root.rows);
  const rows = sortedByTime(rawRows.map((row) => {
    const item = asObject(row);
    return {
      ts_ms: numOrNull(item.ts_ms) || 0,
      source_symbol: String(item.source_symbol || root.symbol || "SPY").toUpperCase(),
      macro: layerLabel(item, "macro"),
      macro_confidence: layerConfidence(item, "macro"),
      asset: layerLabel(item, "asset"),
      asset_confidence: layerConfidence(item, "asset"),
      micro: layerLabel(item, "micro"),
      micro_confidence: layerConfidence(item, "micro"),
    };
  }).filter((row) => row.ts_ms > 0));
  const current = asObject(root.current);
  if (!rows.length && current.layers) {
    rows.push({
      ts_ms: numOrNull(current.ts_ms) || 0,
      source_symbol: String(current.symbol || root.symbol || "SPY").toUpperCase(),
      macro: layerLabel(current, "macro"),
      macro_confidence: layerConfidence(current, "macro"),
      asset: layerLabel(current, "asset"),
      asset_confidence: layerConfidence(current, "asset"),
      micro: layerLabel(current, "micro"),
      micro_confidence: layerConfidence(current, "micro"),
      currentOnly: true,
    });
  }
  const unavailable = asArray(root.unavailable).map((item) => ({
    field: String(asObject(item).field || "regime_history"),
    reason: String(asObject(item).reason || "regime history unavailable"),
  }));
  if (rows.length < 2) {
    unavailable.push({
      field: "rows",
      reason: "Regime history has fewer than two timestamped rows.",
    });
  }
  const latest = rows[rows.length - 1] || {};
  return {
    ok: root.ok !== false && rows.length >= 2,
    ready: rows.length >= 2,
    symbol: String(root.symbol || latest.source_symbol || "SPY").toUpperCase(),
    rows,
    latest,
    unavailable,
    summary: rows.length
      ? `Regime history has ${rows.length} point${rows.length === 1 ? "" : "s"}; latest macro ${latest.macro}, asset ${latest.asset}, micro ${latest.micro}.`
      : "Regime history is unavailable.",
  };
}

function renderRegimeLayer(row, layer) {
  const label = row[layer];
  const confidence = row[`${layer}_confidence`];
  const tone = regimeTone(label);
  return `
    <span class="regimeTimeSegment regimeTime-${escapeHTML(tone)}" title="${escapeHTML(`${fmtTime(row.ts_ms)} ${layer}: ${label}`)}">
      <span>${escapeHTML(label)}</span>
      <small>${confidence == null ? "conf -" : confidence.toFixed(2)}</small>
    </span>
  `;
}

export function renderRegimeHistoryRibbon(mount, vm) {
  if (!mount) return;
  const model = vm || buildRegimeHistoryViewModel({});
  const rows = asArray(model.rows);
  if (!rows.length) {
    mount.innerHTML = `<div class="riskUnavailable">Regime history unavailable.</div>`;
    return;
  }
  const visible = rows.slice(-36);
  mount.innerHTML = `
    <div class="regimeTimeline" role="group" aria-label="${escapeHTML(model.summary)}">
      ${["macro", "asset", "micro"].map((layer) => `
        <div class="regimeTimelineRow">
          <div class="regimeTimelineLabel">${escapeHTML(layer.toUpperCase())}</div>
          <div class="regimeTimelineSegments">
            ${visible.map((row) => renderRegimeLayer(row, layer)).join("")}
          </div>
        </div>
      `).join("")}
      <div class="sr-only">${escapeHTML(model.summary)}</div>
    </div>
  `;
}

function renderUnavailableList(mount, rows) {
  if (!mount) return;
  const items = asArray(rows);
  if (!items.length) {
    mount.innerHTML = '<div class="opsNote">All requested chart inputs are available.</div>';
    return;
  }
  mount.innerHTML = items.map((item) => `
    <div class="opsNote"><strong>${escapeHTML(item.field || "input")}:</strong> ${escapeHTML(item.reason || "unavailable")}</div>
  `).join("");
}

export async function loadRiskChartViews({
  fetchJSON,
  portfolioRisk = null,
  symbol = "SPY",
} = {}) {
  const root = typeof document !== "undefined" ? document.getElementById("positionsRiskCharts") : null;
  if (!root || typeof fetchJSON !== "function") return null;

  const riskPromise = portfolioRisk
    ? Promise.resolve(portfolioRisk)
    : fetchJSON("/api/risk/portfolio", { allowBusinessFalse: true });

  const [riskRes, mcRes, alphaRes, regimeRes] = await Promise.allSettled([
    riskPromise,
    fetchJSON("/api/risk/monte_carlo", { allowBusinessFalse: true }),
    fetchJSON("/api/alpha_decay?limit=200", { allowBusinessFalse: true }),
    fetchJSON(`/api/regime/history?symbol=${encodeURIComponent(symbol || "SPY")}&limit=120`, { allowBusinessFalse: true }),
  ]);

  const riskVm = buildRiskHistoryViewModel(riskRes.status === "fulfilled" ? riskRes.value : {});
  const mcVm = buildMonteCarloRiskViewModel(mcRes.status === "fulfilled" ? mcRes.value : { ok: false, status: "unavailable" });
  const alphaPayload = alphaRes.status === "fulfilled" ? alphaRes.value : { ok: false };
  const alphaSelector = document.getElementById("alphaDecayStrategySelect");
  let alphaVm = buildAlphaDecayViewModel(alphaPayload, { selectedStrategy: alphaSelector && alphaSelector.value });
  const regimeVm = buildRegimeHistoryViewModel(regimeRes.status === "fulfilled" ? regimeRes.value : { ok: false });

  const applyAlphaDecaySelection = (selectedStrategy = "") => {
    alphaVm = buildAlphaDecayViewModel(alphaPayload, { selectedStrategy });
    renderAlphaDecayChart(document.getElementById("alphaDecayChart"), alphaVm);
    renderAlphaDecayStrategySelector(alphaSelector, alphaPayload, alphaVm, applyAlphaDecaySelection);
    setText("alphaDecayMeta", alphaVm.ready ? alphaVm.selectedStrategy : "unavailable");
    setClass("alphaDecayMeta", alphaVm.ready ? "pill ok meta-pill-offset" : "pill unavailable meta-pill-offset");
  };

  renderRiskHistoryChart(document.getElementById("riskHistoryChart"), riskVm);
  renderMonteCarloBars(document.getElementById("monteCarloRiskBars"), mcVm);
  renderMonteCarloFanChart(document.getElementById("monteCarloFanChart"), mcVm);
  renderMonteCarloDistributionChart(document.getElementById("monteCarloDistributionChart"), mcVm);
  applyAlphaDecaySelection(alphaVm.selectedStrategy);
  renderRegimeHistoryRibbon(document.getElementById("regimeHistoryRibbon"), regimeVm);

  setText("riskHistoryMeta", riskVm.ready ? `${riskVm.pointCount} points` : "unavailable");
  setClass("riskHistoryMeta", riskVm.ready ? "pill ok meta-pill-offset" : "pill unavailable meta-pill-offset");
  setText("monteCarloRiskMeta", mcVm.ready ? mcVm.mode : "not ready");
  setClass("monteCarloRiskMeta", mcVm.ready ? "pill ok meta-pill-offset" : "pill warn meta-pill-offset");
  setText("regimeHistoryMeta", regimeVm.ready ? `${regimeVm.rows.length} points` : "current only");
  setClass("regimeHistoryMeta", regimeVm.ready ? "pill ok meta-pill-offset" : "pill warn meta-pill-offset");

  renderUnavailableList(document.getElementById("riskChartNotes"), [
    ...riskVm.unavailable,
    ...mcVm.unavailable,
    ...alphaVm.unavailable,
    ...regimeVm.unavailable,
  ]);

  const out = { risk: riskVm, monteCarlo: mcVm, alphaDecay: alphaVm, regime: regimeVm };
  if (typeof window !== "undefined") window.__LAST_RISK_CHARTS__ = out;
  return out;
}
