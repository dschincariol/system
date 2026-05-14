"use strict";

/*
  ui/charts.js — shared lightweight chart engine
  Extracted from ui/dashboard.js (Phase 3)
*/

// -----------------------------
// Small utilities
// -----------------------------
function _clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

// -----------------------------
// Sparkline (tiny canvas)
// -----------------------------
export function drawSpark(canvas, values) {
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const w = canvas.width = 86;
  const h = canvas.height = 18;
  ctx.clearRect(0, 0, w, h);

  const arr = (values || []).map(Number).filter(Number.isFinite);
  if (arr.length < 2) return;

  let mn = Math.min(...arr);
  let mx = Math.max(...arr);
  if (mn === mx) { mn -= 1; mx += 1; }

  const pad = 2;
  const sx = (i) => pad + (i * (w - pad * 2) / (arr.length - 1));
  const sy = (v) => h - pad - ((v - mn) * (h - pad * 2) / (mx - mn));

  // baseline
  ctx.globalAlpha = 0.35;
  ctx.beginPath();
  ctx.moveTo(pad, sy(0));
  ctx.lineTo(w - pad, sy(0));
  ctx.stroke();
  ctx.globalAlpha = 1.0;

  // line
  ctx.beginPath();
  for (let i = 0; i < arr.length; i++) {
    const x = sx(i);
    const y = sy(arr[i]);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();
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

  const padL = 44;
  const padR = 10;
  const padT = 12;
  const padB = 20;

  ctx.clearRect(0, 0, w, h);

  // frame
  ctx.strokeStyle = "#30363d";
  ctx.lineWidth = 1;
  ctx.strokeRect(0.5, 0.5, w - 1, h - 1);

  if (!Array.isArray(ys) || ys.length < 2) {
    ctx.fillStyle = "#9da7b1";
    ctx.font = "12px Consolas, monospace";
    ctx.fillText("(no data)", 12, 24);
    return;
  }

  const vals = ys.map(Number).filter(Number.isFinite);
  if (vals.length < 2) {
    ctx.fillStyle = "#9da7b1";
    ctx.font = "12px Consolas, monospace";
    ctx.fillText("(no numeric data)", 12, 24);
    return;
  }

  let yMin = Number.isFinite(opts.yMin) ? opts.yMin : Math.min(...vals);
  let yMax = Number.isFinite(opts.yMax) ? opts.yMax : Math.max(...vals);
  if (yMin === yMax) { yMin -= 1; yMax += 1; }

  const yPad = (yMax - yMin) * 0.08;
  yMin -= yPad;
  yMax += yPad;

  // labels
  ctx.fillStyle = "#9da7b1";
  ctx.font = "12px Consolas, monospace";
  if (opts.topLabel) ctx.fillText(opts.topLabel, 8, padT + 10);
  if (opts.bottomLabel) ctx.fillText(opts.bottomLabel, 8, h - 8);

  const fmtY = opts.fmtY || ((v) => v.toFixed(3));
  ctx.fillText(fmtY(yMax), 8, padT + 22);
  ctx.fillText(fmtY(yMin), 8, h - padB - 4);

  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  const xFor = (i) =>
    padL + plotW * (i / Math.max(1, vals.length - 1));

  const yFor = (v) => {
    const t = (v - yMin) / (yMax - yMin);
    return padT + plotH * (1 - _clamp(t, 0, 1));
  };

  // mid gridline
  ctx.strokeStyle = "#20252c";
  ctx.beginPath();
  ctx.moveTo(padL, padT + plotH / 2);
  ctx.lineTo(w - padR, padT + plotH / 2);
  ctx.stroke();

  // line
  ctx.strokeStyle = opts.stroke || "#2ea043";
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(xFor(0), yFor(vals[0]));
  for (let i = 1; i < vals.length; i++) {
    ctx.lineTo(xFor(i), yFor(vals[i]));
  }
  ctx.stroke();
}

// -----------------------------
// Calibration curve
// -----------------------------
export function drawCalibration(canvas, pts) {
  if (!canvas || !Array.isArray(pts) || pts.length < 2) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const W = canvas.width;
  const H = canvas.height;
  ctx.clearRect(0, 0, W, H);

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

  const clamp01 = (v) => Math.max(0, Math.min(1, v));

  ctx.beginPath();
  pts.forEach((p, i) => {
    const x = clamp01(Number(p.conf));
    const y = clamp01(Number(p.acc));
    const px = x0 + x * (x1 - x0);
    const py = y0 - y * (y0 - y1);
    if (i === 0) ctx.moveTo(px, py);
    else ctx.lineTo(px, py);
  });

  ctx.strokeStyle = "rgba(220,220,220,0.85)";
  ctx.lineWidth = 2;
  ctx.stroke();
}
