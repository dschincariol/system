import assert from "node:assert/strict";
import test from "node:test";

import {
  MARKET_STRESS_THRESHOLDS,
  buildMarketStressSparklineModel,
  applyMarketStressSparklineMetadata,
  classifyMarketStressScore,
  drawMarketStressSparkline,
  marketStressSparklineSummary,
  normalizeMarketStressThresholds,
} from "../ui/market_stress.js";
import {
  MARKET_STRESS_THRESHOLDS as SHARED_MARKET_STRESS_THRESHOLDS,
  normalizeMarketStressThresholds as normalizeSharedMarketStressThresholds,
} from "../ui/market_stress_thresholds.js";

function fakeCanvas() {
  const attrs = new Map();
  return {
    attrs,
    setAttribute(name, value) {
      attrs.set(name, String(value));
    },
    getAttribute(name) {
      return attrs.get(name) || null;
    },
    removeAttribute(name) {
      attrs.delete(name);
    },
  };
}

function fakeContext() {
  const calls = [];
  return {
    calls,
    fillStyle: "",
    strokeStyle: "",
    lineWidth: 1,
    clearRect(...args) {
      calls.push(["clearRect", ...args]);
    },
    fillRect(...args) {
      calls.push(["fillRect", this.fillStyle, ...args]);
    },
    beginPath() {
      calls.push(["beginPath"]);
    },
    moveTo(...args) {
      calls.push(["moveTo", ...args]);
    },
    lineTo(...args) {
      calls.push(["lineTo", ...args]);
    },
    stroke() {
      calls.push(["stroke", this.strokeStyle, this.lineWidth]);
    },
    setLineDash(value) {
      calls.push(["setLineDash", [...value]]);
    },
    save() {
      calls.push(["save"]);
    },
    restore() {
      calls.push(["restore"]);
    },
  };
}

test("market stress sparkline keeps normal 0..1 values on the canonical scale", () => {
  const model = buildMarketStressSparklineModel([
    { time: 1, value: 0.10 },
    { time: 2, value: 0.55 },
    { time: 3, value: 0.75 },
    { time: 4, value: 0.92 },
  ], { width: 160, height: 48 });

  assert.equal(model.domainMin, 0);
  assert.equal(model.domainMax, 1);
  assert.deepEqual(model.thresholdLines.map((line) => line.key), ["warning", "critical"]);
  assert.equal(model.bands.length, 3);
  assert.equal(classifyMarketStressScore(0.54).state, "normal");
  assert.equal(classifyMarketStressScore(0.55).state, "warning");
  assert.equal(classifyMarketStressScore(0.75).state, "critical");
  for (const point of model.points) {
    assert.ok(point.y >= 0 && point.y <= model.height, `point ${point.value} rendered at y=${point.y}`);
  }
});

test("market stress exports use the shared threshold source", () => {
  assert.equal(MARKET_STRESS_THRESHOLDS, SHARED_MARKET_STRESS_THRESHOLDS);
  assert.deepEqual(normalizeMarketStressThresholds(), normalizeSharedMarketStressThresholds());
  assert.deepEqual(
    normalizeMarketStressThresholds({ thresholds: { warning: 0.62, critical: 0.81 } }),
    normalizeSharedMarketStressThresholds({ warning: 0.62, critical: 0.81 }),
  );
});

test("market stress sparkline auto-scales scores above 1.0 inside the canvas", () => {
  const model = buildMarketStressSparklineModel([
    { time: 1, value: 0.20 },
    { time: 2, value: 0.80 },
    { time: 3, value: 1.35 },
    { time: 4, value: 0.90 },
  ], { width: 160, height: 48 });

  assert.ok(model.domainMax > 1.35);
  assert.equal(model.observedMax, 1.35);
  assert.equal(model.points[2].band, "critical");
  assert.ok(model.points[2].y > 0 && model.points[2].y < model.height);
  assert.ok(model.thresholdLines.every((line) => line.y > 0 && line.y < model.height));
  assert.match(marketStressSparklineSummary(model), /warning line 0\.550, critical line 0\.750/);
});

test("market stress sparkline draw path exposes threshold rendering metadata", () => {
  const model = buildMarketStressSparklineModel([
    { time: 1, value: 0.20 },
    { time: 2, value: 1.20 },
    { time: 3, value: 0.70 },
  ], { width: 120, height: 48 });
  const ctx = fakeContext();
  const canvas = fakeCanvas();

  drawMarketStressSparkline(ctx, model);
  applyMarketStressSparklineMetadata(canvas, model);

  assert.equal(canvas.getAttribute("data-stress-threshold-warning"), "0.550");
  assert.equal(canvas.getAttribute("data-stress-threshold-critical"), "0.750");
  assert.ok(Number(canvas.getAttribute("data-stress-scale-max")) > 1.2);
  assert.ok(ctx.calls.filter((call) => call[0] === "fillRect").length >= 3);
  assert.ok(ctx.calls.filter((call) => call[0] === "setLineDash").length >= 3);
  assert.ok(ctx.calls.filter((call) => call[0] === "stroke").length >= 3);
});
