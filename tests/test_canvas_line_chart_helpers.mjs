import assert from "node:assert/strict";
import test from "node:test";

import {
  buildLineChartInspectorPoints,
  buildCalibrationViewModel,
  buildLineChartViewModel,
  calibrationVerdict,
  drawCalibration,
  renderLineChart,
} from "../ui/charts.js";

class FakeElement {
  constructor(id = "", ownerDocument = null) {
    this.id = id;
    this.ownerDocument = ownerDocument;
    this.attributes = new Map();
    this.className = "";
    this.innerHTML = "";
    this.textContent = "";
    this.style = {};
    this.listeners = new Map();
    this.classList = {
      add: (...classes) => {
        const next = new Set(String(this.className || "").split(/\s+/).filter(Boolean));
        for (const cls of classes) next.add(cls);
        this.className = Array.from(next).join(" ");
      },
    };
  }
  setAttribute(name, value) {
    this.attributes.set(String(name), String(value));
  }
  getAttribute(name) {
    return this.attributes.get(String(name)) || null;
  }
  hasAttribute(name) {
    return this.attributes.has(String(name));
  }
  addEventListener(name, handler) {
    const key = String(name);
    const handlers = this.listeners.get(key) || [];
    handlers.push(handler);
    this.listeners.set(key, handlers);
  }
  removeEventListener(name, handler) {
    const key = String(name);
    const handlers = this.listeners.get(key) || [];
    this.listeners.set(key, handlers.filter((item) => item !== handler));
  }
  dispatchEvent(event = {}) {
    const key = String(event.type || "");
    for (const handler of this.listeners.get(key) || []) handler(event);
  }
}

class FakeContext {
  constructor() {
    this.calls = [];
    this.strokeStyle = "";
    this.fillStyle = "";
    this.lineWidth = 1;
    this.font = "";
  }
  clearRect(...args) { this.calls.push(["clearRect", ...args]); }
  strokeRect(...args) { this.calls.push(["strokeRect", ...args]); }
  beginPath(...args) { this.calls.push(["beginPath", ...args]); }
  moveTo(...args) { this.calls.push(["moveTo", ...args]); }
  lineTo(...args) { this.calls.push(["lineTo", ...args]); }
  stroke(...args) { this.calls.push(["stroke", ...args]); }
  fillText(...args) { this.calls.push(["fillText", ...args]); }
  fillRect(...args) { this.calls.push(["fillRect", ...args]); }
  arc(...args) { this.calls.push(["arc", ...args]); }
  fill(...args) { this.calls.push(["fill", ...args]); }
  measureText(value) { return { width: String(value).length * 6 }; }
}

class FakeCanvas extends FakeElement {
  constructor(id = "", ownerDocument = null) {
    super(id, ownerDocument);
    this.width = 900;
    this.height = 180;
    this.context = new FakeContext();
  }
  getContext(type) {
    return type === "2d" ? this.context : null;
  }
  getBoundingClientRect() {
    return { left: 0, top: 0, width: this.width, height: this.height };
  }
}

class FakeDocument {
  constructor() {
    this.elements = new Map();
  }
  add(el) {
    el.ownerDocument = this;
    this.elements.set(el.id, el);
    return el;
  }
  createElement() {
    return new FakeElement("", this);
  }
  getElementById(id) {
    return this.elements.get(String(id)) || null;
  }
}

test("line chart clips out-of-range segments without flattening along the edge", () => {
  const vm = buildLineChartViewModel([0, 10, 0], { yMin: 0, yMax: 5 });

  assert.equal(vm.ok, true);
  assert.equal(vm.yMin, 0);
  assert.equal(vm.yMax, 5);
  assert.equal(vm.segments.length, 2);
  assert.deepEqual(
    vm.segments.map((segment) => [segment.from.value, segment.to.value]),
    [[0, 5], [5, 0]],
  );
  assert.equal(
    vm.segments.some((segment) => segment.from.value === 5 && segment.to.value === 5),
    false,
  );

  const allOutside = buildLineChartViewModel([9, 10, 11], { yMin: 0, yMax: 5 });
  assert.equal(allOutside.ok, false);
  assert.equal(allOutside.state, "out_of_range");
  assert.equal(allOutside.segments.length, 0);
});

test("line chart builds two to three timestamp x-axis ticks", () => {
  const t0 = Date.UTC(2026, 0, 2, 14, 30);
  const times = [t0, t0 + 60_000, t0 + 120_000];
  const vm = buildLineChartViewModel([1, 2, 3], {
    xValues: times,
    fmtX: (value) => new Date(Number(value)).toISOString().slice(11, 16),
  });

  assert.equal(vm.usesExplicitXScale, true);
  assert.equal(vm.xTicks.length, 3);
  assert.deepEqual(vm.xTicks.map((tick) => tick.label), ["14:30", "14:31", "14:32"]);
  assert.equal(vm.xMin, times[0]);
  assert.equal(vm.xMax, times[2]);
});

test("line chart builds point inspector rows with formatted values", () => {
  const vm = buildLineChartViewModel([1, 2, 4]);
  const points = buildLineChartInspectorPoints(vm, {
    seriesLabel: "Equity",
    fmtY: (value) => Number(value).toFixed(1),
  });

  assert.equal(points.length, 3);
  assert.deepEqual(
    points.map((point) => point.values[0].valueText),
    ["1.0", "2.0", "4.0"],
  );
  assert.equal(points[2].values[0].label, "Equity");
});

test("line chart renders legend, axis labels, latest value, and keyboard inspector", () => {
  const doc = new FakeDocument();
  const canvas = doc.add(new FakeCanvas("interactiveLineChart"));
  const fallback = doc.add(new FakeElement("interactiveLineChartA11y"));
  const dataWindow = doc.add(new FakeElement("interactiveLineChartDataWindow"));
  let prevented = false;

  renderLineChart(canvas, [1, 2, 4], {
    a11yTitle: "Portfolio equity curve",
    seriesLabel: "Equity",
    xAxisLabel: "run time",
    yAxisLabel: "equity",
    fmtY: (value) => Number(value).toFixed(1),
  });

  const labels = canvas.context.calls
    .filter((call) => call[0] === "fillText")
    .map((call) => String(call[1]));
  assert.ok(labels.includes("Equity"));
  assert.ok(labels.includes("run time"));
  assert.ok(labels.includes("equity"));
  assert.ok(labels.includes("4.0"));
  assert.match(dataWindow.textContent, /Portfolio equity curve: 3; Equity 4\.0/);
  assert.match(fallback.innerHTML, /View chart data table/);

  canvas.dispatchEvent({
    type: "keydown",
    key: "Home",
    preventDefault: () => { prevented = true; },
  });
  assert.equal(prevented, true);
  assert.match(dataWindow.textContent, /Portfolio equity curve: 1; Equity 1\.0/);
  assert.equal(canvas.getAttribute("data-chart-selected-index"), "0");
});

test("line chart renders explicit no-data state without throwing", () => {
  const doc = new FakeDocument();
  const canvas = doc.add(new FakeCanvas("emptyLineChart"));
  const fallback = doc.add(new FakeElement("emptyLineChartA11y"));

  renderLineChart(canvas, [], {
    a11yTitle: "Empty line chart",
    emptyMessage: "No chart points are available.",
  });

  assert.ok(canvas.context.calls.some((call) => call[0] === "fillText" && /No chart points/.test(call[1])));
  assert.equal(canvas.getAttribute("role"), "img");
  assert.equal(canvas.getAttribute("data-chart-a11y-state"), "empty");
  assert.match(fallback.innerHTML, /No chart points are available/);
});

test("drawdown charts keep yMax at zero and clip positive invalid values", () => {
  const vm = buildLineChartViewModel([0, -0.03, 0.02, -0.04], {
    yMin: -0.06,
    yMax: 0,
  });

  assert.equal(vm.yMin, -0.06);
  assert.equal(vm.yMax, 0);
  assert.ok(vm.segments.some((segment) => segment.clipped));
  assert.equal(
    vm.segments.every((segment) =>
      segment.from.value <= 0 &&
      segment.to.value <= 0 &&
      segment.from.value >= -0.06 &&
      segment.to.value >= -0.06
    ),
    true,
  );
  assert.equal(
    vm.segments.some((segment) => segment.from.value === 0 && segment.to.value === 0),
    false,
  );
});

test("calibration view model computes sample-weighted ECE from real bin counts", () => {
  const vm = buildCalibrationViewModel([
    { conf: 0.2, acc: 0.1, n: 30 },
    { conf: 0.6, acc: 0.5, n: 70 },
  ]);

  assert.equal(vm.countAvailable, true);
  assert.equal(vm.totalSampleCount, 100);
  assert.equal(vm.weighting, "sample_weighted");
  assert.equal(Number(vm.ece.toFixed(6)), 0.1);
  assert.equal(vm.verdict.key, "overconfident");
  assert.match(vm.summary, /sample-weighted/);
  assert.match(vm.summary, /100 samples/);
});

test("calibration verdict distinguishes calibrated and underconfident states", () => {
  assert.equal(calibrationVerdict(0.02, -0.01).key, "calibrated");
  assert.equal(calibrationVerdict(0.08, 0.06).key, "underconfident");
  assert.equal(calibrationVerdict(0.08, -0.06).key, "overconfident");
});

test("calibration view model does not invent bin counts when counts are absent", () => {
  const vm = buildCalibrationViewModel({
    sample_count: 250,
    curve: [
      { x: 0.25, y: 0.20 },
      { x: 0.75, y: 0.80 },
    ],
  });

  assert.equal(vm.countAvailable, false);
  assert.equal(vm.totalSampleCount, 250);
  assert.equal(vm.weighting, "equal_bins");
  assert.equal(vm.points.every((point) => point.count === null), true);
  assert.equal(Number(vm.ece.toFixed(6)), 0.05);
});

test("calibration chart renders legend and point inspector data", () => {
  const doc = new FakeDocument();
  const canvas = doc.add(new FakeCanvas("calibCanvas"));
  doc.add(new FakeElement("calibCanvasA11y"));
  const dataWindow = doc.add(new FakeElement("calibCanvasDataWindow"));

  drawCalibration(canvas, [
    { conf: 0.2, acc: 0.1, n: 30 },
    { conf: 0.8, acc: 0.7, n: 70 },
  ]);

  const labels = canvas.context.calls
    .filter((call) => call[0] === "fillText")
    .map((call) => String(call[1]));
  assert.ok(labels.includes("accuracy line"));
  assert.ok(labels.includes("bin count bars"));
  assert.match(dataWindow.textContent, /Confidence calibration: Bin 2/);
  assert.match(dataWindow.textContent, /Accuracy 70\.0%/);
  assert.match(dataWindow.textContent, /Count 70/);
});
