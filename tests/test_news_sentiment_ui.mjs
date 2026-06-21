import assert from "node:assert/strict";
import test from "node:test";

import {
  buildNewsSentimentSparklineModel,
  drawNewsSentimentSparkline,
  loadNewsSentiment,
  newsSentimentSparklineSummary,
} from "../ui/news_panels.js";

class FakeElement {
  constructor(id = "", ownerDocument = null) {
    this.id = id;
    this.ownerDocument = ownerDocument;
    this.attributes = new Map();
    this.className = "";
    this.innerHTML = "";
    this.textContent = "";
    this.style = {};
    this.clientWidth = 900;
    this.clientHeight = 160;
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
  removeAttribute(name) {
    this.attributes.delete(String(name));
  }
  hasAttribute(name) {
    return this.attributes.has(String(name));
  }
  getBoundingClientRect() {
    return { width: this.clientWidth, height: this.clientHeight };
  }
}

class FakeContext {
  constructor() {
    this.calls = [];
    this.fillStyle = "";
    this.strokeStyle = "";
    this.lineWidth = 1;
    this.font = "";
  }
  clearRect(...args) { this.calls.push(["clearRect", ...args]); }
  strokeRect(...args) { this.calls.push(["strokeRect", ...args]); }
  fillRect(...args) { this.calls.push(["fillRect", this.fillStyle, ...args]); }
  beginPath() { this.calls.push(["beginPath"]); }
  moveTo(...args) { this.calls.push(["moveTo", ...args]); }
  lineTo(...args) { this.calls.push(["lineTo", ...args]); }
  stroke() { this.calls.push(["stroke", this.strokeStyle, this.lineWidth]); }
  fill() { this.calls.push(["fill", this.fillStyle]); }
  arc(...args) { this.calls.push(["arc", ...args]); }
  fillText(...args) { this.calls.push(["fillText", ...args]); }
  measureText(value) { return { width: String(value).length * 6 }; }
  setLineDash(value) { this.calls.push(["setLineDash", [...value]]); }
  setTransform(...args) { this.calls.push(["setTransform", ...args]); }
}

class FakeCanvas extends FakeElement {
  constructor(id = "", ownerDocument = null) {
    super(id, ownerDocument);
    this.width = 900;
    this.height = 160;
    this.context = new FakeContext();
  }
  getContext(type) {
    return type === "2d" ? this.context : null;
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

test("news sentiment model renders normal values on fixed negative to positive scale", () => {
  const model = buildNewsSentimentSparklineModel([
    { ts_ms: 1, sentiment: -0.4 },
    { ts_ms: 2, sentiment: 0 },
    { ts_ms: 3, sentiment: 0.6 },
  ], { width: 300, height: 120 });

  assert.equal(model.domainMin, -1);
  assert.equal(model.domainMax, 1);
  assert.equal(model.baseline, 0);
  assert.deepEqual(model.bands.map((band) => band.key), ["positive", "negative"]);
  assert.equal(model.clippedCount, 0);
  assert.equal(model.malformedCount, 0);
  assert.deepEqual(model.points.map((point) => point.context), ["negative", "neutral", "positive"]);
  assert.ok(model.points.every((point) => point.y >= 0 && point.y <= model.height));
  assert.match(newsSentimentSparklineSummary(model), /-1\.000 negative to \+1\.000 positive/);
  assert.match(newsSentimentSparklineSummary(model), /0\.000 neutral baseline/);
});

test("news sentiment draw path exposes zero baseline and positive negative context", () => {
  const model = buildNewsSentimentSparklineModel([
    { time: 1, sentiment: -0.2 },
    { time: 2, sentiment: 0.3 },
  ], { width: 240, height: 100 });
  const ctx = new FakeContext();

  drawNewsSentimentSparkline(ctx, model);

  assert.ok(ctx.calls.filter((call) => call[0] === "fillRect").length >= 2);
  assert.ok(ctx.calls.some((call) => call[0] === "setLineDash" && call[1].length > 0));
  assert.ok(ctx.calls.some((call) => call[0] === "fillText" && call[1] === "0 neutral"));
  assert.ok(ctx.calls.some((call) => call[0] === "fillText" && call[1] === "positive"));
  assert.ok(ctx.calls.some((call) => call[0] === "fillText" && call[1] === "negative"));
});

test("news sentiment empty series produces an explicit no-data summary", () => {
  const model = buildNewsSentimentSparklineModel([], { width: 300, height: 120 });

  assert.equal(model.hasData, false);
  assert.equal(model.points.length, 0);
  assert.equal(model.clippedCount, 0);
  assert.equal(model.malformedCount, 0);
  assert.match(newsSentimentSparklineSummary(model), /no valid sentiment points/);
});

test("news sentiment skips malformed values without treating blanks as neutral", () => {
  const model = buildNewsSentimentSparklineModel([
    { time: 1, sentiment: "" },
    { time: 2, sentiment: null },
    { time: 3, sentiment: "not-a-number" },
    { time: 4, sentiment: "0.25" },
  ], { width: 300, height: 120 });

  assert.equal(model.hasData, true);
  assert.equal(model.points.length, 1);
  assert.equal(model.malformedCount, 3);
  assert.equal(model.points[0].value, 0.25);
  assert.match(newsSentimentSparklineSummary(model), /skipped 3 malformed points/);
});

test("news sentiment clamps out-of-range values and records anomalies", () => {
  const model = buildNewsSentimentSparklineModel([
    { time: 1, sentiment: 1.35 },
    { time: 2, sentiment: -1.2 },
    { time: 3, sentiment: 0.4 },
  ], { width: 300, height: 120 });

  assert.equal(model.clippedCount, 2);
  assert.deepEqual(model.points.map((point) => point.value), [1, -1, 0.4]);
  assert.deepEqual(model.points.slice(0, 2).map((point) => point.quality), [
    "clipped above expected range",
    "clipped below expected range",
  ]);
  assert.ok(model.points.every((point) => point.y >= 0 && point.y <= model.height));
  assert.match(newsSentimentSparklineSummary(model), /clipped 2 out-of-range points/);
  assert.match(newsSentimentSparklineSummary(model), /raw range -1\.200 to \+1\.350/);
});

test("news sentiment loader writes accessible summary and raw/clipped table context", async () => {
  const previousDocument = globalThis.document;
  const doc = new FakeDocument();
  const canvas = doc.add(new FakeCanvas("newsSentimentCanvas"));
  const fallback = doc.add(new FakeElement("newsSentimentCanvasA11y"));
  globalThis.document = doc;

  try {
    await loadNewsSentiment(async () => ({
      ok: true,
      series: [
        { ts_ms: 1, sentiment: 1.2 },
        { ts_ms: 2, sentiment: -0.1 },
      ],
    }));

    assert.equal(canvas.getAttribute("role"), "img");
    assert.equal(canvas.getAttribute("data-chart-a11y-state"), "ready");
    assert.equal(canvas.getAttribute("data-sentiment-scale-min"), "-1.000");
    assert.equal(canvas.getAttribute("data-sentiment-scale-max"), "+1.000");
    assert.equal(canvas.getAttribute("data-sentiment-baseline"), "0.000");
    assert.equal(canvas.getAttribute("data-sentiment-clipped-points"), "1");
    assert.match(canvas.getAttribute("aria-label"), /clipped 1 out-of-range point/);
    assert.match(fallback.innerHTML, /Displayed sentiment/);
    assert.match(fallback.innerHTML, /Raw sentiment/);
    assert.match(fallback.innerHTML, /\+1\.200/);
    assert.match(fallback.innerHTML, /clipped above expected range/);
  } finally {
    globalThis.document = previousDocument;
  }
});

test("news sentiment loader treats null sentiment as unavailable, not neutral", async () => {
  const previousDocument = globalThis.document;
  const doc = new FakeDocument();
  const canvas = doc.add(new FakeCanvas("newsSentimentCanvas"));
  const fallback = doc.add(new FakeElement("newsSentimentCanvasA11y"));
  globalThis.document = doc;

  try {
    await loadNewsSentiment(async () => ({
      ok: true,
      meta: { ready: true, count: 2, valid_sentiment: 1, missing_sentiment: 1 },
      series: [
        { ts_ms: 1, sentiment: null },
        { ts_ms: 2, sentiment: 0.0 },
      ],
    }));

    assert.equal(canvas.getAttribute("data-chart-a11y-state"), "ready");
    assert.equal(canvas.getAttribute("data-sentiment-malformed-points"), "1");
    assert.equal(canvas.getAttribute("data-sentiment-observed-min"), "0.000");
    assert.equal(canvas.getAttribute("data-sentiment-observed-max"), "0.000");
    assert.match(canvas.getAttribute("aria-label"), /latest sentiment 0\.000 \(neutral\)/);
    assert.match(canvas.getAttribute("aria-label"), /skipped 1 malformed point/);
    assert.match(fallback.innerHTML, /Displayed sentiment/);
    assert.doesNotMatch(fallback.innerHTML, /unavailable.*0\.000/s);
  } finally {
    globalThis.document = previousDocument;
  }
});
