import assert from "node:assert/strict";
import test from "node:test";

import { renderChartAccessibility } from "../ui/chart_a11y.js";
import { buildReplayChartModel, buildReplayViewModel, renderReplayPanel, replayMarkerStyle } from "../ui/replay.mjs";

class FakeElement {
  constructor(id = "", ownerDocument = null) {
    this.id = id;
    this.ownerDocument = ownerDocument;
    this.attributes = new Map();
    this.className = "";
    this.innerHTML = "";
    this.textContent = "";
    this.style = {};
    this.value = "";
    this.disabled = false;
    this.min = "";
    this.max = "";
    this.clientWidth = 960;
    this.clientHeight = 280;
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
}

class FakeCanvas extends FakeElement {
  constructor(id = "", ownerDocument = null) {
    super(id, ownerDocument);
    this.width = 1000;
    this.height = 280;
  }
  getContext(type) {
    if (type !== "2d") return null;
    const noop = () => {};
    return {
      setTransform: noop,
      clearRect: noop,
      fillRect: noop,
      strokeRect: noop,
      beginPath: noop,
      moveTo: noop,
      lineTo: noop,
      stroke: noop,
      fillText: noop,
      measureText: (value) => ({ width: String(value).length * 6 }),
      arc: noop,
      rect: noop,
      closePath: noop,
      fill: noop,
    };
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
  createElement(tag) {
    return new FakeElement("", this);
  }
  getElementById(id) {
    return this.elements.get(String(id)) || null;
  }
}

test("replay view model handles missing streams explicitly", () => {
  const vm = buildReplayViewModel({
    ok: true,
    read_only: true,
    date: "2026-01-02",
    symbol: "SPY",
  });

  assert.equal(vm.readOnly, true);
  assert.equal(vm.ready, false);
  assert.equal(vm.noData, true);
  assert.equal(vm.counts.candles, 0);
  assert.ok(vm.gaps.some((gap) => gap.stream === "price"));
  assert.ok(vm.gaps.some((gap) => gap.stream === "risk"));
});

test("replay view model synchronizes selected price, events, risk, and pnl", () => {
  const t0 = Date.UTC(2026, 0, 2, 14, 30);
  const vm = buildReplayViewModel(
    {
      ok: true,
      read_only: true,
      date: "2026-01-02",
      symbol: "SPY",
      candles: [
        { ts_ms: t0, open: 100, high: 101, low: 99, close: 100, volume: 10 },
        { ts_ms: t0 + 60_000, open: 100, high: 102, low: 100, close: 101, volume: 12 },
      ],
      decisions: [{ id: 1, ts_ms: t0 + 30_000, symbol: "SPY", label: "BUY", confidence: 0.8 }],
      orders: [{ id: 2, ts_ms: t0 + 35_000, symbol: "SPY", action: "BUY" }],
      fills: [{ id: 3, ts_ms: t0 + 40_000, symbol: "SPY", side: "BUY", qty: 10, price: 100.5 }],
      risk: [{ ts_ms: t0 + 20_000, gross: 0.4, net: 0.2, drawdown: 0.01 }],
      pnl: [{ ts_ms: t0 + 20_000, equity: 100100, day_pnl: 100 }],
    },
    { selectedTsMs: t0 + 40_000 },
  );

  assert.equal(vm.ready, true);
  assert.equal(vm.selected.price, 100);
  assert.equal(vm.selected.risk.gross, 0.4);
  assert.equal(vm.selected.pnl.day_pnl, 100);
  assert.equal(vm.nearby.decisions.length, 1);
  assert.equal(vm.nearby.orders.length, 1);
  assert.equal(vm.nearby.fills.length, 1);
  assert.equal(vm.markers.length, 3);
});

test("replay chart markers use tokenized color plus shape semantics", () => {
  assert.equal(replayMarkerStyle("decision").shape, "circle");
  assert.equal(replayMarkerStyle("order").shape, "square");

  const buy = replayMarkerStyle("fill", { side: "BUY", qty: 10 });
  const sell = replayMarkerStyle("fill", { side: "SELL", qty: -10 });

  assert.equal(buy.color, "#56B4E9");
  assert.equal(buy.shape, "triangle-up");
  assert.equal(buy.label, "BUY");
  assert.equal(sell.color, "#D55E00");
  assert.equal(sell.shape, "triangle-down");
  assert.equal(sell.label, "SELL");
});

test("replay chart model uses OHLC bodies and high-low wicks", () => {
  const t0 = Date.UTC(2026, 0, 2, 14, 30);
  const vm = buildReplayViewModel({
    ok: true,
    read_only: true,
    date: "2026-01-02",
    symbol: "SPY",
    candles: [
      { ts_ms: t0, open: 100, high: 110, low: 95, close: 106, volume: 10 },
      { ts_ms: t0 + 60_000, open: 106, high: 108, low: 97, close: 99, volume: 12 },
    ],
  });

  const model = buildReplayChartModel(vm, { width: 800, height: 260 });
  const first = model.candles[0];
  const second = model.candles[1];

  assert.equal(model.ok, true);
  assert.equal(model.candles.length, 2);
  assert.ok(first.highY < Math.min(first.openY, first.closeY));
  assert.ok(first.lowY > Math.max(first.openY, first.closeY));
  assert.ok(first.bodyHeight > 1);
  assert.equal(first.up, true);
  assert.equal(second.up, false);
  assert.ok(model.legend.some((item) => item.label === "OHLC"));
  assert.deepEqual(model.xTicks.map((tick) => tick.value), [t0, t0 + 30_000, t0 + 60_000]);
});

test("replay candle normalization corrects impossible OHLC bounds before charting", () => {
  const t0 = Date.UTC(2026, 0, 2, 14, 30);
  const vm = buildReplayViewModel({
    ok: true,
    read_only: true,
    date: "2026-01-02",
    symbol: "SPY",
    candles: [
      { ts_ms: t0, open: 100, high: 98, low: 102, close: 106, volume: 10 },
      { ts_ms: t0 + 60_000, open: 106, high: 107, low: 99, close: 101, volume: 12 },
    ],
  });

  const normalized = vm.streams.candles[0];
  assert.equal(normalized.high, 106);
  assert.equal(normalized.low, 100);
  assert.equal(normalized.raw_high, 98);
  assert.equal(normalized.raw_low, 102);
  assert.equal(normalized.ohlc_corrected, true);

  const model = buildReplayChartModel(vm, { width: 800, height: 260 });
  const first = model.candles[0];
  assert.equal(model.ok, true);
  assert.ok(first.highY <= Math.min(first.openY, first.closeY));
  assert.ok(first.lowY >= Math.max(first.openY, first.closeY));
  assert.ok(first.bodyHeight > 1);
});

test("replay chart model aligns markers by timestamp and anchor price", () => {
  const t0 = Date.UTC(2026, 0, 2, 14, 30);
  const fillTs = t0 + 30_000;
  const decisionTs = t0 + 10_000;
  const vm = buildReplayViewModel({
    ok: true,
    read_only: true,
    date: "2026-01-02",
    symbol: "SPY",
    candles: [
      { ts_ms: t0, open: 100, high: 104, low: 96, close: 101, volume: 10 },
      { ts_ms: t0 + 60_000, open: 101, high: 110, low: 98, close: 108, volume: 12 },
    ],
    decisions: [{ id: 1, ts_ms: decisionTs, symbol: "SPY", label: "BUY", confidence: 0.8 }],
    fills: [{ id: 2, ts_ms: fillTs, symbol: "SPY", side: "BUY", qty: 10, price: 106.5 }],
  });

  const model = buildReplayChartModel(vm, { width: 800, height: 260 });
  const fill = model.markers.find((marker) => marker.markerKind === "fill");
  const decision = model.markers.find((marker) => marker.markerKind === "decision");
  const expectedX = (ts) => model.plot.left + model.plot.width * ((ts - model.domain.minTs) / (model.domain.maxTs - model.domain.minTs));
  const expectedY = (price) => model.plot.top + model.plot.height * (1 - ((price - model.domain.minP) / (model.domain.maxP - model.domain.minP)));

  assert.ok(fill);
  assert.equal(fill.price, 106.5);
  assert.equal(fill.anchor.source, "event_price");
  assert.ok(Math.abs(fill.x - expectedX(fillTs)) < 0.001);
  assert.ok(Math.abs(fill.y - expectedY(106.5)) < 0.001);

  assert.ok(decision);
  assert.equal(decision.price, 101);
  assert.equal(decision.anchor.source, "nearest_close");
  assert.equal(decision.anchor.candleTsMs, t0);
  assert.ok(Math.abs(decision.x - expectedX(decisionTs)) < 0.001);
  assert.ok(Math.abs(decision.y - expectedY(101)) < 0.001);
});

test("chart accessibility helper assigns labels and renders fallback table", () => {
  const doc = new FakeDocument();
  const chart = doc.add(new FakeCanvas("demoChart"));
  const fallback = doc.add(new FakeElement("demoChartA11y"));

  renderChartAccessibility(chart, {
    title: "Demo chart",
    series: [
      { time: Date.UTC(2026, 0, 2, 14, 30), value: 100 },
      { time: Date.UTC(2026, 0, 2, 14, 31), value: 101.25 },
    ],
    timeKey: "time",
    valueKey: "value",
    valueLabel: "close",
    valueFormatter: (v) => Number(v).toFixed(2),
  });

  assert.equal(chart.getAttribute("role"), "img");
  assert.equal(chart.getAttribute("tabindex"), "0");
  assert.match(chart.getAttribute("aria-label"), /Demo chart: latest close 101\.25/);
  assert.match(fallback.innerHTML, /View chart data table/);
  assert.match(fallback.innerHTML, /<table/);
  assert.match(fallback.innerHTML, /101\.25/);
});

test("replay panel render gives the canvas an accessible label and fallback table", () => {
  const previousWindow = globalThis.window;
  globalThis.window = { devicePixelRatio: 1 };
  try {
    const doc = new FakeDocument();
    for (const id of [
      "replayMeta",
      "replayStatus",
      "replayStats",
      "replaySelected",
      "replayEvents",
      "replayGaps",
      "replayTimeline",
      "replayChartA11y",
    ]) {
      doc.add(new FakeElement(id));
    }
    const canvas = doc.add(new FakeCanvas("replayChart"));
    const t0 = Date.UTC(2026, 0, 2, 14, 30);

    renderReplayPanel(
      {
        ok: true,
        read_only: true,
        date: "2026-01-02",
        symbol: "SPY",
        candles: [
          { ts_ms: t0, open: 100, high: 101, low: 99, close: 100, volume: 10 },
          { ts_ms: t0 + 60_000, open: 100, high: 102, low: 100, close: 101, volume: 12 },
        ],
      },
      doc,
    );

    assert.equal(canvas.getAttribute("role"), "img");
    assert.match(canvas.getAttribute("aria-label"), /Historical replay: SPY OHLC candles latest/);
    assert.match(canvas.getAttribute("aria-label"), /C 101\.00/);
    const fallback = doc.getElementById("replayChartA11y").innerHTML;
    assert.match(fallback, /Historical replay data table/);
    assert.match(fallback, /Open/);
    assert.match(fallback, /High/);
    assert.match(fallback, /Low/);
    assert.match(fallback, /Close/);
  } finally {
    globalThis.window = previousWindow;
  }
});

test("replay panel render keeps error and empty states accessible", () => {
  const previousWindow = globalThis.window;
  globalThis.window = { devicePixelRatio: 1 };
  try {
    const doc = new FakeDocument();
    for (const id of [
      "replayMeta",
      "replayStatus",
      "replayStats",
      "replaySelected",
      "replayEvents",
      "replayGaps",
      "replayTimeline",
      "replayChartA11y",
    ]) {
      doc.add(new FakeElement(id));
    }
    const canvas = doc.add(new FakeCanvas("replayChart"));

    renderReplayPanel(
      {
        ok: false,
        read_only: true,
        date: "2026-01-02",
        symbol: "SPY",
        gaps: [{ stream: "replay", code: "load_failed", message: "Replay load failed.", severity: "warn" }],
      },
      doc,
    );

    assert.equal(canvas.getAttribute("data-chart-a11y-state"), "error");
    assert.match(canvas.getAttribute("aria-label"), /Replay load failed/);
    assert.match(doc.getElementById("replayChartA11y").innerHTML, /Replay load failed/);
    assert.match(doc.getElementById("replayStatus").textContent, /no data/);
  } finally {
    globalThis.window = previousWindow;
  }
});
