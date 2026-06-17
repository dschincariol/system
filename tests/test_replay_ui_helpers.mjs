import assert from "node:assert/strict";
import test from "node:test";

import { renderChartAccessibility } from "../ui/chart_a11y.js";
import { buildReplayViewModel, renderReplayPanel, replayMarkerStyle } from "../ui/replay.mjs";

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
    assert.match(canvas.getAttribute("aria-label"), /Historical replay: SPY close 101\.00/);
    assert.match(doc.getElementById("replayChartA11y").innerHTML, /Historical replay data table/);
    assert.match(doc.getElementById("replayChartA11y").innerHTML, /Close/);
  } finally {
    globalThis.window = previousWindow;
  }
});
