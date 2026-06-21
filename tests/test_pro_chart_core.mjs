import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  addSeriesCompat,
  applyMarkersToState,
  applyPriceLinesToState,
  applyWindowBandsToState,
  clearPriceLinesForState,
  clearWindowBandsForState,
  createProChart,
  disconnectResizeObserver,
  ensureLightweightCharts,
  formatProChartHealthText,
  installProChartHealthTicker,
  LIGHTWEIGHT_CHART_DEFAULTS,
  normalizeCandle,
  scheduleStreamReconnect,
  upsertSeriesPoint,
} from "../ui/pro_chart_core.js";
import {
  applyDashboardProChartOverlayInputs,
  getDashboardProChartOverlayState,
  getProChartsState,
  PRO_CHART_PREF_KEYS,
  setDashboardProChartOverlayState,
  setProChartsState,
} from "../ui/pro_chart_prefs.js";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

function withFakeLocalStorage(fn) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
  const store = new Map();
  const fakeStorage = {
    getItem(key) {
      return store.has(key) ? store.get(key) : null;
    },
    setItem(key, value) {
      store.set(key, String(value));
    },
  };
  Object.defineProperty(globalThis, "localStorage", {
    value: fakeStorage,
    configurable: true,
  });
  try {
    return fn(store);
  } finally {
    if (previous) Object.defineProperty(globalThis, "localStorage", previous);
    else delete globalThis.localStorage;
  }
}

test("pro chart preferences keep backward-compatible localStorage keys", () => {
  withFakeLocalStorage((store) => {
    assert.deepEqual(getProChartsState(), { enabled: true, tf: "1m", type: "candle" });

    const next = setProChartsState({ enabled: false, tf: "5m", type: "area" });
    assert.deepEqual(next, { enabled: false, tf: "5m", type: "area" });
    assert.equal(store.get(PRO_CHART_PREF_KEYS.enabled), "0");
    assert.equal(store.get(PRO_CHART_PREF_KEYS.tf), "5m");
    assert.equal(store.get(PRO_CHART_PREF_KEYS.type), "area");

    const overlays = setDashboardProChartOverlayState({ vwap: false, ema: true, trades: false, pnl: true });
    assert.deepEqual(overlays, { vwap: false, ema: true, trades: false, pnl: true });
    assert.equal(store.get("dashboard.proCharts.ov.vwap"), "0");
    assert.equal(store.get("dashboard.proCharts.ov.ema"), "1");
    assert.equal(store.get("dashboard.proCharts.ov.trades"), "0");
    assert.equal(store.get("dashboard.proCharts.ov.pnl"), "1");
    assert.deepEqual(getDashboardProChartOverlayState(), overlays);
  });
});

test("dashboard overlay input sync writes checked state from shared prefs", () => {
  withFakeLocalStorage(() => {
    setDashboardProChartOverlayState({ vwap: false, ema: true, trades: false, pnl: true });
    const inputs = new Map([
      ["proChartsOvVWAP", { checked: true }],
      ["proChartsOvEMA", { checked: false }],
      ["proChartsOvTrades", { checked: true }],
      ["proChartsOvPnL", { checked: false }],
    ]);
    const doc = {
      getElementById(id) {
        return inputs.get(id) || null;
      },
    };

    applyDashboardProChartOverlayInputs(doc);

    assert.equal(inputs.get("proChartsOvVWAP").checked, false);
    assert.equal(inputs.get("proChartsOvEMA").checked, true);
    assert.equal(inputs.get("proChartsOvTrades").checked, false);
    assert.equal(inputs.get("proChartsOvPnL").checked, true);
  });
});

test("series compatibility supports legacy and v5 Lightweight Charts APIs", () => {
  const legacyChart = {
    addLineSeries(options) {
      return { api: "legacy", options };
    },
  };
  assert.deepEqual(addSeriesCompat(legacyChart, "line", { lineWidth: 2 }), {
    api: "legacy",
    options: { lineWidth: 2 },
  });

  const lineDef = { type: "line-def" };
  const modernChart = {
    addSeries(def, options) {
      return { api: "v5", def, options };
    },
  };
  const windowRef = { LightweightCharts: { LineSeries: lineDef } };
  assert.deepEqual(addSeriesCompat(modernChart, "line", { color: "red" }, { windowRef }), {
    api: "v5",
    def: lineDef,
    options: { color: "red" },
  });
});

test("chart construction installs resize cleanup and preserves base options", () => {
  class FakeResizeObserver {
    constructor(callback) {
      this.callback = callback;
      this.observed = [];
      this.disconnected = false;
    }
    observe(el) {
      this.observed.push(el);
    }
    disconnect() {
      this.disconnected = true;
    }
  }

  const container = {
    innerHTML: "old",
    clientWidth: 800,
    clientHeight: 320,
    getBoundingClientRect() {
      return { width: 640.8, height: 360.2 };
    },
  };
  const chart = {
    applied: [],
    applyOptions(options) {
      this.applied.push(options);
    },
  };
  let createArgs = null;
  const windowRef = {
    LightweightCharts: {
      createChart(el, options) {
        createArgs = { el, options };
        return chart;
      },
    },
  };

  const result = createProChart(container, {
    windowRef,
    includeInitialSize: true,
    chartOptions: { rightPriceScale: { scaleMargins: { top: 0.1, bottom: 0.3 } } },
    resizeObserverOptions: { ResizeObserverClass: FakeResizeObserver },
  });

  assert.equal(container.innerHTML, "");
  assert.equal(createArgs.el, container);
  assert.equal(createArgs.options.layout.background.color, "#0a0d12");
  assert.equal(createArgs.options.rightPriceScale.borderColor, "#30363d");
  assert.deepEqual(createArgs.options.rightPriceScale.scaleMargins, { top: 0.1, bottom: 0.3 });
  assert.equal(createArgs.options.width, 800);
  assert.equal(createArgs.options.height, 320);
  assert.equal(container._proChartResizeObserver, result.resizeObserver);

  result.resizeObserver.callback();
  assert.deepEqual(chart.applied.at(-1), { width: 640, height: 360 });

  disconnectResizeObserver(result.resizeObserver, container);
  assert.equal(result.resizeObserver.disconnected, true);
  assert.equal(container._proChartResizeObserver, null);
});

test("chart construction supports explicit fallback sizing without changing defaults", () => {
  class FakeResizeObserver {
    constructor(callback) {
      this.callback = callback;
    }
    observe() {}
    disconnect() {}
  }

  const container = {
    innerHTML: "old",
    clientWidth: 0,
    clientHeight: 0,
    getBoundingClientRect() {
      return { width: 0, height: 0 };
    },
  };
  const chart = {
    applied: [],
    applyOptions(options) {
      this.applied.push(options);
    },
  };
  let createOptions = null;
  const windowRef = {
    LightweightCharts: {
      createChart(_el, options) {
        createOptions = options;
        return chart;
      },
    },
  };

  const result = createProChart(container, {
    windowRef,
    includeInitialSize: true,
    initialSizeFallback: { width: 420, height: 220 },
    resizeObserverOptions: {
      ResizeObserverClass: FakeResizeObserver,
      sizeFallback: { width: 420, height: 220 },
    },
  });

  assert.equal(createOptions.width, 420);
  assert.equal(createOptions.height, 220);
  result.resizeObserver.callback();
  assert.deepEqual(chart.applied.at(-1), { width: 420, height: 220 });
});

test("marker and price-line helpers update whichever primary series is active", () => {
  const markerSeries = {
    markers: null,
    setMarkers(markers) {
      this.markers = markers;
    },
  };
  const state = {
    lineSeries: markerSeries,
    markerLayer: null,
    markerSeries: null,
    priceLineHandles: [],
  };

  assert.equal(applyMarkersToState(state, [{ time: 100, kind: "filled", side: "BUY", qty: 1 }]), true);
  assert.equal(markerSeries.markers.length, 1);
  assert.equal(markerSeries.markers[0].text, "FILL B");

  const priceSeries = {
    created: [],
    removed: [],
    createPriceLine(options) {
      const handle = { id: this.created.length + 1, ...options };
      this.created.push(handle);
      return handle;
    },
    removePriceLine(handle) {
      this.removed.push(handle);
    },
  };
  state.lineSeries = priceSeries;

  const handles = applyPriceLinesToState(state, [{ kind: "stop", price: 98.25 }]);
  assert.equal(handles.length, 1);
  assert.equal(priceSeries.created[0].title, "stop");

  clearPriceLinesForState(state);
  assert.deepEqual(priceSeries.removed, handles);
  assert.deepEqual(state.priceLineHandles, []);
});

test("decision window band primitive attaches, updates, and cleans up", () => {
  const chart = {
    timeScale() {
      return {
        timeToIndex(time) { return (Number(time) - 1_789_500_000) / 60; },
        logicalToCoordinate(index) { return Number(index) * 10; },
      };
    },
  };
  const anchor = {
    attached: [],
    detached: [],
    attachPrimitive(primitive) {
      this.attached.push(primitive);
      primitive.attached({ chart, requestUpdate: () => { this.updated = true; } });
    },
    detachPrimitive(primitive) {
      this.detached.push(primitive);
      primitive.detached();
    },
  };
  const state = {
    candleSeries: anchor,
    history: [{ time: 1_789_500_000 }, { time: 1_789_500_180 }],
    windowBandLayer: null,
    windowBandSeries: null,
  };

  const first = applyWindowBandsToState(state, [
    { kind: "kill_switch_window", start_ts_ms: 1_789_500_000_000 },
  ]);

  assert.equal(first.length, 1);
  assert.equal(first[0].startTime, 1_789_500_000);
  assert.equal(first[0].endTime, 1_789_500_180);
  assert.equal(anchor.attached.length, 1);
  assert.equal(state.windowBandLayer.bands()[0].openEnded, true);

  const second = applyWindowBandsToState(state, [
    { kind: "circuit_breaker_window", start_ts_ms: 1_789_500_060_000, end_ts_ms: 1_789_500_120_000 },
  ]);

  assert.equal(second.length, 1);
  assert.equal(anchor.attached.length, 1);
  assert.equal(anchor.updated, true);
  assert.equal(state.windowBandLayer.bands()[0].endTime, 1_789_500_120);

  clearWindowBandsForState(state);
  assert.equal(anchor.detached.length, 1);
  assert.equal(state.windowBandLayer, null);
  assert.equal(state.windowBandSeries, null);
});

test("candle normalization and upsert behavior are shared", () => {
  assert.deepEqual(normalizeCandle({ t: "10", price: "101.5", volume: "3" }), {
    time: 10,
    open: 101.5,
    high: 101.5,
    low: 101.5,
    close: 101.5,
    volume: 3,
  });
  assert.equal(normalizeCandle({ time: "bad", close: 1 }), null);
  assert.equal(normalizeCandle({ close: 1 }), null);
  assert.equal(normalizeCandle({ time: 0, close: 1 }), null);
  assert.equal(normalizeCandle({ time: -1, close: 1 }), null);
  assert.deepEqual(normalizeCandle({ time: 12, close: 1 }), {
    time: 12,
    open: 1,
    high: 1,
    low: 1,
    close: 1,
    volume: 0,
  });

  const rows = upsertSeriesPoint([{ time: 1, value: 10 }, { time: 3, value: 30 }], { time: 2, value: 20 });
  assert.deepEqual(rows, [{ time: 1, value: 10 }, { time: 3, value: 30 }]);

  const replaced = upsertSeriesPoint(rows, { time: 3, value: 31 });
  assert.deepEqual(replaced.at(-1), { time: 3, value: 31 });

  const limited = upsertSeriesPoint(replaced, { time: 4, value: 40 }, 2);
  assert.deepEqual(limited.map((row) => row.time), [3, 4]);
});

test("health ticker and reconnect primitives preserve lifecycle semantics", () => {
  assert.equal(formatProChartHealthText({ lastUpdateMs: 1_000, streamConnected: true, nowMs: 3_500 }), "live • last candle 2s");
  assert.equal(formatProChartHealthText({ lastUpdateMs: 1_000, streamConnected: false, nowMs: 7_500 }), "stale 6s");

  let intervalCallback = null;
  let cleared = null;
  const state = { lastUpdateMs: 0, streamConnected: false, healthTimer: 9 };
  const healthEl = { textContent: "" };
  const timer = installProChartHealthTicker(state, healthEl, {
    setIntervalFn(callback) {
      intervalCallback = callback;
      return 12;
    },
    clearIntervalFn(value) {
      cleared = value;
    },
  });
  assert.equal(cleared, 9);
  assert.equal(timer, 12);
  intervalCallback();
  assert.equal(healthEl.textContent, "no data");
  state.streamConnected = true;
  intervalCallback();
  assert.equal(healthEl.textContent, "live stream");

  let scheduled = null;
  let opened = 0;
  const streamState = { key: "AAPL::1m", retryBackoffMs: 500, retryTimer: null };
  scheduleStreamReconnect(streamState, {
    key: "AAPL::1m",
    open: () => { opened += 1; },
    documentRef: { hidden: false },
    setTimeoutFn(callback, ms) {
      scheduled = { callback, ms };
      return 99;
    },
  });
  assert.equal(scheduled.ms, 500);
  assert.equal(streamState.retryBackoffMs, 850);
  assert.equal(streamState.retryTimer, 99);
  scheduled.callback();
  assert.equal(opened, 1);
  assert.equal(streamState.retryTimer, null);
});

test("lightweight chart loader returns already-loaded runtime without DOM writes", async () => {
  const runtime = { version: () => "test" };
  assert.equal(await ensureLightweightCharts({ windowRef: { LightweightCharts: runtime }, documentRef: null }), runtime);
});

test("lightweight chart loader uses shared local and CDN defaults", async () => {
  const runtime = { version: () => "loaded" };
  const loadedSrcs = [];
  const windowRef = {};
  const documentRef = {
    createElement(tag) {
      assert.equal(tag, "script");
      return {};
    },
    head: {
      appendChild(script) {
        loadedSrcs.push(script.src);
        if (loadedSrcs.length === 1) {
          script.onerror(new Error("local missing"));
          return;
        }
        windowRef.LightweightCharts = runtime;
        script.onload();
      },
    },
  };

  assert.equal(await ensureLightweightCharts({ windowRef, documentRef }), runtime);
  assert.deepEqual(loadedSrcs, [
    LIGHTWEIGHT_CHART_DEFAULTS.libLocal,
    LIGHTWEIGHT_CHART_DEFAULTS.libCdn,
  ]);
});

test("static imports keep dashboard independent from terminal charting prefs", () => {
  const dashboard = readFileSync(join(ROOT, "ui/dashboard.js"), "utf8");
  const dashboardChart = readFileSync(join(ROOT, "ui/pro_chart_engine.js"), "utf8");
  const portfolioBacktest = readFileSync(join(ROOT, "ui/portfolio_backtest.js"), "utf8");
  const terminalChart = readFileSync(join(ROOT, "ui/terminal/pro_charting.js"), "utf8");
  const prefs = readFileSync(join(ROOT, "ui/pro_chart_prefs.js"), "utf8");
  const core = readFileSync(join(ROOT, "ui/pro_chart_core.js"), "utf8");

  assert.match(dashboard, /from "\.\/pro_chart_prefs\.js"/);
  assert.doesNotMatch(dashboard, /from "\.\/terminal\/pro_charting\.js"/);
  assert.match(dashboardChart, /from "\.\/pro_chart_core\.js"/);
  assert.match(dashboardChart, /from "\.\/pro_chart_prefs\.js"/);
  assert.match(dashboardChart, /applyWindowBandsToState/);
  assert.doesNotMatch(dashboardChart, /terminal\/pro_charting\.js/);
  assert.match(portfolioBacktest, /from "\.\/pro_chart_core\.js"/);
  assert.match(portfolioBacktest, /ensureLightweightCharts/);
  assert.match(portfolioBacktest, /addSeriesCompat/);
  assert.match(portfolioBacktest, /createProChart/);
  assert.doesNotMatch(portfolioBacktest, /function _ensureLightweightCharts/);
  assert.doesNotMatch(portfolioBacktest, /LIGHTWEIGHT_LOCAL_SRC|LIGHTWEIGHT_CDN_SRC/);
  assert.match(terminalChart, /from "\.\.\/pro_chart_core\.js"/);
  assert.match(terminalChart, /from "\.\.\/pro_chart_prefs\.js"/);
  assert.match(terminalChart, /applyWindowBandsToState/);
  assert.match(terminalChart, /export \{ applyProChartsVisibility, getProChartsState, setProChartsState \}/);
  assert.match(prefs, /"proCharts\.enabled"/);
  assert.match(core, /export async function ensureLightweightCharts/);
  assert.match(core, /export function applyWindowBandsToState/);
  assert.match(core, /export function scheduleStreamReconnect/);
});
