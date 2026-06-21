import assert from "node:assert/strict";
import test from "node:test";

import {
  buildIndicatorAccessibilitySummary,
  applyPriceLinesToSeries,
  buildOverlayAccessibilitySummary,
  createIndicatorState,
  decisionMarkerStyle,
  decisionWindowLegendItems,
  indicatorOverlayLegendItems,
  normalizeDecisionOverlayPayload,
  toDecisionWindowBands,
  toLightweightMarkers,
  updateIndicatorState,
  VWAP_OVERLAY_LABEL,
  VWAP_OVERLAY_SEMANTICS,
} from "../ui/decision_overlays.js";

class FakeSeries {
  constructor() {
    this.created = [];
    this.removed = [];
  }

  createPriceLine(options) {
    const handle = { ...options, id: this.created.length + 1 };
    this.created.push(handle);
    return handle;
  }

  removePriceLine(handle) {
    this.removed.push(handle);
  }
}

test("decision overlay markers expose distinct visual semantics", () => {
  const markers = [
    { ts_ms: 1_789_500_000_000, kind: "filled", side: "BUY", qty: 10, reason_code: "fill_executed" },
    { ts_ms: 1_789_500_060_000, kind: "intended", side: "BUY", qty: 4, reason_code: "portfolio_intent" },
    { ts_ms: 1_789_500_120_000, kind: "suppressed", reason_code: "ttl_expired" },
    { ts_ms: 1_789_500_180_000, kind: "blocked", reason_code: "kill_switch_db_global" },
    { ts_ms: 1_789_500_240_000, kind: "risk_capped", side: "BUY", qty: 2, reason_code: "portfolio_risk_cap_scaled" },
  ];

  const normalized = normalizeDecisionOverlayPayload({ markers });
  assert.deepEqual(normalized.markers.map((m) => m.text), ["FILL B", "INTENT", "SUPP", "BLOCK", "CAP"]);
  assert.deepEqual(normalized.markers.map((m) => m.shape), ["arrowUp", "circle", "square", "arrowDown", "arrowUp"]);
  assert.equal(new Set(normalized.markers.map((m) => m.color)).size, 5);

  const lwMarkers = toLightweightMarkers(markers);
  assert.equal(lwMarkers.length, 5);
  assert.equal(lwMarkers[2].shape, "square");
  assert.equal(lwMarkers[3].text, "BLOCK");

  const sellFill = decisionMarkerStyle("filled", "SELL", -2);
  assert.equal(sellFill.shape, "arrowDown");
  assert.equal(sellFill.text, "FILL S");
});

test("decision overlay payload normalizes price levels, windows, and accessibility summary", () => {
  const payload = normalizeDecisionOverlayPayload({
    markers: [{ time: 1_789_500_000, kind: "risk_cap", side: "BUY", qty: 1 }],
    price_lines: [
      { kind: "average_cost", price: "99.5" },
      { kind: "stop", px: 98 },
      { kind: "take_profit", value: 105 },
      { kind: "cap", price: 200 },
    ],
    windows: [
      { kind: "kill_switch_window", start_ts_ms: 1_789_500_000_000 },
      { type: "circuit-breaker-window", start: 1_789_500_060_000, end: 1_789_500_120_000 },
    ],
  });

  assert.equal(payload.markers[0].kind, "risk_capped");
  assert.deepEqual(payload.price_lines.map((line) => line.kind), ["average_cost", "stop", "take_profit", "cap"]);
  assert.deepEqual(payload.windows.map((window) => window.kind), ["kill_switch_window", "circuit_breaker_window"]);
  assert.deepEqual(payload.windows.map((window) => window.start_s), [1_789_500_000, 1_789_500_060]);
  assert.deepEqual(payload.windows.map((window) => window.end_s), [null, 1_789_500_120]);
  assert.match(buildOverlayAccessibilitySummary(payload), /1 risk-capped/);
  assert.match(buildOverlayAccessibilitySummary(payload), /2 active or recent windows/);
  assert.match(buildOverlayAccessibilitySummary(payload), /4 price levels/);
});

test("decision windows convert milliseconds to chart seconds and extend open ends", () => {
  const payload = normalizeDecisionOverlayPayload({
    windows: [
      { kind: "kill_switch_window", start_ts_ms: 1_789_500_000_123 },
      { kind: "drawdown_throttle_window", start_ts_ms: 1_789_500_060_000, end_ts_ms: 1_789_500_120_999 },
    ],
  });

  const bands = toDecisionWindowBands(payload.windows, [
    { time: 1_789_499_940, close: 100 },
    { time: 1_789_500_240, close: 101 },
  ]);

  assert.equal(bands.length, 2);
  assert.equal(bands[0].startTime, 1_789_500_000);
  assert.equal(bands[0].endTime, 1_789_500_240);
  assert.equal(bands[0].openEnded, true);
  assert.equal(bands[1].startTime, 1_789_500_060);
  assert.equal(bands[1].endTime, 1_789_500_120);
  assert.equal(bands[1].openEnded, false);
  assert.match(bands[0].fillColor, /^rgba\(/);
});

test("decision window legend exposes readable band entries", () => {
  const items = decisionWindowLegendItems({
    windows: [
      { kind: "kill_switch_window", start_ts_ms: 1_789_500_000_000 },
      { kind: "suppression_window", start_ts_ms: 1_789_500_060_000 },
    ],
  });

  assert.deepEqual(items.map((item) => item.shape), ["band", "band"]);
  assert.deepEqual(items.map((item) => item.text), ["KILL", "TSE"]);
  assert.ok(items.every((item) => item.fillColor && item.borderColor));
});

test("price-line rendering replaces existing lightweight chart handles", () => {
  const series = new FakeSeries();
  const first = applyPriceLinesToSeries(series, [], [
    { kind: "entry", price: 100.25 },
    { kind: "stop", price: 98.0 },
  ]);

  assert.equal(first.length, 2);
  assert.equal(series.created.length, 2);
  assert.equal(series.removed.length, 0);

  const second = applyPriceLinesToSeries(series, first, [
    { kind: "take_profit", price: 105.0 },
  ]);

  assert.equal(second.length, 1);
  assert.deepEqual(series.removed, first);
  assert.equal(series.created[2].title, "take profit");
});

test("loaded-window VWAP stays cumulative across multi-day candles", () => {
  const state = createIndicatorState([
    { time: 1_789_500_000, close: 100, volume: 10 },
    { time: 1_789_586_400, close: 110, volume: 10 },
    { time: 1_789_672_800, close: 130, volume: 20 },
  ]);

  assert.equal(state.vwapSemantics, "loaded_window");
  assert.deepEqual(
    state.vwap.map((point) => Number(point.value.toFixed(4))),
    [100, 105, 117.5],
  );
  assert.notEqual(state.vwap[1].value, 110);
  assert.notEqual(state.vwap[2].value, 130);
});

test("VWAP overlay label and accessibility text describe loaded-window semantics", () => {
  assert.equal(VWAP_OVERLAY_LABEL, "Loaded-window VWAP");
  assert.equal(VWAP_OVERLAY_SEMANTICS.resetPolicy, "none");
  assert.match(VWAP_OVERLAY_SEMANTICS.description, /does not reset at trading-session boundaries/);

  const indicatorSummary = buildIndicatorAccessibilitySummary({ vwap: true, ema: false, equity: false });
  assert.match(indicatorSummary, /Loaded-window VWAP/);
  assert.match(indicatorSummary, /does not reset at trading-session boundaries/);

  const legendItems = indicatorOverlayLegendItems({ vwap: true, ema: false, equity: false });
  assert.deepEqual(legendItems.map((item) => item.label), ["Loaded-window VWAP"]);
});

test("indicator state updates streaming candles without full recompute", () => {
  let state = createIndicatorState([
    { time: 10, close: 100, volume: 10 },
    { time: 20, close: 102, volume: 10 },
  ]);

  const appended = updateIndicatorState(state, { time: 30, close: 104, volume: 10 });
  state = appended.state;
  assert.equal(appended.needsRebuild, false);
  assert.equal(state.vwap.length, 3);
  assert.equal(state.vwap[2].value, 102);

  const replaced = updateIndicatorState(state, { time: 30, close: 110, volume: 10 });
  state = replaced.state;
  assert.equal(replaced.needsRebuild, false);
  assert.equal(state.vwap.length, 3);
  assert.equal(state.vwap[2].value, 104);

  const outOfOrder = updateIndicatorState(state, { time: 25, close: 99, volume: 10 });
  assert.equal(outOfOrder.needsRebuild, true);
});
