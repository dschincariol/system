import assert from "node:assert/strict";
import test from "node:test";

import { buildReplayViewModel } from "../ui/replay.mjs";

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
