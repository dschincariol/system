import assert from "node:assert/strict";
import test from "node:test";

import {
  clearSelectedSymbolContext,
  getSelectedSymbolContext,
  initSelectedSymbolContextFromUrl,
  normalizeSelectedSymbol,
  persistSelectedSymbolToUrl,
  subscribeSelectedSymbolContext,
  updateSelectedSymbolContext,
} from "../ui/symbol_context.mjs";

function fakeWindow(href) {
  const state = { href };
  return {
    get location() {
      return new URL(state.href);
    },
    history: {
      state: {},
      replaceState(nextState, _title, nextUrl) {
        this.state = nextState;
        state.href = new URL(nextUrl, state.href).toString();
      },
    },
    get href() {
      return state.href;
    },
  };
}

test("selected symbol normalization is uppercase and conservative", () => {
  assert.equal(normalizeSelectedSymbol(" spy "), "SPY");
  assert.equal(normalizeSelectedSymbol("brk.b"), "BRK.B");
  assert.equal(normalizeSelectedSymbol(" eth-usd "), "ETH-USD");
  assert.equal(normalizeSelectedSymbol("<bad>"), "BAD");
  assert.equal(normalizeSelectedSymbol(""), "");
});

test("symbol context update notifies subscribers and supports unsubscribe", () => {
  clearSelectedSymbolContext({ notify: false, persistUrl: false });
  const seen = [];
  const unsubscribe = subscribeSelectedSymbolContext((next, prev) => {
    seen.push({ next, prev });
  });

  const first = updateSelectedSymbolContext({
    symbol: "aapl",
    source: "test",
    ts_ms: 123,
    persistUrl: false,
  });
  unsubscribe();
  updateSelectedSymbolContext({
    symbol: "msft",
    source: "after_unsubscribe",
    ts_ms: 456,
    persistUrl: false,
  });

  assert.equal(first.symbol, "AAPL");
  assert.equal(first.source, "test");
  assert.equal(first.ts_ms, 123);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].next.symbol, "AAPL");
  assert.equal(seen[0].prev.symbol, "");
  assert.equal(getSelectedSymbolContext().symbol, "MSFT");
});

test("URL helpers preserve existing params and hash", () => {
  const win = fakeWindow("https://example.test/ui/dashboard.html?screen=analyze#overview");
  assert.equal(persistSelectedSymbolToUrl("nvda", { windowRef: win }), true);
  assert.equal(win.location.searchParams.get("screen"), "analyze");
  assert.equal(win.location.searchParams.get("symbol"), "NVDA");
  assert.equal(win.location.hash, "#overview");

  assert.equal(persistSelectedSymbolToUrl("", { windowRef: win }), true);
  assert.equal(win.location.searchParams.get("screen"), "analyze");
  assert.equal(win.location.searchParams.has("symbol"), false);
  assert.equal(win.location.hash, "#overview");
});

test("context can initialize from a symbol query parameter", () => {
  clearSelectedSymbolContext({ notify: false, persistUrl: false });
  const win = fakeWindow("https://example.test/ui/dashboard.html?symbol=tsla&screen=overview");
  const ctx = initSelectedSymbolContextFromUrl({
    windowRef: win,
    source: "url_test",
    ts_ms: 789,
  });
  assert.deepEqual(ctx, {
    symbol: "TSLA",
    source: "url_test",
    ts_ms: 789,
  });
});
