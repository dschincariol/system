import assert from "node:assert/strict";
import test from "node:test";

import {
  formatFxPrice,
  formatLotQty,
  isFxSymbol,
  normalizeFxSymbol,
  pipDecimals,
  pipValueDisplay,
} from "../ui/fx_format.js";

test("FX symbol detection mirrors accepted display forms", () => {
  assert.equal(isFxSymbol("EURUSD"), true);
  assert.equal(isFxSymbol("EUR/USD"), true);
  assert.equal(isFxSymbol("eur_usd"), true);
  assert.equal(normalizeFxSymbol("EUR/USD"), "EURUSD");
  assert.equal(isFxSymbol("SPY"), false);
  assert.equal(isFxSymbol("BTCUSD"), false);
});

test("FX prices use pip-aware decimal places", () => {
  assert.equal(pipDecimals("EURUSD"), 5);
  assert.equal(pipDecimals("USDJPY"), 3);
  assert.equal(formatFxPrice("EURUSD", 1.083456), "1.08346");
  assert.equal(formatFxPrice("USDJPY", 157.1234), "157.123");
  assert.equal(formatFxPrice("EURUSD", Number.NaN), "—");
});

test("pip distance is JPY-aware", () => {
  assert.equal(pipValueDisplay("EURUSD", 1.1000, 1.1010), "10.0 pips");
  assert.equal(pipValueDisplay("USDJPY", 157.10, 157.35), "25 pips");
  assert.equal(pipValueDisplay("SPY", 100, 101), "—");
});

test("lot quantity renders lots plus units and degrades for non-FX", () => {
  assert.equal(formatLotQty("EURUSD", 150000), "1.50 lots (150,000)");
  assert.equal(formatLotQty("USDJPY", 25000, 50000), "0.50 lots (25,000)");
  assert.equal(formatLotQty("SPY", 12.3456789), "12.345679");
  assert.equal(formatLotQty("EURUSD", Infinity), "—");
});
