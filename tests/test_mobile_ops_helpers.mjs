import test from "node:test";
import assert from "node:assert/strict";

import {
  canFireKillSwitch,
  canStartKillSwitchHold,
  describeEmergencyConsequences,
  normalizeAlertRows,
  normalizePositionRows,
} from "../ui/mobile/mobile_helpers.mjs";

test("kill switch confirmation requires exact phrase and completed hold", () => {
  assert.equal(canStartKillSwitchHold({ typedPhrase: "KILL" }), true);
  assert.equal(canStartKillSwitchHold({ typedPhrase: "kill" }), true);
  assert.equal(canStartKillSwitchHold({ typedPhrase: "STOP" }), false);
  assert.equal(canFireKillSwitch({ typedPhrase: "KILL", holdComplete: false }), false);
  assert.equal(canFireKillSwitch({ typedPhrase: "KILL", holdComplete: true }), true);
  assert.equal(canFireKillSwitch({ typedPhrase: "KILL", holdComplete: true, pending: true }), false);
});

test("position normalizer excludes zero quantity rows", () => {
  const rows = normalizePositionRows({
    rows: [
      { symbol: "SPY", qty: 2, avg_px: 501 },
      { symbol: "QQQ", qty: 0, avg_px: 430 },
    ],
  });
  assert.deepEqual(rows.map((row) => row.symbol), ["SPY"]);
});

test("alert normalizer keeps active alerts sorted by severity", () => {
  const rows = normalizeAlertRows({
    rows: [
      { id: 1, severity: "INFO", status: "active", message: "one" },
      { id: 2, severity: "CRIT", status: "active", message: "two" },
      { id: 3, severity: "WARN", status: "resolved", message: "three" },
    ],
  });
  assert.deepEqual(rows.map((row) => row.id), [2, 1]);
});

test("emergency consequence preview states the action boundary", () => {
  const text = describeEmergencyConsequences({
    status: { execution_allowed: true },
    pnl: { ok: true, total: 10, unrealized: 5 },
    positions: { rows: [{ symbol: "SPY", qty: 1 }] },
    killSwitches: { state: [] },
  });
  assert.match(text, /activates the global kill switch/);
  assert.match(text, /does not submit a mobile flatten order/);
  assert.match(text, /Open positions visible now: 1/);
});
