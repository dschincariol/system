import assert from "node:assert/strict";
import test from "node:test";

import { fxSessionStatus } from "../ui/fx_session.js";

const ms = (text) => Date.parse(text);

test("FX session is open on Wednesday UTC midday", () => {
  const status = fxSessionStatus(ms("2026-06-24T12:00:00Z"));
  assert.equal(status.open, true);
  assert.match(status.label, /open/);
  assert.ok(status.nextChangeMs > ms("2026-06-24T12:00:00Z"));
});

test("FX session is closed during Saturday and before Sunday open", () => {
  const saturday = fxSessionStatus(ms("2026-06-27T12:00:00Z"));
  assert.equal(saturday.open, false);
  assert.match(saturday.label, /closed/);

  const beforeOpen = fxSessionStatus(ms("2026-06-28T21:59:00Z"));
  const afterOpen = fxSessionStatus(ms("2026-06-28T22:01:00Z"));
  assert.equal(beforeOpen.open, false);
  assert.equal(afterOpen.open, true);
});

test("FX session is deterministic and boundary overridable", () => {
  const now = ms("2026-06-28T21:30:00Z");
  const opts = { openWeekdayUtc: 0, openHourUtc: 21, closeWeekdayUtc: 5, closeHourUtc: 21 };
  assert.deepEqual(fxSessionStatus(now, opts), fxSessionStatus(now, opts));
  const status = fxSessionStatus(now, opts);
  assert.equal(status.open, true);
  assert.match(status.label, /Fri 21:00 UTC/);
});

test("overrides can pin the presentation mirror to a known FX-04 boundary", () => {
  const status = fxSessionStatus(ms("2026-01-04T21:30:00Z"), {
    openWeekdayUtc: 0,
    openHourUtc: 22,
    closeWeekdayUtc: 5,
    closeHourUtc: 22,
  });
  assert.equal(status.open, false);
  assert.equal(status.nextChangeMs, ms("2026-01-04T22:00:00Z"));
});
