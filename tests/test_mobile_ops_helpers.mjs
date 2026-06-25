import test from "node:test";
import assert from "node:assert/strict";

import {
  canFireKillSwitch,
  canStartKillSwitchHold,
  describeEmergencyConsequences,
  normalizeAlertRows,
  normalizePositionRows,
  summarizePnlTrend,
} from "../ui/mobile/mobile_helpers.mjs";
import { renderMobilePnl, renderMobileRuntimeNarrative } from "../ui/mobile/mobile.js";
import { summarizeRuntimeStatus } from "../ui/runtime_status_summary.js";

function ok(data) {
  return { ok: true, data };
}

function installDocument(ids) {
  const elements = new Map(ids.map((id) => [id, {
    id,
    textContent: "",
    innerHTML: "",
    className: "",
    dataset: {},
    attributes: {},
    style: { setProperty() {} },
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
  }]));
  globalThis.document = {
    getElementById(id) {
      return elements.get(id) || null;
    },
  };
  return elements;
}

function cleanupDocument() {
  delete globalThis.document;
}

function baseRuntimeEndpoints() {
  return {
    status: ok({ state: "LIVE", execution_barrier: { allowed: true, mode: "live" } }),
    systemState: ok({ state: "LIVE", ok: true }),
    health: ok({ ok: true, status: "ready", training: { allowed: true, mode: "online" } }),
    readiness: ok({ ready: true, ok: true }),
    executionBarrier: ok({ allowed: true, mode: "live" }),
    marketStress: ok({ ok: true, stress: { stress_score: 0.12 } }),
  };
}

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
      { id: 4, severity: "HIGH", status: "active", message: "four" },
      { id: 5, severity: "WARN", status: "active", message: "five" },
    ],
  });
  assert.deepEqual(rows.map((row) => row.id), [2, 4, 5, 1]);
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

test("PnL trend summarizer reports compact trend when history is available", () => {
  const trend = summarizePnlTrend({
    ok: true,
    meta: { ready: true },
    data: {
      total: 19,
      history: [
        { ts_ms: 1000, total: 10 },
        { ts_ms: 3000, total: 19 },
        { ts_ms: 2000, total: 16 },
      ],
    },
  });
  assert.equal(trend.available, true);
  assert.equal(trend.direction, "up");
  assert.equal(trend.text, "PnL trend: up $9.00 over 3 points ($10.00 to $19.00).");
});

test("PnL trend summarizer explains unavailable snapshot-only data", () => {
  const trend = summarizePnlTrend({
    ok: true,
    meta: { ready: true },
    data: { total: 19 },
  });
  assert.equal(trend.available, false);
  assert.match(trend.text, /returned only the latest snapshot/);
});

test("mobile runtime narrative renders the shared runtime summary", () => {
  const elements = installDocument([
    "runtimeHeadline",
    "runtimeMeaning",
    "runtimeSummaryBadge",
    "runtimeNextList",
    "runtimeStatusNote",
    "runtimeNarrative",
  ]);
  try {
    const endpoints = baseRuntimeEndpoints();
    const expected = summarizeRuntimeStatus({
      systemState: endpoints.systemState.data,
      stressPayload: endpoints.marketStress.data,
      barrierPayload: endpoints.executionBarrier.data,
      healthPayload: endpoints.health.data,
      readinessPayload: endpoints.readiness.data,
    });
    const rendered = renderMobileRuntimeNarrative(endpoints);
    assert.equal(rendered.headline, expected.headline);
    assert.equal(elements.get("runtimeHeadline").textContent, expected.headline);
    assert.equal(elements.get("runtimeMeaning").textContent, expected.meaning);
    assert.match(elements.get("runtimeNextList").innerHTML, /<li>Check Alerts if anything looks unusual\.<\/li>/);
  } finally {
    cleanupDocument();
  }
});

test("mobile runtime narrative updates for readiness, health, and barrier changes", () => {
  const elements = installDocument([
    "runtimeHeadline",
    "runtimeMeaning",
    "runtimeSummaryBadge",
    "runtimeNextList",
    "runtimeStatusNote",
    "runtimeNarrative",
  ]);
  try {
    const endpoints = baseRuntimeEndpoints();
    renderMobileRuntimeNarrative(endpoints);
    assert.equal(elements.get("runtimeHeadline").textContent, "System running normally");

    endpoints.readiness = ok({ ready: false, ok: false, waiting_on: ["feature_store"] });
    renderMobileRuntimeNarrative(endpoints);
    assert.equal(elements.get("runtimeHeadline").textContent, "System is protecting itself");

    endpoints.readiness = ok({ ready: true, ok: true });
    endpoints.health = ok({
      ok: true,
      execution_degraded: { active: true, reason_codes: ["broker_down"] },
      training: { allowed: true, mode: "online" },
    });
    renderMobileRuntimeNarrative(endpoints);
    assert.equal(elements.get("runtimeHeadline").textContent, "Critical runtime blockers are active");

    endpoints.health = ok({ ok: true, training: { allowed: true, mode: "online" } });
    endpoints.executionBarrier = ok({ allowed: false, mode: "safe", reason: "mode_safe" });
    renderMobileRuntimeNarrative(endpoints);
    assert.equal(elements.get("runtimeHeadline").textContent, "Trading is blocked");
  } finally {
    cleanupDocument();
  }
});

test("mobile runtime narrative remains useful when one status endpoint fails", () => {
  const elements = installDocument([
    "runtimeHeadline",
    "runtimeMeaning",
    "runtimeSummaryBadge",
    "runtimeNextList",
    "runtimeStatusNote",
    "runtimeNarrative",
  ]);
  try {
    const endpoints = baseRuntimeEndpoints();
    endpoints.systemState = { ok: false, data: null, error: "route_down" };
    renderMobileRuntimeNarrative(endpoints);
    assert.equal(elements.get("runtimeHeadline").textContent, "System running normally");
    assert.match(elements.get("runtimeStatusNote").textContent, /Partial status: systemState: route_down/);
  } finally {
    cleanupDocument();
  }
});

test("mobile PnL render writes compact trend and unavailable states", () => {
  const elements = installDocument([
    "pnlTotal",
    "pnlMeta",
    "pnlTotalDetail",
    "pnlUnrealized",
    "pnlRealized",
    "pnlTrend",
    "pnlBadge",
  ]);
  try {
    renderMobilePnl({
      pnl: ok({
        ok: true,
        meta: { ready: true },
        total: 14,
        unrealized: 4,
        realized: 10,
        data: {
          history: [
            { ts_ms: 1000, total: 10 },
            { ts_ms: 2000, total: 14 },
          ],
        },
      }),
    });
    assert.equal(elements.get("pnlTotal").textContent, "$14.00");
    assert.equal(elements.get("pnlTrend").textContent, "PnL trend: up $4.00 over 2 points ($10.00 to $14.00).");

    renderMobilePnl({
      pnl: ok({
        ok: true,
        meta: { ready: true },
        data: { total: 14 },
      }),
    });
    assert.match(elements.get("pnlTrend").textContent, /returned only the latest snapshot/);
  } finally {
    cleanupDocument();
  }
});
