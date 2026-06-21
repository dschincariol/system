import assert from "node:assert/strict";
import test from "node:test";

import {
  buildOperatorOverviewModel,
  buildOverviewDecisionModel,
  buildOverviewStatusModel,
  buildOverviewTrustModel,
  renderOperatorOverview,
} from "../ui/operator_overview.js";

const NOW = Date.UTC(2026, 0, 2, 12, 0, 0);
const FRESH_TS = NOW - 3_000;

function runtimeInputs(overrides = {}) {
  return {
    nowMs: NOW,
    systemState: { state: "LIVE", ts_ms: FRESH_TS },
    healthPayload: {
      ok: true,
      ts_ms: FRESH_TS,
      prices: { ok: true },
    },
    readinessPayload: { ready: true, ts_ms: FRESH_TS },
    executionBarrier: { allowed: true, mode: "live", ts_ms: FRESH_TS },
    stressPayload: { ok: true, stress: { stress_score: 0.2, ts_ms: FRESH_TS, z_vix: 0.4 } },
    alerts: [],
    ...overrides,
  };
}

class FakeElement {
  constructor(id) {
    this.id = id;
    this.innerHTML = "";
    this.textContent = "";
    this.className = "";
    this.attributes = {};
  }

  setAttribute(name, value) {
    this.attributes[name] = String(value);
  }

  getAttribute(name) {
    return this.attributes[name];
  }
}

function fakeDocument() {
  const ids = [
    "operatorOverviewCard",
    "overviewStatusTile",
    "overviewStatusIcon",
    "overviewStatusWord",
    "overviewStatusScore",
    "overviewStatusFreshness",
    "overviewStatusHeadline",
    "overviewStatusMeaning",
    "overviewStatusNext",
    "overviewStatusFallback",
    "overviewDecisionTile",
    "overviewDecisionTitle",
    "overviewDecisionMeta",
    "overviewDecisionSummary",
    "overviewDecisionFallback",
    "overviewDecisionStepper",
    "overviewDecisionTimeline",
    "overviewTrustTile",
    "overviewTrustFallback",
    "overviewRiskBars",
    "overviewRiskUnavailable",
    "overviewPnlTrendText",
    "overviewPnlSparkline",
    "overviewStressDriver",
  ];
  const elements = new Map(ids.map((id) => [id, new FakeElement(id)]));
  return {
    elements,
    getElementById(id) {
      return elements.get(id) || null;
    },
  };
}

test("overview status maps clean runtime state to SAFE in the DOM", () => {
  const model = buildOperatorOverviewModel({
    ...runtimeInputs(),
    decisionsPayload: { decisions: [] },
    uiMetrics: {
      ok: true,
      pnl: { today_pnl: 12.5 },
      exposure: { gross: 0.2, net: 0.1 },
      risk: { max_drawdown_pct: 0.01, vol_proxy: 0.01 },
      sources: { pnl: { endpoint: "/api/ui/metrics", ts_ms: FRESH_TS } },
    },
    portfolioRisk: {
      history: [{ gross: 0.2, net: 0.1, drawdown: 0.01, vol_proxy: 0.01 }],
      caps: { gross: 1.0, net: 0.6, drawdown: 0.06, vol_target: 0.02 },
    },
  });
  const doc = fakeDocument();
  renderOperatorOverview(model, { document: doc });

  assert.equal(model.status.word, "SAFE");
  assert.equal(doc.getElementById("overviewStatusWord").textContent, "SAFE");
  assert.match(doc.getElementById("overviewStatusTile").getAttribute("aria-label"), /SAFE/);
  assert.match(doc.getElementById("overviewTrustTile").getAttribute("aria-label"), /Gross Exposure/);
});

test("overview status maps stale data to CAUTION with stale fallback text", () => {
  const staleTs = NOW - 360_000;
  const status = buildOverviewStatusModel(runtimeInputs({
    systemState: { state: "LIVE", ts_ms: staleTs },
    healthPayload: { ok: true, ts_ms: staleTs, prices: { ok: true } },
    readinessPayload: { ready: true, ts_ms: staleTs },
    executionBarrier: { allowed: true, ts_ms: staleTs },
    stressPayload: { ok: true, stress: { stress_score: 0.2, ts_ms: staleTs } },
  }));

  const doc = fakeDocument();
  renderOperatorOverview({
    status,
    decision: buildOverviewDecisionModel({ decisionsPayload: { decisions: [] } }),
    trust: buildOverviewTrustModel({}),
  }, { document: doc });

  assert.equal(status.word, "CAUTION");
  assert.equal(status.freshness.stale, true);
  assert.match(doc.getElementById("overviewStatusTile").getAttribute("aria-label"), /Backend 6m ago/);
});

test("overview status surfaces partial health-score coverage", () => {
  const status = buildOverviewStatusModel(runtimeInputs({
    systemState: null,
    healthPayload: null,
    readinessPayload: null,
    executionBarrier: null,
    stressPayload: null,
    alerts: [],
  }));

  const doc = fakeDocument();
  renderOperatorOverview({
    status,
    decision: buildOverviewDecisionModel({ decisionsPayload: { decisions: [] } }),
    trust: buildOverviewTrustModel({}),
  }, { document: doc });

  assert.notEqual(status.word, "SAFE");
  assert.equal(status.score, 100);
  assert.equal(status.coverageText, "1/4 factors");
  assert.equal(status.scoreText, "Health 100/100 (1/4 factors)");
  assert.match(doc.getElementById("overviewStatusScore").textContent, /1\/4 factors/);
  assert.match(doc.getElementById("overviewStatusTile").getAttribute("aria-label"), /1\/4 factors/);
});

test("overview status maps blocked execution to STOP", () => {
  const status = buildOverviewStatusModel(runtimeInputs({
    executionBarrier: { allowed: false, mode: "safe", reason: "kill_switch", ts_ms: FRESH_TS },
  }));

  const doc = fakeDocument();
  renderOperatorOverview({
    status,
    decision: buildOverviewDecisionModel({ decisionsPayload: { decisions: [] } }),
    trust: buildOverviewTrustModel({ executionBarrier: { allowed: false } }),
  }, { document: doc });

  assert.equal(status.word, "STOP");
  assert.equal(doc.getElementById("overviewStatusIcon").textContent, "X");
  assert.match(doc.getElementById("overviewStatusHeadline").textContent, /Trading is blocked/);
});

test("overview decision tile renders no-decision state without a fake flow", () => {
  const decision = buildOverviewDecisionModel({ decisionsPayload: { ok: true, decisions: [] } });
  const doc = fakeDocument();
  renderOperatorOverview({
    status: buildOverviewStatusModel(runtimeInputs()),
    decision,
    trust: buildOverviewTrustModel({}),
  }, { document: doc });

  assert.equal(decision.state, "empty");
  assert.equal(doc.getElementById("overviewDecisionTitle").textContent, "No recent decisions");
  assert.match(doc.getElementById("overviewDecisionStepper").innerHTML, /No decision flow to display/);
  assert.match(doc.getElementById("overviewDecisionTile").getAttribute("aria-label"), /No recent decisions/);
});

test("overview trust tile renders risk unavailable fallback", () => {
  const trust = buildOverviewTrustModel({});
  const doc = fakeDocument();
  renderOperatorOverview({
    status: buildOverviewStatusModel(runtimeInputs()),
    decision: buildOverviewDecisionModel({ decisionsPayload: { decisions: [] } }),
    trust,
  }, { document: doc });

  assert.equal(trust.riskUnavailable, true);
  assert.match(doc.getElementById("overviewRiskBars").innerHTML, /data unavailable/);
  assert.match(doc.getElementById("overviewRiskUnavailable").textContent, /Risk headroom unavailable/);
  assert.match(doc.getElementById("overviewTrustTile").getAttribute("aria-label"), /Risk headroom unavailable/);
});
