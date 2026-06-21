import assert from "node:assert/strict";
import test from "node:test";

import { computeHealthScore, renderHealthScoreSummary } from "../ui/health_score.js";

class FakeElement {
  constructor(id) {
    this.id = id;
    this.className = "";
    this.textContent = "";
    this.title = "";
    this.dataset = {};
    this.attributes = {};
    this.children = [];
  }

  setAttribute(name, value) {
    this.attributes[name] = String(value);
  }

  getAttribute(name) {
    return this.attributes[name];
  }

  appendChild(child) {
    this.children.push(child);
    return child;
  }

  replaceChildren(...children) {
    this.children = children;
  }
}

function fullHealthyInputs(overrides = {}) {
  return {
    alerts: [],
    health: {
      ok: true,
      prices: { ok: true },
    },
    readiness: { ready: true },
    systemState: { state: "LIVE" },
    systemStatus: {
      data_status: "RUNNING",
      market_data_latency_ms: 42,
      healthy_providers: 2,
      execution_enabled: true,
    },
    executionBarrier: { allowed: true, mode: "live" },
    executionDegraded: false,
    ...overrides,
  };
}

function factorByKey(scorecard, key) {
  return scorecard.factors.find((factor) => factor.key === key);
}

test("health score reports full coverage when all factors are available", () => {
  const scorecard = computeHealthScore(fullHealthyInputs());

  assert.equal(scorecard.score, 100);
  assert.equal(scorecard.availableCount, 4);
  assert.equal(scorecard.totalCount, 4);
  assert.equal(scorecard.coverageText, "4/4 factors");
  assert.equal(scorecard.coverageLevel, "full");
  assert.equal(scorecard.coverageClassName, "ok");
  assert.equal(scorecard.partialCoverage, false);
  assert.equal(scorecard.lowCoverage, false);
  assert.equal(scorecard.badgeClassName, "ok");
  assert.equal(scorecard.badgeLabel, "stable");
});

test("health score keeps available-factor score but flags low partial coverage", () => {
  const scorecard = computeHealthScore({ alerts: [] });

  assert.equal(scorecard.score, 100);
  assert.equal(scorecard.availableCount, 1);
  assert.equal(scorecard.totalCount, 4);
  assert.equal(scorecard.coverageText, "1/4 factors");
  assert.equal(scorecard.coverageLevel, "low");
  assert.equal(scorecard.coverageClassName, "high");
  assert.equal(scorecard.coverageLabel, "low coverage");
  assert.equal(scorecard.partialCoverage, true);
  assert.equal(scorecard.lowCoverage, true);
  assert.equal(scorecard.badgeClassName, "high");
  assert.equal(scorecard.badgeLabel, "low coverage");
  assert.match(scorecard.summary, /Only 1\/4 health factors are available/);
  assert.equal(factorByKey(scorecard, "runtime").available, false);
  assert.equal(factorByKey(scorecard, "data").available, false);
  assert.equal(factorByKey(scorecard, "execution").available, false);
});

test("health score reports unavailable when all factors are missing", () => {
  const scorecard = computeHealthScore({});

  assert.equal(scorecard.score, null);
  assert.equal(scorecard.availableCount, 0);
  assert.equal(scorecard.coverageText, "0/4 factors");
  assert.equal(scorecard.coverageLevel, "none");
  assert.equal(scorecard.coverageClassName, "unavailable");
  assert.equal(scorecard.badgeClassName, "unavailable");
  assert.equal(scorecard.badgeLabel, "waiting");
  assert.equal(scorecard.factors.every((factor) => factor.available === false), true);
});

test("health score discounts degraded execution while keeping full coverage", () => {
  const scorecard = computeHealthScore(fullHealthyInputs({ executionDegraded: true }));
  const execution = factorByKey(scorecard, "execution");

  assert.equal(scorecard.availableCount, 4);
  assert.equal(scorecard.coverageLevel, "full");
  assert.equal(scorecard.score, 92);
  assert.equal(scorecard.badgeClassName, "warn");
  assert.equal(scorecard.badgeLabel, "watch");
  assert.equal(execution.classification, "warning");
  assert.equal(execution.status, "degraded");
});

test("health score marks active critical alerts as degraded", () => {
  const scorecard = computeHealthScore(fullHealthyInputs({
    alerts: [{ severity: "CRIT", message: "risk limit breached" }],
  }));
  const alerts = factorByKey(scorecard, "alerts");

  assert.equal(scorecard.availableCount, 4);
  assert.equal(scorecard.coverageLevel, "full");
  assert.equal(scorecard.score, 80);
  assert.equal(scorecard.badgeClassName, "crit");
  assert.equal(scorecard.badgeLabel, "degraded");
  assert.equal(alerts.classification, "critical");
  assert.equal(alerts.status, "critical active");
  assert.match(scorecard.summary, /core health factors are failing or blocked/);
});

test("health score renderer applies low-coverage visual state", () => {
  const previousDocument = globalThis.document;
  globalThis.document = {
    createElement(tag) {
      return new FakeElement(tag);
    },
  };

  try {
    const scorecard = computeHealthScore({ alerts: [] });
    const cardEl = new FakeElement("healthScoreBar");
    cardEl.className = "healthScoreBar";
    const valueEl = new FakeElement("healthScoreValue");
    const badgeEl = new FakeElement("healthScoreBadge");
    const coverageEl = new FakeElement("healthScoreCoverage");
    const summaryEl = new FakeElement("healthScoreSummary");
    const factorsEl = new FakeElement("healthScoreFactors");

    renderHealthScoreSummary(scorecard, {
      cardEl,
      valueEl,
      badgeEl,
      coverageEl,
      summaryEl,
      factorsEl,
    });

    assert.equal(valueEl.textContent, "100");
    assert.match(cardEl.className, /is-health-coverage-low/);
    assert.equal(cardEl.dataset.coverage, "low");
    assert.match(badgeEl.className, /high/);
    assert.equal(badgeEl.textContent, "low coverage");
    assert.match(coverageEl.className, /healthScoreCoverage/);
    assert.match(coverageEl.className, /high/);
    assert.equal(coverageEl.dataset.coverage, "low");
    assert.equal(coverageEl.textContent, "1/4 factors");
    assert.match(coverageEl.getAttribute("aria-label"), /low coverage: 1\/4 factors/);
    assert.equal(factorsEl.children.length, 4);
  } finally {
    globalThis.document = previousDocument;
  }
});
