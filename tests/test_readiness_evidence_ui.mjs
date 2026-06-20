import test from "node:test";
import assert from "node:assert/strict";

import {
  brokerActivationReadinessDecision,
  guardBrokerActivationWithReadinessEvidence,
  normalizeReadinessEvidence,
  readinessEvidenceUrlForBrokerActivation,
  renderReadinessEvidencePanel,
} from "../ui/readiness_evidence.js";

class FakeElement {
  constructor() {
    this.innerHTML = "";
    this.textContent = "";
    this.className = "";
    this.attrs = {};
  }

  setAttribute(key, value) {
    this.attrs[key] = value;
  }
}

function fakeRoot(ids) {
  const elements = Object.fromEntries(ids.map((id) => [id, new FakeElement()]));
  return {
    getElementById(id) {
      return elements[id] || null;
    },
    elements,
  };
}

function sampleEvidence() {
  return {
    ok: false,
    status: "blocked",
    mode: "live",
    execution_mode: "live",
    ts_ms: 1_800_000_000_000,
    items: [
      {
        id: "broker.config_test",
        title: "Broker config and connection test",
        status: "blocked",
        severity: "critical",
        blocking: true,
        source_subsystem: "api.api_broker_config",
        source_route: "/api/broker/config",
        source_config_key: "broker.last_test",
        category: "broker",
        freshness: { age_ms: 120_000, stale: true },
        detail: "broker connection test is stale",
        remediation: "Run a passing broker connection test.",
      },
      {
        id: "providers.readiness",
        title: "Required provider readiness",
        status: "warning",
        severity: "warning",
        blocking: false,
        source_subsystem: "runtime.health",
        source_route: "/api/operator/provider_telemetry",
        source_config_key: "LIVE_DATA_REQUIRED_PROVIDERS",
        category: "data",
        freshness: { age_ms: 10_000, stale: false },
        detail: "provider degraded",
        remediation: "Fix provider.",
      },
    ],
    blockers: [
      {
        id: "broker.config_test",
        title: "Broker config and connection test",
        status: "blocked",
        severity: "critical",
        blocking: true,
        source_subsystem: "api.api_broker_config",
        category: "broker",
        freshness: { age_ms: 120_000, stale: true },
        remediation: "Run a passing broker connection test.",
      },
    ],
    warnings: [],
    unavailable: [],
    action_guards: {
      broker_activation: {
        allowed: false,
        requires_confirmation: true,
        blockers: [
          {
            id: "broker.config_test",
            title: "Broker config and connection test",
            status: "blocked",
            blocking: true,
            category: "broker",
            freshness: { age_ms: 120_000, stale: true },
          },
        ],
        warnings: [],
      },
    },
  };
}

test("readiness evidence normalization groups blockers without color-only state", () => {
  const model = normalizeReadinessEvidence(sampleEvidence());
  assert.equal(model.ok, false);
  assert.equal(model.status, "blocked");
  assert.equal(model.categories[0].category, "broker");
  assert.equal(model.categories[0].blocking, 1);
  assert.equal(model.items[0].status, "blocked");
});

test("readiness evidence renderer writes grouped blockers and text status tokens", () => {
  const root = fakeRoot([
    "readinessEvidencePanel",
    "readinessEvidenceMeta",
    "readinessEvidenceGroups",
    "readinessEvidenceBlockers",
    "readinessEvidenceNotes",
  ]);

  const model = renderReadinessEvidencePanel(sampleEvidence(), { root });

  assert.equal(model.status, "blocked");
  assert.match(root.elements.readinessEvidenceMeta.textContent, /BLOCKED/);
  assert.match(root.elements.readinessEvidenceGroups.innerHTML, /BLOCKED/);
  assert.match(root.elements.readinessEvidenceGroups.innerHTML, /#screen=execution/);
  assert.match(root.elements.readinessEvidenceBlockers.innerHTML, /Broker config and connection test/);
  assert.equal(root.elements.readinessEvidencePanel.attrs["data-readiness-status"], "blocked");
});

test("broker activation guard uses authoritative readiness evidence route", async () => {
  const url = readinessEvidenceUrlForBrokerActivation({ active_broker: "alpaca", paper_live_mode: "live" });
  assert.equal(url, "/api/operator/readiness_evidence?mode=live&execution_mode=live&broker=alpaca");

  const decision = brokerActivationReadinessDecision(sampleEvidence());
  assert.equal(decision.ok, false);
  assert.equal(decision.blockers[0].id, "broker.config_test");

  let fetchedUrl = "";
  const guarded = await guardBrokerActivationWithReadinessEvidence({
    payload: { active_broker: "alpaca", paper_live_mode: "live" },
    fetchJSON: async (nextUrl) => {
      fetchedUrl = nextUrl;
      return sampleEvidence();
    },
    toast: () => {},
  });

  assert.equal(fetchedUrl, url);
  assert.equal(guarded.ok, false);
  assert.equal(guarded.reason, "readiness_blocked");
});
