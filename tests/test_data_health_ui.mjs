import assert from "node:assert/strict";
import test from "node:test";

import {
  DATA_HEALTH_FEATURE_VISIBILITY_TARGETS,
  fetchDataHealthScreen,
  normalizeDataHealthScreen,
  renderDataHealthScreen,
} from "../ui/data_health.js";

const NOW = Date.UTC(2026, 0, 2, 12, 0, 0);
const FRESH_TS = NOW - 12_000;

function fulfilled(value) {
  return { status: "fulfilled", value };
}

function rejected(reason) {
  return { status: "rejected", reason };
}

class FakeClassList {
  constructor(element) {
    this.element = element;
  }

  _tokens() {
    return new Set(String(this.element.className || "").split(/\s+/).filter(Boolean));
  }

  contains(token) {
    return this._tokens().has(token);
  }

  add(...tokens) {
    const next = this._tokens();
    for (const token of tokens) next.add(token);
    this.element.className = [...next].join(" ");
  }

  remove(...tokens) {
    const next = this._tokens();
    for (const token of tokens) next.delete(token);
    this.element.className = [...next].join(" ");
  }
}

class FakeElement {
  constructor(id, className = "") {
    this.id = id;
    this.className = className;
    this.innerHTML = "";
    this.textContent = "";
    this.dataset = {};
    this.attributes = {};
    this.classList = new FakeClassList(this);
    this.offsetWidth = 1;
  }

  setAttribute(name, value) {
    this.attributes[name] = String(value);
  }

  getAttribute(name) {
    return this.attributes[name] || null;
  }
}

function fakeDocument() {
  const elements = new Map([
    ["dataHealthSummaryGrid", new FakeElement("dataHealthSummaryGrid")],
    ["dataPipelinePill", new FakeElement("dataPipelinePill", "pill dim")],
    ["dataProvidersPill", new FakeElement("dataProvidersPill", "pill dim")],
    ["dataFreshnessPill", new FakeElement("dataFreshnessPill", "pill dim")],
    ["dataBarrierPill", new FakeElement("dataBarrierPill", "pill dim")],
    ["dataTelemetryPill", new FakeElement("dataTelemetryPill", "pill dim")],
    ["dataUpdatedPill", new FakeElement("dataUpdatedPill", "pill dim mono")],
    ["dataHealthNotes", new FakeElement("dataHealthNotes")],
    ["dataProvidersMeta", new FakeElement("dataProvidersMeta", "pill dim meta-pill-offset")],
    ["dataProvidersBody", new FakeElement("dataProvidersBody")],
    ["dataProvidersFallback", new FakeElement("dataProvidersFallback")],
    ["dataRuntimeGrid", new FakeElement("dataRuntimeGrid")],
    ["dataRuntimeNotes", new FakeElement("dataRuntimeNotes")],
  ]);
  return {
    elements,
    getElementById(id) {
      return elements.get(id) || null;
    },
  };
}

function healthyResults() {
  return {
    ingestion: fulfilled({
      ok: true,
      running: true,
      status: "RUNNING",
      active_child: "prices",
      healthy_providers: 1,
      fresh_rows: 42,
      fresh_symbols: 3,
      price_age_ms: 12_000,
      last_price_ts_ms: FRESH_TS,
      updated_ts_ms: FRESH_TS,
      summary: {
        active_child: "prices",
        visible_jobs_running: 2,
      },
    }),
    feedStatus: fulfilled({
      ok: true,
      price_freshness: {
        ok: true,
        status: "fresh",
        source: "prices",
        age_s: 12,
        last_ts_ms: FRESH_TS,
      },
    }),
    telemetry: fulfilled({
      system_state: "LIVE",
      ts_ms: FRESH_TS,
      cpu_percent: 5.5,
      thread_count: 8,
      process_rss_mb: 250,
      memory_percent: 2.5,
      db_size_mb: 99.2,
      providers: { healthy: 1, total: 1 },
      alerts: { last_hour: 0, critical_open: 0 },
      execution: { n_fills: 4, last_fill_ts_ms: FRESH_TS },
      supervisor: { n_jobs: 6, delegated: false },
      health: { ok: true },
      vol_target: { enabled: true, target_vol: 0.02, scale: 1.1 },
    }),
    barrier: fulfilled({ allowed: true, ts_ms: FRESH_TS }),
    provider: fulfilled({
      provider_telemetry: {
        healthy_providers: 1,
        price_age_ms: 12_000,
        last_price_ts_ms: FRESH_TS,
        updated_ts_ms: FRESH_TS,
        child_pid: 123,
        providers: {
          polygon: {
            ok: true,
            running: true,
            last_price_ts_ms: FRESH_TS,
            owner: "ingestion",
            last_seq: 10,
          },
        },
      },
    }),
    featureVisibility: fulfilled({
      ok: true,
      structured_documents: { status: "available" },
      graph_features: { status: "shadow_only" },
    }),
  };
}

test("data health fetch boundary owns the screen endpoints", async () => {
  const calls = [];
  const fetchJSON = async (path, options = undefined) => {
    calls.push([path, options]);
    return { ok: true, path };
  };

  const result = await fetchDataHealthScreen(fetchJSON);

  assert.deepEqual(calls.map((call) => call[0]), [
    "/api/ingestion/status",
    "/api/feeds",
    "/api/telemetry",
    "/api/execution/barrier",
    "/api/operator/provider_telemetry",
    "/api/data/feature_visibility?limit=12",
  ]);
  assert.deepEqual(calls[3][1], { allowBusinessFalse: true });
  assert.deepEqual(calls[5][1], { allowBusinessFalse: true });
  assert.equal(result.feedStatus.status, "fulfilled");
  assert.equal(result.provider.status, "fulfilled");
});

test("data health normalization keeps provider, freshness, and fallback state explicit", () => {
  const model = normalizeDataHealthScreen({
    ...healthyResults(),
    featureVisibility: rejected(new Error("route unavailable")),
  }, { nowMs: NOW });

  assert.equal(model.summaryStats[0].value, "RUNNING");
  assert.equal(model.summaryStats[4].value, "1/1");
  assert.equal(model.providerEntries.length, 1);
  assert.equal(model.providerEntries[0].name, "polygon");
  assert.equal(model.providerEntries[0].statusText, "LIVE");
  assert.match(model.providersFallback, /active child prices/);
  assert.match(model.providersFallback, /pid 123/);
  assert.ok(model.healthNotes.includes("structured document and graph feature visibility unavailable"));
  assert.deepEqual(model.featureVisibility, {
    ok: false,
    structured_documents: {
      status: "unavailable",
      warnings: ["feature visibility route unavailable"],
    },
    graph_features: {
      status: "unavailable",
      warnings: ["feature visibility route unavailable"],
    },
  });
});

test("data health renderer writes the existing dashboard DOM contract", () => {
  const model = normalizeDataHealthScreen(healthyResults(), { nowMs: NOW });
  const doc = fakeDocument();
  let featureTargets = null;
  let featurePayload = null;

  renderDataHealthScreen(model, {
    document: doc,
    renderFeatureVisibility(targets, payload) {
      featureTargets = targets;
      featurePayload = payload;
    },
  });

  assert.deepEqual(featureTargets, DATA_HEALTH_FEATURE_VISIBILITY_TARGETS);
  assert.equal(featurePayload.ok, true);
  assert.match(doc.getElementById("dataHealthSummaryGrid").innerHTML, /Pipeline/);
  assert.equal(doc.getElementById("dataPipelinePill").textContent, "pipeline RUNNING");
  assert.equal(doc.getElementById("dataProvidersPill").textContent, "providers 1/1");
  assert.equal(doc.getElementById("dataProvidersMeta").textContent, "1/1 healthy");
  assert.match(doc.getElementById("dataProvidersBody").innerHTML, /polygon/);
  assert.match(doc.getElementById("dataProvidersBody").innerHTML, /seq 10/);
  assert.match(doc.getElementById("dataRuntimeGrid").innerHTML, /Supervisor/);
  assert.match(doc.getElementById("dataRuntimeNotes").innerHTML, /execution barrier clear/);
});
