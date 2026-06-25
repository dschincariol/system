import assert from "node:assert/strict";
import test from "node:test";

import {
  applyConnectionSafetyGuard,
  createConnectionFreshnessModel,
  getConnectionStateSummary,
  renderConnectionCardMetadata,
  renderGlobalConnectionBanner,
  resetConnectionStateForTests,
  summarizeConnectionRows,
} from "../ui/panel_state.js";
import { fetchJSON } from "../ui/api_client.js";

const SPECS = [
  {
    key: "health",
    label: "Health",
    endpoint: "/api/health",
    group: "safety",
    cardIds: ["systemHealthCard"],
    staleMs: 100,
    critical: true,
    safetyCritical: true,
  },
  {
    key: "broker",
    label: "Broker",
    endpoint: "/api/broker",
    group: "broker",
    cardIds: ["brokerPanel"],
    staleMs: 100,
    critical: true,
    safetyCritical: true,
  },
];

class FakeClassList {
  constructor(el) {
    this.el = el;
  }

  add(...names) {
    const next = new Set(String(this.el.className || "").split(/\s+/).filter(Boolean));
    names.forEach((name) => next.add(name));
    this.el.className = Array.from(next).join(" ");
  }

  remove(...names) {
    const remove = new Set(names);
    this.el.className = String(this.el.className || "")
      .split(/\s+/)
      .filter((name) => name && !remove.has(name))
      .join(" ");
  }

  contains(name) {
    return String(this.el.className || "").split(/\s+/).includes(name);
  }
}

class FakeElement {
  constructor(tagName = "div", ownerDocument = null, id = "") {
    this.tagName = String(tagName || "div").toUpperCase();
    this.ownerDocument = ownerDocument;
    this.id = id;
    this.children = [];
    this.parentElement = null;
    this.dataset = {};
    this.attributes = {};
    this.style = {};
    this.className = "";
    this.classList = new FakeClassList(this);
    this.disabled = false;
    this.title = "";
    this._innerHTML = "";
    this._textContent = "";
  }

  appendChild(child) {
    child.parentElement = this;
    child.ownerDocument = child.ownerDocument || this.ownerDocument;
    this.children.push(child);
    return child;
  }

  insertBefore(child, before) {
    child.parentElement = this;
    child.ownerDocument = child.ownerDocument || this.ownerDocument;
    const idx = this.children.indexOf(before);
    if (idx >= 0) this.children.splice(idx, 0, child);
    else this.children.push(child);
    return child;
  }

  setAttribute(name, value) {
    this.attributes[name] = String(value);
  }

  removeAttribute(name) {
    delete this.attributes[name];
  }

  get firstChild() {
    return this.children[0] || null;
  }

  get nextSibling() {
    if (!this.parentElement) return null;
    const idx = this.parentElement.children.indexOf(this);
    return idx >= 0 ? this.parentElement.children[idx + 1] || null : null;
  }

  get innerHTML() {
    return this._innerHTML;
  }

  set innerHTML(value) {
    this._innerHTML = String(value ?? "");
  }

  get textContent() {
    return this._textContent;
  }

  set textContent(value) {
    this._textContent = String(value ?? "");
  }

  matches(selector) {
    if (selector.startsWith("#")) return this.id === selector.slice(1);
    if (selector.startsWith(".")) return String(this.className || "").split(/\s+/).includes(selector.slice(1));
    if (/^[a-z][a-z0-9-]*$/i.test(selector)) return this.tagName.toLowerCase() === selector.toLowerCase();
    return false;
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }

  querySelectorAll(selector) {
    const matches = [];
    const visit = (node) => {
      if (node !== this && node.matches(selector)) matches.push(node);
      node.children.forEach(visit);
    };
    this.children.forEach(visit);
    return matches;
  }
}

class FakeDocument {
  constructor(ids = []) {
    this.elements = new Map();
    this.body = this.createElement("body");
    ids.forEach((id) => this.addElement(id));
  }

  createElement(tagName) {
    return new FakeElement(tagName, this);
  }

  addElement(id, tagName = "div") {
    const el = new FakeElement(tagName, this, id);
    this.elements.set(id, el);
    this.body.appendChild(el);
    return el;
  }

  getElementById(id) {
    return this.elements.get(id) || null;
  }

  querySelectorAll(selector) {
    if (selector.startsWith("#")) {
      const el = this.getElementById(selector.slice(1));
      return el ? [el] : [];
    }
    return this.body.querySelectorAll(selector);
  }
}

test("connection freshness model reports connected, degraded, and disconnected states", () => {
  let now = 1000;
  const model = createConnectionFreshnessModel({ specs: SPECS, now: () => now });

  model.recordSuccess("/api/health?scope=dashboard", { startedAt: 980, endedAt: 1000, status: 200 });
  model.recordSuccess("/api/broker", { startedAt: 970, endedAt: 1000, status: 200 });
  let summary = model.summary();
  assert.equal(summary.state, "connected");
  assert.equal(summary.safetyGuardActive, false);
  assert.equal(summary.criticalRows.every((row) => row.state === "fresh"), true);

  now = 1125;
  summary = model.summary();
  assert.equal(summary.state, "degraded");
  assert.equal(summary.stale.length, 2);
  assert.equal(summary.safetyGuardActive, true);
  assert.match(summary.problemLines.join(" | "), /last good/);

  model.recordFailure("/api/health", { startedAt: 1130, endedAt: 1140, error: new Error("timeout") });
  summary = model.summary();
  assert.equal(summary.state, "degraded");
  assert.equal(summary.failed[0].label, "Health");

  const failedModel = createConnectionFreshnessModel({ specs: SPECS, now: () => now });
  failedModel.recordFailure("/api/health", { startedAt: 1130, endedAt: 1140, error: "down" });
  failedModel.recordFailure("/api/broker", { startedAt: 1130, endedAt: 1140, error: "down" });
  summary = failedModel.summary();
  assert.equal(summary.state, "disconnected");
  assert.equal(summary.safetyGuardActive, true);
});

test("connection summary exposes offline read-only fallback", () => {
  const summary = summarizeConnectionRows([], { offline: true, readOnly: true, nowMs: 1000 });
  assert.equal(summary.state, "offline_readonly");
  assert.equal(summary.safetyGuardActive, true);
  assert.equal(summary.readOnly, true);
});

test("connection banner, card metadata, and action guard render stale safety data", () => {
  let now = 1000;
  const model = createConnectionFreshnessModel({ specs: SPECS, now: () => now });
  model.recordSuccess("/api/health", { startedAt: 980, endedAt: 1000, status: 200 });
  model.recordSuccess("/api/broker", { startedAt: 970, endedAt: 1000, status: 200 });
  now = 1125;
  const summary = model.summary();

  const doc = new FakeDocument([
    "dashboardDegradedBanner",
    "dashboardDegradedBannerTitle",
    "dashboardDegradedBannerDetail",
    "systemHealthCard",
    "brokerPanel",
    "btnRunPipeline",
  ]);
  doc.getElementById("systemHealthCard").appendChild(doc.createElement("h2"));
  doc.getElementById("brokerPanel").appendChild(doc.createElement("h2"));

  renderGlobalConnectionBanner(doc, summary);
  assert.match(doc.getElementById("dashboardDegradedBanner").className, /\bwarn\b/);
  assert.equal(doc.getElementById("dashboardDegradedBanner").dataset.connectionState, "degraded");
  assert.equal(doc.getElementById("dashboardDegradedBannerTitle").textContent, "Connection: degraded/retrying");
  assert.match(doc.getElementById("dashboardDegradedBannerDetail").textContent, /Latest success/);

  renderConnectionCardMetadata(doc, summary);
  const healthMeta = doc.getElementById("systemHealthCard").querySelector(".panelConnectionMeta");
  assert.ok(healthMeta);
  assert.equal(healthMeta.dataset.connectionState, "stale");
  assert.match(healthMeta.innerHTML, /source <span class="mono">\/api\/health<\/span>/);
  assert.match(healthMeta.innerHTML, /last updated 125ms ago/);
  assert.match(healthMeta.innerHTML, /exceeds 100ms threshold/);

  const button = doc.getElementById("btnRunPipeline");
  applyConnectionSafetyGuard(doc, summary);
  assert.equal(button.disabled, true);
  assert.equal(button.attributes["aria-disabled"], "true");
  assert.equal(button.classList.contains("freshness-guarded-action"), true);
  assert.match(button.title, /Fresh safety data required/);

  now = 1130;
  model.recordSuccess("/api/health", { startedAt: 1120, endedAt: 1130, status: 200 });
  model.recordSuccess("/api/broker", { startedAt: 1120, endedAt: 1130, status: 200 });
  applyConnectionSafetyGuard(doc, model.summary());
  assert.equal(button.disabled, false);
  assert.equal(button.classList.contains("freshness-guarded-action"), false);
  assert.equal(button.attributes["aria-disabled"], undefined);
});

test("shared fetch client records critical read success and failure", async () => {
  const previousFetch = globalThis.fetch;
  resetConnectionStateForTests();
  try {
    globalThis.fetch = async (url) => new Response(JSON.stringify({ ok: true, path: url }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
    await fetchJSON("/api/health?surface=test", { allowBusinessFalse: true });
    let summary = getConnectionStateSummary({ offline: false });
    const health = summary.rows.find((row) => row.key === "health");
    assert.equal(health.state, "fresh");
    assert.equal(health.lastSuccessTs > 0, true);

    globalThis.fetch = async () => {
      throw new Error("network down");
    };
    await assert.rejects(() => fetchJSON("/api/broker", { allowBusinessFalse: true }), /network down/);
    summary = getConnectionStateSummary({ offline: false });
    const broker = summary.rows.find((row) => row.key === "broker");
    assert.equal(broker.state, "failed");
    assert.match(broker.lastError, /network down/);
  } finally {
    resetConnectionStateForTests();
    globalThis.fetch = previousFetch;
  }
});
