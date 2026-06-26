import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
  buildAlertItems,
  buildDataSourceItems,
  buildDecisionItems,
  buildJobItems,
  buildStaticCommandItems,
  filterCommandItems,
  fuzzyScoreText,
  initCommandPalette,
  isPaletteJobActionAllowed,
  isSafePaletteJobAction,
  normalizeDataSourceRows,
  normalizeDecisionRows,
  parseDecisionIdQuery,
} from "../ui/command_palette.mjs";
import { DASHBOARD_SCREEN_DEFINITIONS } from "../ui/view_router.js";

const ROOT = new URL("..", import.meta.url).pathname;

class FakeClassList {
  constructor(el) {
    this.el = el;
  }
  _set(next) {
    this.el.className = Array.from(next).join(" ");
  }
  add(...names) {
    const next = new Set(String(this.el.className || "").split(/\s+/).filter(Boolean));
    names.forEach((name) => next.add(name));
    this._set(next);
  }
  remove(...names) {
    const remove = new Set(names);
    const next = new Set(String(this.el.className || "").split(/\s+/).filter((name) => name && !remove.has(name)));
    this._set(next);
  }
  toggle(name, force) {
    const next = new Set(String(this.el.className || "").split(/\s+/).filter(Boolean));
    const shouldAdd = force === undefined ? !next.has(name) : !!force;
    if (shouldAdd) next.add(name);
    else next.delete(name);
    this._set(next);
  }
  contains(name) {
    return String(this.el.className || "").split(/\s+/).includes(name);
  }
}

class FakeElement {
  constructor(tagName = "div", ownerDocument = null) {
    this.tagName = String(tagName || "div").toUpperCase();
    this.ownerDocument = ownerDocument;
    this.id = "";
    this.children = [];
    this.parentElement = null;
    this.attributes = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.classList = new FakeClassList(this);
    this.textContent = "";
    this.value = "";
    this.title = "";
    this.href = "";
    this.eventListeners = {};
    this.focused = false;
    this.selected = false;
    this._innerHTML = "";
  }
  appendChild(child) {
    child.parentElement = this;
    child.ownerDocument = child.ownerDocument || this.ownerDocument;
    this.children.push(child);
    if (child.id && this.ownerDocument) this.ownerDocument.elements.set(child.id, child);
    return child;
  }
  setAttribute(name, value) {
    const key = String(name);
    const text = String(value);
    this.attributes[key] = text;
    if (key === "id") {
      this.id = text;
      if (this.ownerDocument) this.ownerDocument.elements.set(text, this);
    } else if (key === "class") {
      this.className = text;
    } else if (key === "href") {
      this.href = text;
    } else if (key.startsWith("data-")) {
      this.dataset[key.slice(5).replace(/-([a-z])/g, (_, ch) => ch.toUpperCase())] = text;
    }
  }
  getAttribute(name) {
    const key = String(name);
    if (key === "id") return this.id || null;
    if (key === "class") return this.className || null;
    if (key === "href") return this.href || this.attributes.href || null;
    return this.attributes[key] ?? null;
  }
  hasAttribute(name) {
    return Object.prototype.hasOwnProperty.call(this.attributes, String(name));
  }
  removeAttribute(name) {
    delete this.attributes[String(name)];
  }
  addEventListener(type, handler) {
    (this.eventListeners[type] ||= []).push(handler);
  }
  dispatchEvent(event) {
    const e = event || {};
    e.target ||= this;
    e.preventDefault ||= () => { e.defaultPrevented = true; };
    for (const handler of this.eventListeners[e.type] || []) handler(e);
    return !e.defaultPrevented;
  }
  click() {
    this.dispatchEvent({ type: "click" });
  }
  focus() {
    if (this.ownerDocument) this.ownerDocument.activeElement = this;
    this.focused = true;
  }
  select() {
    this.selected = true;
  }
  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }
  querySelectorAll(selector) {
    const out = [];
    const selectors = String(selector || "").split(",").map((part) => part.trim()).filter(Boolean);
    const visit = (node) => {
      if (node !== this && selectors.some((sel) => node.matches(sel))) out.push(node);
      node.children.forEach(visit);
    };
    this.children.forEach(visit);
    return out;
  }
  matches(selector) {
    const sel = String(selector || "").trim();
    if (!sel) return false;
    if (sel.startsWith("#")) return this.id === sel.slice(1);
    if (sel.startsWith(".")) return this.classList.contains(sel.slice(1));
    if (sel === "[data-command-palette]") return this.getAttribute("data-command-palette") != null;
    if (sel === "[data-screen-target]") return this.getAttribute("data-screen-target") != null;
    if (sel === "[id][data-screens]") return !!this.id && this.getAttribute("data-screens") != null;
    return this.tagName.toLowerCase() === sel.toLowerCase();
  }
  set innerHTML(value) {
    this._innerHTML = String(value || "");
    this.children = [];
    if (this._innerHTML.includes("dashboardCommandPaletteInput")) {
      const input = this.ownerDocument.createElement("input");
      input.id = "dashboardCommandPaletteInput";
      input.className = "commandPaletteInput";
      input.setAttribute("aria-expanded", "true");
      input.setAttribute("aria-controls", "dashboardCommandPaletteList");
      const close = this.ownerDocument.createElement("button");
      close.id = "dashboardCommandPaletteClose";
      const status = this.ownerDocument.createElement("div");
      status.id = "dashboardCommandPaletteStatus";
      status.className = "commandPaletteStatus";
      const list = this.ownerDocument.createElement("div");
      list.id = "dashboardCommandPaletteList";
      list.className = "commandPaletteList";
      this.appendChild(input);
      this.appendChild(close);
      this.appendChild(status);
      this.appendChild(list);
    } else if (this._innerHTML.includes("commandPaletteItemTitle")) {
      const title = this.ownerDocument.createElement("span");
      title.className = "commandPaletteItemTitle";
      const subtitle = this.ownerDocument.createElement("span");
      subtitle.className = "commandPaletteItemSubtitle";
      const badge = this.ownerDocument.createElement("span");
      badge.className = this._innerHTML.includes("is-confirm") ? "commandPaletteBadge is-confirm" : "commandPaletteBadge";
      this.appendChild(title);
      this.appendChild(subtitle);
      this.appendChild(badge);
    }
  }
  get innerHTML() {
    return this._innerHTML;
  }
}

class FakeDocument {
  constructor() {
    this.elements = new Map();
    this.eventListeners = {};
    this.head = this.createElement("head");
    this.body = this.createElement("body");
    this.activeElement = null;
  }
  createElement(tagName) {
    return new FakeElement(tagName, this);
  }
  getElementById(id) {
    return this.elements.get(String(id)) || null;
  }
  querySelectorAll(selector) {
    return this.body.querySelectorAll(selector);
  }
  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }
  addEventListener(type, handler) {
    (this.eventListeners[type] ||= []).push(handler);
  }
  dispatchEvent(event) {
    const e = event || {};
    e.target ||= this;
    e.preventDefault ||= () => { e.defaultPrevented = true; };
    for (const handler of this.eventListeners[e.type] || []) handler(e);
    return !e.defaultPrevented;
  }
}

test("fuzzy scoring rewards direct and sequential matches", () => {
  assert.ok(fuzzyScoreText("job", "Job Console") > 0);
  assert.ok(fuzzyScoreText("jc", "Job Console") > 0);
  assert.equal(fuzzyScoreText("zz", "Job Console"), 0);
  assert.ok(fuzzyScoreText("job", "Job Console") > fuzzyScoreText("jc", "Job Console"));
});

test("filterCommandItems sorts useful commands above weaker matches", () => {
  const items = [
    { id: "screen:overview", title: "Go to Overview", keywords: ["screen"], priority: 80 },
    { id: "panel:jobs", title: "Open Job Console", keywords: ["jobs"], priority: 60 },
    { id: "symbol:spy", title: "Focus symbol SPY", keywords: ["ticker"], priority: 40 },
  ];
  const results = filterCommandItems(items, "job", { limit: 5 });
  assert.equal(results[0].id, "panel:jobs");
});

test("decision ID parser accepts pasted IDs without broad text matches", () => {
  assert.equal(parseDecisionIdQuery("12345"), "12345");
  assert.equal(parseDecisionIdQuery("decision 12345"), "12345");
  assert.equal(parseDecisionIdQuery("#12345"), "12345");
  assert.equal(parseDecisionIdQuery("decision latest"), "");
});

test("palette job actions use backend safety metadata", () => {
  const pollPrices = {
    name: "poll_prices",
    safety: "data_refresh",
    action_policy: { start: { enabled: true }, stop: { enabled: true } },
  };
  const brokerApplyOrders = {
    name: "broker_apply_orders",
    safety: "execution_sensitive",
    action_policy: { start: { enabled: true, safety_confirmation_required: true } },
  };
  const unavailable = {
    name: "llm_factor_discovery",
    safety: "unavailable",
    action_policy: { start: { enabled: false, disabled_reason: "Missing prerequisite" } },
  };

  assert.equal(isSafePaletteJobAction(pollPrices, "start"), true);
  assert.equal(isPaletteJobActionAllowed(pollPrices, "start"), true);
  assert.equal(isSafePaletteJobAction(brokerApplyOrders, "start"), false);
  assert.equal(isPaletteJobActionAllowed(brokerApplyOrders, "start"), true);
  assert.equal(isSafePaletteJobAction(unavailable, "start"), false);
  assert.equal(isPaletteJobActionAllowed(unavailable, "start"), false);
  assert.equal(isSafePaletteJobAction("poll_prices", "start"), false);
  assert.equal(isSafePaletteJobAction(pollPrices, "delete"), false);
});

test("static command items cover every dashboard screen and DOM navigation target", () => {
  const doc = new FakeDocument();
  const terminal = doc.createElement("a");
  terminal.id = "appTerminalLink";
  terminal.href = "/ui/terminal/terminal.html?symbol=SPY";
  terminal.textContent = "Terminal";
  terminal.setAttribute("data-command-palette", "true");
  terminal.setAttribute("data-command-title", "Open Trading Terminal");
  terminal.setAttribute("data-command-type", "Navigation");
  terminal.setAttribute("data-command-context", "Terminal / active symbol context");
  doc.body.appendChild(terminal);

  const operator = doc.createElement("a");
  operator.id = "appOperatorLink";
  operator.href = "/operator/";
  operator.textContent = "Operator";
  operator.setAttribute("data-command-palette", "true");
  operator.setAttribute("data-command-title", "Open Operator Console");
  operator.setAttribute("data-command-type", "Navigation");
  doc.body.appendChild(operator);

  const dataSources = doc.createElement("a");
  dataSources.id = "appDataSourcesLink";
  dataSources.href = "/ui/data_sources.html";
  dataSources.textContent = "Sources";
  dataSources.setAttribute("data-command-palette", "true");
  dataSources.setAttribute("data-command-title", "Open Data Sources");
  dataSources.setAttribute("data-command-type", "Navigation");
  doc.body.appendChild(dataSources);

  const items = buildStaticCommandItems(doc, {
    screenDefinitions: DASHBOARD_SCREEN_DEFINITIONS,
    isScreenAllowed: (screen) => screen !== "analyze",
    getPersonaLabel: () => "Operations",
  });
  const ids = new Set(items.map((item) => item.id));

  for (const screen of DASHBOARD_SCREEN_DEFINITIONS) {
    assert.equal(ids.has(`screen:${screen.key}`), true, `${screen.key} screen missing`);
  }
  assert.equal(ids.has("nav:appterminallink"), true);
  assert.equal(ids.has("nav:appoperatorlink"), true);
  assert.equal(ids.has("nav:appdatasourceslink"), true);
  assert.match(items.find((item) => item.id === "screen:analyze").subtitle, /Hidden in Operations/);
});

test("dynamic item builders include decisions, alerts, data sources, and confirmed job actions", async () => {
  const decisions = normalizeDecisionRows({
    decisions: [{ id: 42, symbol: "SPY", action: "BUY", certainty: 0.91, risk_impact: "low" }],
  });
  assert.equal(decisions.length, 1);
  assert.equal(buildDecisionItems(decisions, { openDecision: () => {} })[0].id, "decision-row:42");

  let openedAlert = "";
  const alertItem = buildAlertItems({
    rows: [{ id: 7, severity: "CRIT", symbol: "QQQ", event_title: "Execution degraded", ts_ms: 1000 }],
  }, {
    openAlert: (row) => { openedAlert = row.id; },
  })[0];
  assert.equal(alertItem.badge, "Alert");
  await alertItem.run();
  assert.equal(openedAlert, "7");

  const sources = normalizeDataSourceRows({
    sources: [{ source_key: "prices:polygon", display_name: "Polygon Prices", provider_name: "polygon", enabled: true, runnable_state: "healthy" }],
  });
  assert.equal(buildDataSourceItems(sources, { openDataSource: () => {} })[0].id, "data-source:prices:polygon");

  const guarded = buildJobItems(null, [{
    name: "broker_apply_orders",
    running: false,
    safety: "execution_sensitive",
    action_policy: { start: { enabled: true, safety_confirmation_required: true } },
  }], {
    runJobAction: () => {},
  }).find((item) => item.id === "job-start:broker_apply_orders");
  assert.equal(guarded.confirm, true);
  assert.equal(guarded.badge, "Guarded");
});

test("keyboard palette flow opens, searches, selects, restores focus, and clears active descendant", async () => {
  const previousWindow = globalThis.window;
  globalThis.window = { __dashboardCommandPalette: null };
  const doc = new FakeDocument();
  const prior = doc.createElement("button");
  prior.id = "beforePalette";
  doc.body.appendChild(prior);
  prior.focus();

  const runs = [];
  const palette = initCommandPalette({
    document: doc,
    enableDynamic: false,
    screenDefinitions: [{ key: "overview", label: "Overview", aliases: ["home"], keywords: ["status"] }],
    navigateToScreen: (screen) => runs.push(screen),
    limit: 5,
  });

  try {
    doc.dispatchEvent({ type: "keydown", key: "k", ctrlKey: true });
    const root = doc.getElementById("dashboardCommandPaletteRoot");
    const input = doc.getElementById("dashboardCommandPaletteInput");
    const status = doc.getElementById("dashboardCommandPaletteStatus");
    assert.equal(root.classList.contains("is-open"), true);
    assert.equal(input.getAttribute("aria-expanded"), "true");
    assert.match(status.textContent, /1 result/);
    assert.equal(input.getAttribute("aria-activedescendant"), "dashboardCommandPaletteOption0");

    input.value = "overview";
    input.dispatchEvent({ type: "input" });
    input.dispatchEvent({ type: "keydown", key: "Enter" });
    assert.deepEqual(runs, ["overview"]);
    assert.equal(root.classList.contains("is-open"), false);
    assert.equal(input.getAttribute("aria-expanded"), "false");
    assert.equal(input.getAttribute("aria-activedescendant"), null);

    palette.open();
    input.dispatchEvent({ type: "keydown", key: "Escape" });
    assert.equal(root.classList.contains("is-open"), false);
    assert.equal(doc.activeElement, prior);
    assert.equal(input.getAttribute("aria-activedescendant"), null);
  } finally {
    globalThis.window = previousWindow;
  }
});

test("command palette source does not mutate jobs directly", () => {
  const source = readFileSync(join(ROOT, "ui", "command_palette.mjs"), "utf8");
  assert.doesNotMatch(source, /\/api\/jobs\/start/);
  assert.doesNotMatch(source, /\/api\/jobs\/stop/);
  assert.doesNotMatch(source, /postJSON/);
  assert.match(source, /runJobAction/);
});
