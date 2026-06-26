import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  STATUS_TOKENS,
  buildTableView,
  compareTableValues,
  filterTableRows,
  statusClassName,
  statusPillClasses,
  sortTableRows,
} from "../ui/utils.js";
import {
  ALERT_SEVERITY_ORDER,
  alertLifecycleState,
  buildAlertIncidentGroups,
  cellColor,
  defaultAlertRecommendedAction,
  filterAlerts,
  normalizeAlert,
  normalizeSeverity,
  renderHeatmap,
  renderIncidentQueue,
  severityRank,
  summarizeAlertLifecycleCounts,
} from "../ui/alerts.js";
import {
  alertLifecycleSummary,
  _ifNothingChanges,
  _meaningForAlert,
  normalizeAlertLifecycle,
  notificationPolicySummary,
  renderAlertLifecycleCountSummary,
  _recommendedPosture,
  _safeToIgnore,
  _scoreCell,
  openIncidentDrawer,
} from "../ui/alerts_ui.js";
import { initDecisionBarEngine, updateDecisionHeader } from "../ui/decision_bar.js";

const columns = [
  { key: "symbol", accessor: (row) => row && row.symbol },
  { key: "qty", accessor: (row) => row && row.qty },
  { key: "updated", accessor: (row) => row && row.updated },
  { key: "meta", accessor: (row) => row && row.meta },
];

function hexToRgb(hex) {
  const normalized = String(hex || "").replace("#", "");
  return [0, 2, 4].map((start) => Number.parseInt(normalized.slice(start, start + 2), 16) / 255);
}

function relativeLuminance(hex) {
  const [r, g, b] = hexToRgb(hex).map((channel) => (
    channel <= 0.03928
      ? channel / 12.92
      : ((channel + 0.055) / 1.055) ** 2.4
  ));
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrastRatio(a, b) {
  const high = Math.max(relativeLuminance(a), relativeLuminance(b));
  const low = Math.min(relativeLuminance(a), relativeLuminance(b));
  return (high + 0.05) / (low + 0.05);
}

class FakeElement {
  constructor(id = "") {
    this.id = id;
    this.children = [];
    this.dataset = {};
    this.style = {};
    this.attributes = {};
    this.listeners = {};
    this.className = "";
    this.disabled = false;
    this.hidden = false;
    this.tabIndex = 0;
    this._innerHTML = "";
    this._textContent = "";
  }

  appendChild(child) {
    this.children.push(child);
    child.parentElement = this;
    return child;
  }

  setAttribute(name, value) {
    this.attributes[name] = String(value);
  }

  removeAttribute(name) {
    delete this.attributes[name];
  }

  addEventListener(name, handler) {
    this.listeners[name] = handler;
  }

  querySelector() {
    return null;
  }

  closest() {
    return null;
  }

  contains(node) {
    return this === node || this.children.includes(node);
  }

  get lastElementChild() {
    return this.children[this.children.length - 1] || null;
  }

  get innerHTML() {
    return this._innerHTML;
  }

  set innerHTML(value) {
    this._innerHTML = String(value ?? "");
    if (this._innerHTML === "") this.children = [];
  }

  get textContent() {
    return this._textContent;
  }

  set textContent(value) {
    this._textContent = String(value ?? "");
  }
}

async function withFakeDocument(ids, fn) {
  const previousDocument = globalThis.document;
  const previousWindow = globalThis.window;
  const elements = new Map(ids.map((id) => [id, new FakeElement(id)]));
  const root = new FakeElement("root");
  globalThis.document = {
    body: root,
    head: new FakeElement("head"),
    createElement: (tag) => new FakeElement(tag),
    getElementById: (id) => elements.get(id) || null,
    querySelectorAll: () => [],
  };
  globalThis.window = {
    __LAST_ALERTS_FAILED__: false,
  };
  try {
    return await fn(elements);
  } finally {
    globalThis.document = previousDocument;
    globalThis.window = previousWindow;
  }
}

test("table helpers filter nested values without mutating malformed payloads", () => {
  const rows = [
    { symbol: "SPY", qty: "10", meta: { route: "broker live" } },
    null,
    { symbol: "QQQ", qty: "5", meta: ["portfolio", "intent"] },
  ];

  const filtered = filterTableRows(rows, columns, "intent");
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].symbol, "QQQ");
  assert.equal(rows.length, 3);
  assert.deepEqual(filterTableRows("bad", columns, "x"), []);
});

test("table helpers sort numbers, dates, strings, nulls, and bad rows safely", () => {
  const rows = [
    { symbol: "Beta", qty: "$1,200.50", updated: "2026-05-10T12:00:00Z" },
    { symbol: "alpha", qty: "5", updated: "2026-05-12T12:00:00Z" },
    { symbol: "Gamma", qty: "15%", updated: "2026-05-11T12:00:00Z" },
    { symbol: "", qty: null, updated: null },
    null,
  ];

  assert.deepEqual(sortTableRows(rows, columns, "qty", "asc").map((row) => row && row.symbol), [
    "alpha",
    "Gamma",
    "Beta",
    "",
    null,
  ]);
  assert.deepEqual(sortTableRows(rows, columns, "qty", "desc").map((row) => row && row.symbol), [
    "Beta",
    "Gamma",
    "alpha",
    "",
    null,
  ]);
  assert.deepEqual(sortTableRows(rows, columns, "updated", "desc").slice(0, 3).map((row) => row && row.symbol), [
    "alpha",
    "Gamma",
    "Beta",
  ]);
  assert.equal(compareTableValues("2026-05-10T00:00:00Z", "2026-05-11T00:00:00Z"), -1);
  assert.equal(compareTableValues(new Date("2026-05-10T00:00:00Z"), new Date("2026-05-11T00:00:00Z")), -1);
});

test("buildTableView returns stable counts and max-row slices", () => {
  const rows = [
    { symbol: "MSFT", qty: 2 },
    { symbol: "AAPL", qty: 3 },
    { symbol: "AMZN", qty: 4 },
  ];
  const view = buildTableView(rows, columns, {
    query: "a",
    sortKey: "symbol",
    sortDir: "asc",
    maxRows: 1,
  });

  assert.deepEqual(view.visibleRows.map((row) => row.symbol), ["AAPL"]);
  assert.equal(view.totalRows, 3);
  assert.equal(view.filteredRowsCount, 2);
  assert.equal(view.hiddenRowsCount, 1);
});

test("dashboard and terminal table wiring uses shared helpers and preserves drilldowns", () => {
  const dashboardHtml = readFileSync(new URL("../ui/dashboard.html", import.meta.url), "utf8");
  const dashboardJs = readFileSync(new URL("../ui/dashboard.js", import.meta.url), "utf8");
  const terminalHtml = readFileSync(new URL("../ui/terminal/terminal.html", import.meta.url), "utf8");
  const terminalJs = readFileSync(new URL("../ui/terminal/terminal.js", import.meta.url), "utf8");

  assert.match(dashboardJs, /buildTableView/);
  assert.match(terminalJs, /buildTableView/);

  for (const tableId of ["recentDecisions", "executionOrders", "executionFills", "suppressedTrades", "executionBySymbolTca", "executionOutcomes", "executionTrace"]) {
    assert.match(dashboardHtml, new RegExp(`data-dashboard-table-filter="${tableId}"`));
  }
  for (const tableId of ["executionOrders", "executionFills", "suppressedTrades", "executionBySymbolTca", "executionOutcomes", "executionTrace"]) {
    assert.match(dashboardHtml, new RegExp(`data-dashboard-table-sort="${tableId}"`));
  }
  for (const inputId of ["posFilter", "ordFilter", "fillsFilter", "ordStatusFilter", "fillsStatusFilter"]) {
    assert.match(terminalHtml, new RegExp(`id="${inputId}"`));
  }
  for (const elementId of ["ordersMeta", "terminalTcaSummary"]) {
    assert.match(terminalHtml, new RegExp(`id="${elementId}"`));
  }
  assert.match(terminalJs, /data-terminal-table-sort/);
  assert.match(terminalJs, /status_bucket/);
  assert.match(terminalJs, /reasonText/);
  assert.match(terminalJs, /fill_vwap/);
  assert.match(terminalJs, /implementation_shortfall_bps/);
  assert.match(terminalJs, /child_fills/);
  assert.match(terminalJs, /renderTerminalTcaSummary/);
  assert.match(terminalJs, /setTerminalStatusFilter/);

  assert.match(dashboardJs, /renderExecutionOrdersRows[\s\S]*decisionLookupForOrderIntent[\s\S]*_decisionLookupAttr/);
  assert.match(dashboardJs, /renderExecutionFillsRows[\s\S]*normalizeDecisionLookup[\s\S]*_decisionLookupAttr/);
  assert.match(dashboardJs, /renderSuppressedRows[\s\S]*_decisionLookupAttr/);
  assert.match(dashboardJs, /renderExecutionOutcomeRows[\s\S]*normalizeDecisionLookup[\s\S]*_decisionLookupAttr/);
  assert.match(dashboardJs, /renderExecutionTraceRows[\s\S]*normalizeDecisionLookup[\s\S]*_decisionLookupAttr/);
});

test("terminal blotter rows sort and filter by status reason and TCA fields", () => {
  const blotterColumns = [
    { key: "symbol", accessor: (row) => row && row.symbol },
    { key: "status_bucket", accessor: (row) => row && `${row.status_bucket} ${row.status_label}` },
    { key: "reasonText", accessor: (row) => row && `${row.reason_code || ""} ${row.reason || ""}` },
    { key: "slippage_bps", accessor: (row) => row && row.slippage_bps, searchable: false },
    { key: "implementation_shortfall_bps", accessor: (row) => row && row.implementation_shortfall_bps, searchable: false },
    { key: "child_fill_count", accessor: (row) => row && `${row.child_fill_count || 1} ${row.client_order_id || ""}` },
  ];
  const rows = [
    { symbol: "AAPL", status_bucket: "filled", status_label: "Filled", slippage_bps: 4.5, implementation_shortfall_bps: 5.0, child_fill_count: 1, client_order_id: "cid-1" },
    { symbol: "MSFT", status_bucket: "partial", status_label: "Partial fills", slippage_bps: 18.2, implementation_shortfall_bps: 20.0, child_fill_count: 3, client_order_id: "cid-2" },
    { symbol: "TSLA", status_bucket: "suppressed", status_label: "Suppressed", reason_code: "max_position", reason: "Max position cap", slippage_bps: null, child_fill_count: 0, client_order_id: "supp-1" },
  ];

  assert.deepEqual(
    buildTableView(rows, blotterColumns, { query: "suppressed", sortKey: "symbol", sortDir: "asc" }).visibleRows.map((row) => row.symbol),
    ["TSLA"]
  );
  assert.deepEqual(
    buildTableView(rows, blotterColumns, { query: "cid-2", sortKey: "symbol", sortDir: "asc" }).visibleRows.map((row) => row.symbol),
    ["MSFT"]
  );
  assert.deepEqual(
    buildTableView(rows, blotterColumns, { query: "", sortKey: "slippage_bps", sortDir: "desc" }).visibleRows.map((row) => row.symbol),
    ["MSFT", "AAPL", "TSLA"]
  );
});

test("shared status vocabulary covers desktop mobile terminal states", () => {
  assert.deepEqual(Object.keys(STATUS_TOKENS), [
    "neutral",
    "info",
    "ok",
    "warn",
    "high",
    "crit",
    "blocked",
    "unavailable",
  ]);

  assert.equal(statusClassName("bad"), "crit");
  assert.equal(statusClassName("danger"), "crit");
  assert.equal(statusClassName("dim"), "neutral");
  assert.equal(statusClassName("blocked"), "blocked");
  assert.equal(statusPillClasses("blocked").includes("status-blocked"), true);

  assert.equal(normalizeAlert({ severity: "HIGH", message: "elevated" }).severity, "HIGH");
  assert.equal(severityRank("CRIT") > severityRank("HIGH"), true);
  assert.equal(severityRank("HIGH") > severityRank("WARN"), true);
  assert.equal(cellColor({ severity: "HIGH", message: "elevated" }).key, "high");
  assert.equal(cellColor({ severity: "CRIT", message: "critical" }).key, "crit");
});

test("alert severity normalization keeps HIGH between WARN and CRIT", () => {
  assert.deepEqual(ALERT_SEVERITY_ORDER, ["INFO", "WARN", "HIGH", "CRIT"]);
  assert.equal(normalizeSeverity("high"), "HIGH");
  assert.equal(normalizeSeverity("WARNING"), "WARN");
  assert.equal(normalizeSeverity("critical"), "CRIT");
  assert.equal(severityRank("INFO") < severityRank("WARN"), true);
  assert.equal(severityRank("WARN") < severityRank("HIGH"), true);
  assert.equal(severityRank("HIGH") < severityRank("CRIT"), true);
});

test("WARN+ alert filtering includes WARN HIGH and CRIT only", () => {
  const now = Date.now();
  const rows = [
    { id: "info", severity: "INFO", ts_ms: now, message: "info" },
    { id: "warn", severity: "WARN", ts_ms: now, message: "warn" },
    { id: "high", severity: "HIGH", ts_ms: now, message: "high" },
    { id: "crit", severity: "CRIT", ts_ms: now, message: "crit" },
  ];
  const localState = {
    isAcked: () => false,
    isSnoozed: () => false,
  };

  assert.deepEqual(
    filterAlerts(rows, { sev: "WARN", rangeMs: 60_000 }, localState).map((row) => row.id),
    ["warn", "high", "crit"]
  );
  assert.deepEqual(
    filterAlerts(rows, { sev: "HIGH", rangeMs: 60_000 }, localState).map((row) => row.id),
    ["high", "crit"]
  );
});

test("alert lifecycle helpers preserve shelved and re-triggered state", () => {
  const now = Date.now();
  const row = normalizeAlert({
    id: "life-1",
    severity: "HIGH",
    ts_ms: now - 60_000,
    message: "latency high",
    acked: false,
    ack_expired: true,
    retriggered: true,
    acked_by: "ops",
    ack_expires_ts_ms: now - 10_000,
    shelved: true,
    shelved_ts_ms: now,
    shelve_expires_ts_ms: now + 900_000,
    shelve_reason: "known exchange status incident",
    lifecycle: [
      { ts_ms: now - 60_000, state: "triggered", actor: "system", reason: "latency high" },
      { ts_ms: now - 10_000, state: "retriggered", actor: "ops", reason: "ack timeout expired before resolution" },
    ],
    notification_policy: {
      severity: "HIGH",
      suppressed: true,
      rate_limit_ms: 600_000,
      next_escalation_ts_ms: now + 900_000,
      explanation: "notifications suppressed while shelved",
    },
  });
  const localState = {
    isAcked: () => false,
    isSnoozed: () => false,
  };

  assert.equal(row.shelved, true);
  assert.equal(row.ack_expired, true);
  assert.equal(row.shelve_reason, "known exchange status incident");
  assert.deepEqual(
    normalizeAlertLifecycle(row).map((item) => item.state),
    ["triggered", "retriggered", "shelved"]
  );
  assert.match(alertLifecycleSummary(row, now), /Shelved until/);
  assert.match(notificationPolicySummary(row, now), /HIGH notifications suppressed/);
  assert.deepEqual(
    filterAlerts([row], { sev: "WARN", rangeMs: 120_000, changedOnly: true }, localState).map((item) => item.id),
    []
  );
  assert.deepEqual(
    filterAlerts([row], { sev: "WARN", rangeMs: 120_000, changedOnly: false }, localState).map((item) => item.id),
    ["life-1"]
  );
});

test("alert lifecycle summary separates alarms, notifications, stale, and shelved unresolved states", async () => {
  const now = Date.now();
  const rows = [
    { id: "info", severity: "INFO", ts_ms: now - 5_000, message: "background context" },
    {
      id: "acked",
      severity: "HIGH",
      ts_ms: now - 60_000,
      message: "risk elevated",
      acked: true,
      acked_ts_ms: now - 30_000,
      ack_expires_ts_ms: now + 600_000,
    },
    {
      id: "shelved",
      severity: "WARN",
      ts_ms: now - 60_000,
      message: "known upstream outage",
      shelved: true,
      shelved_ts_ms: now - 10_000,
      shelve_expires_ts_ms: now + 900_000,
      notification_policy: { suppressed: true, severity: "WARN" },
    },
    {
      id: "stale",
      severity: "CRIT",
      ts_ms: now - 60 * 60 * 1000,
      message: "data path broken",
      rule_id: "DATA_PATH_BROKEN",
    },
    {
      id: "resolved",
      severity: "HIGH",
      ts_ms: now - 60_000,
      message: "fixed",
      status: "resolved",
      resolved_ts_ms: now - 1_000,
    },
  ];

  const summary = summarizeAlertLifecycleCounts(rows, now);
  assert.equal(summary.alarms, 3);
  assert.equal(summary.notifications, 1);
  assert.equal(summary.acknowledged, 1);
  assert.equal(summary.shelved, 1);
  assert.equal(summary.suppressed, 1);
  assert.equal(summary.stale, 1);
  assert.equal(summary.resolved, 1);
  assert.equal(alertLifecycleState(rows[1], now).label, "Acknowledged, unresolved");
  assert.equal(alertLifecycleState(rows[2], now).label, "Shelved, unresolved");
  assert.match(defaultAlertRecommendedAction(rows[1], now), /Acknowledged only/);
  assert.match(defaultAlertRecommendedAction(rows[2], now), /Shelved only/);

  await withFakeDocument(["summary"], (elements) => {
    renderAlertLifecycleCountSummary(elements.get("summary"), rows, now);
    assert.match(elements.get("summary").innerHTML, /Open alarms/);
    assert.match(elements.get("summary").innerHTML, /Acknowledged/);
    assert.match(elements.get("summary").innerHTML, /still unresolved/);
    assert.match(elements.get("summary").innerHTML, /Shelved/);
    assert.match(elements.get("summary").innerHTML, /Stale/);
    assert.match(elements.get("summary").attributes["aria-label"], /shelved, 1 suppressed, 1 stale/);
  });
});

test("alert floods collapse into parent incidents with lifecycle and action labels", async () => {
  const now = Date.now();
  const rows = [
    { id: "flood-1", symbol: "AAPL", severity: "WARN", horizon_s: 60, rule_id: "FEED_STALE", ts_ms: now - 4_000, message: "feed stale 10s" },
    { id: "flood-2", symbol: "AAPL", severity: "HIGH", horizon_s: 60, rule_id: "FEED_STALE", ts_ms: now - 3_000, message: "feed stale 12s" },
    { id: "flood-3", symbol: "AAPL", severity: "WARN", horizon_s: 60, rule_id: "FEED_STALE", ts_ms: now - 2_000, message: "feed stale 14s" },
    { id: "solo", symbol: "MSFT", severity: "CRIT", horizon_s: 60, rule_id: "BROKER_DOWN", ts_ms: now - 1_000, message: "broker down" },
  ];

  const groups = buildAlertIncidentGroups(rows, now);
  const flood = groups.find((group) => group.key.includes("FEED_STALE".toLowerCase()));
  assert.ok(flood);
  assert.equal(flood.count, 3);
  assert.equal(flood.parent.id, "flood-2");
  assert.equal(flood.lifecycle.actionability, "alarm");

  await withFakeDocument([], () => {
    const incidentHost = new FakeElement("incidents");
    renderIncidentQueue(incidentHost, rows, { nowMs: now });
    const groupedItem = incidentHost.children.find((child) => child.dataset.groupCount === "3");
    assert.ok(groupedItem);
    assert.equal(groupedItem.dataset.actionability, "alarm");
    assert.match(groupedItem.innerHTML, /3 related/);
    assert.match(groupedItem.innerHTML, /Recommended action/);
    assert.match(groupedItem.innerHTML, /Alarm/);
  });
});

test("alert heatmap scoring and incident list keep HIGH distinct", async () => {
  const now = Date.now();
  const rows = [
    { id: "warn", symbol: "AAPL", severity: "WARN", horizon_s: 60, confidence: 0.99, expected_z: 1.0, message: "warning" },
    { id: "high", symbol: "AAPL", severity: "HIGH", horizon_s: 60, confidence: 0.70, expected_z: 1.0, message: "high" },
    { id: "crit", symbol: "MSFT", severity: "CRIT", horizon_s: 60, confidence: 0.80, expected_z: 1.0, message: "critical", ts_ms: now },
  ];

  assert.equal(_scoreCell(rows.slice(0, 2), severityRank).id, "high");

  await withFakeDocument([], () => {
    const heatmapHost = new FakeElement("heatmap");
    renderHeatmap(heatmapHost, rows);
    const highCell = heatmapHost.children.find((child) => child.dataset && child.dataset.severity === "HIGH");
    assert.ok(highCell);
    assert.equal(highCell.dataset.status, "high");
    assert.match(highCell.innerHTML, /hmSwatch/);
    assert.match(highCell.innerHTML, /hmStatusLabel/);
    assert.match(highCell.innerHTML, /HIGH/);

    const incidentHost = new FakeElement("incidents");
    renderIncidentQueue(incidentHost, [
      { id: "info", severity: "INFO", message: "info", ts_ms: now - 10_000 },
      { id: "high", severity: "HIGH", message: "high", ts_ms: now - 5_000 },
      { id: "warn", severity: "WARN", message: "warn", ts_ms: now - 1_000 },
      { id: "crit", severity: "CRIT", message: "crit", ts_ms: now - 20_000 },
    ]);
    assert.deepEqual(
      incidentHost.children.map((child) => child.dataset.sev),
      ["CRIT", "HIGH", "WARN", "INFO"]
    );
    assert.equal(incidentHost.children[1].dataset.status, "high");
    assert.match(incidentHost.children[1].innerHTML, />\s*HIGH\s*</);
  });
});

test("incident drawer renders HIGH title state and recommendations", async () => {
  const ids = [
    "incidentOverlay",
    "drawerTitle",
    "drawerSubtitle",
    "drawerMeaning",
    "drawerSteps",
    "drawerFacts",
    "drawerRaw",
    "drawerPosture",
    "drawerDecisionConfidence",
    "drawerSafeIgnore",
    "drawerNoChange",
    "drawerWhyPosture",
    "drawerAlertState",
    "btnIncidentAck",
    "btnIncidentShelve",
    "btnIncidentResolve",
    "drawerActionStatus",
    "drawerLifecycle",
    "drawerNotificationPolicy",
    "drawerSimilar",
  ];
  const row = {
    id: "a1",
    severity: "HIGH",
    symbol: "AAPL",
    message: "elevated risk",
    ts_ms: Date.now(),
    expected_z: 1.8,
    confidence: 0.72,
    reason: "model drift",
    lifecycle: [
      { ts_ms: Date.now() - 1000, state: "triggered", actor: "system", reason: "model drift" },
    ],
    notification_policy: {
      severity: "HIGH",
      suppressed: false,
      rate_limit_ms: 600_000,
      next_escalation_ts_ms: Date.now() + 600_000,
      explanation: "severity-aware notifications active",
    },
  };
  const interactions = [];

  assert.match(_meaningForAlert(row), /Elevated risk|high severity/i);
  assert.match(_recommendedPosture(row), /Validate|Hold risk/i);
  assert.match(_safeToIgnore(row), /^No/);
  assert.match(_ifNothingChanges(row), /degraded|risk|diverge/i);

  await withFakeDocument(ids, async (elements) => {
    await openIncidentDrawer(row, {
      fetchJSON: async () => ({ alert: row }),
      getLastAlerts: () => [],
      postUiInteraction: async (payload) => interactions.push(payload),
      ackAlert: async () => ({ ok: true }),
      shelveAlert: async () => ({ ok: true }),
      resolveAlert: async () => ({ ok: true }),
      reloadAlerts: () => {},
      isAckedLocal: () => false,
      isResolvedLocal: () => false,
    });

    assert.equal(elements.get("drawerTitle").textContent, "HIGH • AAPL");
    assert.equal(elements.get("drawerAlertState").textContent, "HIGH");
    assert.match(elements.get("drawerAlertState").className, /high/);
    assert.match(elements.get("drawerFacts").innerHTML, /<div class="kvK">severity<\/div><div class="kvV">HIGH<\/div>/);
    assert.match(elements.get("drawerLifecycle").innerHTML, /Triggered/);
    assert.match(elements.get("drawerNotificationPolicy").textContent, /HIGH notifications active/);
    assert.equal(elements.get("incidentOverlay").style.display, "block");
    assert.equal(interactions[0].detail.severity, "HIGH");
  });
});

test("decision header counts WARN+ with shared severity order", async () => {
  const ids = ["pillSystem", "pillCrit", "pillWarn", "pillData", "pillModel", "pillExec", "pillUpdated"];
  await withFakeDocument(ids, (elements) => {
    initDecisionBarEngine({
      getLastAlerts: () => [
        { severity: "INFO", resolved: false },
        { severity: "WARN", resolved: false },
        { severity: "HIGH", resolved: false },
        { severity: "CRIT", resolved: false },
        { severity: "HIGH", resolved: true },
      ],
      getLastHealth: () => ({ prices: { ok: true }, labels: { ok: true }, providers: { ok: true }, ts_ms: Date.now() }),
      getLastSystemState: () => ({ state: "LIVE", ts_ms: Date.now() }),
      getLastExecutionBarrier: () => ({ allowed: true, ts_ms: Date.now() }),
      getLastPromotionStatus: () => ({ enabled: true, allowed: true, updated_ts_ms: Date.now() }),
      isExecutionDegraded: () => false,
    });

    updateDecisionHeader("just now");
    assert.equal(elements.get("pillCrit").textContent, "CRIT: 1");
    assert.equal(elements.get("pillWarn").textContent, "WARN+: 3");
  });
});

test("status tokens and styles keep contrast and high-contrast fallbacks", () => {
  for (const token of Object.values(STATUS_TOKENS)) {
    assert.equal(contrastRatio(token.color, "#0f1522") >= 4.5, true, `${token.key} text contrast`);
    assert.equal(contrastRatio(token.color, "#0f1522") >= 3, true, `${token.key} non-text contrast`);
  }

  for (const path of ["../ui/base.css", "../ui/mobile/mobile.css", "../ui/terminal/terminal_theme.css"]) {
    const css = readFileSync(new URL(path, import.meta.url), "utf8");
    assert.match(css, /prefers-reduced-motion:\s*reduce/);
    assert.match(css, /prefers-contrast:\s*more/);
    assert.match(css, /forced-colors:\s*active/);
  }
});

test("alert heatmap has non-color semantics in production renderer", () => {
  const alertsJs = readFileSync(new URL("../ui/alerts.js", import.meta.url), "utf8");
  const dashboardJs = readFileSync(new URL("../ui/dashboard.js", import.meta.url), "utf8");

  assert.match(alertsJs, /setAttribute\("role", "grid"\)/);
  assert.match(alertsJs, /setAttribute\("aria-label", "Alert heatmap by symbol and horizon"\)/);
  assert.match(alertsJs, /class="hmSwatch"[\s\S]*esc\(c\.glyph/);
  assert.match(alertsJs, /class="hmStatusLabel"/);
  assert.match(alertsJs, /cell\.dataset\.status = c\.key/);
  assert.match(alertsJs, /cell\.setAttribute\("aria-label", aria\)/);
  assert.match(dashboardJs, /hmStatus-unavailable/);
  assert.match(dashboardJs, /data-status="unavailable"/);
  assert.match(dashboardJs, /role="gridcell"/);
  assert.match(dashboardJs, /statusAriaLabel\(token\.key/);
});
