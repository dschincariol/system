"use strict";

import { formatAgeMs, numOrNull } from "./utils.js";

const PANEL_STATES = new Set(["fresh", "stale", "empty", "error"]);

const CONNECTION_STATES = new Set(["connected", "degraded", "disconnected", "offline_readonly"]);

export const DASHBOARD_CONNECTION_ENDPOINTS = Object.freeze([
  Object.freeze({
    key: "health",
    label: "Health",
    endpoint: "/api/health",
    group: "safety",
    cardIds: Object.freeze(["systemHealthCard"]),
    staleMs: 45_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "readiness",
    label: "Readiness",
    endpoint: "/api/readiness",
    group: "safety",
    cardIds: Object.freeze(["operatorStartupCard", "readinessEvidencePanel"]),
    staleMs: 45_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "execution_barrier",
    label: "Execution barrier",
    endpoint: "/api/execution/barrier",
    group: "safety",
    cardIds: Object.freeze(["systemHealthCard", "jobConsoleCard"]),
    staleMs: 20_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "broker",
    label: "Broker snapshot",
    endpoint: "/api/broker",
    group: "broker",
    cardIds: Object.freeze(["brokerPanel", "positionsLiveBookCard"]),
    staleMs: 60_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "broker_config",
    label: "Broker config",
    endpoint: "/api/broker/config",
    group: "broker",
    cardIds: Object.freeze(["brokerConfigPanel"]),
    staleMs: 120_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "risk_portfolio",
    label: "Portfolio risk",
    endpoint: "/api/risk/portfolio",
    group: "risk",
    cardIds: Object.freeze(["positionsExposureSummaryCard"]),
    staleMs: 60_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "risk_summary",
    label: "Risk summary",
    endpoint: "/api/risk/summary",
    group: "risk",
    cardIds: Object.freeze(["positionsExposureSummaryCard"]),
    staleMs: 60_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "ui_metrics",
    label: "Canonical UI metrics",
    endpoint: "/api/ui/metrics",
    group: "risk",
    cardIds: Object.freeze(["positionsExposureSummaryCard", "livePnlCard"]),
    staleMs: 60_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "pnl",
    label: "PnL",
    endpoint: "/api/pnl",
    group: "pnl",
    cardIds: Object.freeze(["livePnlCard"]),
    staleMs: 60_000,
    critical: true,
    safetyCritical: false,
  }),
  Object.freeze({
    key: "alerts",
    label: "Alerts",
    endpoint: "/api/alerts/timeline",
    group: "alerts",
    cardIds: Object.freeze(["alertsCard"]),
    staleMs: 60_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "ingestion",
    label: "Ingestion",
    endpoint: "/api/ingestion/status",
    group: "data",
    cardIds: Object.freeze(["dataHealthSummaryCard", "dataProviderTelemetryCard"]),
    staleMs: 60_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "feeds",
    label: "Feeds",
    endpoint: "/api/feeds",
    group: "data",
    cardIds: Object.freeze(["dataHealthSummaryCard"]),
    staleMs: 90_000,
    critical: true,
    safetyCritical: false,
  }),
  Object.freeze({
    key: "provider_telemetry",
    label: "Provider telemetry",
    endpoint: "/api/operator/provider_telemetry",
    group: "data",
    cardIds: Object.freeze(["dataProviderTelemetryCard"]),
    staleMs: 90_000,
    critical: true,
    safetyCritical: false,
  }),
  Object.freeze({
    key: "terminal_snapshot",
    label: "Terminal snapshot",
    endpoint: "/api/terminal/snapshot",
    group: "terminal",
    cardIds: Object.freeze(["executionSnapshotCard"]),
    staleMs: 45_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "terminal_positions",
    label: "Terminal positions",
    endpoint: "/api/terminal/positions",
    group: "terminal",
    cardIds: Object.freeze(["positionsLiveBookCard"]),
    staleMs: 45_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "terminal_orders",
    label: "Terminal orders",
    endpoint: "/api/terminal/orders",
    group: "terminal",
    cardIds: Object.freeze(["executionOrdersCard"]),
    staleMs: 45_000,
    critical: true,
    safetyCritical: true,
  }),
  Object.freeze({
    key: "terminal_fills",
    label: "Terminal fills",
    endpoint: "/api/terminal/fills",
    group: "terminal",
    cardIds: Object.freeze(["executionFillsCard"]),
    staleMs: 60_000,
    critical: true,
    safetyCritical: false,
  }),
]);

const DASHBOARD_SAFETY_ACTION_SELECTORS = Object.freeze([
  "#btnRunPipeline",
  "#btnFixIssues",
  "#btnTogglePromotions",
  "#btnRollbackChampion",
  "#btnTrainSizePolicy",
  "#btnRunChallenger",
  "#brokerConfigActivateBtn",
  "#brokerConfigDisableBtn",
  '[data-job="broker_apply_orders"][data-action="start"]',
  '[data-job="portfolio_rebalance"][data-action="start"]',
  '[data-job="train_model_v2"][data-action="start"]',
]);

function escapeHTML(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizePanelState(state) {
  const normalized = String(state || "").toLowerCase();
  return PANEL_STATES.has(normalized) ? normalized : "fresh";
}

function normalizeConnectionState(state) {
  const normalized = String(state || "").toLowerCase();
  return CONNECTION_STATES.has(normalized) ? normalized : "connected";
}

function normalizeEndpointPath(path) {
  const raw = String(path || "").trim();
  if (!raw) return "";
  try {
    return new URL(raw, "http://dashboard.local").pathname;
  } catch {
    return raw.split("?")[0] || raw;
  }
}

function endpointSpecFor(path, specs = DASHBOARD_CONNECTION_ENDPOINTS) {
  const normalizedPath = normalizeEndpointPath(path);
  let best = null;
  for (const spec of specs || []) {
    const endpoint = normalizeEndpointPath(spec.endpoint);
    if (!endpoint) continue;
    if (normalizedPath === endpoint || normalizedPath.startsWith(`${endpoint}/`)) {
      if (!best || endpoint.length > normalizeEndpointPath(best.endpoint).length) best = spec;
    }
  }
  return best;
}

function describeError(error) {
  if (!error) return "";
  if (typeof error === "string") return error;
  if (error && typeof error.message === "string" && error.message.trim()) return error.message.trim();
  return String(error || "");
}

function compactDetail(value, maxLen = 140) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  return text.length > maxLen ? `${text.slice(0, maxLen - 1)}…` : text;
}

function entryState(entry, nowMs) {
  if (!entry || !entry.lastAttemptTs) return "unknown";
  if (entry.lastFailureTs && (!entry.lastSuccessTs || entry.lastFailureTs >= entry.lastSuccessTs)) {
    return "failed";
  }
  const ageMs = entry.lastSuccessTs ? Math.max(0, nowMs - entry.lastSuccessTs) : null;
  if (ageMs != null && ageMs >= entry.staleMs) return "stale";
  return "fresh";
}

function stateSeverity(state) {
  if (state === "failed") return 4;
  if (state === "stale") return 3;
  if (state === "unknown") return 2;
  return 1;
}

function formatLatency(latencyMs) {
  const n = numOrNull(latencyMs);
  if (n == null) return "latency —";
  if (n < 1000) return `${Math.round(n)}ms`;
  return `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}s`;
}

function endpointSummaryLine(row, nowMs) {
  if (!row) return "";
  const ageText = row.lastSuccessTs ? formatAgeMs(Math.max(0, nowMs - row.lastSuccessTs)) : "no good snapshot";
  if (row.state === "failed") {
    return `${row.label} failed${row.lastError ? `: ${compactDetail(row.lastError, 80)}` : ""}; last good ${ageText}`;
  }
  if (row.state === "stale") {
    return `${row.label} stale: last good ${ageText} ago`;
  }
  if (row.state === "unknown") {
    return `${row.label} not polled yet`;
  }
  return `${row.label} fresh ${ageText} ago`;
}

function hasKnownOfflineNavigator() {
  try {
    return typeof navigator !== "undefined" && navigator && navigator.onLine === false;
  } catch {
    return false;
  }
}

function normalizeSpecs(specs) {
  return (Array.isArray(specs) && specs.length ? specs : DASHBOARD_CONNECTION_ENDPOINTS)
    .map((spec) => ({
      ...spec,
      endpoint: normalizeEndpointPath(spec.endpoint),
      staleMs: Number.isFinite(Number(spec.staleMs)) ? Number(spec.staleMs) : 60_000,
      cardIds: Object.freeze(Array.isArray(spec.cardIds) ? spec.cardIds.slice() : (spec.cardId ? [spec.cardId] : [])),
      critical: spec.critical !== false,
      safetyCritical: spec.safetyCritical === true,
    }));
}

export function createConnectionFreshnessModel({
  specs = DASHBOARD_CONNECTION_ENDPOINTS,
  now = () => Date.now(),
} = {}) {
  const normalizedSpecs = normalizeSpecs(specs);
  const entries = new Map();
  const listeners = new Set();

  const notify = () => {
    const summary = model.summary();
    for (const listener of listeners) {
      try {
        listener(summary);
      } catch {}
    }
  };

  const update = (path, patch = {}) => {
    const spec = endpointSpecFor(path, normalizedSpecs);
    if (!spec) return null;
    const nowMs = numOrNull(patch.nowMs) ?? now();
    const existing = entries.get(spec.key) || {
      ...spec,
      attempts: 0,
      successes: 0,
      failures: 0,
      lastAttemptTs: 0,
      lastSuccessTs: 0,
      lastFailureTs: 0,
      latencyMs: null,
      lastError: "",
      status: null,
      sourceEndpoint: spec.endpoint,
    };
    const next = {
      ...existing,
      ...patch,
      lastAttemptTs: patch.lastAttemptTs || patch.endedAt || nowMs,
      attempts: existing.attempts + 1,
    };
    if (patch.outcome === "success") {
      next.successes = existing.successes + 1;
      next.lastSuccessTs = patch.endedAt || nowMs;
      next.lastError = "";
      next.status = patch.status ?? existing.status ?? null;
      next.latencyMs = numOrNull(patch.latencyMs) ?? (
        numOrNull(patch.startedAt) != null ? Math.max(0, (patch.endedAt || nowMs) - Number(patch.startedAt)) : existing.latencyMs
      );
    } else if (patch.outcome === "failure") {
      next.failures = existing.failures + 1;
      next.lastFailureTs = patch.endedAt || nowMs;
      next.lastError = describeError(patch.error);
      next.latencyMs = numOrNull(patch.latencyMs) ?? (
        numOrNull(patch.startedAt) != null ? Math.max(0, (patch.endedAt || nowMs) - Number(patch.startedAt)) : existing.latencyMs
      );
    }
    entries.set(spec.key, next);
    notify();
    return next;
  };

  const rows = () => {
    const nowMs = now();
    return normalizedSpecs.map((spec) => {
      const entry = entries.get(spec.key) || { ...spec, sourceEndpoint: spec.endpoint };
      const state = entryState(entry, nowMs);
      const ageMs = entry.lastSuccessTs ? Math.max(0, nowMs - entry.lastSuccessTs) : null;
      const staleReason =
        state === "failed"
          ? `last request failed${entry.lastError ? `: ${compactDetail(entry.lastError, 90)}` : ""}`
          : state === "stale"
            ? `last good ${formatAgeMs(ageMs)} ago exceeds ${formatAgeMs(entry.staleMs)} threshold`
            : state === "unknown"
              ? "no successful dashboard poll yet"
              : "";
      return {
        ...entry,
        state,
        ageMs,
        staleReason,
      };
    });
  };

  const model = {
    recordSuccess(path, meta = {}) {
      return update(path, {
        ...meta,
        outcome: "success",
        endedAt: numOrNull(meta.endedAt) ?? now(),
      });
    },
    recordFailure(path, meta = {}) {
      return update(path, {
        ...meta,
        outcome: "failure",
        endedAt: numOrNull(meta.endedAt) ?? now(),
      });
    },
    rows,
    summary(options = {}) {
      return summarizeConnectionRows(rows(), { nowMs: now(), ...options });
    },
    subscribe(listener) {
      if (typeof listener !== "function") return () => {};
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    reset() {
      entries.clear();
      notify();
    },
  };
  return model;
}

export function summarizeConnectionRows(rows = [], {
  nowMs = Date.now(),
  offline = hasKnownOfflineNavigator(),
  readOnly = false,
} = {}) {
  const allRows = Array.isArray(rows) ? rows : [];
  const criticalRows = allRows.filter((row) => row && row.critical !== false);
  const attemptedCritical = criticalRows.filter((row) => Number(row.lastAttemptTs || 0) > 0);
  const failed = criticalRows.filter((row) => row.state === "failed");
  const stale = criticalRows.filter((row) => row.state === "stale");
  const unknown = criticalRows.filter((row) => row.state === "unknown");
  const safetyProblems = criticalRows.filter(
    (row) => row.safetyCritical && (row.state === "failed" || row.state === "stale")
  );
  const noCriticalSuccess = attemptedCritical.length > 0 && criticalRows.every((row) => !row.lastSuccessTs);
  const visibleProblems = [...failed, ...stale]
    .sort((a, b) => stateSeverity(b.state) - stateSeverity(a.state) || String(a.label).localeCompare(String(b.label)));

  let state = "connected";
  if (offline) state = "offline_readonly";
  else if (noCriticalSuccess && failed.length) state = "disconnected";
  else if (failed.length || stale.length) state = "degraded";
  else if (!attemptedCritical.length && unknown.length) state = "degraded";

  const newestSuccessTs = Math.max(0, ...criticalRows.map((row) => Number(row.lastSuccessTs || 0)));
  const newestFailureTs = Math.max(0, ...criticalRows.map((row) => Number(row.lastFailureTs || 0)));
  const slowestLatencyMs = Math.max(0, ...criticalRows.map((row) => Number(row.latencyMs || 0)).filter((n) => Number.isFinite(n)));
  const freshnessLabel = newestSuccessTs > 0 ? `${formatAgeMs(Math.max(0, nowMs - newestSuccessTs))} ago` : "no successful critical read";

  return {
    state: normalizeConnectionState(state),
    offline: !!offline,
    readOnly: !!readOnly,
    rows: allRows,
    criticalRows,
    failed,
    stale,
    unknown,
    safetyProblems,
    safetyGuardActive: safetyProblems.length > 0 || state === "offline_readonly" || state === "disconnected",
    newestSuccessTs,
    newestFailureTs,
    slowestLatencyMs: slowestLatencyMs || null,
    freshnessLabel,
    problemLines: visibleProblems.slice(0, 5).map((row) => endpointSummaryLine(row, nowMs)).filter(Boolean),
  };
}

export const connectionFreshnessModel = createConnectionFreshnessModel();

export function recordConnectionSuccess(path, meta = {}) {
  return connectionFreshnessModel.recordSuccess(path, meta);
}

export function recordConnectionFailure(path, meta = {}) {
  return connectionFreshnessModel.recordFailure(path, meta);
}

export function subscribeConnectionState(listener) {
  return connectionFreshnessModel.subscribe(listener);
}

export function getConnectionStateSummary(options = {}) {
  return connectionFreshnessModel.summary(options);
}

export function resetConnectionStateForTests() {
  connectionFreshnessModel.reset();
}

function connectionBannerView(summary = {}) {
  const state = normalizeConnectionState(summary.state);
  const rows = Array.isArray(summary.criticalRows) ? summary.criticalRows : [];
  const attempted = rows.filter((row) => Number(row.lastAttemptTs || 0) > 0).length;
  const total = rows.length;
  const latency = summary.slowestLatencyMs != null ? `slowest ${formatLatency(summary.slowestLatencyMs)}` : "latency —";
  const sourceText = `${attempted}/${total} critical feeds observed`;
  if (state === "connected") {
    return {
      tone: "ok",
      title: "Connection: connected",
      detail: `Critical dashboard reads are current; latest success ${summary.freshnessLabel}; ${latency}; ${sourceText}.`,
    };
  }
  if (state === "offline_readonly") {
    return {
      tone: "offline",
      title: "Connection: offline/read-only fallback",
      detail: `Browser or backend connectivity is offline. Dangerous actions stay guarded and panels show last-good snapshots where available. ${summary.problemLines?.[0] || sourceText}.`,
    };
  }
  if (state === "disconnected") {
    return {
      tone: "crit",
      title: "Connection: disconnected",
      detail: `No critical dashboard read currently has a good response. ${summary.problemLines?.join(" | ") || sourceText}.`,
    };
  }
  return {
    tone: "warn",
    title: "Connection: degraded/retrying",
    detail: `${summary.problemLines?.join(" | ") || "One or more critical reads are stale or retrying."} Latest success ${summary.freshnessLabel}; ${latency}.`,
  };
}

export function renderGlobalConnectionBanner(rootDocument = document, summary = getConnectionStateSummary()) {
  const banner = rootDocument && rootDocument.getElementById ? rootDocument.getElementById("dashboardDegradedBanner") : null;
  const titleEl = rootDocument && rootDocument.getElementById ? rootDocument.getElementById("dashboardDegradedBannerTitle") : null;
  const detailEl = rootDocument && rootDocument.getElementById ? rootDocument.getElementById("dashboardDegradedBannerDetail") : null;
  if (!banner || !titleEl || !detailEl) return;
  const view = connectionBannerView(summary);
  banner.className = `opsBanner is-visible ${view.tone}`;
  banner.dataset.connectionState = normalizeConnectionState(summary.state);
  titleEl.textContent = view.title;
  detailEl.textContent = view.detail;
}

function ensureConnectionMetaElement(rootDocument, card) {
  if (!card) return null;
  let meta = card.querySelector(".panelConnectionMeta");
  if (!meta) {
    const doc = card.ownerDocument || rootDocument || document;
    meta = doc.createElement("div");
    meta.className = "panelConnectionMeta";
    const panelState = card.querySelector(".panelStateRow");
    if (panelState && panelState.nextSibling) {
      card.insertBefore(meta, panelState.nextSibling);
    } else if (panelState) {
      card.appendChild(meta);
    } else {
      const header = card.querySelector("h2");
      if (header && header.nextSibling) card.insertBefore(meta, header.nextSibling);
      else if (header) card.appendChild(meta);
      else card.insertBefore(meta, card.firstChild);
    }
  }
  return meta;
}

function worstRowForCard(rows, cardId) {
  return (rows || [])
    .filter((row) => Array.isArray(row.cardIds) && row.cardIds.includes(cardId) && Number(row.lastAttemptTs || 0) > 0)
    .sort((a, b) => stateSeverity(b.state) - stateSeverity(a.state) || Number(b.lastAttemptTs || 0) - Number(a.lastAttemptTs || 0))[0] || null;
}

export function renderConnectionCardMetadata(rootDocument = document, summary = getConnectionStateSummary()) {
  if (!rootDocument || typeof rootDocument.getElementById !== "function") return;
  const rows = Array.isArray(summary.rows) ? summary.rows : [];
  const cardIds = Array.from(new Set(rows.flatMap((row) => Array.isArray(row.cardIds) ? row.cardIds : [])));
  for (const cardId of cardIds) {
    const card = rootDocument.getElementById(cardId);
    if (!card) continue;
    const row = worstRowForCard(rows, cardId);
    if (!row) continue;
    const meta = ensureConnectionMetaElement(rootDocument, card);
    if (!meta) continue;
    const lastText = row.lastSuccessTs ? `${formatAgeMs(row.ageMs)} ago` : "never";
    const staleText = row.staleReason || "fresh";
    meta.dataset.connectionState = row.state;
    meta.innerHTML = `
      <span>source <span class="mono">${escapeHTML(row.sourceEndpoint || row.endpoint)}</span></span>
      <span>last updated ${escapeHTML(lastText)}</span>
      <span>${escapeHTML(formatLatency(row.latencyMs))}</span>
      <span>${escapeHTML(staleText)}</span>
    `;
  }
}

export function applyConnectionSafetyGuard(rootDocument = document, summary = getConnectionStateSummary()) {
  if (!rootDocument || typeof rootDocument.querySelectorAll !== "function") return;
  const active = !!summary.safetyGuardActive;
  const reasons = (summary.safetyProblems || []).map((row) => row.label).slice(0, 4);
  const reasonText = active
    ? `Fresh safety data required: ${reasons.length ? reasons.join(", ") : normalizeConnectionState(summary.state)}.`
    : "";
  const nodes = new Set();
  for (const selector of DASHBOARD_SAFETY_ACTION_SELECTORS) {
    try {
      rootDocument.querySelectorAll(selector).forEach((node) => nodes.add(node));
    } catch {}
  }
  nodes.forEach((node) => {
    if (!node || typeof node.setAttribute !== "function") return;
    if (active) {
      if (node.dataset.connectionGuardPrevDisabled === undefined) {
        node.dataset.connectionGuardPrevDisabled = node.disabled ? "1" : "0";
      }
      node.disabled = true;
      node.classList.add("freshness-guarded-action");
      node.setAttribute("aria-disabled", "true");
      node.title = reasonText;
    } else if (node.dataset.connectionGuardPrevDisabled !== undefined) {
      const wasDisabled = node.dataset.connectionGuardPrevDisabled === "1";
      delete node.dataset.connectionGuardPrevDisabled;
      if (!wasDisabled) node.disabled = false;
      node.classList.remove("freshness-guarded-action");
      node.removeAttribute("aria-disabled");
      if (node.title && node.title.startsWith("Fresh safety data required:")) node.title = "";
    }
  });
}

export function updateConnectionStateSurfaces({
  document: rootDocument = document,
  readOnly = false,
  offline = hasKnownOfflineNavigator(),
} = {}) {
  const summary = getConnectionStateSummary({ readOnly, offline });
  try {
    if (typeof window !== "undefined") {
      window.__DASHBOARD_CONNECTION_SUMMARY__ = summary;
    }
  } catch {}
  renderGlobalConnectionBanner(rootDocument, summary);
  renderConnectionCardMetadata(rootDocument, summary);
  applyConnectionSafetyGuard(rootDocument, summary);
  return summary;
}

export function applyStalenessState(el, ageMs, warnMs = 60_000, critMs = 300_000) {
  if (!el) return;
  el.classList.remove("data-stale", "data-stale-warning", "data-stale-critical");
  const age = numOrNull(ageMs);
  if (age == null) return;
  if (age >= warnMs) {
    el.classList.add("data-stale", "data-stale-warning");
  }
  if (age >= critMs) {
    el.classList.add("data-stale-critical");
  }
}

export function stalenessClassNames(ageMs, warnMs = 60_000, critMs = 300_000) {
  const age = numOrNull(ageMs);
  if (age == null || age < warnMs) return "";
  return age >= critMs
    ? "data-stale data-stale-warning data-stale-critical"
    : "data-stale data-stale-warning";
}

function ensurePanelStateElements(cardId) {
  const card = document.getElementById(cardId);
  if (!card) return null;

  let row = card.querySelector(".panelStateRow");
  if (!row) {
    row = document.createElement("div");
    row.className = "panelStateRow";
    row.innerHTML = `
      <span class="panelStateBadge is-empty">empty</span>
      <span class="panelStateText">Waiting for data.</span>
    `;
    const header = card.querySelector("h2");
    if (header && header.nextSibling) {
      card.insertBefore(row, header.nextSibling);
    } else if (header) {
      card.appendChild(row);
    } else {
      card.insertBefore(row, card.firstChild);
    }
  }

  return {
    card,
    row,
    badge: row.querySelector(".panelStateBadge"),
    text: row.querySelector(".panelStateText"),
  };
}

export function setPanelState(cardId, { state = "fresh", reason = "" } = {}) {
  const nodes = ensurePanelStateElements(cardId);
  if (!nodes) return;

  const normalized = normalizePanelState(state);
  const nextReason = String(reason || "No additional detail provided.");

  if (nodes.badge) {
    nodes.badge.className = `panelStateBadge is-${normalized}`;
    nodes.badge.textContent = normalized;
  }
  if (nodes.text) {
    nodes.text.textContent = nextReason;
  }
  nodes.card.dataset.panelState = normalized;
}

export function setSurfaceState(surfaceId, { state = "fresh", reason = "" } = {}) {
  const el = document.getElementById(surfaceId);
  if (!el) return;

  const normalized = normalizePanelState(state);
  const nextReason = String(reason || "");

  if (!nextReason || normalized === "fresh") {
    el.className = "surfaceOverlayState";
    el.textContent = "";
    return;
  }

  el.className = `surfaceOverlayState is-visible is-${normalized}`;
  el.textContent = `${normalized.toUpperCase()}: ${nextReason}`;
}
