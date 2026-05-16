/*
  FILE: ui/dashboard.js

  Main browser controller for the dashboard. This module coordinates fetches,
  refresh scheduling, shared client-side state, and the composition of the
  extracted UI modules that render alerts, policy state, portfolio panels, and
  operator-facing status.
*/

import {
  esc,
  escapeHTML,
  fmtTime,
  fmtNum,
  _fmtPct,
  _clamp,
  barWidth,
  _debounce,
  ageMsFromTimestamp,
  buildTableView,
  formatAgeMs,
  formatDecimal,
  formatPercent,
  formatSigned,
  freshnessTone,
  numOrNull,
  pickTimestamp,
  safeJoin
} from "./utils.js";

import { fetchJSON, fetchWithTimeout as _fetchWithTimeout } from "./api_client.js";
import {
  applyStalenessState,
  setPanelState,
  setSurfaceState,
  stalenessClassNames
} from "./panel_state.js";

import {
  renderLineChart,
  drawCalibration
} from "./charts.js";

import {
  normalizeAlertsPayload,
  filterAlerts,
  renderHeatmap,
  renderIncidentQueue,
  severityRank
} from "./alerts.js";

import {
  loadPolicyState,
  saveOperatorMode,
  saveExpertUnlock,
  applyPolicyToDOM,
  requireExpertUnlock,
  requireConfirmIfDegraded
} from "./policy.js";

import { renderKillSwitchPills } from "./kill_switch_ui.js";

import {
  wireDecisionBarClicks,
  updateDecisionHeader
} from "./decision_bar.js";

import { initDecisionBarRuntime } from "./decision_bar_boot.js";

import {
  detectExecutionDegradation,
  isExecutionDegraded,
  buildExecutionAlert
} from "./execution_degradation.js";

import {
  updateManipulationStateFromAlerts,
  hardBlockActionIfManipulated
} from "./safety_banner.js";

import {
  initPromotionSafetyEngine,
  maybeAutoResumePromotionsAfterRecovery,
  handlePromotionToggle,
  handleAutoFix
} from "./promotion_safety.js";

import { scheduleRefreshTasks } from "./refresh_scheduler.js";
import { startDashboardRefreshScheduler } from "./refresh_runtime.js";
import {
  buildSnapshotBundle,
  copySnapshotBundle
} from "./snapshot.js";
import {
  renderSystemState,
  renderSystemStatusHeader,
  renderOperatorStartupPanel
} from "./system_state.js";
import { buildStartupDiagnostics } from "./runtime_diagnostics.js";
import { summarizeRuntimeStatus } from "./runtime_status_summary.js";

import {
  openWhyModal,
  closeWhyModal,
  openPromoWhyModal,
  closePromoWhyModal
} from "./why_modal.js";
import {
  buildDecisionDetailUrl,
  buildDecisionRelatedSummary,
  buildDecisionStageRows,
  hasDecisionLookup,
  normalizeDecisionLookup
} from "./decision_drilldown.mjs";

import {
  closeIncidentDrawer,
  loadAlertsUI
} from "./alerts_ui.js";
import {
  buildPromotionActionPayload,
  buildRollbackConsequencePreview,
  formatGateState,
  formatPromotionGateValue,
  modelLabel,
  normalizePromotionGatePayload,
  promotionGateStateTone,
  summarizeCooldown,
  validatePromotionActionInput
} from "./promotion_gate.mjs";
import { loadOperatorSummary } from "./operator_summary.js";
import {
  loadMarketStress,
  loadMarketStressHistory
} from "./market_stress.js";

import { loadExecutionByConfidence } from "./execution_metrics.js";
import {
  loadPortfolioBacktestLatest,
  getLastPortfolioBacktestSummary
} from "./portfolio_backtest.js";
import { loadPerformanceDivergence } from "./model_performance_divergence.mjs";

import {
  loadProCharts,
  bindProChartSymbolWatcher,
  getDashboardChartRuntime
} from "./pro_chart_engine.js";
import { initReplayPanel } from "./replay.mjs";

import {
  loadPortfolio,
  loadBroker,
  loadEquityDrift,
  loadDriftExplainer,
  loadEquityReconciliation
} from "./portfolio.js";
import {
  canonicalExposureValues,
  canonicalPnlValues,
  canonicalSourceNotes,
  normalizeUiMetricsPayload
} from "./ui_metrics.js";
import {
  initMetricTooltips,
  applyInlineMetricAnnotation,
  setMetricValueAttribute
} from "./tooltip.js";
import {
  computeHealthScore,
  renderHealthScoreSummary
} from "./health_score.js";
import {
  applyDashboardPersonaView,
  getDefaultDashboardScreen,
  getActiveDashboardPersona,
  isDashboardScreenAllowed,
  wireDashboardPersonaControls
} from "./view_router.js";
import { initCommandPalette, isSafePaletteJobAction } from "./command_palette.mjs";

import {
  loadNewsPanels,
  loadNewsSentiment
} from "./news_panels.js";

import { loadTelemetry } from "./telemetry_panel.js";

import {
  loadSocialPressure,
  loadSocialRegimes,
  loadSocialBlocks
} from "./social_panels.js";
import { loadWeatherWidgets } from "./weather_widgets.js";

import {
  isReadOnlyMode,
  setReadOnlyMode,
  applyReadOnlyBanner,
  hardBlockIfReadOnly
} from "./read_only_mode.js";

import {
  getProChartsState,
  setProChartsState,
  applyProChartsVisibility
} from "./terminal/pro_charting.js";
import {
  getSelectedSymbolContext,
  initSelectedSymbolContextFromUrl,
  normalizeSelectedSymbol,
  subscribeSelectedSymbolContext,
  updateSelectedSymbolContext
} from "./symbol_context.mjs";

/* ui/dashboard.js — Market Impact Dashboard controller */
async function _refreshProCharts() {
  return loadProCharts(fetchJSON);
}

window._refreshProCharts = _refreshProCharts;
// -----------------------------
// Compatibility + globals (Phase 7–10)
// -----------------------------

// Some older code paths still call these names:
function _isExecutionDegraded() {
  try {
    return !!isExecutionDegraded();
  } catch {
    return false;
  }
}

function getSnapshotBundleState() {
  return {
    OPERATOR_MODE,
    EXPERT_UNLOCK,
    isExecutionDegraded: _isExecutionDegraded,
    lastAlerts: Array.isArray(_lastAlerts) ? _lastAlerts : [],
    lastHealth: _lastHealth || window.__LAST_HEALTH__ || null,
  };
}

// Used by loadHealth() for localStorage parsing (safe default)
const _EXEC_CONF_STATE_KEY = "exec_conf_state_v1";
const DASHBOARD_SCREEN_KEY = "dashboard.screen.v1";
const DASHBOARD_SCREEN_LABELS = Object.freeze({
  overview: "Overview",
  operate: "Operate",
  explain: "Explain",
  analyze: "Analyze",
  data: "Data Health",
  positions: "Positions",
  execution: "Execution",
});
const DASHBOARD_SCREEN_ALIASES = Object.freeze({
  health: "data",
  exposure: "positions",
  orders: "execution",
});
const DASHBOARD_SCREENS = new Set(Object.keys(DASHBOARD_SCREEN_LABELS));
let ACTIVE_DASHBOARD_SCREEN = "overview";
let DASHBOARD_BOOTED = false;
let _activeScreenRefreshSeq = 0;
const OPERATOR_BASE_URL = (() => {
  try {
    const raw = String(window.OPERATOR_BASE || "").trim();
    if (raw) return new URL(raw, window.location.origin).toString();
  } catch {}
  return new URL("/operator/", window.location.origin).toString();
})();
const OPERATOR_WS_URL = (() => {
  try {
    const explicit = String(window.OPERATOR_WS_URL || "").trim();
    if (explicit) return explicit;
    const raw = String(window.OPERATOR_BASE || "").trim();
    if (!raw) return "";
    const url = new URL(raw, window.location.origin);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.pathname = "/ws/operator";
    url.search = "";
    url.hash = "";
    return url.toString();
  } catch {
    return "";
  }
})();
const OPERATOR_WS_RETRY_BASE_MS = 1000;
const OPERATOR_WS_RETRY_MAX_MS = 30000;
let _operatorWs = null;
let _operatorWsRetryTimer = null;
let _operatorWsRetryMs = OPERATOR_WS_RETRY_BASE_MS;
let _operatorWsStopped = false;
let _latestHealthStateTs = 0;
let _latestPnLStateTs = 0;
let _latestSystemStatusTs = 0;
let _lastNotificationStatus = null;
let _wsRenderTimer = null;
let _pendingWsHealth = null;
let _pendingWsHealthTs = 0;
let _pendingWsPnl = null;
let _pendingWsPnlTs = 0;
let _pendingWsTradingTs = 0;
const _notificationTestPending = new Set();
let _lastOperatorRealtimeMessageTs = 0;
const _dashboardLiveState = {
  health: null,
  readiness: null,
  systemState: null,
  executionBarrier: null,
  stressPayload: null,
  uiMetrics: null,
  pnl: null,
  decisions: [],
  advisories: [],
  alerts: [],
  notificationStatus: null,
  governance: null,
  executionOverlays: null,
  failures: [],
  timestamps: {
    health: 0,
    readiness: 0,
    systemState: 0,
    executionBarrier: 0,
    stressPayload: 0,
    uiMetrics: 0,
    pnl: 0,
    decisions: 0,
    advisories: 0,
    alerts: 0,
    notificationStatus: 0,
    governance: 0,
    executionOverlays: 0,
  },
  errors: {
    health: "",
    pnl: "",
    decisions: "",
    advisories: "",
    alerts: "",
    notificationStatus: "",
    governance: "",
    executionOverlays: "",
    uiMetrics: "",
  },
};

function coerceRealtimeTs(...values) {
  for (const value of values) {
    if (value === undefined || value === null || value === "") continue;
    if (typeof value === "number" && Number.isFinite(value) && value > 0) {
      return value;
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (!trimmed) continue;
      const numeric = Number(trimmed);
      if (Number.isFinite(numeric) && numeric > 0) return numeric;
      const parsed = Date.parse(trimmed);
      if (Number.isFinite(parsed) && parsed > 0) return parsed;
      continue;
    }
    const numeric = Number(value);
    if (Number.isFinite(numeric) && numeric > 0) return numeric;
  }
  return 0;
}

function pickFiniteNumber(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function extractRealtimeTs(payload, fallback = 0) {
  if (!payload || typeof payload !== "object") {
    return coerceRealtimeTs(fallback);
  }
  const direct = coerceRealtimeTs(
    payload.ts_ms,
    payload.updated_ts_ms,
    payload.created_ts_ms,
    payload.timestamp_ms,
    payload.timestamp,
    payload.ts,
    payload.at,
    payload.updated_at,
    payload.created_at
  );
  if (direct) return direct;
  if (payload.body && typeof payload.body === "object") {
    const nestedBody = extractRealtimeTs(payload.body, 0);
    if (nestedBody) return nestedBody;
  }
  if (payload.data && typeof payload.data === "object") {
    const nestedData = extractRealtimeTs(payload.data, 0);
    if (nestedData) return nestedData;
  }
  return coerceRealtimeTs(fallback);
}

function extractCollectionRealtimeTs(value) {
  if (Array.isArray(value)) {
    return value.reduce((latest, item) => Math.max(latest, extractCollectionRealtimeTs(item)), 0);
  }
  if (!value || typeof value !== "object") return 0;

  let latest = extractRealtimeTs(value, 0);
  const nestedKeys = [
    "rows",
    "data",
    "broker",
    "portfolio",
    "positions",
    "orders",
    "fills",
    "open_orders",
    "recent_fills",
  ];
  for (const key of nestedKeys) {
    if (value[key] !== undefined) {
      latest = Math.max(latest, extractCollectionRealtimeTs(value[key]));
    }
  }
  return latest;
}

function extractTradingRealtimeTs(payload, fallback = 0) {
  const nested = extractCollectionRealtimeTs(payload);
  return nested || coerceRealtimeTs(fallback) || Date.now();
}

function normalizeOperatorHealthPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (payload.body && typeof payload.body === "object") return payload.body;
  if (payload.prices || payload.labels || payload.model || payload.ok !== undefined) return payload;
  return null;
}

function normalizePnLPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (payload.ok === true && payload.data && typeof payload.data === "object") {
    return payload;
  }
  const source = payload.data && typeof payload.data === "object" ? payload.data : payload;
  const total = pickFiniteNumber(
    source.total,
    source.total_pnl,
    source.day_pnl,
    source.daily_pnl
  );
  const unrealized = pickFiniteNumber(source.unrealized, source.unrealized_pnl);
  const realized = pickFiniteNumber(source.realized, source.realized_pnl);
  const hasAnyValue =
    total !== null ||
    unrealized !== null ||
    realized !== null;
  if (!hasAnyValue && payload.ok === false) return null;
  return {
    ok: payload.ok !== false,
    data: {
      total,
      unrealized,
      realized,
    },
    ts_ms: coerceRealtimeTs(payload.ts_ms, source.ts_ms) || Date.now(),
  };
}

function normalizeCanonicalPnLAsPayload(metrics) {
  const normalized = normalizeUiMetricsPayload(metrics);
  if (!normalized || normalized.ok === false) return null;
  const values = canonicalPnlValues(normalized);
  if (values.missing) {
    return {
      ok: false,
      error: values.source && values.source.reason ? values.source.reason : "canonical_pnl_missing",
      data: {},
      __canonical: normalized,
      __canonicalPnl: values,
    };
  }
  return {
    ok: true,
    data: {
      total: values.today ?? values.total,
      today_pnl: values.today,
      total_pnl: values.total,
      unrealized: values.unrealized,
      realized: values.realized,
    },
    ts_ms: values.tsMs || normalized.tsMs || Date.now(),
    __canonical: normalized,
    __canonicalPnl: values,
  };
}

async function fetchCanonicalUiMetrics() {
  return normalizeUiMetricsPayload(await fetchJSON("/api/ui/metrics", { allowBusinessFalse: true }));
}

function setOperatorWsIndicator(state = "polling") {
  const badge = document.getElementById("operatorWsStatus");
  const dot = document.getElementById("operatorWsDot");
  const label = document.getElementById("operatorWsLabel");
  if (!badge || !dot || !label) return;

  const states = {
    polling: { tone: "neutral", label: "polling", color: "#6e7681", title: "Polling only" },
    connecting: { tone: "neutral", label: "connecting", color: "#6e7681", title: "Connecting to operator WebSocket" },
    connected: { tone: "ok", label: "connected", color: "#2ea043", title: "Operator WebSocket connected" },
    retrying: { tone: "crit", label: "retrying", color: "#ff6b6b", title: "Operator WebSocket disconnected; polling fallback active" },
    disconnected: { tone: "crit", label: "disconnected", color: "#ff6b6b", title: "Operator WebSocket unavailable; polling fallback active" },
  };

  const next = states[state] || states.polling;
  badge.className = buildPillClassName(badge, next.tone);
  badge.title = next.title;
  setTextContent(label, next.label);
  dot.style.background = next.color;
}

function describeUiError(error) {
  const text = String(error && error.message ? error.message : (error || "request_failed"))
    .replace(/\s+/g, " ")
    .trim();
  return text || "request_failed";
}

function createFailureItem(key, label, error) {
  return {
    key: String(key || "").trim(),
    label: String(label || key || "route").trim(),
    message: describeUiError(error),
  };
}

function normalizeFailureItems(failures) {
  return asArray(failures)
    .map((item) => {
      if (!item) return null;
      if (typeof item === "string") {
        return { key: "", label: "route", message: item };
      }
      return {
        key: String(item.key || item.label || "").trim(),
        label: String(item.label || item.key || "route").trim(),
        message: String(item.message || item.error || "").trim() || "request_failed",
      };
    })
    .filter(Boolean);
}

function collectStructuredIssues(payload, limit = 6) {
  const out = [];
  const seen = new Set();
  const push = (value, prefix = "") => {
    if (out.length >= limit) return;
    const text = String(value || "").replace(/\s+/g, " ").trim();
    if (!text) return;
    const next = prefix ? `${prefix}${text}` : text;
    if (seen.has(next)) return;
    seen.add(next);
    out.push(next);
  };

  const visit = (source, prefix = "") => {
    if (!source || typeof source !== "object") return;
    asArray(source.issues).forEach((item) => {
      if (item && typeof item === "object") {
        push(item.message || item.detail || item.code, prefix);
      } else {
        push(item, prefix);
      }
    });
    asArray(source.reasons).forEach((item) => {
      if (item && typeof item === "object") {
        push(item.message || item.detail || item.code, prefix);
      } else {
        push(item, prefix);
      }
    });
    asArray(source.waiting_on).forEach((item) => push(item, `${prefix}waiting on `));
  };

  const root = asObject(payload);
  visit(root);
  visit(asObject(root.readiness));
  visit(asObject(root.health));
  return out;
}

function buildHealthDetailLines({
  health = null,
  systemState = null,
  readiness = null,
  executionBarrier = null,
  failures = [],
} = {}) {
  const normalizedFailures = normalizeFailureItems(failures);
  const safeHealth = normalizeOperatorHealthPayload(health) || asObject(health);
  const safeReadiness = asObject(readiness);
  const safeBarrier = asObject(executionBarrier || safeHealth.execution_barrier);
  const safeSystemState = asObject(systemState);
  const startup = buildStartupDiagnostics({
    readiness: safeReadiness,
    health: safeHealth,
    systemState: safeSystemState,
    broker: null,
  });

  const lines = [];
  const latestTs = pickTimestamp(
    safeHealth.ts_ms,
    safeReadiness.ts_ms,
    asObject(safeReadiness.readiness).ts_ms,
    safeSystemState.ts_ms,
    safeBarrier.ts_ms,
    asObject(safeHealth.timestamps).ts_ms
  );
  const latestAgeMs = ageMsFromTimestamp(latestTs);
  lines.push(`Snapshot: ${latestTs ? `${fmtTime(latestTs)} (${formatAgeMs(latestAgeMs)} old)` : "unavailable"}`);

  if (normalizedFailures.length) {
    lines.push("Critical route failures:");
    normalizedFailures.slice(0, 4).forEach((item) => {
      lines.push(`- ${item.label}: ${item.message}`);
    });
  }

  const systemLabel = String(
    safeSystemState.state || safeSystemState.system_state || safeSystemState.status || "UNKNOWN"
  ).trim() || "UNKNOWN";
  lines.push(`System state: ${systemLabel}`);

  if (safeReadiness && Object.keys(safeReadiness).length) {
    lines.push(`Readiness: ${safeReadiness.ready === true ? "READY" : "BLOCKED"}`);
    collectStructuredIssues(safeReadiness, 4).forEach((item) => {
      lines.push(`- ${item}`);
    });
  } else {
    lines.push("Readiness: unavailable");
  }

  if (safeBarrier && Object.keys(safeBarrier).length) {
    lines.push(
      `Execution gate: ${safeBarrier.allowed === true ? "ALLOWED" : "BLOCKED"}${safeBarrier.reason ? ` (${safeBarrier.reason})` : ""}`
    );
  }

  const db = asObject(safeHealth.db);
  if (db && Object.keys(db).length) {
    lines.push(`Database: ${db.ok === false ? "DEGRADED" : "OK"}${db.quick_check ? ` (${db.quick_check})` : ""}`);
  }

  const requiredTables = asObject(safeHealth.required_tables);
  const missingTables = asArray(requiredTables.missing);
  if (missingTables.length) {
    lines.push(`Missing tables: ${missingTables.join(", ")}`);
  }

  const providers = asObject(safeHealth.providers);
  const prices = asObject(safeHealth.prices);
  if (providers && Object.keys(providers).length) {
    lines.push(
      `Providers: ${formatDecimal(providers.healthy, 0)}/${formatDecimal(providers.total, 0)} healthy`
    );
  }
  if (prices && Object.keys(prices).length) {
    lines.push(
      `Price freshness: ${prices.ok === true ? "fresh" : "stale"}${prices.age_s != null ? ` (${formatAgeMs(Number(prices.age_s) * 1000)})` : ""}`
    );
  }

  if (startup.blockers.length) {
    lines.push("Operational blockers:");
    startup.blockers.slice(0, 6).forEach((item) => {
      lines.push(`- ${item}`);
    });
  }

  if (!normalizedFailures.length && !startup.blockers.length && safeHealth.ok === true && safeReadiness.ready === true && safeBarrier.allowed !== false) {
    lines.push("No critical backend blockers reported by the latest shared snapshots.");
  }

  return lines.join("\n");
}

function renderDashboardDegradedBanner({
  health = null,
  systemState = null,
  readiness = null,
  executionBarrier = null,
  failures = [],
} = {}) {
  const banner = document.getElementById("dashboardDegradedBanner");
  const titleEl = document.getElementById("dashboardDegradedBannerTitle");
  const detailEl = document.getElementById("dashboardDegradedBannerDetail");
  if (!banner || !titleEl || !detailEl) return;

  const normalizedFailures = normalizeFailureItems(failures);
  const safeHealth = normalizeOperatorHealthPayload(health) || asObject(health);
  const safeReadiness = asObject(readiness);
  const safeBarrier = asObject(executionBarrier || safeHealth.execution_barrier);
  const safeSystemState = asObject(systemState);
  const startup = buildStartupDiagnostics({
    readiness: safeReadiness,
    health: safeHealth,
    systemState: safeSystemState,
    broker: null,
  });

  const blockers = [
    ...normalizedFailures.map((item) => `${item.label}: ${item.message}`),
    ...startup.blockers,
  ].filter(Boolean);

  const degraded =
    normalizedFailures.length > 0 ||
    safeHealth.ok === false ||
    safeReadiness.ready === false ||
    safeBarrier.allowed === false ||
    String(safeSystemState.state || "").trim().toUpperCase() !== "LIVE" ||
    blockers.length > 0;

  banner.className = "opsBanner";
  if (!degraded) {
    return;
  }

  const latestTs = pickTimestamp(
    safeHealth.ts_ms,
    safeReadiness.ts_ms,
    asObject(safeReadiness.readiness).ts_ms,
    safeSystemState.ts_ms,
    safeBarrier.ts_ms
  );
  const latestAgeMs = ageMsFromTimestamp(latestTs);
  const tone =
    normalizedFailures.length > 0 || safeBarrier.allowed === false
      ? "crit"
      : "warn";

  banner.classList.add("is-visible", tone);
  if (normalizedFailures.length > 0) {
    titleEl.textContent = "Critical backend routes are unavailable";
  } else if (safeBarrier.allowed === false) {
    titleEl.textContent = "Execution is blocked";
  } else if (safeReadiness.ready === false) {
    titleEl.textContent = "Readiness gates are blocking normal operation";
  } else {
    titleEl.textContent = "Runtime is degraded";
  }

  const detailParts = [];
  if (blockers.length) {
    detailParts.push(blockers.slice(0, 3).join(" | "));
  }
  if (latestTs) {
    detailParts.push(`Last backend snapshot ${formatAgeMs(latestAgeMs)} old (${fmtTime(latestTs)})`);
  } else {
    detailParts.push("No current backend snapshot timestamp is available.");
  }
  detailEl.textContent = detailParts.join(". ");
}

function renderDashboardSystemStatus(health, systemState = window.__LAST_SYSTEM_STATE__ || null) {
  const hEl = document.getElementById("healthStatus");
  const hDet = document.getElementById("healthDetails");
  const readiness = window.__LAST_READINESS__ || null;
  const executionBarrier = window.__LAST_EXECUTION_BARRIER__ || null;
  const failures = normalizeFailureItems(window.__LAST_REFRESH_FAILURES__ || []);
  const safeHealth = normalizeOperatorHealthPayload(health) || asObject(health);
  const startup = buildStartupDiagnostics({
    readiness,
    health: safeHealth,
    systemState,
    broker: null,
  });
  const degraded =
    failures.length > 0 ||
    !systemState ||
    systemState.state !== "LIVE" ||
    safeHealth.ok === false ||
    (readiness && readiness.ready === false) ||
    (executionBarrier && executionBarrier.allowed === false) ||
    startup.blockers.length > 0 ||
    _isExecutionDegraded();
  const failed = failures.length > 0 || !safeHealth || !Object.keys(safeHealth).length;

  if (hEl) {
    setStatusTone(
      hEl,
      failed ? "bad" : degraded ? "warn" : "ok",
      failed ? "FAILED" : degraded ? "DEGRADED" : "LIVE"
    );
  }

  const root = document.documentElement;
  if (degraded) {
    root.classList.add("system-degraded");
  } else {
    root.classList.remove("system-degraded");
  }

  if (hDet) {
    hDet.textContent = buildHealthDetailLines({
      health: safeHealth,
      systemState,
      readiness,
      executionBarrier,
      failures,
    });
  }

  renderDashboardDegradedBanner({
    health: safeHealth,
    systemState,
    readiness,
    executionBarrier,
    failures,
  });

  const latestTs = pickTimestamp(
    safeHealth && safeHealth.ts_ms,
    readiness && readiness.ts_ms,
    systemState && systemState.ts_ms,
    executionBarrier && executionBarrier.ts_ms
  );
  const latestAgeMs = ageMsFromTimestamp(latestTs);
  updateDashboardLiveState({
    health: safeHealth,
    readiness,
    systemState,
    executionBarrier,
    failures,
  }, { sourceTs: latestTs || Date.now() });
  setPanelState("systemHealthCard", {
    state: failed ? "error" : (latestAgeMs != null && latestAgeMs >= 300_000 ? "stale" : "fresh"),
    reason: failed
      ? "Health snapshot is unavailable or missing required sections."
      : degraded
        ? `Fresh health snapshot with active blockers. ${latestTs ? `Backend ${formatAgeMs(latestAgeMs)} old.` : "Timestamp unavailable."}`
        : `Fresh health snapshot. ${latestTs ? `Backend ${formatAgeMs(latestAgeMs)} old.` : "Timestamp unavailable."}`,
  });

  updateDecisionHeader();
}

function renderTelemetryStripFromDashboard() {
  try {
    const tNav = document.getElementById("tNav");
    if (!tNav) return;

    const tReturn = document.getElementById("tReturn");
    const tDD = document.getElementById("tDD");
    const tSharpe = document.getElementById("tSharpe");
    const tCalmar = document.getElementById("tCalmar");
    const tPromotion = document.getElementById("tPromotion");

    const promotionStatus = window.__LAST_PROMOTION_STATUS__ || null;
    const backtestSummary = getLastPortfolioBacktestSummary();

    setTextContent(tNav, "Net Asset Value —");
    setMetricValueAttribute(tNav, null);

    if (tReturn) {
      setMetricValueAttribute(
        tReturn,
        backtestSummary && Number.isFinite(backtestSummary.totalReturn)
          ? Number(backtestSummary.totalReturn)
          : null
      );
      setTextContent(
        tReturn,
        backtestSummary && Number.isFinite(backtestSummary.totalReturn)
          ? `RET ${_fmtPct(backtestSummary.totalReturn)}`
          : "Total Return —"
      );
    }
    if (tDD) {
      setMetricValueAttribute(
        tDD,
        backtestSummary && Number.isFinite(backtestSummary.maxDrawdown)
          ? Number(backtestSummary.maxDrawdown)
          : null
      );
      setTextContent(
        tDD,
        backtestSummary && Number.isFinite(backtestSummary.maxDrawdown)
          ? `DD ${_fmtPct(backtestSummary.maxDrawdown)}`
          : "Max Drawdown —"
      );
    }
    if (tSharpe) {
      setMetricValueAttribute(
        tSharpe,
        backtestSummary && Number.isFinite(backtestSummary.sharpe)
          ? Number(backtestSummary.sharpe)
          : null
      );
      setTextContent(
        tSharpe,
        backtestSummary && Number.isFinite(backtestSummary.sharpe)
          ? `Sharpe ${Number(backtestSummary.sharpe).toFixed(2)}`
          : "Sharpe Ratio —"
      );
    }
    if (tCalmar) {
      setMetricValueAttribute(tCalmar, null);
    }
    if (tPromotion) {
      let promotionValue = null;
      let promotionText = "Model Promotion Status —";

      if (promotionStatus && promotionStatus.enabled === false) {
        promotionValue = "OFF";
        promotionText = "Promotion OFF";
      } else if (promotionStatus && promotionStatus.allowed === false) {
        promotionValue = "BLOCKED";
        promotionText = "Promotion BLOCKED";
      } else if (promotionStatus && promotionStatus.allowed === true) {
        promotionValue = "ALLOWED";
        promotionText = "Promotion ALLOWED";
      }

      setMetricValueAttribute(tPromotion, promotionValue);
      setTextContent(tPromotion, promotionText);
      applyInlineMetricAnnotation(tPromotion, "promotion_status", promotionValue);
    }
  } catch {}
}

function renderTopLevelHealthScore() {
  try {
    const scorecard = computeHealthScore({
      alerts: Array.isArray(_lastAlerts) ? _lastAlerts : null,
      health: _lastHealth || window.__LAST_HEALTH__ || null,
      readiness: window.__LAST_READINESS__ || null,
      systemState: window.__LAST_SYSTEM_STATE__ || null,
      systemStatus: window.__LAST_SYSTEM_STATUS_HEADER__ || null,
      executionBarrier: window.__LAST_EXECUTION_BARRIER__ || null,
      executionDegraded: _isExecutionDegraded(),
    });
    renderHealthScoreSummary(scorecard);
  } catch {}
}

function scheduleOperatorWsReconnect() {
  if (_operatorWsStopped) return;
  if (_operatorWsRetryTimer) return;

  const delay = _operatorWsRetryMs;
  _operatorWsRetryTimer = window.setTimeout(() => {
    _operatorWsRetryTimer = null;
    connectOperatorRealtime();
  }, delay);
  _operatorWsRetryMs = Math.min(_operatorWsRetryMs * 2, OPERATOR_WS_RETRY_MAX_MS);
  setOperatorWsIndicator("retrying");
}

function scheduleOperatorWsRender() {
  if (_wsRenderTimer) return;

  _wsRenderTimer = window.setTimeout(async () => {
    _wsRenderTimer = null;

    const healthPayload = _pendingWsHealth;
    const healthTs = _pendingWsHealthTs;
    const pnlPayload = _pendingWsPnl;
    const pnlTs = _pendingWsPnlTs;
    const statusTs = Math.max(
      coerceRealtimeTs(_pendingWsTradingTs),
      coerceRealtimeTs(healthTs),
      coerceRealtimeTs(pnlTs)
    ) || Date.now();

    _pendingWsHealth = null;
    _pendingWsHealthTs = 0;
    _pendingWsPnl = null;
    _pendingWsPnlTs = 0;
    _pendingWsTradingTs = 0;

    if (healthPayload && healthTs >= _latestHealthStateTs) {
      _latestHealthStateTs = healthTs;
      _lastHealth = healthPayload;
      window.__LAST_HEALTH__ = _lastHealth;
      await loadHealth(_lastHealth);
    }

    if (pnlPayload) {
      await loadPnL(pnlPayload, { sourceTs: pnlTs });
    }

    renderDashboardSystemStatus(_lastHealth, window.__LAST_SYSTEM_STATE__);
    await loadSystemStatusHeader({
      health: _lastHealth,
      systemState: window.__LAST_SYSTEM_STATE__,
      executionBarrier: window.__LAST_EXECUTION_BARRIER__,
      __sourceTs: statusTs,
    });
    renderTelemetryStripFromDashboard();
    renderTopLevelHealthScore();
    renderRecommendedActionCard();
    updateProChartsPanelState();
  }, 80);
}

function handleOperatorRealtimeMessage(raw) {
  let message = null;
  try {
    message = JSON.parse(raw);
  } catch {
    return;
  }
  if (!message || typeof message !== "object") return;

  const type = String(message.type || "").trim();
  const payload = message.payload;
  const receivedAt = Date.now();
  _lastOperatorRealtimeMessageTs = receivedAt;

  if (type === "health_update") {
    const normalizedHealth = normalizeOperatorHealthPayload(payload);
    if (!normalizedHealth) return;
    _pendingWsHealth = normalizedHealth;
    _pendingWsHealthTs = extractRealtimeTs(payload, receivedAt);
    scheduleOperatorWsRender();
    return;
  }

  if (type === "pnl_update") {
    const normalizedPnL = normalizePnLPayload(payload);
    if (!normalizedPnL) return;
    _pendingWsPnl = normalizedPnL;
    _pendingWsPnlTs = extractRealtimeTs(normalizedPnL, receivedAt);
    scheduleOperatorWsRender();
    return;
  }

  if (type === "trading_update") {
    _pendingWsTradingTs = Math.max(
      coerceRealtimeTs(_pendingWsTradingTs),
      extractTradingRealtimeTs(payload, receivedAt)
    );
    scheduleOperatorWsRender();
  }
}

function connectOperatorRealtime() {
  if (_operatorWsStopped || typeof window === "undefined" || typeof window.WebSocket !== "function") {
    setOperatorWsIndicator("polling");
    return;
  }

  if (!OPERATOR_WS_URL) {
    setOperatorWsIndicator("polling");
    return;
  }

  if (_operatorWs && (_operatorWs.readyState === WebSocket.OPEN || _operatorWs.readyState === WebSocket.CONNECTING)) {
    return;
  }

  setOperatorWsIndicator("connecting");

  try {
    const ws = new WebSocket(OPERATOR_WS_URL);
    _operatorWs = ws;

    ws.onopen = () => {
      _operatorWsRetryMs = OPERATOR_WS_RETRY_BASE_MS;
      setOperatorWsIndicator("connected");
    };

    ws.onmessage = (event) => {
      if (!event || typeof event.data !== "string") return;
      handleOperatorRealtimeMessage(event.data);
    };

    ws.onerror = () => {
      setOperatorWsIndicator("retrying");
    };

    ws.onclose = () => {
      if (_operatorWs === ws) {
        _operatorWs = null;
      }
      if (_operatorWsStopped) {
        setOperatorWsIndicator("polling");
        return;
      }
      scheduleOperatorWsReconnect();
    };
  } catch {
    scheduleOperatorWsReconnect();
  }
}

function startOperatorRealtime() {
  _operatorWsStopped = false;
  setOperatorWsIndicator("polling");
  connectOperatorRealtime();

  if (!window.__OPERATOR_WS_PAGEHIDE_BOUND__) {
    window.__OPERATOR_WS_PAGEHIDE_BOUND__ = true;
    window.addEventListener("pagehide", () => {
      _operatorWsStopped = true;
      if (_operatorWsRetryTimer) {
        clearTimeout(_operatorWsRetryTimer);
        _operatorWsRetryTimer = null;
      }
      if (_wsRenderTimer) {
        clearTimeout(_wsRenderTimer);
        _wsRenderTimer = null;
      }
      const ws = _operatorWs;
      _operatorWs = null;
      try {
        ws && ws.close();
      } catch {}
      setOperatorWsIndicator("polling");
    });
  }
}

function layoutDashboardPanels(screen = ACTIVE_DASHBOARD_SCREEN) {
  const left = document.querySelector(".dashboard-left");
  const center = document.querySelector(".dashboard-center");
  const right = document.querySelector(".dashboard-right");
  if (!left || !center || !right) return;

  const moveById = (id, target) => {
    const el = document.getElementById(id);
    if (!el || !target || el.parentElement === target) return;
    target.appendChild(el);
  };

  const layouts = {
    overview: {
      left: [
        "operatorSummaryCard",
        "livePnlCard",
        "marketStressPanel",
      ],
      center: [
        "recentDecisionsCard",
        "alertsCard",
        "executionAdvisoryCard",
        "proChartsCard",
      ],
      right: [
        "systemHealthCard",
        "executionCostCard",
      ],
    },
    operate: {
      left: [
        "operatorStartupCard",
        "systemHealthCard",
        "alertsCard",
      ],
      center: [
        "jobConsoleCard",
        "executionAdvisoryCard",
        "systemStateCard",
        "promotionsSafetyCard",
      ],
      right: [
        "driftStatusCard",
        "trainingStatusCard",
        "executionCostCard",
        "brokerPanel",
        "execOverlaysPanel",
      ],
    },
    explain: {
      left: [
        "recentDecisionsCard",
        "humanAlignmentCard",
        "portfolioCard",
      ],
      center: [
        "governanceSummaryCard",
        "strategyStatusCard",
        "promotionAuditCard",
        "systemStateCard",
      ],
      right: [
        "executionAdvisoryCard",
        "promotionsSafetyCard",
        "driftExplainerPanel",
        "equityDriftPanel",
        "competitionOpsCard",
      ],
    },
    analyze: {
      left: [
        "socialPressureCard",
        "socialRegimesCard",
        "socialBlocksCard",
        "newsPanelsCard",
        "weatherWidgetsCard",
        "relevanceStatsCard",
        "sizePolicyCard",
        "championChallengerCard",
        "validationScoresCard",
      ],
      center: [
        "portfolioBacktestCard",
        "temporalEvalCard",
        "temporalShadowCard",
        "calibrationCurvesCard",
        "temporalModelsCard",
        "proChartsCard",
      ],
      right: [
        "strategyMetricsCard",
        "modelMetricsCard",
        "telemetryCard",
        "jobHistoryCard",
        "competitionOpsCard",
        "driftExplainerPanel",
        "equityDriftPanel",
        "trainingStatusCard",
        "execOverlaysPanel",
        "brokerPanel",
      ],
    },
    data: {
      left: [
        "dataHealthSummaryCard",
      ],
      center: [
        "dataProviderTelemetryCard",
      ],
      right: [
        "dataRuntimeSignalsCard",
      ],
    },
    positions: {
      left: [
        "positionsExposureSummaryCard",
      ],
      center: [
        "positionsTargetsCard",
      ],
      right: [
        "positionsLiveBookCard",
        "positionsDiagnosticsCard",
      ],
    },
    execution: {
      left: [
        "executionSnapshotCard",
      ],
      center: [
        "executionOrdersCard",
        "executionFillsCard",
      ],
      right: [
        "executionMetricsSummaryCard",
        "executionCostCard",
        "execOverlaysPanel",
      ],
    },
  };

  const spec = layouts[normalizeDashboardScreen(screen)] || layouts.overview;
  spec.left.forEach((id) => moveById(id, left));
  spec.center.forEach((id) => moveById(id, center));
  spec.right.forEach((id) => moveById(id, right));
}

function normalizeDashboardScreen(value) {
  const raw = String(value || "").trim().toLowerCase();
  const v = DASHBOARD_SCREEN_ALIASES[raw] || raw;
  return DASHBOARD_SCREENS.has(v) ? v : "overview";
}

function getDashboardScreenLabel(screen) {
  const normalized = normalizeDashboardScreen(screen);
  return DASHBOARD_SCREEN_LABELS[normalized] || normalized;
}

function getDashboardScreenFromHash() {
  const raw = String(window.location.hash || "").replace(/^#/, "").trim().toLowerCase();
  if (!raw) return "";
  const aliased = DASHBOARD_SCREEN_ALIASES[raw] || raw;
  return DASHBOARD_SCREENS.has(aliased) ? aliased : "";
}

function syncDashboardScreenHash(screen, mode = "replace") {
  const normalized = normalizeDashboardScreen(screen);
  const targetHash = `#${normalized}`;
  if (window.location.hash === targetHash) return;
  try {
    const url = new URL(window.location.href);
    url.hash = normalized;
    const next = `${url.pathname}${url.search}${url.hash}`;
    if (mode === "push") {
      window.history.pushState({ dashboardScreen: normalized }, "", next);
    } else {
      window.history.replaceState({ dashboardScreen: normalized }, "", next);
    }
  } catch {
    window.location.hash = normalized;
  }
}

function applyDashboardScreen(screen, options = {}) {
  const { syncHash = false, hashMode = "replace" } = options;
  const requestedScreen = normalizeDashboardScreen(screen);
  const persona = getActiveDashboardPersona();
  ACTIVE_DASHBOARD_SCREEN = isDashboardScreenAllowed(persona, requestedScreen)
    ? requestedScreen
    : normalizeDashboardScreen(getDefaultDashboardScreen(persona));

  try {
    localStorage.setItem(DASHBOARD_SCREEN_KEY, ACTIVE_DASHBOARD_SCREEN);
  } catch {}

  if (syncHash) {
    syncDashboardScreenHash(ACTIVE_DASHBOARD_SCREEN, hashMode);
  }

  document.body.dataset.dashboardScreen = ACTIVE_DASHBOARD_SCREEN;
  layoutDashboardPanels(ACTIVE_DASHBOARD_SCREEN);

  const label = document.getElementById("dashboardScreenLabel");
  if (label) label.textContent = getDashboardScreenLabel(ACTIVE_DASHBOARD_SCREEN);
  updateSurfaceLinks();

  document.querySelectorAll("[data-screen-target]").forEach((btn) => {
    const target = normalizeDashboardScreen(btn.getAttribute("data-screen-target"));
    btn.classList.toggle("is-active", target === ACTIVE_DASHBOARD_SCREEN);
  });

  document.querySelectorAll("[data-screens]").forEach((el) => {
    const raw = String(el.getAttribute("data-screens") || "");
    const screens = raw
      .split(",")
      .map((part) => String(part || "").trim())
      .filter(Boolean)
      .map((part) => normalizeDashboardScreen(part));
    const visible = screens.includes(ACTIVE_DASHBOARD_SCREEN);
    el.classList.toggle("dashboard-screen-hidden", !visible);
  });

  applyDashboardPersonaView({
    root: document,
    screen: ACTIVE_DASHBOARD_SCREEN,
  });

  if (DASHBOARD_BOOTED) {
    void refreshActiveScreenData();
  }
}

function buildSharedRefreshTasks(preloaded = {}) {
  const hasOwn = (key) => Object.prototype.hasOwnProperty.call(preloaded || {}, key);
  const systemState = hasOwn("systemState")
    ? preloaded.systemState
    : null;
  const health = hasOwn("health")
    ? preloaded.health
    : _lastHealth;
  const readiness = hasOwn("readiness")
    ? preloaded.readiness
    : (window.__LAST_READINESS__ || null);
  const executionBarrier = hasOwn("executionBarrier")
    ? preloaded.executionBarrier
    : (window.__LAST_EXECUTION_BARRIER__ || null);
  const stressPayload = hasOwn("stressPayload")
    ? preloaded.stressPayload
    : (window.__LAST_MARKET_STRESS__ || null);
  const sharedFailures = hasOwn("sharedFailures")
    ? preloaded.sharedFailures
    : (window.__LAST_REFRESH_FAILURES__ || []);

  return [
    loadHealth({
      health,
      readiness,
      sharedFailures,
    }),
    loadSystemStatusHeader({
      health,
      systemState: systemState || window.__LAST_SYSTEM_STATE__ || null,
      executionBarrier,
    }),
    loadOperatorSummary(fetchJSON, {
      systemState: systemState || window.__LAST_SYSTEM_STATE__ || null,
      executionBarrier,
      health,
      readiness,
      stressPayload,
      sharedFailures,
    }),
    loadExecutionBarrier(),
    loadTelemetry(fetchJSON),
    loadPromotionStatus(),
  ];
}

function buildScreenRefreshTasks(screen, preloaded = {}) {
  const normalized = normalizeDashboardScreen(screen);
  const tasksByScreen = {
    overview: [
      loadPnL(),
      loadPerformanceDivergence(fetchJSON, document, renderLineChart),
      loadMarketStress(fetchJSON),
      loadMarketStressHistory(fetchJSON),
      loadNotificationStatus(),
      loadAlerts(),
      loadDecisions(),
      loadExecutionAdvisories(),
      loadExecutionByConfidence(fetchJSON, toast, OPERATOR_MODE),
      loadProCharts(fetchJSON),
    ],
    operate: [
      loadStructuredReadiness(),
      loadOperatorStartupPanel(),
      loadOperatorSidecarStatus(),
      loadNotificationStatus(),
      loadJobs(),
      loadLog(),
      loadStructuredLogViewer(),
      loadSupervisorStatus(),
      loadAlerts(),
      loadExecutionAdvisories(),
      loadExecutionOverlays(),
      loadBroker(fetchJSON),
      loadExecutionByConfidence(fetchJSON, toast, OPERATOR_MODE),
    ],
    explain: [
      loadDecisions(),
      loadHumanAlignmentSummary(),
      loadGovernanceSummary(),
      loadPromotionGate(),
      loadStrategyStatus(),
      loadPerformanceDivergence(fetchJSON, document, renderLineChart),
      loadPromotionAudit(),
      loadCausalScores(),
      loadPortfolio(fetchJSON),
      loadDriftExplainer(fetchJSON),
      loadEquityDrift(fetchJSON),
      loadEquityReconciliation(fetchJSON),
      loadExecutionAdvisories(),
    ],
    analyze: [
      loadTemporalEval(),
      loadTemporalShadowEval(),
      loadPerformanceDivergence(fetchJSON, document, renderLineChart),
      loadSocialPressure({ symbol: _getSelectedSymbolOptional() }),
      loadSocialRegimes({ symbol: _getSelectedSymbolOptional() }),
      loadSocialBlocks({ symbol: _getSelectedSymbolOptional() }),
      loadWeatherWidgets({ symbol: _getActiveSymbol("SPY") }),
      loadPromotionAudit(),
      loadCausalScores(),
      refreshCalibCurves(),
      loadPromotionGate(),
      loadModelRegistry(),
      loadValidation(),
      loadStrategyMetrics(),
      loadModelMetrics(),
      loadNewsPanels(fetchJSON, { symbol: _getSelectedSymbolOptional() }),
      loadNewsSentiment(fetchJSON, { symbol: _getSelectedSymbolOptional() }),
      loadPortfolioBacktestLatest(fetchJSON),
      loadExecutionOverlays(),
      loadModelDiagnostics(),
      loadConfidenceMass(),
      loadJobHistory(),
      loadConfidenceTrends(),
      loadRelevanceStats(),
      loadBroker(fetchJSON),
      loadDriftExplainer(fetchJSON),
      loadEquityDrift(fetchJSON),
    ],
    data: [
      loadDataHealthScreen(),
    ],
    positions: [
      loadPositionsExposureScreen(),
    ],
    execution: [
      loadDashboardExecutionScreen(),
      loadExecutionByConfidence(fetchJSON, toast, OPERATOR_MODE),
      loadExecutionOverlays(),
    ],
  };
  return tasksByScreen[normalized] || tasksByScreen.overview;
}

async function refreshActiveScreenData(preloaded = {}) {
  const seq = ++_activeScreenRefreshSeq;
  const sharedTasks = buildSharedRefreshTasks(preloaded);
  const screenTasks = buildScreenRefreshTasks(ACTIVE_DASHBOARD_SCREEN, preloaded);
  const tasks = [...sharedTasks, ...screenTasks];
  await Promise.allSettled(tasks);
  if (seq !== _activeScreenRefreshSeq) return;
  updateDecisionHeader();
  renderTelemetryStripFromDashboard();
  renderTopLevelHealthScore();
  renderRecommendedActionCard();
  updateProChartsPanelState();
}

function wireDashboardScreens() {
  const buttons = Array.from(document.querySelectorAll("[data-screen-target]"));
  if (!buttons.length) return;

  const applyLocationScreen = () => {
    const fromHash = getDashboardScreenFromHash();
    if (!fromHash || fromHash === ACTIVE_DASHBOARD_SCREEN) return;
    applyDashboardScreen(fromHash);
  };

  if (!window.__dashboardScreenHistoryBound) {
    window.__dashboardScreenHistoryBound = true;
    window.addEventListener("popstate", applyLocationScreen);
    window.addEventListener("hashchange", applyLocationScreen);
  }

  buttons.forEach((btn) => {
    if (btn._boundDashboardScreen) return;
    btn._boundDashboardScreen = true;
    btn.addEventListener("click", () => {
      const target = btn.getAttribute("data-screen-target");
      applyDashboardScreen(target, { syncHash: true, hashMode: "push" });
      try {
        window.scrollTo({ top: 0, behavior: "smooth" });
      } catch {}
    });
  });

  let initial = getDashboardScreenFromHash() || "overview";
  if (!getDashboardScreenFromHash() && _launchContext && _launchContext.screen) {
    initial = _launchContext.screen;
  } else if (!getDashboardScreenFromHash()) {
    try {
      initial = localStorage.getItem(DASHBOARD_SCREEN_KEY) || initial;
    } catch {}
  }
  applyDashboardScreen(initial, { syncHash: true, hashMode: "replace" });
}

// Debounced renderer holder (global filters use it)
let _debouncedRender = null;

function _initDebouncedRender() {
  if (!_debouncedRender) {
    _debouncedRender = _debounce(() => {
      loadAlerts();
    }, 300);
  }
}

// Some older UI paths call applyModeToDOM(); keep it as a thin wrapper.
function applyModeToDOM() {
  applyPolicyToDOM({
    operatorMode: OPERATOR_MODE,
    expertUnlocked: EXPERT_UNLOCK
  });
}

function setOpsError(msg) {
  const el = document.getElementById("console");
  if (!el) return;

  const lines = (el.textContent || "").split("\n");
  lines.push(`[ops] ${msg}`);
  el.textContent = lines.slice(-500).join("\n");
}

function toast(msg, level = "ok", ms = 2200) {
  let el = document.getElementById("uiToast");
  if (!el) {
    el = document.createElement("div");
    el.id = "uiToast";
    el.style.position = "fixed";
    el.style.bottom = "16px";
    el.style.right = "16px";
    el.style.zIndex = "99999";
    el.style.padding = "10px 14px";
    el.style.borderRadius = "10px";
    el.style.border = "1px solid #30363d";
    el.style.background = "#0b0f15";
    el.style.color = "#e6edf3";
    el.style.boxShadow = "0 12px 30px rgba(0,0,0,.35)";
    el.style.fontSize = "13px";
    document.body.appendChild(el);
  }

  el.textContent = msg;
  el.className = buildPillClassName(
    el,
    level === "bad" ? "crit" : level === "warn" ? "warn" : level === "crit" ? "crit" : "ok"
  );
  el.style.display = "block";

  clearTimeout(el._t);
  el._t = setTimeout(() => {
    el.style.display = "none";
  }, ms);
}

function followJob(name) {
  setSelectedJob(name);
}

function setStatusTone(el, tone, text) {
  if (!el) return;
  const normalized = String(tone || "bad").trim().toLowerCase();
  const nextTone = normalized === "warn" ? "warn" : normalized === "ok" ? "ok" : "bad";
  el.textContent = text;
  el.className = "status " + nextTone;
}

function setStatus(el, ok, text) {
  setStatusTone(el, ok ? "ok" : "bad", text);
}

const UI_INTERACTION_SESSION_ID = (() => {
  try {
    const key = "dashboard_ui_session_v1";
    const existing = localStorage.getItem(key);
    if (existing) return existing;
    const created = `dashboard-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    localStorage.setItem(key, created);
    return created;
  } catch {
    return `dashboard-${Date.now()}`;
  }
})();
let _currentDecisionModalId = null;
let _currentDecisionModalPayload = null;
let _currentExecutionAdvisoryItem = null;
let _launchContext = {
  screen: "",
  symbol: "",
  decisionId: "",
  advisoryId: "",
};
let _symbolContextRefreshTimer = null;

function dashboardRiskRank(value) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "critical" || token === "crit" || token === "high") return 3;
  if (token === "medium" || token === "med" || token === "warn") return 2;
  if (token === "low") return 1;
  return 0;
}

const DASHBOARD_TABLE_DEFAULTS = Object.freeze({
  recentDecisions: { sortKey: "ts_ms", sortDir: "desc", maxRows: 12 },
  executionOrders: { sortKey: "updatedTs", sortDir: "desc", maxRows: 40 },
  executionFills: { sortKey: "ts_ms", sortDir: "desc", maxRows: 40 },
  suppressedTrades: { sortKey: "ts_ms", sortDir: "desc", maxRows: 12 },
});
const DASHBOARD_TABLE_STATE = {};
const DASHBOARD_TABLE_RENDERERS = new Map();

const RECENT_DECISION_TABLE_COLUMNS = Object.freeze([
  { key: "symbol", accessor: (row) => row && row.symbol },
  { key: "action", accessor: (row) => row && row.action },
  { key: "risk", accessor: (row) => row && row.risk_impact, compare: (left, right) => dashboardRiskRank(left) - dashboardRiskRank(right) },
  { key: "confidence", accessor: (row) => row && (row.confidence_raw ?? row.confidence ?? row.certainty) },
  { key: "why", accessor: (row) => row && row.why },
  { key: "ts_ms", accessor: (row) => row && row.ts_ms, searchable: false },
]);
const EXECUTION_ORDER_TABLE_COLUMNS = Object.freeze([
  { key: "symbol", accessor: (row) => row && row.symbol },
  { key: "side", accessor: (row) => row && row.side },
  { key: "size", accessor: (row) => row && row.size },
  { key: "status", accessor: (row) => row && row.status },
  { key: "updatedTs", accessor: (row) => row && row.updatedTs, searchable: false, hidden: true },
]);
const EXECUTION_FILL_TABLE_COLUMNS = Object.freeze([
  { key: "symbol", accessor: (row) => row && row.symbol },
  { key: "px", accessor: (row) => row && row.px },
  { key: "qty", accessor: (row) => row && row.qty },
  { key: "ts_ms", accessor: (row) => row && row.ts_ms, searchable: false },
]);
const SUPPRESSED_TRADE_TABLE_COLUMNS = Object.freeze([
  { key: "ts_ms", accessor: (row) => row && row.ts_ms, searchable: false },
  { key: "symbol", accessor: (row) => row && row.symbol },
  { key: "reason", accessor: (row) => row && row.reason },
  { key: "lineage", accessor: (row) => row && row.lineage },
]);

function getDashboardTableState(tableId) {
  const key = String(tableId || "").trim();
  const defaults = DASHBOARD_TABLE_DEFAULTS[key] || {};
  if (!DASHBOARD_TABLE_STATE[key]) {
    DASHBOARD_TABLE_STATE[key] = {
      query: "",
      sortKey: defaults.sortKey || "",
      sortDir: defaults.sortDir || "asc",
    };
  }
  return DASHBOARD_TABLE_STATE[key];
}

function rerenderDashboardTable(tableId) {
  const render = DASHBOARD_TABLE_RENDERERS.get(String(tableId || "").trim());
  if (typeof render === "function") render();
}

function setDashboardTableSort(tableId, sortKey, explicitDir = "") {
  const state = getDashboardTableState(tableId);
  const key = String(sortKey || "").trim();
  if (!key) return;
  if (state.sortKey === key) {
    state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
  } else {
    state.sortKey = key;
    state.sortDir = String(explicitDir || "asc").toLowerCase() === "desc" ? "desc" : "asc";
  }
  rerenderDashboardTable(tableId);
}

function syncDashboardTableControls(tableId, view) {
  const state = getDashboardTableState(tableId);
  const root = document;
  root.querySelectorAll(`[data-dashboard-table-filter="${tableId}"]`).forEach((input) => {
    if (input && input !== document.activeElement) input.value = state.query || "";
  });
  root.querySelectorAll(`[data-dashboard-table-sort="${tableId}"]`).forEach((button) => {
    const key = String(button.getAttribute("data-sort-key") || "").trim();
    const active = !!key && key === state.sortKey;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-sort", active ? (state.sortDir === "desc" ? "descending" : "ascending") : "none");
    const indicator = button.querySelector(".tableSortIndicator");
    if (indicator) indicator.textContent = active ? (state.sortDir === "desc" ? "v" : "^") : "";
  });
  root.querySelectorAll(`[data-dashboard-table-sort-select="${tableId}"]`).forEach((select) => {
    const next = `${state.sortKey}:${state.sortDir}`;
    if (select && select.value !== next) select.value = next;
  });
  root.querySelectorAll(`[data-dashboard-table-count="${tableId}"]`).forEach((node) => {
    if (!node) return;
    const total = view ? Number(view.totalRows || 0) : 0;
    const filtered = view ? Number(view.filteredRowsCount || 0) : 0;
    const visible = view ? Number(view.visibleRowsCount || 0) : 0;
    node.textContent = state.query
      ? `${visible}/${total} shown`
      : (visible < filtered ? `${visible}/${filtered} shown` : `${total} rows`);
  });
}

function renderDashboardTableView({
  tableId,
  bodyId,
  columns,
  rows,
  emptyText,
  filteredEmptyText,
  rowToHtml,
  maxRows,
  colspan,
}) {
  const state = getDashboardTableState(tableId);
  const defaults = DASHBOARD_TABLE_DEFAULTS[tableId] || {};
  const view = buildTableView(rows, columns, {
    query: state.query,
    sortKey: state.sortKey || defaults.sortKey,
    sortDir: state.sortDir || defaults.sortDir,
    maxRows: maxRows || defaults.maxRows,
  });
  const body = document.getElementById(bodyId);
  if (body) {
    if (!view.visibleRows.length) {
      renderEmptyTableBody(
        bodyId,
        colspan || columns.filter((column) => column && column.hidden !== true).length || columns.length,
        view.totalRows > 0 ? (filteredEmptyText || "No rows match the current filter.") : emptyText
      );
    } else {
      body.innerHTML = view.visibleRows.map(rowToHtml).join("");
    }
  }
  syncDashboardTableControls(tableId, view);
  return view;
}

function wireDashboardTableControls() {
  if (wireDashboardTableControls._bound) return;
  wireDashboardTableControls._bound = true;

  document.addEventListener("input", (event) => {
    const target = event && event.target;
    const input = target && typeof target.closest === "function"
      ? target.closest("[data-dashboard-table-filter]")
      : null;
    if (!input) return;
    const tableId = input.getAttribute("data-dashboard-table-filter");
    const state = getDashboardTableState(tableId);
    state.query = input.value || "";
    rerenderDashboardTable(tableId);
  });

  document.addEventListener("change", (event) => {
    const target = event && event.target;
    const select = target && typeof target.closest === "function"
      ? target.closest("[data-dashboard-table-sort-select]")
      : null;
    if (!select) return;
    const [sortKey, sortDir] = String(select.value || "").split(":");
    setDashboardTableSort(select.getAttribute("data-dashboard-table-sort-select"), sortKey, sortDir);
  });

  document.addEventListener("click", (event) => {
    const target = event && event.target;
    const button = target && typeof target.closest === "function"
      ? target.closest("[data-dashboard-table-sort]")
      : null;
    if (!button) return;
    event.preventDefault();
    setDashboardTableSort(
      button.getAttribute("data-dashboard-table-sort"),
      button.getAttribute("data-sort-key"),
      button.getAttribute("data-sort-default")
    );
  });
}

async function postUiInteraction(payload) {
  try {
    await _fetchWithTimeout("/api/ui/interaction", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        actor: "operator",
        source: "dashboard",
        session_id: UI_INTERACTION_SESSION_ID,
        ...(payload || {}),
      }),
    });
  } catch {
    // passive analytics only; never block UI
  }
}

function _decisionLookupAttr(input, extraClass = "") {
  const lookup = normalizeDecisionLookup(input || {});
  if (!hasDecisionLookup(lookup)) return "";
  const payload = JSON.stringify(lookup);
  const classes = ["table-row", "decisionDrilldownRow", extraClass].filter(Boolean).join(" ");
  return ` class="${classes}" tabindex="0" role="button" data-decision-lookup="${escapeHTML(payload)}" title="Open decision drill-down"`;
}

function _plainTableRowClass(extraClass = "") {
  const classes = ["table-row", extraClass].filter(Boolean).join(" ");
  return ` class="${classes}"`;
}

function _parseDecisionLookupAttr(el) {
  if (!el || typeof el.getAttribute !== "function") return null;
  try {
    const parsed = JSON.parse(String(el.getAttribute("data-decision-lookup") || "{}"));
    const lookup = normalizeDecisionLookup(parsed);
    return hasDecisionLookup(lookup) ? lookup : null;
  } catch {
    return null;
  }
}

async function _openDecisionLookup(lookup, surface = "") {
  const normalized = normalizeDecisionLookup({ ...(lookup || {}), surface });
  if (!hasDecisionLookup(normalized)) {
    toast("Decision details unavailable for this row", "warn", 2400);
    return;
  }
  await postUiInteraction({
    decision_id: normalized.decisionId || null,
    alert_id: normalized.sourceAlertId || null,
    interaction_type: "decision_open",
    detail: {
      panel: surface || normalized.surface || "decision_surface",
      lookup: normalized,
    },
  });
  await openDecisionModal(normalized);
}

function wireDecisionDrilldownActivation() {
  if (document._boundDecisionDrilldownActivation) return;
  document._boundDecisionDrilldownActivation = true;

  const activate = async (event) => {
    const target = event && event.target;
    if (!target || typeof target.closest !== "function") return;
    const row = target.closest("[data-decision-lookup]");
    if (!row) return;
    const interactive = target.closest("button,a,input,select,textarea");
    if (interactive && interactive !== row) return;
    const lookup = _parseDecisionLookupAttr(row);
    if (!lookup) return;
    event.preventDefault();
    await _openDecisionLookup(lookup, lookup.surface || "decision_row");
  };

  document.addEventListener("click", (event) => {
    void activate(event);
  });
  document.addEventListener("keydown", (event) => {
    if (!event || (event.key !== "Enter" && event.key !== " ")) return;
    void activate(event);
  });
}

function decisionLookupForDecision(decision, surface = "recent_decisions") {
  return normalizeDecisionLookup({
    decisionId: decision && (decision.decision_id || decision.id),
    sourceAlertId: decision && (decision.source_alert_id || decision.alert_id),
    surface,
  });
}

function decisionLookupForOrderIntent(row, surface = "portfolio_order_intents") {
  return normalizeDecisionLookup({
    decisionId: row && row.decision_id,
    sourceAlertId: row && (row.source_alert_id || row.alert_id),
    portfolioOrderId: row && (row.source_order_id || row.portfolio_orders_id || row.id),
    clientOrderId: row && row.client_order_id,
    surface,
  });
}

function decisionLookupForLedger(row, surface = "suppressed_trades") {
  return normalizeDecisionLookup({
    sourceAlertId: row && (row.source_alert_id || row.alert_id),
    ledgerId: row && row.id,
    portfolioOrderId: row && (row.portfolio_orders_id || row.source_order_id),
    clientOrderId: row && row.client_order_id,
    surface,
  });
}

function _getActiveSymbol(fallback = "") {
  const contextSymbol = getSelectedSymbolContext().symbol;
  if (contextSymbol) return contextSymbol;
  const globalInput = document.getElementById("globalSymbol");
  const fromInput = globalInput && typeof globalInput.value === "string"
    ? globalInput.value.trim().toUpperCase()
    : "";
  return normalizeSelectedSymbol(fromInput) || normalizeSelectedSymbol(fallback) || "SPY";
}

function _getSelectedSymbolOptional() {
  return getSelectedSymbolContext().symbol || "";
}

function _isSelectedSymbol(symbol) {
  const selected = _getSelectedSymbolOptional();
  return !!selected && normalizeSelectedSymbol(symbol) === selected;
}

function _symbolContextClassFor(symbol) {
  return _isSelectedSymbol(symbol) ? "symbolContextMatch" : "";
}

function _symbolContextClassForText(text) {
  const selected = _getSelectedSymbolOptional();
  if (!selected) return "";
  const haystack = String(text || "").toUpperCase();
  return haystack.includes(selected) ? "symbolContextMatch" : "";
}

function _tableRowClassForSymbol(symbol, extraClass = "") {
  return ["table-row", _symbolContextClassFor(symbol), extraClass]
    .filter(Boolean)
    .join(" ");
}

function _symbolContextEmptyHint(rows, label = "rows") {
  const selected = _getSelectedSymbolOptional();
  if (!selected) return "";
  const hasMatch = asArray(rows).some((row) => _isSelectedSymbol(row && row.symbol));
  return hasMatch ? "" : `No ${label} for ${selected} in this feed; showing the latest available data.`;
}

function _buildTerminalUrl({ symbol = "", tf = "", type = "", source = "dashboard", screen = ACTIVE_DASHBOARD_SCREEN, decisionId = "", advisoryId = "" } = {}) {
  const url = new URL("/ui/terminal/terminal.html", window.location.origin);
  const sym = _getActiveSymbol(symbol);
  if (sym) url.searchParams.set("symbol", sym);
  if (tf) url.searchParams.set("tf", String(tf));
  if (type) url.searchParams.set("type", String(type));
  if (source) url.searchParams.set("source", String(source));
  if (screen) url.searchParams.set("screen", String(screen));
  if (decisionId) url.searchParams.set("decision_id", String(decisionId));
  if (advisoryId) url.searchParams.set("advisory_id", String(advisoryId));
  return url.toString();
}

function _buildOperatorUrl({ symbol = "", source = "dashboard", screen = ACTIVE_DASHBOARD_SCREEN, decisionId = "", advisoryId = "", focus = "" } = {}) {
  const url = new URL(OPERATOR_BASE_URL, window.location.origin);
  const sym = _getActiveSymbol(symbol);
  if (sym) url.searchParams.set("symbol", sym);
  if (source) url.searchParams.set("source", String(source));
  if (screen) url.searchParams.set("screen", String(screen));
  if (focus) url.searchParams.set("focus", String(focus));
  if (decisionId) url.searchParams.set("decision_id", String(decisionId));
  if (advisoryId) url.searchParams.set("advisory_id", String(advisoryId));
  return url.toString();
}

function _openContextUrl(url) {
  try {
    window.open(url, "_blank", "noopener");
  } catch {
    window.location.href = url;
  }
}

function updateSurfaceLinks() {
  const terminalLink = document.getElementById("appTerminalLink");
  const operatorLink = document.getElementById("appOperatorLink");
  const operatorConsoleLink = document.getElementById("operatorConsoleLink");
  if (terminalLink) {
    terminalLink.href = _buildTerminalUrl({ source: "dashboard_nav" });
  }
  if (operatorLink) {
    operatorLink.href = _buildOperatorUrl({ source: "dashboard_nav" });
  }
  if (operatorConsoleLink) {
    operatorConsoleLink.href = _buildOperatorUrl({ source: "operate_card" });
  }
}

function syncDashboardSymbolInput(ctx = getSelectedSymbolContext()) {
  const globalInput = document.getElementById("globalSymbol");
  if (!globalInput) return;

  const symbol = normalizeSelectedSymbol(ctx.symbol);
  if (symbol && globalInput.value !== symbol) {
    globalInput.value = symbol;
  } else if (!symbol && normalizeSelectedSymbol(globalInput.value) && ctx.source === "clear") {
    globalInput.value = "";
  }

  const source = String(ctx.source || "").trim();
  globalInput.title = symbol
    ? `Selected symbol context: ${symbol}${source ? ` (${source})` : ""}`
    : "No selected symbol context. Symbol-aware panels show their default data.";
}

function updateSymbolContextFromInput(source = "global_symbol") {
  const globalInput = document.getElementById("globalSymbol");
  if (!globalInput) return getSelectedSymbolContext();
  return updateSelectedSymbolContext({
    symbol: globalInput.value,
    source,
    persistUrl: true,
  });
}

function scheduleSymbolAwarePanelRefresh() {
  try {
    if (_symbolContextRefreshTimer) clearTimeout(_symbolContextRefreshTimer);
  } catch {}

  _symbolContextRefreshTimer = setTimeout(async () => {
    _symbolContextRefreshTimer = null;
    const selected = _getSelectedSymbolOptional();
    const tasks = [];

    tasks.push(_refreshProCharts());

    if (ACTIVE_DASHBOARD_SCREEN === "overview" || ACTIVE_DASHBOARD_SCREEN === "explain") {
      tasks.push(loadDecisions());
      tasks.push(loadExecutionAdvisories());
    }

    if (ACTIVE_DASHBOARD_SCREEN === "analyze") {
      tasks.push(loadSocialPressure({ symbol: selected }));
      tasks.push(loadSocialRegimes({ symbol: selected }));
      tasks.push(loadSocialBlocks({ symbol: selected }));
      tasks.push(loadWeatherWidgets({ symbol: selected || "SPY" }));
      tasks.push(loadNewsPanels(fetchJSON, { symbol: selected }));
      tasks.push(loadNewsSentiment(fetchJSON, { symbol: selected }));
    }

    if (ACTIVE_DASHBOARD_SCREEN === "positions") {
      tasks.push(loadPositionsExposureScreen());
    }

    if (ACTIVE_DASHBOARD_SCREEN === "execution") {
      tasks.push(loadDashboardExecutionScreen());
    }

    await Promise.allSettled(tasks);
    updateProChartsPanelState();
  }, 50);
}

function wireDashboardSymbolContext() {
  if (window.__dashboardSymbolContextBound) return;
  window.__dashboardSymbolContextBound = true;

  subscribeSelectedSymbolContext((ctx, prev) => {
    syncDashboardSymbolInput(ctx);
    updateSurfaceLinks();
    if (ctx.symbol !== prev.symbol) {
      scheduleSymbolAwarePanelRefresh();
    }
  }, { emit: true });
}

async function loadOperatorSidecarStatus() {
  const pill = document.getElementById("operatorSidecarPill");
  const wsPill = document.getElementById("operatorSidecarWsPill");
  const detail = document.getElementById("operatorSidecarDetail");
  const directLink = document.getElementById("operatorConsoleDirectLink");
  const consoleLink = document.getElementById("operatorConsoleLink");

  if (!pill && !detail && !directLink && !consoleLink) return;

  try {
    const payload = await fetchJSON("/api/operator/sidecar_status", { allowBusinessFalse: true });
    const reachable = payload && payload.reachable === true;
    const directUrl = String(payload.direct_url || "http://127.0.0.1:4001/");
    const sameOriginUrl = String(payload.same_origin_url || "/operator/");
    const ws = payload.websocket && typeof payload.websocket === "object" ? payload.websocket : {};

    if (pill) {
      pill.textContent = reachable ? "Sidecar: reachable" : "Sidecar: unavailable";
      pill.className = buildPillClassName(pill, reachable ? "ok" : "warn");
      pill.title = reachable ? `Node operator sidecar responded at ${directUrl}` : String(payload.detail || payload.error || "sidecar unavailable");
    }
    if (wsPill) {
      wsPill.textContent = ws.proxy_enabled ? "Realtime: proxied" : "Realtime: direct WS";
      wsPill.className = buildPillClassName(wsPill, ws.proxy_enabled ? "ok" : "neutral");
      wsPill.title = String(ws.deferred_reason || ws.direct_url || "Operator realtime channel");
    }
    if (detail) {
      detail.textContent = reachable
        ? `Same-origin console is available at ${sameOriginUrl}; direct port 4001 remains available.`
        : `Same-origin console shell is available, but Node sidecar calls will report unavailable until ${directUrl} responds.`;
    }
    if (directLink) directLink.href = directUrl;
    if (consoleLink) consoleLink.href = _buildOperatorUrl({ source: "operate_card" });
  } catch (e) {
    if (pill) {
      pill.textContent = "Sidecar: status error";
      pill.className = buildPillClassName(pill, "warn");
      pill.title = describeUiError(e);
    }
    if (wsPill) {
      wsPill.textContent = "Realtime: direct WS";
      wsPill.className = buildPillClassName(wsPill, "neutral");
    }
    if (detail) {
      detail.textContent = `Could not check Node operator sidecar: ${describeUiError(e)}`;
    }
  }
}

function applyDashboardLaunchParams() {
  try {
    const params = new URLSearchParams(window.location.search || "");
    const urlSymbolContext = initSelectedSymbolContextFromUrl({
      source: "url",
      persistUrl: false,
    });
    _launchContext = {
      screen: String(params.get("screen") || "").trim().toLowerCase(),
      symbol: urlSymbolContext.symbol || String(params.get("symbol") || "").trim().toUpperCase(),
      decisionId: String(params.get("decision_id") || "").trim(),
      advisoryId: String(params.get("advisory_id") || "").trim(),
    };

    const globalSymbolInput = document.getElementById("globalSymbol");
    if (globalSymbolInput && _launchContext.symbol) {
      globalSymbolInput.value = _launchContext.symbol;
    }
  } catch {}
}

function openDecisionContextInTerminal() {
  const decision = _currentDecisionModalPayload && _currentDecisionModalPayload.decision
    ? _currentDecisionModalPayload.decision
    : null;
  if (!decision) return;
  _openContextUrl(_buildTerminalUrl({
    symbol: decision.symbol,
    source: "decision_modal",
    screen: ACTIVE_DASHBOARD_SCREEN,
    decisionId: decision.decision_id || _currentDecisionModalId,
  }));
}

function openDecisionContextInOperator() {
  const decision = _currentDecisionModalPayload && _currentDecisionModalPayload.decision
    ? _currentDecisionModalPayload.decision
    : null;
  if (!decision) return;
  _openContextUrl(_buildOperatorUrl({
    symbol: decision.symbol,
    source: "decision_modal",
    screen: ACTIVE_DASHBOARD_SCREEN,
    decisionId: decision.decision_id || _currentDecisionModalId,
    focus: "decision",
  }));
}

function openAdvisoryContextInTerminal() {
  const item = _currentExecutionAdvisoryItem;
  if (!item) return;
  _openContextUrl(_buildTerminalUrl({
    symbol: item.symbol,
    source: "execution_advisory_modal",
    screen: ACTIVE_DASHBOARD_SCREEN,
    advisoryId: item.advisory_id,
  }));
}

function openAdvisoryContextInOperator() {
  const item = _currentExecutionAdvisoryItem;
  if (!item) return;
  _openContextUrl(_buildOperatorUrl({
    symbol: item.symbol,
    source: "execution_advisory_modal",
    screen: ACTIVE_DASHBOARD_SCREEN,
    advisoryId: item.advisory_id,
    focus: "execution",
  }));
}

try {
  window.__postUiInteraction = postUiInteraction;
} catch {}

function _decisionRiskPillClass(riskImpact) {
  const risk = String(riskImpact || "").toLowerCase();
  if (risk === "high") return "crit";
  if (risk === "medium") return "warn";
  return "dim";
}

function renderDecisionCard(decision) {
  const lookup = decisionLookupForDecision(decision, "recent_decisions");
  const card = document.createElement("button");
  card.type = "button";
  const selectedClass = _symbolContextClassFor(decision && decision.symbol);
  card.className = ["decisionCard", selectedClass].filter(Boolean).join(" ");
  if (selectedClass) {
    card.title = `Matches selected symbol ${_getSelectedSymbolOptional()}`;
  }

  const symbol = escapeHTML(String(decision.symbol || "UNKNOWN"));
  const action = escapeHTML(String(decision.action || "hold"));
  const sizeDelta = Number.isFinite(Number(decision.size_delta_pct))
    ? `${Number(decision.size_delta_pct).toFixed(2)}%`
    : "0.00%";
  const certainty = Number.isFinite(Number(decision.certainty))
    ? `${Math.round(Number(decision.certainty) * 100)}%`
    : "0%";
  const rawConfidence = Number.isFinite(Number(decision.confidence_raw))
    ? `${Math.round(Number(decision.confidence_raw) * 100)}% raw`
    : "raw —";
  const strength = Number.isFinite(Number(decision.prediction_strength))
    ? `strength ${Number(decision.prediction_strength).toFixed(2)}`
    : "strength —";
  const risk = String(decision.risk_impact || "low").toLowerCase();
  const why = escapeHTML(String(decision.why || "Decision details available, but no explanation text was stored."));
  const tsLabel = decision.ts_ms ? fmtTime(decision.ts_ms) : "—";

  card.innerHTML = `
    <div class="decisionCardTop">
      <span class="pill dim mono">${symbol}</span>
      <span class="pill ok">${escapeHTML(action)}</span>
      <span class="pill ${_decisionRiskPillClass(risk)}">risk: ${escapeHTML(risk)}</span>
    </div>
    <div class="decisionMetaRow">
      <span class="pill dim">delta ${escapeHTML(sizeDelta)}</span>
      <span class="pill dim">certainty ${escapeHTML(certainty)}</span>
      <span class="pill dim">${escapeHTML(rawConfidence)}</span>
      <span class="pill dim">${escapeHTML(strength)}</span>
      <span class="pill dim">${escapeHTML(tsLabel)}</span>
    </div>
    <div class="decisionCardWhy">${why}</div>
  `;

  card.addEventListener("click", async () => {
    await _openDecisionLookup(lookup, "recent_decisions");
  });

  return card;
}

async function loadDecisions() {
  const grid = document.getElementById("decisionsGrid");
  const empty = document.getElementById("decisionsEmpty");
  if (!grid || !empty) return;

  const hasRenderedCards = grid.childElementCount > 0;
  const hasCachedRows = Array.isArray(_dashboardLiveState.decisions) && _dashboardLiveState.decisions.length > 0;
  if (!hasRenderedCards) {
    empty.textContent = hasCachedRows ? "" : "Refreshing decision support…";
    if (!hasCachedRows) {
      grid.innerHTML = "";
    }
  }

  try {
    const payload = await fetchJSON("/api/ui/decisions?limit=12");
    const rows = Array.isArray(payload.decisions) ? payload.decisions : [];
    const latestTs = extractCollectionRealtimeTs(rows) || Date.now();
    updateDashboardLiveState({ decisions: rows }, { sourceTs: latestTs });

    const renderRecentDecisions = () => {
      DASHBOARD_TABLE_RENDERERS.set("recentDecisions", renderRecentDecisions);
      const state = getDashboardTableState("recentDecisions");
      const defaults = DASHBOARD_TABLE_DEFAULTS.recentDecisions;
      const view = buildTableView(rows, RECENT_DECISION_TABLE_COLUMNS, {
        query: state.query,
        sortKey: state.sortKey || defaults.sortKey,
        sortDir: state.sortDir || defaults.sortDir,
        maxRows: defaults.maxRows,
      });
      grid.innerHTML = "";
      view.visibleRows.forEach((row) => grid.appendChild(renderDecisionCard(row)));
      empty.textContent = !view.totalRows
        ? "No recent decisions available."
        : (!view.visibleRows.length
          ? "No recent decisions match the current filter."
          : _symbolContextEmptyHint(view.filteredRows, "recent decisions"));
      syncDashboardTableControls("recentDecisions", view);
      return view;
    };

    const decisionView = renderRecentDecisions();
    if (!rows.length) {
      setPanelState("recentDecisionsCard", {
        state: "empty",
        reason: "The dashboard can reach the decision endpoint, but it returned no recent decision rows.",
      });
      return;
    }
    setPanelState("recentDecisionsCard", {
      state: ageMsFromTimestamp(latestTs) >= 300_000 ? "stale" : "fresh",
      reason: safeJoin([
        `Loaded ${decisionView.filteredRowsCount}/${rows.length} decision rows. Latest decision ${formatAgeMs(ageMsFromTimestamp(latestTs))} old.`,
        _symbolContextEmptyHint(decisionView.filteredRows, "recent decisions"),
      ], " "),
    });
  } catch (e) {
    empty.textContent = `Decisions unavailable: ${e.message}`;
    updateDashboardLiveState({}, { errorKey: "decisions", error: e });
    setPanelState("recentDecisionsCard", {
      state: "error",
      reason: `Decision feed unavailable: ${describeUiError(e)}`,
    });
  }
}

function _humanAlignmentPillClass(action) {
  const value = String(action || "").toLowerCase();
  if (value.includes("threshold") || value.includes("severity")) return "warn";
  if (value.includes("runbook")) return "crit";
  return "dim";
}

async function loadHumanAlignmentSummary() {
  const metaEl = document.getElementById("humanAlignmentMeta");
  const bodyEl = document.getElementById("humanAlignmentBody");
  const recEl = document.getElementById("humanAlignmentRecommendations");
  if (!metaEl || !bodyEl || !recEl) return;

  metaEl.textContent = "loading";
  bodyEl.innerHTML = "";
  recEl.innerHTML = "<div class=\"small\">Loading recommendations…</div>";

  try {
    const payload = await fetchJSON("/api/operator/human_alignment?limit=6&lookback_h=24");
    const summary = payload && payload.summary ? payload.summary : {};
    const rules = Array.isArray(payload && payload.top_rules) ? payload.top_rules : [];
    const recommendations = Array.isArray(payload && payload.recommendations) ? payload.recommendations : [];

    metaEl.textContent = `${summary.lookback_hours || 24}h • alerts ${summary.alerts || 0} • opens ${summary.opens || 0}`;

    if (!rules.length) {
      bodyEl.innerHTML = `
        <tr>
          <td colspan="6" class="small">No recent interaction data available.</td>
        </tr>
      `;
    } else {
      bodyEl.innerHTML = rules.map((rule) => `
        <tr>
          <td>${escapeHTML(String(rule.display_rule || rule.rule_id || "unknown"))}</td>
          <td><span class="pill dim">${escapeHTML(String(rule.severity || "—"))}</span></td>
          <td>${escapeHTML(String(rule.sample_count || 0))}</td>
          <td>${escapeHTML(String(rule.open_count || 0))}</td>
          <td>${escapeHTML(String(Math.round(Number(rule.ack_rate || 0) * 100)))}%</td>
          <td>${escapeHTML(String(Math.round(Number(rule.ignore_rate || 0) * 100)))}%</td>
        </tr>
      `).join("");
    }

    if (!recommendations.length) {
      recEl.innerHTML = "<div class=\"small\">No recommendations yet. More interaction history is needed.</div>";
      return;
    }

    recEl.innerHTML = recommendations.map((rec) => `
      <div class="decisionCard" style="cursor:default;">
        <div class="decisionCardTop">
          <span class="pill ${_humanAlignmentPillClass(rec.action)}">${escapeHTML(String(rec.action || "review"))}</span>
          <span class="pill dim mono">${escapeHTML(String(rec.display_rule || rec.rule_id || "unknown"))}</span>
        </div>
        <div class="decisionCardWhy">${escapeHTML(String(rec.reason || ""))}</div>
      </div>
    `).join("");
  } catch (e) {
    metaEl.textContent = "unavailable";
    bodyEl.innerHTML = `
      <tr>
        <td colspan="6" class="small">Human-alignment analytics unavailable: ${escapeHTML(String(e && e.message ? e.message : e))}</td>
      </tr>
    `;
    recEl.innerHTML = "";
  }
}

function _executionUrgencyPillClass(urgency) {
  const value = String(urgency || "").toLowerCase();
  if (value === "high") return "crit";
  if (value === "medium") return "warn";
  return "ok";
}

async function markExecutionAdvisory(advisoryId, action) {
  const act = String(action || "").toLowerCase();
  if (!advisoryId || !act) return;
  await postJSON("/api/execution/advisories/action", {
    advisory_id: Number(advisoryId),
    action: act,
    actor: "operator",
    detail: { source: "dashboard_execution_advisory" },
  });
}

function populateExecutionAdvisoryModal(item) {
  const titleEl = document.getElementById("executionAdvisoryModalTitle");
  const summaryEl = document.getElementById("executionAdvisoryModalSummary");
  const historyEl = document.getElementById("executionAdvisoryModalHistory");
  const featuresSummaryEl = document.getElementById("executionAdvisoryModalFeaturesSummary");
  const featuresEl = document.getElementById("executionAdvisoryModalFeatures");
  const rationaleSummaryEl = document.getElementById("executionAdvisoryModalRationaleSummary");
  const rationaleEl = document.getElementById("executionAdvisoryModalRationale");
  const features = (item && item.features && typeof item.features === "object") ? item.features : {};
  const advisory = (item && item.advisory && typeof item.advisory === "object") ? item.advisory : {};
  const history = (advisory && advisory.history && typeof advisory.history === "object") ? advisory.history : {};

  if (titleEl) {
    titleEl.textContent = `${String(item && item.symbol || "Execution")} advisory`;
  }
  _currentExecutionAdvisoryItem = item || null;

  _renderDecisionKvs(summaryEl, [
    ["Time", item && item.ts_ms ? fmtTime(item.ts_ms) : "—"],
    ["Recommendation", String(item && item.recommendation || "—")],
    ["Urgency", String(item && item.urgency || "—")],
    ["Route", `${String(item && item.order_type || "—")} / ${String(item && item.aggressiveness || "—")}`],
    ["Expected slippage", Number.isFinite(Number(item && item.expected_slippage_bps)) ? `${Number(item.expected_slippage_bps).toFixed(2)} bps` : "—"],
    ["Last action", String((item && item.last_action && item.last_action.action) || "pending")],
  ]);

  _renderDecisionKvs(historyEl, [
    ["History source", String(features.history_source || history.source || "—")],
    ["Sample count", String(features.history_sample_n ?? history.sample_n ?? 0)],
    ["Avg slip", Number.isFinite(Number(features.history_avg_slippage_bps)) ? `${Number(features.history_avg_slippage_bps).toFixed(2)} bps` : "—"],
    ["P95 slip", Number.isFinite(Number(features.history_p95_slippage_bps)) ? `${Number(features.history_p95_slippage_bps).toFixed(2)} bps` : "—"],
    ["Avg latency", Number.isFinite(Number(features.history_avg_latency_ms)) ? `${Math.round(Number(features.history_avg_latency_ms))} ms` : "—"],
    ["Broker", String(item && item.broker || "—")],
  ]);

  renderStructuredSummary(featuresSummaryEl, [
    {
      label: "Expected slippage",
      value: Number.isFinite(Number(item && item.expected_slippage_bps)) ? `${Number(item.expected_slippage_bps).toFixed(2)} bps` : "—",
      meta: "Projected execution cost for the recommended route.",
    },
    {
      label: "History sample",
      value: String(features.history_sample_n ?? history.sample_n ?? 0),
      meta: String(features.history_source || history.source || "history source unavailable"),
    },
    {
      label: "Latency",
      value: Number.isFinite(Number(features.history_avg_latency_ms)) ? `${Math.round(Number(features.history_avg_latency_ms))} ms` : "—",
      meta: "Average observed execution latency for similar flows.",
    },
    {
      label: "Broker",
      value: String(item && item.broker || "—"),
      meta: "Broker context captured with the advisory snapshot.",
    },
  ], {
    emptyText: "No execution-feature diagnostics were stored for this advisory.",
    rawTarget: featuresEl,
    rawPayload: features,
  });

  renderStructuredSummary(rationaleSummaryEl, [
    {
      label: "Recommendation",
      value: String(item && item.recommendation || "—"),
      meta: String(advisory.rationale || item && item.rationale || "No written rationale was stored."),
    },
    {
      label: "Last action",
      value: String((item && item.last_action && item.last_action.action) || "pending"),
      meta: String((item && item.last_action && item.last_action.actor) || "No operator action recorded."),
    },
    {
      label: "Urgency",
      value: String(item && item.urgency || "—"),
      meta: String(item && item.execution_mode || "Execution mode unavailable"),
    },
  ], {
    emptyText: "No rationale snapshot was stored for this advisory.",
    rawTarget: rationaleEl,
    rawPayload: {
      rationale: advisory.rationale || item && item.rationale || "",
      history: history,
      last_action: item && item.last_action || {},
    },
  });
}

function openExecutionAdvisoryModal(item) {
  const modal = document.getElementById("executionAdvisoryModal");
  if (!modal) return;
  populateExecutionAdvisoryModal(item || {});
  modal.style.display = "block";
}

function closeExecutionAdvisoryModal() {
  const modal = document.getElementById("executionAdvisoryModal");
  if (!modal) return;
  modal.style.display = "none";
  _currentExecutionAdvisoryItem = null;
}

async function loadExecutionAdvisories() {
  const metaEl = document.getElementById("executionAdvisoryMeta");
  const bodyEl = document.getElementById("executionAdvisoryBody");
  if (!metaEl || !bodyEl) return;

  metaEl.textContent = "loading";
  bodyEl.innerHTML = `
    <tr>
      <td colspan="6" class="small">(loading...)</td>
    </tr>
  `;

  try {
    const payload = await fetchJSON("/api/execution/advisories?limit=8");
    const items = Array.isArray(payload.items) ? payload.items : [];
    const summary = payload.summary || {};
    const latestTs = extractCollectionRealtimeTs(items) || Date.now();
    updateDashboardLiveState({ advisories: items }, { sourceTs: latestTs });

    metaEl.textContent = `${summary.count || 0} total • ${summary.high_urgency || 0} high`;

    if (!items.length) {
      bodyEl.innerHTML = `
        <tr class="table-row">
          <td colspan="6" class="metric-meta">No execution advisories available yet.</td>
        </tr>
      `;
      setPanelState("executionAdvisoryCard", {
        state: "empty",
        reason: "The advisory endpoint returned successfully, but there are no current execution advisories.",
      });
      return;
    }

    bodyEl.innerHTML = items.map((item) => {
      const actionText = item.rejected
        ? "rejected"
        : item.approved
          ? "approved"
          : "pending";
      const route = `${String(item.order_type || "—")} / ${String(item.aggressiveness || "—")}`;
      const slip = Number.isFinite(Number(item.expected_slippage_bps))
        ? `${Number(item.expected_slippage_bps).toFixed(2)} bps`
        : "—";
      const rationale = escapeHTML(String(item.rationale || ""));
      return `
        <tr class="${_tableRowClassForSymbol(item.symbol)}" data-advisory-id="${Number(item.advisory_id || 0)}">
          <td>
            <div>${escapeHTML(String(item.symbol || "UNKNOWN"))}</div>
            <div class="mono metric-meta">${escapeHTML(String(item.side || ""))} • ${escapeHTML(String(item.execution_mode || ""))}</div>
          </td>
          <td><span class="pill ${_executionUrgencyPillClass(item.urgency)}">${escapeHTML(String(item.urgency || "low"))}</span></td>
          <td class="mono metric-meta">${escapeHTML(route)}</td>
          <td class="mono table-cell-num">${escapeHTML(slip)}</td>
          <td>
            <div class="small">${escapeHTML(String(item.recommendation || ""))}</div>
            <div class="metric-meta">${rationale}</div>
          </td>
          <td>
            <div class="mono metric-meta">${escapeHTML(actionText)}</div>
            <div style="display:flex; gap:6px; flex-wrap:wrap;">
              <button class="btn btnSmall" data-exec-detail="${Number(item.advisory_id || 0)}">Details</button>
              <button class="btn btnSmall" data-exec-terminal="${Number(item.advisory_id || 0)}">Terminal</button>
              <button class="btn btnSmall" data-exec-approve="${Number(item.advisory_id || 0)}">Approve</button>
              <button class="btn btnSmall" data-exec-reject="${Number(item.advisory_id || 0)}">Reject</button>
            </div>
          </td>
        </tr>
      `;
    }).join("");

    bodyEl.querySelectorAll("button[data-exec-detail]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const id = Number(btn.getAttribute("data-exec-detail"));
        const item = items.find((row) => Number(row && row.advisory_id) === id);
        if (item) openExecutionAdvisoryModal(item);
      });
    });

    bodyEl.querySelectorAll("button[data-exec-terminal]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const id = Number(btn.getAttribute("data-exec-terminal"));
        const item = items.find((row) => Number(row && row.advisory_id) === id);
        if (!item) return;
        _openContextUrl(_buildTerminalUrl({
          symbol: item.symbol,
          source: "execution_advisory",
          screen: ACTIVE_DASHBOARD_SCREEN,
          advisoryId: item.advisory_id,
        }));
      });
    });

    bodyEl.querySelectorAll("button[data-exec-approve]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        try {
          await markExecutionAdvisory(btn.getAttribute("data-exec-approve"), "approve");
          toast("Execution advisory approved", "ok", 2200);
          await loadExecutionAdvisories();
        } catch (e) {
          toast(`Approval failed: ${e.message}`, "bad", 3200);
        }
      });
    });

    bodyEl.querySelectorAll("button[data-exec-reject]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        try {
          await markExecutionAdvisory(btn.getAttribute("data-exec-reject"), "reject");
          toast("Execution advisory rejected", "warn", 2200);
          await loadExecutionAdvisories();
        } catch (e) {
          toast(`Reject failed: ${e.message}`, "bad", 3200);
        }
      });
    });
    setPanelState("executionAdvisoryCard", {
      state: ageMsFromTimestamp(latestTs) >= 300_000 ? "stale" : "fresh",
      reason: `Loaded ${items.length} execution advisories. Latest advisory ${formatAgeMs(ageMsFromTimestamp(latestTs))} old.`,
    });
  } catch (e) {
    metaEl.textContent = "unavailable";
    bodyEl.innerHTML = `
      <tr>
        <td colspan="6" class="small">Execution advisories unavailable: ${escapeHTML(String(e && e.message ? e.message : e))}</td>
      </tr>
    `;
    updateDashboardLiveState({}, { errorKey: "advisories", error: e });
    setPanelState("executionAdvisoryCard", {
      state: "error",
      reason: `Execution advisories unavailable: ${describeUiError(e)}`,
    });
  }
}

async function loadGovernanceSummary() {
  const metaEl = document.getElementById("governanceMeta");
  const bodyEl = document.getElementById("governanceSummaryBody");
  const rawEl = document.getElementById("governanceSummaryRaw");
  if (!metaEl || !bodyEl) return;

  metaEl.textContent = "loading";
  bodyEl.innerHTML = "";

  try {
    const payload = await fetchJSON("/api/governance/summary");
    const champions = Array.isArray(payload.champions) ? payload.champions : [];
    const alerts = Array.isArray(payload.governance_alerts) ? payload.governance_alerts : [];
    const logs = Array.isArray(payload.logs) ? payload.logs : [];
    const shadow = Array.isArray(payload.shadow_scores) ? payload.shadow_scores : [];
    const topChampion = champions[0] || {};
    const replayStatus = payload.replay_status || {};
    const promotionStatus = payload.promotion_status || {};

    metaEl.textContent = `${champions.length} champions • replay ${replayStatus.status || "unknown"}`;
    const snapshot = {
      promotion_status: promotionStatus,
      replay_status: replayStatus,
      governance_alerts: alerts,
      top_champion: topChampion,
      top_shadow_score: shadow[0] || null,
      latest_log: logs[0] || null,
    };
    renderStructuredSummary(bodyEl, [
      {
        label: "Promotion status",
        value: promotionStatus.allowed === true ? "Allowed" : promotionStatus.allowed === false ? "Blocked" : "Unknown",
        meta: String(promotionStatus.reason || promotionStatus.mode || "No promotion reason provided."),
      },
      {
        label: "Replay status",
        value: String(replayStatus.status || "unknown"),
        meta: replayStatus.ts_ms ? `Updated ${fmtTime(replayStatus.ts_ms)}` : "Replay timestamp unavailable",
      },
      {
        label: "Governance alerts",
        value: String(alerts.length),
        meta: alerts[0] ? String(alerts[0].message || alerts[0].reason || "Latest governance alert available.") : "No governance alerts reported.",
      },
      {
        label: "Top champion",
        value: String(topChampion.model_name || topChampion.symbol || topChampion.strategy_name || "—"),
        meta: topChampion.ts_ms ? `Updated ${fmtTime(topChampion.ts_ms)}` : "Champion timestamp unavailable",
      },
      {
        label: "Top shadow score",
        value: shadow[0] && Number.isFinite(Number(shadow[0].score)) ? Number(shadow[0].score).toFixed(3) : "—",
        meta: shadow[0] ? String(shadow[0].model_name || shadow[0].symbol || "Shadow score available") : "No shadow score available.",
      },
      {
        label: "Latest log",
        value: String((logs[0] && (logs[0].event || logs[0].action || logs[0].status)) || "—"),
        meta: logs[0] && logs[0].ts_ms ? fmtTime(logs[0].ts_ms) : "No governance log timestamp available",
      },
    ], {
      emptyText: "Governance summary did not return any structured sections.",
      rawTarget: rawEl,
      rawPayload: snapshot,
    });
    updateDashboardLiveState({ governance: snapshot }, { sourceTs: extractCollectionRealtimeTs(snapshot) || Date.now() });
    setPanelState("governanceSummaryCard", {
      state: "fresh",
      reason: `Governance summary loaded with ${champions.length} champions and ${alerts.length} governance alerts.`,
    });
  } catch (e) {
    metaEl.textContent = "unavailable";
    renderStructuredSummary(bodyEl, [], {
      emptyText: `Governance summary unavailable: ${e.message}`,
      rawTarget: rawEl,
      rawPayload: { error: e && e.message ? e.message : String(e) },
    });
    updateDashboardLiveState({}, { errorKey: "governance", error: e });
    setPanelState("governanceSummaryCard", {
      state: "error",
      reason: `Governance summary unavailable: ${describeUiError(e)}`,
    });
  }
}

function _renderDecisionKvs(el, entries) {
  if (!el) return;
  el.innerHTML = "";
  entries.forEach(([label, value]) => {
    const dt = document.createElement("div");
    dt.className = "small";
    dt.textContent = `${label}`;
    const dd = document.createElement("div");
    dd.className = "mono small";
    dd.textContent = value;
    el.appendChild(dt);
    el.appendChild(dd);
  });
}

function populateDecisionModal(payload) {
  const decision = payload && payload.decision ? payload.decision : null;
  const titleEl = document.getElementById("decisionModalTitle");
  const summaryEl = document.getElementById("decisionModalSummary");
  const stagesSummaryEl = document.getElementById("decisionModalStagesSummary");
  const relatedEl = document.getElementById("decisionModalRelated");
  const allocEl = document.getElementById("decisionModalAllocation");
  const inputsSummaryEl = document.getElementById("decisionModalInputsSummary");
  const inputsEl = document.getElementById("decisionModalInputs");
  const riskSummaryEl = document.getElementById("decisionModalRiskSummary");
  const riskEl = document.getElementById("decisionModalRisk");

  if (!decision) {
    const loading = !!(payload && payload.loading);
    if (titleEl) titleEl.textContent = loading ? "Loading decision..." : "Decision unavailable";
    if (summaryEl) summaryEl.innerHTML = "";
    if (allocEl) allocEl.innerHTML = "";
    renderStructuredSummary(stagesSummaryEl, buildDecisionStageRows(payload || {}), {
      emptyText: loading ? "Loading decision details." : "No decision stages returned.",
      rawTarget: relatedEl,
      rawPayload: payload || {},
    });
    renderStructuredSummary(inputsSummaryEl, [], {
      emptyText: loading ? "Loading model input details." : String((payload && payload.error) || "No decision detail returned."),
      rawTarget: inputsEl,
      rawPayload: payload || { error: "no_decision_detail" },
    });
    renderStructuredSummary(riskSummaryEl, [], {
      emptyText: loading ? "Loading risk and policy detail." : "No risk-gate detail returned.",
      rawTarget: riskEl,
      rawPayload: {},
    });
    _currentDecisionModalPayload = null;
    return;
  }

  _currentDecisionModalPayload = payload || { decision };

  if (titleEl) {
    titleEl.textContent = `${decision.symbol || "Decision"} ${decision.action || "hold"}`;
  }

  _renderDecisionKvs(summaryEl, [
    ["Time", decision.ts_ms ? fmtTime(decision.ts_ms) : "—"],
    ["Model", String(decision.model_name || "—")],
    ["Model version", String(decision.model_version || decision.model_ts_ms || "—")],
    ["Certainty", Number.isFinite(Number(decision.certainty)) ? `${Math.round(Number(decision.certainty) * 100)}%` : "—"],
    ["Confidence", Number.isFinite(Number(decision.confidence)) ? `${Math.round(Number(decision.confidence) * 100)}%` : "—"],
    ["Raw confidence", Number.isFinite(Number(decision.confidence_raw)) ? `${Math.round(Number(decision.confidence_raw) * 100)}%` : "—"],
    ["Prediction strength", Number.isFinite(Number(decision.prediction_strength)) ? Number(decision.prediction_strength).toFixed(3) : "—"],
    ["Risk impact", String(decision.risk_impact || "low")],
    ["Why", String(decision.why || "—")],
  ]);

  renderStructuredSummary(stagesSummaryEl, buildDecisionStageRows(payload || {}), {
    emptyText: "No decision stages returned.",
    rawTarget: relatedEl,
    rawPayload: {
      meta: payload && payload.meta || {},
      related_summary: buildDecisionRelatedSummary(payload || {}),
      stages: payload && payload.stages || [],
      related: payload && payload.related || {},
    },
  });

  _renderDecisionKvs(allocEl, [
    ["From weight", fmtNum(Number(decision.from_weight || 0), 4)],
    ["To weight", fmtNum(Number(decision.to_weight || 0), 4)],
    ["Delta %", Number.isFinite(Number(decision.size_delta_pct)) ? `${Number(decision.size_delta_pct).toFixed(2)}%` : "0.00%"],
    ["Portfolio state", decision.portfolio_state ? fmtNum(Number(decision.portfolio_state.weight || 0), 4) : "—"],
    ["Views", String((decision.interaction_summary && decision.interaction_summary.views) || 0)],
  ]);

  renderStructuredSummary(inputsSummaryEl, [
    {
      label: "Predicted z",
      value: Number.isFinite(Number(decision.predicted_z)) ? Number(decision.predicted_z).toFixed(4) : "—",
      meta: Number.isFinite(Number(decision.prediction_strength)) ? `Strength ${Number(decision.prediction_strength).toFixed(3)}` : "Prediction strength unavailable",
    },
    {
      label: "Model confidence",
      value: Number.isFinite(Number(decision.confidence)) ? `${Math.round(Number(decision.confidence) * 100)}%` : "—",
      meta: Number.isFinite(Number(decision.confidence_raw)) ? `Raw ${Math.round(Number(decision.confidence_raw) * 100)}%` : "Raw confidence unavailable",
    },
    {
      label: "Horizon",
      value: Number.isFinite(Number(decision.horizon_s)) ? `${Math.round(Number(decision.horizon_s))}s` : "—",
      meta: String(decision.regime || "Regime unavailable"),
    },
    {
      label: "Feature hash",
      value: String(decision.features_hash || "—"),
      meta: "Feature snapshot identifier used for this decision.",
    },
  ], {
    emptyText: "No structured model-input summary is available for this decision.",
    rawTarget: inputsEl,
    rawPayload: {
      predicted_z: decision.predicted_z,
      confidence: decision.confidence,
      confidence_raw: decision.confidence_raw,
      prediction_strength: decision.prediction_strength,
      horizon_s: decision.horizon_s,
      regime: decision.regime,
      features_hash: decision.features_hash,
      feature_set_tag: decision.feature_set_tag,
      explain: decision.explain || {},
      explain_json: decision.explain_json || {},
      extra: decision.extra || {},
      extra_json: decision.extra_json || {},
      components: decision.components || decision.components_json || {},
    },
  });

  renderStructuredSummary(riskSummaryEl, [
    {
      label: "Risk impact",
      value: String(decision.risk_impact || "—"),
      meta: String(decision.severity || "No alert severity attached."),
    },
    {
      label: "Rule ID",
      value: String(decision.rule_id || "—"),
      meta: "Rule or gate identifier that shaped the decision.",
    },
    {
      label: "Interaction views",
      value: String((decision.interaction_summary && decision.interaction_summary.views) || 0),
      meta: String((decision.interaction_summary && decision.interaction_summary.acks) || 0) + " acknowledgements recorded",
    },
  ], {
    emptyText: "No structured risk-gate summary is available for this decision.",
    rawTarget: riskEl,
    rawPayload: {
      risk_impact: decision.risk_impact,
      alert_severity: decision.severity || null,
      rule_id: decision.rule_id || null,
      stages: payload && payload.stages || [],
      execution_policy_audit: payload && payload.related && payload.related.execution_policy_audit || [],
      trade_attribution_ledger: payload && payload.related && payload.related.trade_attribution_ledger || [],
      interaction_summary: decision.interaction_summary || {},
    },
  });
}

async function openDecisionModal(decisionId, alertId = null) {
  const modal = document.getElementById("decisionModal");
  if (!modal) return;
  const lookup = (decisionId && typeof decisionId === "object")
    ? normalizeDecisionLookup(decisionId)
    : normalizeDecisionLookup({ decisionId, sourceAlertId: alertId });
  _currentDecisionModalId = lookup.decisionId || null;
  modal.style.display = "block";
  populateDecisionModal({ loading: true, decision: null, stages: [{ label: "Decision path", status: "loading", summary: "Loading decision details." }] });

  try {
    const payload = await fetchJSON(buildDecisionDetailUrl(lookup), { allowBusinessFalse: true });
    populateDecisionModal(payload);
  } catch (e) {
    populateDecisionModal({
      error: e.message || String(e),
      decision: {
        decision_id: lookup.decisionId || null,
        alert_id: lookup.sourceAlertId || null,
        symbol: "Decision",
        action: "unavailable",
        why: e.message || String(e),
      },
      stages: [
        { label: "Decision path", status: "unavailable", summary: e.message || String(e) },
      ],
    });
  }
}

async function closeDecisionModal() {
  const modal = document.getElementById("decisionModal");
  if (!modal || modal.style.display === "none" || modal.style.display === "") {
    return;
  }
  const title = document.getElementById("decisionModalTitle");
  const match = title && typeof title.textContent === "string" ? title.textContent : "";
  modal.style.display = "none";
  await postUiInteraction({
    decision_id: _currentDecisionModalId,
    interaction_type: "decision_close",
    detail: { title: match || "" },
  });
  _currentDecisionModalId = null;
  _currentDecisionModalPayload = null;
}

async function postJSON(path, obj) {
  const res = await _fetchWithTimeout(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(obj || {}),
    cache: "no-store",
  });
  const txt = await res.text();
  let data = null;
  try {
    data = txt ? JSON.parse(txt) : null;
  } catch (e) {
    console.warn("JSON parse error", path);
    data = null;
  }
  if (!res.ok) {
    const msg = (data && data.error) ? data.error : txt;
    throw new Error(`${res.status} ${res.statusText}: ${msg}`);
  }
  if (!data || typeof data !== "object") {
    throw new Error(`invalid_json_response: ${path}`);
  }
  if (data.ok === false) {
    throw new Error(String(data.error || `api_error: ${path}`));
  }
  return data;
}

async function postJSONAllowBusinessFalse(path, obj) {
  const res = await _fetchWithTimeout(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(obj || {}),
    cache: "no-store",
  });
  const txt = await res.text();
  let data = null;
  try {
    data = txt ? JSON.parse(txt) : null;
  } catch (e) {
    console.warn("JSON parse error", path);
    data = null;
  }
  if (!res.ok) {
    const msg = (data && data.error) ? data.error : txt;
    throw new Error(`${res.status} ${res.statusText}: ${msg}`);
  }
  if (!data || typeof data !== "object") {
    throw new Error(`invalid_json_response: ${path}`);
  }
  return data;
}

// -----------------------------
// Mode + visibility helpers
// -----------------------------
function _setExpertUnlock(on) {
  EXPERT_UNLOCK = !!on;
  saveExpertUnlock(EXPERT_UNLOCK);
  applyPolicyToDOM({
    operatorMode: OPERATOR_MODE,
    expertUnlocked: EXPERT_UNLOCK
  });
}

function _parseRangeToMs(r) {
  const map = { "15m": 15*60e3, "1h": 60*60e3, "6h": 6*60*60e3, "24h": 24*60*60e3, "7d": 7*24*60*60e3 };
  return map[r] || map["6h"];
}

function _getGlobalFilters() {
  const range = (document.getElementById("globalRange")?.value || "6h").trim();
  const sev = (document.getElementById("globalSev")?.value || "WARN").trim();
  const symEl = document.getElementById("globalSymbol");
const sym = (symEl && typeof symEl.value === "string"
  ? symEl.value
  : ""
).trim().toUpperCase();
  const changedOnly = !!document.getElementById("globalChangedOnly")?.checked;
  return { range, sev, sym, changedOnly };
}

// local ack/snooze (UI-only) to reduce noise for operators
const _ACK_KEY = "ui_ack_map";
const _RESOLVED_KEY = "ui_resolved_map";
const _SNOOZE_KEY = "ui_snooze_map";
const _LOCAL_ALERT_ACK_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const _LOCAL_ALERT_RESOLVE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const _LOCAL_ALERT_STATE_MAX = 500;
function _normalizeAlertStorageKey(id) {
  const key = String(id == null ? "" : id).trim();
  return key && key !== "undefined" && key !== "null" ? key : "";
}
function _normalizeLocalAlertEntry(kind, value) {
  if (value == null) return null;
  if (kind === "ack") {
    const ackedAt = Number(
      (value && typeof value === "object")
        ? (value.acked_at ?? value.at ?? value.ts)
        : value
    );
    return Number.isFinite(ackedAt) && ackedAt > 0 ? { acked_at: ackedAt } : null;
  }
  if (kind === "resolved") {
    const resolvedAt = Number(
      (value && typeof value === "object")
        ? (value.resolved_at ?? value.at ?? value.ts)
        : value
    );
    return Number.isFinite(resolvedAt) && resolvedAt > 0 ? { resolved_at: resolvedAt } : null;
  }
  const until = Number(
    (value && typeof value === "object")
      ? (value.until ?? value.expires_at ?? value.ts)
      : value
  );
  return Number.isFinite(until) && until > 0 ? { until } : null;
}
function _loadMap(key, kind = "ack") {
  const now = Date.now();
  let parsed = {};
  try {
    parsed = JSON.parse(localStorage.getItem(key) || "{}") || {};
  } catch {
    parsed = {};
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};

  const entries = [];
  for (const [rawId, rawValue] of Object.entries(parsed)) {
    const id = _normalizeAlertStorageKey(rawId);
    const entry = _normalizeLocalAlertEntry(kind, rawValue);
    if (!id || !entry) continue;
    if (kind === "ack") {
      if ((now - Number(entry.acked_at || 0)) > _LOCAL_ALERT_ACK_TTL_MS) continue;
      entries.push([id, { acked_at: Number(entry.acked_at) }]);
    } else if (kind === "resolved") {
      if ((now - Number(entry.resolved_at || 0)) > _LOCAL_ALERT_RESOLVE_TTL_MS) continue;
      entries.push([id, { resolved_at: Number(entry.resolved_at) }]);
    } else if (Number(entry.until || 0) > now) {
      entries.push([id, { until: Number(entry.until) }]);
    }
  }

  entries.sort((a, b) => {
    const aTs = Number((a[1] || {}).acked_at ?? (a[1] || {}).resolved_at ?? (a[1] || {}).until ?? 0);
    const bTs = Number((b[1] || {}).acked_at ?? (b[1] || {}).resolved_at ?? (b[1] || {}).until ?? 0);
    return bTs - aTs;
  });

  const out = Object.fromEntries(entries.slice(0, _LOCAL_ALERT_STATE_MAX));
  try {
    localStorage.setItem(key, JSON.stringify(out));
  } catch {}
  return out;
}
function _saveMap(key, obj) {
  try { localStorage.setItem(key, JSON.stringify(obj || {})); } catch {}
}
function _clearLocalAlertState(key, kind, id) {
  const normalizedId = _normalizeAlertStorageKey(id);
  if (!normalizedId) return;
  const current = _loadMap(key, kind);
  if (!(normalizedId in current)) return;
  delete current[normalizedId];
  _saveMap(key, current);
}
function _hasServerAckState(row) {
  if (!row || typeof row !== "object") return false;
  if (!row.acked) return false;
  return String(row.acked_by || "").trim().toLowerCase() !== "local";
}
function _hasServerResolvedState(row) {
  if (!row || typeof row !== "object") return false;
  if (String(row.status || "").trim().toLowerCase() !== "resolved" && !row.resolved) return false;
  return String(row.resolved_reason || "").trim().toLowerCase() !== "local fallback";
}
function _pruneLocalAlertState(rows = []) {
  const alertsById = new Map();
  const activeIds = new Set(
    asArray(rows)
      .map((row) => {
        const key = _normalizeAlertStorageKey(row && row.id);
        if (key) alertsById.set(key, row);
        return key;
      })
      .filter(Boolean)
  );
  const ackMap = _loadMap(_ACK_KEY, "ack");
  const nextAck = {};
  for (const [id, entry] of Object.entries(ackMap)) {
    const row = alertsById.get(id);
    if (_hasServerAckState(row)) continue;
    const ackedAt = Number((entry || {}).acked_at || 0);
    if (!activeIds.has(id) && (Date.now() - ackedAt) > _LOCAL_ALERT_ACK_TTL_MS) continue;
    nextAck[id] = { acked_at: ackedAt };
  }
  const resolvedMap = _loadMap(_RESOLVED_KEY, "resolved");
  const nextResolved = {};
  for (const [id, entry] of Object.entries(resolvedMap)) {
    const row = alertsById.get(id);
    if (_hasServerResolvedState(row)) continue;
    const resolvedAt = Number((entry || {}).resolved_at || 0);
    if (!activeIds.has(id) && (Date.now() - resolvedAt) > _LOCAL_ALERT_RESOLVE_TTL_MS) continue;
    nextResolved[id] = { resolved_at: resolvedAt };
  }
  _saveMap(_ACK_KEY, nextAck);
  _saveMap(_RESOLVED_KEY, nextResolved);
  _saveMap(_SNOOZE_KEY, _loadMap(_SNOOZE_KEY, "snooze"));
}
function _ackAlertLocal(id) {
  const key = _normalizeAlertStorageKey(id);
  if (!key) return;
  const m = _loadMap(_ACK_KEY, "ack");
  m[key] = { acked_at: Date.now() };
  _saveMap(_ACK_KEY, m);
}
function _resolveAlertLocal(id) {
  const key = _normalizeAlertStorageKey(id);
  if (!key) return;
  const m = _loadMap(_RESOLVED_KEY, "resolved");
  m[key] = { resolved_at: Date.now() };
  _saveMap(_RESOLVED_KEY, m);
}
function _snoozeAlertLocal(id, minutes) {
  const key = _normalizeAlertStorageKey(id);
  if (!key) return;
  const m = _loadMap(_SNOOZE_KEY, "snooze");
  m[key] = { until: Date.now() + (minutes * 60 * 1000) };
  _saveMap(_SNOOZE_KEY, m);
}
function _isSnoozedLocal(id) {
  const key = _normalizeAlertStorageKey(id);
  if (!key) return false;
  const m = _loadMap(_SNOOZE_KEY, "snooze");
  return Number((m[key] || {}).until || 0) > Date.now();
}
function _isAckedLocal(id) {
  const key = _normalizeAlertStorageKey(id);
  if (!key) return false;
  const m = _loadMap(_ACK_KEY, "ack");
  return Number((m[key] || {}).acked_at || 0) > 0;
}
function _isResolvedLocal(id) {
  const key = _normalizeAlertStorageKey(id);
  if (!key) return false;
  const m = _loadMap(_RESOLVED_KEY, "resolved");
  return Number((m[key] || {}).resolved_at || 0) > 0;
}
function _applyAlertStatePatch(id, patch = {}) {
  const key = _normalizeAlertStorageKey(id);
  if (!key) return;
  _lastAlerts = asArray(_lastAlerts).map((row) => {
    if (_normalizeAlertStorageKey(row && row.id) !== key) return row;
    return { ...(row || {}), ...patch };
  });
  window._lastAlerts = _lastAlerts;
}
async function ackAlertPersisted(row) {
  const alertId = _normalizeAlertStorageKey(row && row.id);
  if (!alertId) {
    return { ok: false, persistence: "none", error: "missing alert id" };
  }
  if (hardBlockIfReadOnly({ actionName: "acknowledge alert", toastFn: toast })) {
    return { ok: false, persistence: "none", blocked: true };
  }

  try {
    await postJSON(`/api/alerts/${encodeURIComponent(alertId)}/ack`, {
      actor: "operator",
      source: "dashboard",
    });
    _clearLocalAlertState(_ACK_KEY, "ack", alertId);
    _applyAlertStatePatch(alertId, {
      acked: true,
      acked_by: "operator",
    });
    try {
      await postUiInteraction({
        alert_id: alertId,
        interaction_type: "alert_ack",
        detail: {
          persistence: "server",
          panel: "incident_drawer",
        }
      });
    } catch {}
    return { ok: true, persistence: "server" };
  } catch (e) {
    _ackAlertLocal(alertId);
    _applyAlertStatePatch(alertId, {
      acked: true,
      acked_by: "local",
    });
    try {
      await postUiInteraction({
        alert_id: alertId,
        interaction_type: "alert_ack",
        detail: {
          persistence: "local",
          panel: "incident_drawer",
          error: String(e && e.message ? e.message : e || ""),
        }
      });
    } catch {}
    return {
      ok: true,
      persistence: "local",
      error: String(e && e.message ? e.message : e || "ack failed"),
    };
  }
}
async function resolveAlertPersisted(row) {
  const alertId = _normalizeAlertStorageKey(row && row.id);
  if (!alertId) {
    return { ok: false, persistence: "none", error: "missing alert id" };
  }
  if (hardBlockIfReadOnly({ actionName: "resolve alert", toastFn: toast })) {
    return { ok: false, persistence: "none", blocked: true };
  }

  try {
    await postJSON(`/api/alerts/${encodeURIComponent(alertId)}/resolve`, {
      actor: "operator",
      reason: "resolved in dashboard",
      source: "dashboard",
    });
    _clearLocalAlertState(_ACK_KEY, "ack", alertId);
    _clearLocalAlertState(_RESOLVED_KEY, "resolved", alertId);
    _applyAlertStatePatch(alertId, {
      acked: true,
      acked_by: "operator",
      status: "resolved",
      resolved: true,
      resolved_reason: "resolved in dashboard",
    });
    try {
      await postUiInteraction({
        alert_id: alertId,
        interaction_type: "alert_resolve",
        detail: {
          persistence: "server",
          panel: "incident_drawer",
        }
      });
    } catch {}
    return { ok: true, persistence: "server" };
  } catch (e) {
    _resolveAlertLocal(alertId);
    _applyAlertStatePatch(alertId, {
      status: "resolved",
      resolved: true,
      resolved_reason: "local fallback",
    });
    try {
      await postUiInteraction({
        alert_id: alertId,
        interaction_type: "alert_resolve",
        detail: {
          persistence: "local",
          panel: "incident_drawer",
          error: String(e && e.message ? e.message : e || ""),
        }
      });
    } catch {}
    return {
      ok: true,
      persistence: "local",
      error: String(e && e.message ? e.message : e || "resolve failed"),
    };
  }
}

function _safeParseJSON(s) {
  try {
    return JSON.parse(String(s));
  } catch {
    return null;
  }
}

const _RELEVANCE_SNAPSHOT_KEY = "relevance_stats_snapshot";

function _parseRelevanceStats(stats) {
  const rows = [];

  if (!stats || typeof stats !== "object") return rows;

  for (const k of Object.keys(stats)) {
    const v = stats[k] || {};
    let symbol = k;
    let horizon = "";

    if (k.includes(":")) {
      const parts = k.split(":");
      symbol = parts[0];
      horizon = parts[1];
    }

    rows.push({
      key: k,
      symbol,
      horizon,
      relevance: Number(v.relevance ?? v.value ?? NaN),
      mean_abs_z: Number(v.mean_abs_z ?? v.abs_z ?? NaN),
      n: Number(v.n ?? NaN),
    });
  }

  return rows;
}

function _diffRelevance(prev, cur) {
  const out = [];
  const p = prev || {};
  const c = cur || {};

  const keys = new Set([...Object.keys(p), ...Object.keys(c)]);
  for (const k of keys) {
    if (!p[k]) {
      out.push(`+ ${k} added`);
      continue;
    }
    if (!c[k]) {
      out.push(`- ${k} removed`);
      continue;
    }

    const fields = ["relevance", "mean_abs_z", "n"];
    for (const f of fields) {
      const pv = p[k]?.[f];
      const cv = c[k]?.[f];
      if (Number.isFinite(pv) && Number.isFinite(cv) && Math.abs(pv - cv) > 1e-6) {
        out.push(`${k}.${f}: ${pv} → ${cv}`);
      }
    }
  }
  return out;
}

// -----------------------------
// Heatmap + incident queue rendering
// -----------------------------
function _scoreCell(rows) {
  // severity * confidence * |z|
  let best = null;
  for (const r of rows) {
    const sevR = severityRank(r.severity);
    const conf = Number(r.confidence);
    const z = Math.abs(Number(r.expected_z));
    if (!Number.isFinite(conf) || !Number.isFinite(z)) continue;
    const score = sevR * conf * (0.6 + Math.min(3.0, z));
    if (!best || score > best.score) best = { score, r };
  }
  return best ? best.r : null;
}


function _meaningForAlert(r) {
  const sym = (r.symbol === "EXECUTION") ? "Execution" : r.symbol;
  if (sym === "EXECUTION") return "Execution quality looks degraded. Treat signals cautiously; avoid amplifying with aggressive actions.";
  if (r.severity === "CRIT") return "This is high severity and likely needs attention now.";
  if (r.severity === "WARN") return "This is a warning. Check context and monitor for escalation.";
  return "Informational alert. Usually safe to monitor.";
}

function _recommendedPosture(r) {
  if (r.severity === "CRIT") return "Act now";
  if (r.severity === "WARN") return "Monitor closely";
  return "Observe only";
}

function _decisionConfidence(r) {
  const c = Number(r.confidence);
  const z = Math.abs(Number(r.expected_z));
  if (c >= 0.85 && z >= 1.5) return "High confidence decision";
  if (c >= 0.65) return "Moderate confidence decision";
  return "Low confidence decision";
}

function _safeToIgnore(r) {
  if (r.severity === "INFO") return "Yes — informational";
  if (r.severity === "WARN" && Number(r.confidence) < 0.6) return "Likely safe short-term";
  return "No";
}

function _ifNothingChanges(r) {
  if (r.severity === "CRIT")
    return "Likely escalation or downstream impact within this horizon.";
  if (r.severity === "WARN")
    return "May self-resolve, but repeated alerts increase risk.";
  return "No material impact expected.";
}

function _stepsForAlert(r) {
  const sym = (r.symbol === "EXECUTION") ? "Execution" : r.symbol;
  if (sym === "EXECUTION") {
    return [
      "Check broker connectivity/latency and fill slippage.",
      "Confirm data freshness (prices/labels).",
      "If degradation persists, avoid risky actions (pipeline/promotion) until stable."
    ];
  }
  return [
    "Open Why for context (signals + priors).",
    "Confirm data freshness and drift status.",
    "If repeated, investigate symbol-specific execution costs."
  ];
}

function _renderSteps(list) {
  return `<ol style="margin:0; padding-left: 18px;">${(list||[]).map(s=>`<li>${esc(s)}</li>`).join("")}</ol>`;
}

function _findSimilarAlerts(row, all) {
  return (all || [])
    .filter(a =>
      a.id !== row.id &&
      a.symbol === row.symbol &&
      a.severity === row.severity
    )
    .slice(0, 3);
}

// -----------------------------
// Existing code continues
// -----------------------------

function setPill(id, ok, text) {
  const el = document.getElementById(id);
  if (!el) return;
  const nextText = String(text || "—");
  el.className = buildPillClassName(el, ok ? "ok" : "crit");
  el.textContent = nextText;
  applyStalenessState(el);
  flashOnContentChange(el, nextText);
}

function normalizePillTone(tone) {
  const raw = String(tone || "dim").trim().toLowerCase();
  if (raw === "bad" || raw === "err") return "crit";
  if (raw === "dim" || raw === "muted") return "neutral";
  return raw || "neutral";
}

function pillClassNames(tone) {
  const raw = String(tone || "dim").trim().toLowerCase();
  const normalized = normalizePillTone(raw);
  const classes = ["pill", normalized];
  if (normalized === "neutral") classes.push("dim", "status-neutral");
  if (normalized === "warn") classes.push("status-warn");
  if (raw === "bad") classes.push("bad");
  if (raw === "err") classes.push("bad", "err");
  return Array.from(new Set(classes.filter(Boolean))).join(" ");
}

function structuralPillClasses(el) {
  if (!el) return [];
  return ["mono", "meta-pill-offset"].filter((cls) => el.classList.contains(cls));
}

function buildPillClassName(el, tone) {
  return Array.from(new Set([
    ...pillClassNames(tone).split(/\s+/).filter(Boolean),
    ...structuralPillClasses(el),
  ])).join(" ");
}

function flashOnContentChange(el, value, force = false) {
  if (!el) return;
  if (!force && el.dataset.flashOnChange !== "true") return;
  const nextValue = String(value ?? "");
  if (el.dataset.lastValue !== undefined && el.dataset.lastValue !== nextValue) {
    el.classList.remove("update-flash");
    void el.offsetWidth;
    el.classList.add("update-flash");
  }
  el.dataset.lastValue = nextValue;
}

function setTextContent(el, text, forceFlash = false) {
  if (!el) return;
  const nextText = String(text ?? "—");
  el.textContent = nextText;
  flashOnContentChange(el, nextText, forceFlash);
}

function setPillTone(id, tone, text, staleAgeMs = null, staleWarnMs = 60_000, staleCritMs = 300_000) {
  const el = document.getElementById(id);
  if (!el) return;
  const nextText = String(text || "—");
  el.className = buildPillClassName(el, tone);
  el.textContent = nextText;
  applyStalenessState(el, staleAgeMs, staleWarnMs, staleCritMs);
  flashOnContentChange(el, nextText);
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function renderNotes(containerId, notes, emptyText = "No active issues reported.") {
  const el = document.getElementById(containerId);
  if (!el) return;
  const rows = asArray(notes).filter(Boolean);
  if (!rows.length) {
    el.innerHTML = `<div class="opsNote">${escapeHTML(emptyText)}</div>`;
    return;
  }
  el.innerHTML = rows
    .map((note) => `<div class="opsNote">${escapeHTML(String(note))}</div>`)
    .join("");
}

function renderStructuredSummary(target, rows, { emptyText = "No structured details available.", rawTarget = null, rawPayload = null } = {}) {
  const el = typeof target === "string" ? document.getElementById(target) : target;
  const rawEl = typeof rawTarget === "string" ? document.getElementById(rawTarget) : rawTarget;
  if (!el) return;

  const safeRows = asArray(rows).filter((row) => row && (row.label || row.value !== undefined || row.meta));
  if (!safeRows.length) {
    el.innerHTML = `
      <div class="structuredSummaryRow">
        <div class="structuredSummaryLabel">Status</div>
        <div class="structuredSummaryValue">Unavailable</div>
        <div class="structuredSummaryMeta">${escapeHTML(String(emptyText || "No structured details available."))}</div>
      </div>
    `;
  } else {
    el.innerHTML = safeRows.map((row) => `
      <div class="structuredSummaryRow">
        <div class="structuredSummaryLabel">${escapeHTML(String(row.label || "—"))}</div>
        <div class="structuredSummaryValue">${escapeHTML(String(row.value ?? "—"))}</div>
        <div class="structuredSummaryMeta">${escapeHTML(String(row.meta || "—"))}</div>
      </div>
    `).join("");
  }

  if (rawEl) {
    rawEl.textContent = rawPayload == null
      ? "(no raw payload)"
      : JSON.stringify(rawPayload, null, 2);
  }
}

function updateDashboardLiveState(patch = {}, meta = {}) {
  const hasOwn = (key) => Object.prototype.hasOwnProperty.call(patch || {}, key);
  const sourceTs = coerceRealtimeTs(meta.sourceTs);

  const assign = (key, extractor) => {
    if (!hasOwn(key)) return;
    _dashboardLiveState[key] = patch[key];
    const extracted = typeof extractor === "function"
      ? extractor(patch[key], sourceTs)
      : sourceTs;
    if (Number.isFinite(extracted) && extracted > 0) {
      _dashboardLiveState.timestamps[key] = extracted;
    }
    if (Object.prototype.hasOwnProperty.call(_dashboardLiveState.errors, key)) {
      _dashboardLiveState.errors[key] = "";
    }
  };

  assign("health", (value, fallback) => extractRealtimeTs(value, fallback));
  assign("readiness", (value, fallback) => extractRealtimeTs(value, fallback));
  assign("systemState", (value, fallback) => extractRealtimeTs(value, fallback));
  assign("executionBarrier", (value, fallback) => extractRealtimeTs(value, fallback));
  assign("stressPayload", (value, fallback) => extractRealtimeTs(value, fallback));
  assign("pnl", (_, fallback) => fallback || Date.now());
  assign("decisions", (value, fallback) => extractCollectionRealtimeTs(value) || fallback || Date.now());
  assign("advisories", (value, fallback) => extractCollectionRealtimeTs(value) || fallback || Date.now());
  assign("alerts", (value, fallback) => extractCollectionRealtimeTs(value) || fallback || Date.now());
  assign("notificationStatus", (value, fallback) => extractRealtimeTs(value, fallback) || fallback || Date.now());
  assign("governance", (value, fallback) => extractCollectionRealtimeTs(value) || fallback || Date.now());
  assign("executionOverlays", (value, fallback) => extractCollectionRealtimeTs(value) || fallback || Date.now());

  if (hasOwn("failures")) {
    _dashboardLiveState.failures = normalizeFailureItems(patch.failures);
  }

  const errorKey = String(meta.errorKey || "").trim();
  if (errorKey && Object.prototype.hasOwnProperty.call(_dashboardLiveState.errors, errorKey)) {
    _dashboardLiveState.errors[errorKey] = describeUiError(meta.error || meta.message || "request_failed");
  }

  renderRecommendedActionCard();
}

function normalizeRecommendationAction(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) return "";
  if (text.includes("do nothing") || text.includes("blocked") || text.includes("halt")) return "DO NOTHING";
  if (text.includes("buy") || text.includes("long") || text.includes("add")) return "BUY";
  if (text.includes("sell") || text.includes("short") || text.includes("trim") || text.includes("reduce") || text.includes("exit")) return "SELL";
  if (text.includes("hold") || text.includes("wait") || text.includes("monitor") || text.includes("observe")) return "HOLD";
  return "";
}

function collectRecommendationNotes({ summary, decision, advisory, blockers, failures }) {
  const notes = [];
  const seen = new Set();
  const push = (value) => {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    if (!text || seen.has(text)) return;
    seen.add(text);
    notes.push(text);
  };

  asArray(failures).forEach((item) => push(`${item.label}: ${item.message}`));
  asArray(blockers).forEach((item) => push(item));
  if (decision) {
    push(decision.why);
    push(decision.explain && decision.explain.summary);
  }
  if (advisory) {
    push(advisory.recommendation);
    push(advisory.rationale);
  }
  push(summary && summary.headline);
  push(summary && summary.meaning);
  asArray(summary && summary.next).forEach((item) => push(item));

  return notes.slice(0, 4);
}

function computeRecommendedAction() {
  const health = _dashboardLiveState.health || _lastHealth || window.__LAST_HEALTH__ || null;
  const readiness = _dashboardLiveState.readiness || window.__LAST_READINESS__ || null;
  const systemState = _dashboardLiveState.systemState || window.__LAST_SYSTEM_STATE__ || null;
  const executionBarrier = _dashboardLiveState.executionBarrier || window.__LAST_EXECUTION_BARRIER__ || null;
  const stressPayload = _dashboardLiveState.stressPayload || window.__LAST_MARKET_STRESS__ || null;
  const failures = normalizeFailureItems(_dashboardLiveState.failures.length ? _dashboardLiveState.failures : (window.__LAST_REFRESH_FAILURES__ || []));
  const summary = summarizeRuntimeStatus({
    systemState,
    stressPayload,
    barrierPayload: executionBarrier,
    healthPayload: health,
    readinessPayload: readiness,
  });

  const decision = asArray(_dashboardLiveState.decisions)[0] || null;
  const advisoryItems = asArray(_dashboardLiveState.advisories);
  const advisory = advisoryItems.find((item) => item && item.rejected !== true) || advisoryItems[0] || null;
  const blockers = [
    ...failures.map((item) => `${item.label}: ${item.message}`),
    ...asArray(summary.blockers),
    ...collectStructuredIssues(readiness, 4).map((item) => `Readiness: ${item}`),
  ].filter(Boolean);

  const updatedTs = pickTimestamp(
    _dashboardLiveState.timestamps.decisions,
    _dashboardLiveState.timestamps.advisories,
    _dashboardLiveState.timestamps.readiness,
    _dashboardLiveState.timestamps.executionBarrier,
    _dashboardLiveState.timestamps.health
  );
  const ageMs = ageMsFromTimestamp(updatedTs);
  const blocked = failures.length > 0 || (executionBarrier && executionBarrier.allowed === false) || (readiness && readiness.ready === false);

  let action = "HOLD";
  let target = "Waiting for a clearer signal.";
  let reason = summary.meaning || "Decision support is still converging.";
  let confidence = "confidence waiting";
  let blocking = blockers.length ? "guarded" : "clear";

  if (blocked) {
    action = "DO NOTHING";
    target = executionBarrier && executionBarrier.allowed === false
      ? "Execution is blocked by runtime safety gates."
      : readiness && readiness.ready === false
        ? "Readiness gates are not satisfied."
        : "Backend state is partially unavailable.";
    reason = blockers[0] || summary.meaning || "Do not increase risk until the blocking condition clears.";
    confidence = "confidence blocked";
    blocking = failures.length ? "error" : "blocked";
  } else if (decision) {
    action = normalizeRecommendationAction(decision.action || decision.recommendation) || "HOLD";
    target = decision.symbol
      ? `${String(decision.symbol).toUpperCase()} • latest portfolio decision`
      : "Latest portfolio decision";
    const certainty = pickFiniteNumber(decision.certainty, decision.confidence_raw, decision.confidence);
    confidence = certainty == null ? "confidence n/a" : `confidence ${Math.round(certainty * 100)}%`;
    blocking = advisory && String(advisory.urgency || "").toLowerCase() === "high"
      ? "urgent"
      : (summary.blockers.length ? "guarded" : "clear");
    reason = String(decision.why || "").trim() || summary.meaning || "Latest portfolio decision is available without explanation text.";
  } else if (advisory) {
    action = normalizeRecommendationAction(advisory.side || advisory.recommendation) || "HOLD";
    target = advisory.symbol
      ? `${String(advisory.symbol).toUpperCase()} • execution advisory`
      : "Execution advisory is available.";
    confidence = advisory.urgency
      ? `confidence ${String(advisory.urgency).toLowerCase()}`
      : "confidence advisory";
    blocking = String(advisory.urgency || "").toLowerCase() === "high" ? "urgent" : "guarded";
    reason = String(advisory.recommendation || advisory.rationale || summary.meaning || "Execution advisory available.").trim();
  } else if (blockers.length) {
    action = "DO NOTHING";
    target = "Runtime blockers are active.";
    reason = blockers[0];
    confidence = "confidence blocked";
    blocking = "guarded";
  } else {
    action = "HOLD";
    target = "No recent decisions or execution advisories are visible yet.";
    reason = summary.meaning || "Stay flat until the next decision cycle publishes a clear action.";
    confidence = "confidence waiting";
    blocking = "awaiting signals";
  }

  let state = "fresh";
  if (failures.length) state = "error";
  else if (!decision && !advisory) state = "empty";
  else if (ageMs != null && ageMs >= 300_000) state = "stale";

  return {
    action,
    target,
    reason,
    confidence,
    blocking,
    notes: collectRecommendationNotes({ summary, decision, advisory, blockers, failures }),
    updatedTs,
    ageMs,
    state,
  };
}

function renderRecommendedActionCard() {
  const primaryEl = document.getElementById("recommendedActionPrimary");
  const targetEl = document.getElementById("recommendedActionTarget");
  const reasonEl = document.getElementById("recommendedActionReason");
  const confidenceEl = document.getElementById("recommendedActionConfidence");
  const blockingEl = document.getElementById("recommendedActionBlocking");
  const updatedEl = document.getElementById("recommendedActionUpdated");
  const notesEl = document.getElementById("recommendedActionNotes");
  const statePill = document.getElementById("recommendedActionStatePill");

  if (!primaryEl || !targetEl || !reasonEl || !confidenceEl || !blockingEl || !updatedEl || !notesEl || !statePill) {
    return;
  }

  const rec = computeRecommendedAction();
  primaryEl.textContent = rec.action;
  primaryEl.className = `recommendedActionTitle ${rec.action === "BUY" ? "pnl-positive" : rec.action === "SELL" ? "pnl-negative" : ""}`.trim();
  targetEl.textContent = rec.target;
  reasonEl.textContent = rec.reason;

  confidenceEl.className = buildPillClassName(confidenceEl, rec.confidence.includes("blocked") ? "crit" : rec.confidence.includes("waiting") ? "dim" : "ok");
  confidenceEl.textContent = rec.confidence;

  const blockingTone = rec.blocking === "clear"
    ? "ok"
    : (rec.blocking === "awaiting signals" ? "dim" : rec.blocking === "urgent" ? "warn" : "crit");
  blockingEl.className = buildPillClassName(blockingEl, blockingTone);
  blockingEl.textContent = `status ${rec.blocking}`;

  updatedEl.className = buildPillClassName(updatedEl, rec.ageMs != null && rec.ageMs >= 300_000 ? "warn" : "dim");
  updatedEl.textContent = rec.updatedTs
    ? `updated ${formatAgeMs(rec.ageMs)} ago`
    : "updated unavailable";

  renderNotes("recommendedActionNotes", rec.notes, "Decision support is waiting for a fresh decision, advisory, or runtime update.");
  setPanelState("recommendedActionCard", {
    state: rec.state,
    reason: `${rec.target} ${rec.updatedTs ? `• backend ${formatAgeMs(rec.ageMs)} old` : "• no backend timestamp"}`,
  });

  statePill.className = buildPillClassName(
    statePill,
    rec.state === "error" ? "crit" : rec.state === "stale" ? "warn" : rec.state === "empty" ? "dim" : "ok"
  );
  statePill.textContent = rec.state;
}

function updateProChartsPanelState() {
  const runtime = getDashboardChartRuntime();
  if (!runtime || !document.getElementById("proChartsCard")) return;

  const ageMs = ageMsFromTimestamp(runtime.lastUpdateMs);
  const state = runtime.error
    ? "error"
    : !runtime.hasHistory
      ? "empty"
      : (ageMs != null && ageMs >= 5_000 ? "stale" : "fresh");
  const reason = runtime.error
    ? runtime.error
    : !runtime.hasHistory
      ? "Live chart did not receive any candles yet."
      : `Chart feed ${ageMs == null ? "timestamp unavailable" : `${formatAgeMs(ageMs)} old`}.`;

  setPanelState("proChartsCard", { state, reason });
  setSurfaceState("proChartsSurfaceState", { state, reason });
}

function getRealtimeSchedulerState() {
  return {
    connected: !!(_operatorWs && _operatorWs.readyState === WebSocket.OPEN),
    lastMessageTs: _lastOperatorRealtimeMessageTs,
  };
}

if (typeof window !== "undefined") {
  window.__setDashboardPanelState__ = setPanelState;
  window.__updateDashboardProChartState__ = updateProChartsPanelState;
}

function renderEmptyTableBody(bodyId, colspan, message) {
  const body = document.getElementById(bodyId);
  if (!body) return;
  body.innerHTML = `<tr class="table-row"><td colspan="${intOr(colspan, 1)}" class="metric-meta">${escapeHTML(message)}</td></tr>`;
}

function renderAlertsUnavailable(message) {
  const detail = escapeHTML(String(message || "alerts unavailable"));
  const heatmap = document.getElementById("alertsHeatmap");
  if (heatmap) {
    heatmap.innerHTML = `<div class="hmCell hmHead" style="grid-column:1 / -1;">Alerts unavailable</div><div class="hmCell" style="grid-column:1 / -1;">${detail}</div>`;
  }

  const incidentList = document.getElementById("incidentList");
  if (incidentList) {
    incidentList.innerHTML = `<div class="opsNote">Alerts unavailable. ${detail}</div>`;
  }

  const tableBody = document.querySelector("#alerts tbody");
  if (tableBody) {
    tableBody.innerHTML = `<tr class="table-row"><td colspan="9" class="metric-meta">Alerts unavailable. ${detail}</td></tr>`;
  }
}

function intOr(value, fallback = 0) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

function extractIngestionStatus(payload) {
  const root = asObject(payload);
  const nested = asObject(root.ingestion);
  return Object.keys(nested).length ? nested : root;
}

function extractProviderTelemetry(payload) {
  const root = asObject(payload);
  const nested = asObject(root.provider_telemetry);
  return Object.keys(nested).length ? nested : root;
}

function buildStatGridMarkup(items) {
  return asArray(items)
    .map((item) => {
      const label = escapeHTML(String((item || {}).label || "—"));
      const value = escapeHTML(String((item || {}).value || "—"));
      const meta = escapeHTML(String((item || {}).meta || "—"));
      return `
        <div class="opsStat">
          <div class="opsStatLabel metric-label">${label}</div>
          <div class="opsStatValue metric-value">${value}</div>
          <div class="opsStatMeta metric-meta">${meta}</div>
        </div>
      `;
    })
    .join("");
}

async function openPromotionExplainModal() {
  try {
    const data = await fetchJSON("/api/promotion/explain");
    renderPromotionGate(data && data.gate ? data.gate : data);
    openPromoWhyModal(data && data.gate ? data.gate : data);
  } catch (e) {
    openPromoWhyModal({
      ok: false,
      status: "unavailable",
      reason: String(e && e.message ? e.message : e),
    });
  }
}

function wirePromotionExplainUI() {
  const btn = document.getElementById("btnWhyNotPromoted");
  const close = document.getElementById("btnClosePromoModal");
  const modal = document.getElementById("promoModal");

  if (btn && !btn._boundPromotionExplain) {
    btn._boundPromotionExplain = true;
    btn.addEventListener("click", () => openPromotionExplainModal());
  }

  if (close && !close._boundPromotionExplainClose) {
    close._boundPromotionExplainClose = true;
    close.addEventListener("click", () => { if (modal) modal.style.display = "none"; });
  }

  if (modal && !modal._boundPromotionExplainBackdrop) {
    modal._boundPromotionExplainBackdrop = true;
    modal.addEventListener("click", (e) => {
      if (e && e.target === modal) modal.style.display = "none";
    });
  }
}


let selectedJob = "poll_prices";


let _killSwitchSnapshot = null;

let { operatorMode: OPERATOR_MODE, expertUnlocked: EXPERT_UNLOCK } =
  loadPolicyState();

initPromotionSafetyEngine({
  isExecutionDegraded,
  hardBlockActionIfManipulated,
  toast,
  fetchJSON,
  loadPromotionStatus,
  loadSizePolicy: loadSizePolicyUI,

  refresh,
  getManipBlockedSyms: () => {
  if (window._manipBlockedSyms && window._manipBlockedSyms instanceof Set) {
    return window._manipBlockedSyms;
  }
  return new Set();
}
});

applyPolicyToDOM({
  operatorMode: OPERATOR_MODE,
  expertUnlocked: EXPERT_UNLOCK
});

function setSelectedJob(name) {
  selectedJob = name;
  const el = document.getElementById("selectedJob");
  if (el) el.textContent = name;
  syncLogViewerSourceLabel();
  if (getLogViewerSource() === "selected_job") {
    void loadStructuredLogViewer();
  }
}

function getLogViewerSource() {
  const el = document.getElementById("logViewerSource");
  return String(el && el.value ? el.value : "selected_job");
}

function normalizeLogViewerLimit(value, fallback = 200) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(25, Math.min(800, Math.round(n)));
}

function syncLogViewerSourceLabel() {
  const sourceEl = document.getElementById("logViewerSource");
  if (!sourceEl) return;
  const selectedOption = sourceEl.querySelector('option[value="selected_job"]');
  if (selectedOption) {
    const jobLabel = String(selectedJob || "none").trim() || "none";
    selectedOption.textContent = `Selected Job (${jobLabel})`;
  }
}

function parseLogViewerResponse(data) {
  const rawLines = Array.isArray(data && data.lines)
    ? data.lines.map((line) => String(line == null ? "" : line))
    : String((data && (data.log ?? data.text ?? data.data)) || "").split(/\r?\n/);
  const lines = rawLines.filter((line, idx) => !(idx === rawLines.length - 1 && line === ""));
  const text = String((data && (data.log ?? data.text)) || lines.join("\n"));
  return {
    ok: !(data && data.ok === false),
    source: String((data && data.source) || ""),
    text,
    lines,
    rawLineCount: Number((data && data.raw_line_count) || lines.length || 0),
    filteredLineCount: Number((data && data.filtered_line_count) || lines.length || 0),
    appliedFilters: asObject(data && data.applied_filters),
    error: String((data && data.error) || ""),
  };
}

function detectLogViewerLevel(line) {
  const text = String(line || "").toUpperCase();
  if (/\b(CRIT|CRITICAL|FATAL)\b/.test(text)) return "CRIT";
  if (/\b(ERROR|ERR)\b/.test(text)) return "ERROR";
  if (/\b(WARN|WARNING|HIGH)\b/.test(text)) return "WARN";
  if (/\bINFO\b/.test(text)) return "INFO";
  if (/\bDEBUG\b/.test(text)) return "DEBUG";
  if (/\bTRACE\b/.test(text)) return "TRACE";
  return "";
}

function filterLogViewerLinesClient(lines, state) {
  const level = String((state && state.level) || "").trim().toUpperCase();
  const needle = String((state && state.search) || "").trim().toLowerCase();
  const limit = normalizeLogViewerLimit(state && state.limit, 200);
  let out = asArray(lines).map((line) => String(line == null ? "" : line));

  if (level && level !== "ALL") {
    out = out.filter((line) => detectLogViewerLevel(line) === level);
  }
  if (needle) {
    out = out.filter((line) => line.toLowerCase().includes(needle));
  }
  if (limit > 0 && out.length > limit) {
    out = out.slice(-limit);
  }
  return out;
}

function getLogViewerState() {
  const source = getLogViewerSource();
  const levelEl = document.getElementById("logViewerLevel");
  const searchEl = document.getElementById("logViewerSearch");
  const limitEl = document.getElementById("logViewerLimit");
  const limit = normalizeLogViewerLimit(limitEl && limitEl.value ? limitEl.value : 200, 200);
  return {
    source,
    level: String(levelEl && levelEl.value ? levelEl.value : "ALL").trim().toUpperCase() || "ALL",
    search: String(searchEl && searchEl.value ? searchEl.value : "").trim(),
    limit,
  };
}

function buildLogViewerRequest(state) {
  const limit = normalizeLogViewerLimit(state && state.limit, 200);
  const params = new URLSearchParams();
  if (state && state.search) params.set("q", state.search);
  if (state && state.level && state.level !== "ALL") params.set("level", state.level);
  params.set("limit", String(limit));

  if ((state && state.source) === "operator_runtime") {
    return {
      sourceLabel: "Operator Runtime",
      path: `/api/operator/logs?${params.toString()}`,
      metaNote: "Python operator runtime log tail.",
    };
  }
  if ((state && state.source) === "operator_stderr") {
    return {
      sourceLabel: "Operator Stderr",
      path: `/api/operator/stderr_tail?${params.toString()}`,
      metaNote: "Python operator stderr tail.",
    };
  }

  const tail = Math.max(limit, Math.min(4000, Math.max(limit * 4, 400)));
  params.set("name", String(selectedJob || "poll_prices"));
  params.set("tail", String(tail));
  return {
    sourceLabel: `Job ${String(selectedJob || "poll_prices")}`,
    path: `/api/jobs/log?${params.toString()}`,
    metaNote: "Search applies inside the recent tail window fetched from the job manager.",
  };
}

function renderLogViewerState({
  lines = [],
  sourceLabel = "Logs",
  state = {},
  rawLineCount = 0,
  filteredLineCount = 0,
  note = "",
  error = "",
} = {}) {
  const outputEl = document.getElementById("logViewerOutput");
  const emptyEl = document.getElementById("logViewerEmpty");
  const metaEl = document.getElementById("logViewerMeta");
  const sourcePill = document.getElementById("logViewerSourcePill");
  const levelPill = document.getElementById("logViewerLevelPill");
  const matchPill = document.getElementById("logViewerMatchPill");
  const updatedPill = document.getElementById("logViewerUpdatedPill");
  if (!outputEl || !emptyEl || !metaEl || !sourcePill || !levelPill || !matchPill || !updatedPill) return;

  const lineList = asArray(lines).map((line) => String(line == null ? "" : line));
  const stickBottom =
    Math.abs((outputEl.scrollTop + outputEl.clientHeight) - outputEl.scrollHeight) < 24;

  sourcePill.className = buildPillClassName(sourcePill, "neutral");
  sourcePill.textContent = sourceLabel || "source —";
  levelPill.className = buildPillClassName(levelPill, state.level && state.level !== "ALL" ? "warn" : "neutral");
  levelPill.textContent = state.level && state.level !== "ALL"
    ? `level ${state.level}`
    : "level all";
  matchPill.className = buildPillClassName(matchPill, lineList.length > 0 ? "ok" : "neutral");
  matchPill.textContent = `${lineList.length} match${lineList.length === 1 ? "" : "es"}`;
  updatedPill.className = buildPillClassName(updatedPill, "neutral");
  updatedPill.textContent = `updated ${fmtTime(Date.now())}`;

  const metaParts = [];
  if (Number.isFinite(Number(rawLineCount)) && rawLineCount > 0) {
    metaParts.push(`scanned ${Number(rawLineCount)} lines`);
  }
  if (Number.isFinite(Number(filteredLineCount)) && filteredLineCount > 0 && filteredLineCount !== rawLineCount) {
    metaParts.push(`endpoint returned ${Number(filteredLineCount)} filtered lines`);
  }
  if (state.search) metaParts.push(`search "${state.search}"`);
  if (state.level && state.level !== "ALL") metaParts.push(`level ${state.level}`);
  if (state.limit) metaParts.push(`cap ${state.limit}`);
  if (note) metaParts.push(note);
  if (error) metaParts.push(`fallback: ${error}`);
  metaEl.textContent = metaParts.join(" • ") || "Recent log tail.";

  if (!lineList.length) {
    outputEl.textContent = "";
    emptyEl.textContent = error
      ? "Log source unavailable. The viewer stayed responsive."
      : "No log lines match the current filters.";
    emptyEl.style.display = "block";
    return;
  }

  emptyEl.style.display = "none";
  outputEl.textContent = lineList.join("\n");
  if (stickBottom) outputEl.scrollTop = outputEl.scrollHeight;
}

function setJobStatusPill(running, exitCode) {
  const el = document.getElementById("jobStatus");
  if (!el) return;
  if (running) {
    el.className = buildPillClassName(el, "ok");
    setTextContent(el, "running");
  } else {
    el.className = buildPillClassName(el, "neutral");
    setTextContent(el, (exitCode === null || exitCode === undefined) ? "idle" : `exited rc=${exitCode}`);
  }
}

function jobActionSafetyReason(name, action) {
  const normalizedName = String(name || "").trim();
  const normalizedAction = String(action || "").trim().toLowerCase();
  if (!normalizedName || normalizedAction !== "start") return "";
  if (isSafePaletteJobAction(normalizedName, normalizedAction)) return "";
  if (isReadOnlyMode()) {
    return "Execution barrier/read-only mode blocks this job.";
  }
  return "";
}

function applyJobActionSafety(btn) {
  if (!btn) return;
  const name = String(btn.getAttribute("data-job") || "").trim();
  const action = String(btn.getAttribute("data-action") || "").trim().toLowerCase();
  const unsafeStart = !!name && action === "start" && !isSafePaletteJobAction(name, action);
  const reason = jobActionSafetyReason(name, action);
  btn.classList.toggle("dangerAction", unsafeStart);
  if (reason) {
    btn.dataset.safetyBlocked = "1";
    btn.disabled = true;
    btn.setAttribute("aria-disabled", "true");
    btn.title = reason;
    return;
  }
  if (btn.dataset.safetyBlocked === "1") {
    delete btn.dataset.safetyBlocked;
    btn.removeAttribute("aria-disabled");
    btn.title = btn.dataset.previousTitle || "";
    delete btn.dataset.previousTitle;
    if (btn.dataset.jobState !== "running") {
      btn.disabled = false;
    }
  }
}

function syncJobActionSafetyState() {
  document.querySelectorAll("button[data-job][data-action]").forEach(applyJobActionSafety);
}

function setJobButtonState(name, state) {
  document.querySelectorAll(`button[data-job="${name}"]`).forEach(btn => {
    btn.dataset.jobState = state || "";
    btn.disabled = (state === "running");
    btn.classList.toggle("job-running", state === "running");
    btn.classList.toggle("job-error", state === "error");

    let label = btn.getAttribute("data-label") || btn.textContent.trim();
    if (!btn.getAttribute("data-label")) btn.setAttribute("data-label", label);

    if (state === "running") btn.textContent = `⏳ ${label}`;
    else if (state === "error") btn.textContent = `❌ ${label}`;
    else btn.textContent = label;
    applyJobActionSafety(btn);
  });
}

function flashCommandPaletteTarget(target) {
  if (!target) return;
  const el = typeof target === "string" ? document.getElementById(target) : target;
  if (!el) return;
  if (typeof el.scrollIntoView === "function") {
    el.scrollIntoView({ behavior: "smooth", block: "start" });
  }
  const hadTabIndex = el.hasAttribute("tabindex");
  const oldTabIndex = el.getAttribute("tabindex");
  if (!hadTabIndex && typeof el.focus === "function") {
    el.setAttribute("tabindex", "-1");
  }
  try {
    if (typeof el.focus === "function") el.focus({ preventScroll: true });
  } catch {}
  el.classList.add("commandPaletteTargetFlash");
  window.setTimeout(() => {
    el.classList.remove("commandPaletteTargetFlash");
    if (!hadTabIndex) {
      el.removeAttribute("tabindex");
    } else if (oldTabIndex !== null) {
      el.setAttribute("tabindex", oldTabIndex);
    }
  }, 1500);
}

function commandPaletteNavigateToScreen(screen) {
  applyDashboardScreen(screen, { syncHash: true, hashMode: "push" });
  try {
    window.scrollTo({ top: 0, behavior: "smooth" });
  } catch {}
}

function commandPaletteNavigateToPanel(screen, panelId) {
  const normalizedScreen = normalizeDashboardScreen(screen);
  applyDashboardScreen(normalizedScreen, { syncHash: true, hashMode: "push" });
  if (ACTIVE_DASHBOARD_SCREEN !== normalizedScreen) {
    toast("That panel is hidden by the current dashboard persona", "warn", 3200);
    return;
  }
  window.requestAnimationFrame(() => {
    window.requestAnimationFrame(() => {
      const el = document.getElementById(panelId);
      if (el) flashCommandPaletteTarget(el);
    });
  });
}

function commandPaletteFocusSymbol(symbol) {
  const normalized = String(symbol || "").trim().toUpperCase();
  if (!normalized) return;
  applyDashboardScreen("overview", { syncHash: true, hashMode: "push" });
  window.requestAnimationFrame(() => {
    const input = document.getElementById("globalSymbol");
    if (!input) return;
    input.value = normalized;
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
    flashCommandPaletteTarget(input);
    try {
      input.select();
    } catch {}
  });
}

async function commandPaletteFocusModel(model) {
  applyDashboardScreen("analyze", { syncHash: true, hashMode: "push" });
  if (ACTIVE_DASHBOARD_SCREEN !== "analyze") {
    toast("Model registry is hidden by the current dashboard persona", "warn", 3200);
    return;
  }
  try {
    await loadModelRegistry();
  } catch {}
  window.requestAnimationFrame(() => {
    const body = document.getElementById("modelRegistryBody");
    const card = document.getElementById("championChallengerCard");
    const tokens = [
      model && model.stage,
      model && model.modelName,
      model && model.kind,
      model && model.label,
    ]
      .map((part) => String(part || "").trim().toLowerCase())
      .filter(Boolean);
    const row = Array.from((body && body.querySelectorAll("tr")) || []).find((tr) => {
      const text = String(tr.textContent || "").toLowerCase();
      return tokens.some((token) => token && text.includes(token));
    });
    flashCommandPaletteTarget(row || card);
  });
}

async function commandPaletteSelectJob(name) {
  const normalized = String(name || "").trim();
  if (!normalized) return;
  applyDashboardScreen("operate", { syncHash: true, hashMode: "push" });
  setSelectedJob(normalized);
  await Promise.allSettled([
    loadJobs(),
    loadLog(),
    loadStructuredLogViewer(),
  ]);
  window.requestAnimationFrame(() => {
    flashCommandPaletteTarget(document.getElementById("selectedJob") || document.getElementById("jobConsoleCard"));
  });
}

function confirmJobAction(name, action) {
  const normalizedAction = String(action || "").trim().toLowerCase();
  const normalizedName = String(name || "").trim();
  const endpoint = normalizedAction === "stop" ? "/api/jobs/stop" : "/api/jobs/start";
  return window.confirm(
    `Confirm ${normalizedAction} job "${normalizedName}"?\n\n` +
    `This uses the existing ${endpoint} endpoint. Continue only if this is the intended dashboard job action.`
  );
}

function wireJobActionButtons() {
  document.querySelectorAll("button[data-job][data-action]").forEach((btn) => {
    if (!btn.dataset.previousTitle && btn.title) {
      btn.dataset.previousTitle = btn.title;
    }
    applyJobActionSafety(btn);
    if (btn._boundJobAction) return;
    btn._boundJobAction = true;
    btn.addEventListener("click", async () => {
      const name = String(btn.getAttribute("data-job") || "").trim();
      const action = String(btn.getAttribute("data-action") || "").trim().toLowerCase();
      if (!name || !action) return;
      try {
        await jobAction(name, action);
      } catch (e) {
        toast(`Job ${action} failed: ${e && e.message ? e.message : e}`, "bad", 4200);
      }
    });
  });
}

/* -----------------------------
   Loaders
----------------------------- */
// -----------------------------
// Portfolio backtest charts (equity + drawdown)
// Uses: GET /api/backtest/portfolio/latest
// -----------------------------

function _fmtMoney(x) {
  const v = Number(x || 0);
  const s = Math.abs(v) >= 1000 ? v.toFixed(0) : v.toFixed(2);
  return (v < 0 ? "-" : "") + "$" + String(s).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

async function loadSizePolicyUI() {

  const body = document.getElementById("sizePolicyBody");
  const pill = document.getElementById("sizePolicyPill");
  if (!body || !pill) return;

  function fmt(x, k=6) {
    if (x === null || x === undefined || !Number.isFinite(Number(x))) return "";
    return Number(x).toFixed(k);
  }

  try {
    const j = await fetchJSON("/api/strategy/size_policy");
    if (!j || !j.ok) return;

    const p = j.policy;
    if (!p) {
      pill.textContent = "policy: none";
      pill.className = buildPillClassName(pill, "crit");
      body.innerHTML = `<tr class="table-row"><td colspan="6" class="metric-meta">No policy yet. Train to enable.</td></tr>`;
      return;
    }

    pill.textContent = `policy: ${p.method} buckets=${p.buckets} lookback=${p.lookback_days}d`;
    pill.className = buildPillClassName(pill, _isExecutionDegraded() ? "warn" : "ok");

    body.innerHTML = "";
    for (const r of (j.points || [])) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="mono">${r.bucket_idx}</td>
        <td class="mono">${fmt(r.conf_lo,2)}–${fmt(r.conf_hi,2)}</td>
        <td class="mono">${r.n}</td>
        <td class="mono">${fmt(r.mean_net_ret,6)}</td>
        <td class="mono">${fmt(r.std_net_ret,6)}</td>
        <td class="mono">${
  isExecutionDegraded()
    ? Math.min(Number(r.factor), 0.5).toFixed(3) + " (throttled)"
    : fmt(r.factor,3)
}</td>
      `;
      body.appendChild(tr);
    }
  } catch (e) {
    // ignore
  }
}

// ---------- Strategy Status ----------
async function loadStrategyStatus() {
  const panel = document.getElementById('strategyStatusPanel');
  const table = document.getElementById('strategyStatusTable');

  try {
    const r = await fetch('/api/strategy/status', { cache: 'no-store' });
    const raw = await r.text();
    let j = null;
    try {
      j = raw ? JSON.parse(raw) : null;
    } catch {}

    if (panel) panel.textContent = j && j.ok ? 'active' : 'idle';

    if (table && Array.isArray(j?.rows)) {
      table.innerHTML = '';
      j.rows.forEach(r => {
        table.insertAdjacentHTML('beforeend', `
          <tr>
            <td class="mono">${esc(r.key)}</td>
            <td class="mono">${esc(r.value)}</td>
          </tr>
        `);
      });
    }
  } catch {
    if (panel) panel.textContent = 'unavailable';
  }
}

// ---------- Portfolio ----------

let _lastAlerts = Array.isArray(window.__INIT_ALERTS__) ? window.__INIT_ALERTS__ : [];
window._lastAlerts = _lastAlerts;
let _pauseRefresh = false;

// last successful /api/health snapshot (for decision bar derivation)
let _lastHealth = null;

// Decision bar engine (Phase 7)
initDecisionBarRuntime({
  getLastAlerts: () => _lastAlerts,
  getLastHealth: () => _lastHealth,
  getLastSystemState: () => window.__LAST_SYSTEM_STATE__ || null,
  getLastExecutionBarrier: () => window.__LAST_EXECUTION_BARRIER__ || null,
  getLastPromotionStatus: () => window.__LAST_PROMOTION_STATUS__ || null,
  isExecutionDegraded: isExecutionDegraded,
  updateDecisionHeader
});


// explain_json is fetched on-demand via /api/alerts/by_id

async function loadAlerts() {
  try {
    await loadAlertsUI({
      fetchJSON,
      filterAlerts,
      renderHeatmap,
      renderIncidentQueue,
      postUiInteraction,
      updateManipulationStateFromAlerts,
      _getGlobalFilters,
      _isAckedLocal,
      _isResolvedLocal,
      _isSnoozedLocal,
      ackAlert: ackAlertPersisted,
      resolveAlert: resolveAlertPersisted,
      reloadAlerts: loadAlerts,
      OPERATOR_MODE,
      openWhyModal,
      setLastAlerts: (rows) => {
        _lastAlerts = rows;
        window._lastAlerts = _lastAlerts;
        _pruneLocalAlertState(rows);
      },
      getLastAlerts: () => _lastAlerts,
      updateDecisionHeader
    });
    window.__LAST_ALERTS_FAILED__ = false;
    updateDashboardLiveState({ alerts: _lastAlerts }, {
      sourceTs: extractCollectionRealtimeTs(_lastAlerts) || Date.now(),
    });
    setPanelState("alertsCard", {
      state: Array.isArray(_lastAlerts) && _lastAlerts.length
        ? (ageMsFromTimestamp(extractCollectionRealtimeTs(_lastAlerts)) >= 300_000 ? "stale" : "fresh")
        : "empty",
      reason: Array.isArray(_lastAlerts) && _lastAlerts.length
        ? `${_lastAlerts.filter((row) => row && !row.resolved).length} unresolved alerts currently visible.`
        : "No unresolved alerts are visible right now.",
    });
  } catch (e) {
    console.error("loadAlerts failed", e);
    window.__LAST_ALERTS_FAILED__ = true;
    setOpsError(`alerts load failed: ${e && e.message ? e.message : e}`);
    renderAlertsUnavailable(e && e.message ? e.message : e);
    updateDecisionHeader("alerts unavailable");
    updateDashboardLiveState({}, { errorKey: "alerts", error: e });
    setPanelState("alertsCard", {
      state: "error",
      reason: `Alerts unavailable: ${describeUiError(e)}`,
    });
  } finally {
    renderTopLevelHealthScore();
  }
}
async function loadValidation() {
  const payload = await fetchJSON("/api/validation");
  const rows = Array.isArray(payload)
    ? payload
    : Array.isArray(payload && payload.rows)
      ? payload.rows
      : [];
  const tbody = document.getElementById("validation"); // tbody id="validation"
  if (!tbody) return;

  tbody.innerHTML = (rows || []).map(r => `
    <tr>
      <td>${esc(r.symbol)}</td>
      <td>${esc(r.horizon_s)}</td>
      <td>${fmtNum(r.mae)}</td>
      <td>${fmtNum(r.rmse)}</td>
      <td>${esc(r.n)}</td>
      <td>${fmtTime(r.ts_ms)}</td>
    </tr>
  `).join("");
}

async function loadTemporalModels() {
  const res = await fetchJSON("/api/temporal/models?limit=20");
  const rows = (res && res.rows) ? res.rows : [];
  const tbody = document.getElementById("temporalModels");
  if (!tbody) return;

  tbody.innerHTML = (rows || []).map(r => {
    const m = r.metrics || {};
    const metricsShort = JSON.stringify({
      rmse: m.rmse,
      dir_acc: m.directional_acc,
      n_train: m.n_train,
      n_eval: m.n_eval
    });
    return `
      <tr>
        <td>${esc(r.model_name)}</td>
        <td>${esc(r.window)}</td>
        <td>${esc(r.input_dim)}</td>
        <td>${esc(r.weights_bytes)}</td>
        <td><code>${esc(metricsShort)}</code></td>
        <td>${fmtTime(r.ts_ms)}</td>
      </tr>
    `;
  }).join("");
}

async function loadJobs() {
  const data = await fetchJSON("/api/jobs");
  const jobs = (data && data.jobs) ? data.jobs : [];
  const cur = jobs.find(j => j.name === selectedJob);
  syncLogViewerSourceLabel();
  if (!cur) {
    setJobStatusPill(false, null);
    return;
  }
  setJobStatusPill(!!cur.running, cur.exit_code);
setJobButtonState(
  cur.name,
  cur.running ? "running" :
  (cur.exit_code && cur.exit_code !== 0 ? "error" : "idle")
);

}

async function loadLog() {
  if (!selectedJob) return;

  const el = document.getElementById("console");
  if (!el) return;

  try {
    const data = await fetchJSON(
      `/api/jobs/log?name=${encodeURIComponent(selectedJob)}&tail=800`
    );

    const stickBottom =
      Math.abs((el.scrollTop + el.clientHeight) - el.scrollHeight) < 20;

    const lines = Array.isArray(data && data.lines)
      ? data.lines.map((line) => String(line == null ? "" : line))
      : String((data && (data.log ?? data.text ?? "")) || "").split(/\r?\n/);
    const log = lines.slice(-800).join("\n");
    el.textContent = log;

    if (stickBottom) el.scrollTop = el.scrollHeight;
  } catch {
    // ignore transient errors
  }
}

async function loadStructuredLogViewer() {
  const outputEl = document.getElementById("logViewerOutput");
  if (!outputEl) return;

  syncLogViewerSourceLabel();
  const state = getLogViewerState();
  const request = buildLogViewerRequest(state);

  try {
    const payload = await fetchJSON(request.path);
    const parsed = parseLogViewerResponse(payload);
    const lines = filterLogViewerLinesClient(parsed.lines, state);
    renderLogViewerState({
      lines,
      sourceLabel: request.sourceLabel,
      state,
      rawLineCount: parsed.rawLineCount,
      filteredLineCount: parsed.filteredLineCount,
      note: request.metaNote,
      error: parsed.ok ? "" : parsed.error,
    });
  } catch (e) {
    renderLogViewerState({
      lines: [],
      sourceLabel: request.sourceLabel,
      state,
      rawLineCount: 0,
      filteredLineCount: 0,
      note: request.metaNote,
      error: String(e && e.message ? e.message : e || "log viewer unavailable"),
    });
  }
}

function wireLogViewerControls() {
  syncLogViewerSourceLabel();

  const sourceEl = document.getElementById("logViewerSource");
  const levelEl = document.getElementById("logViewerLevel");
  const searchEl = document.getElementById("logViewerSearch");
  const limitEl = document.getElementById("logViewerLimit");
  const refreshBtn = document.getElementById("btnRefreshLogViewer");

  if (sourceEl && !sourceEl._boundLogViewer) {
    sourceEl._boundLogViewer = true;
    sourceEl.addEventListener("change", () => {
      void loadStructuredLogViewer();
    });
  }
  if (levelEl && !levelEl._boundLogViewer) {
    levelEl._boundLogViewer = true;
    levelEl.addEventListener("change", () => {
      void loadStructuredLogViewer();
    });
  }
  if (limitEl && !limitEl._boundLogViewer) {
    limitEl._boundLogViewer = true;
    limitEl.addEventListener("change", () => {
      void loadStructuredLogViewer();
    });
  }
  if (refreshBtn && !refreshBtn._boundLogViewer) {
    refreshBtn._boundLogViewer = true;
    refreshBtn.addEventListener("click", () => {
      void loadStructuredLogViewer();
    });
  }
  if (searchEl && !searchEl._boundLogViewer) {
    searchEl._boundLogViewer = true;
    const debouncedRefresh = _debounce(() => {
      void loadStructuredLogViewer();
    }, 250);
    searchEl.addEventListener("input", debouncedRefresh);
    searchEl.addEventListener("change", () => {
      void loadStructuredLogViewer();
    });
  }
}

async function loadDataHealthScreen() {
  const summaryGrid = document.getElementById("dataHealthSummaryGrid");
  if (!summaryGrid) return;

  const [ingestionRes, telemetryRes, barrierRes, providerRes] = await Promise.allSettled([
    fetchJSON("/api/ingestion/status"),
    fetchJSON("/api/telemetry"),
    fetchJSON("/api/execution/barrier", { allowBusinessFalse: true }),
    fetchJSON("/api/operator/provider_telemetry"),
  ]);

  const ingestionPayload = ingestionRes.status === "fulfilled" ? ingestionRes.value : null;
  const telemetry = telemetryRes.status === "fulfilled" ? telemetryRes.value : null;
  const barrier = barrierRes.status === "fulfilled" ? barrierRes.value : null;
  const providerPayload = providerRes.status === "fulfilled" ? providerRes.value : null;

  const ingestion = extractIngestionStatus(ingestionPayload);
  const providerTelemetry = extractProviderTelemetry(providerPayload);
  const telemetryProviders = asObject(telemetry && telemetry.providers);
  const providerMap = asObject(
    Object.keys(asObject(providerTelemetry.providers)).length
      ? providerTelemetry.providers
      : ingestion.providers
  );
  const providerEntries = Object.entries(providerMap)
    .map(([name, raw]) => {
      const row = asObject(raw);
      const updatedTs = pickTimestamp(row.updated_ts_ms, row.last_price_ts_ms, row.ts_ms);
      const ageMs = numOrNull(row.price_age_ms) ?? ageMsFromTimestamp(updatedTs);
      let statusText = String(row.status || "").trim().toUpperCase();
      if (!statusText) {
        if (row.ok === false) statusText = "DEGRADED";
        else if (row.running === false) statusText = "STOPPED";
        else if (row.running === true || row.ok === true) statusText = "LIVE";
        else statusText = "UNKNOWN";
      }
      const tone = row.ok === false || statusText === "STOPPED"
        ? "crit"
        : freshnessTone(ageMs, 60_000, 300_000);
      const notes = safeJoin([
        row.error ? `error ${row.error}` : "",
        row.owner ? `owner ${row.owner}` : "",
        numOrNull(row.last_seq) != null && Number(row.last_seq) > 0 ? `seq ${intOr(row.last_seq, 0)}` : "",
      ]);
      return {
        name,
        statusText,
        tone,
        ageMs,
        updatedTs,
        notes: notes || "—",
      };
    })
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));

  const healthyProviders = numOrNull(providerTelemetry.healthy_providers)
    ?? numOrNull(ingestion.healthy_providers)
    ?? numOrNull(telemetryProviders.healthy);
  const totalProviders = Math.max(
    providerEntries.length,
    numOrNull(telemetryProviders.total) ?? 0,
    numOrNull(healthyProviders) ?? 0
  );
  const priceAgeMs = numOrNull(providerTelemetry.price_age_ms) ?? numOrNull(ingestion.price_age_ms);
  const latestPriceTs = pickTimestamp(providerTelemetry.last_price_ts_ms, ingestion.last_price_ts_ms);
  const updatedTs = pickTimestamp(
    providerTelemetry.updated_ts_ms,
    ingestion.updated_ts_ms,
    telemetry && telemetry.ts_ms,
    barrier && barrier.ts_ms,
    latestPriceTs
  );
  const updatedAgeMs = ageMsFromTimestamp(updatedTs);
  const pipelineStatus = ingestionRes.status === "fulfilled"
    ? String(ingestion.status || (ingestion.running ? "RUNNING" : "STOPPED") || "UNKNOWN")
    : "UNAVAILABLE";
  const pipelineTone = ingestionRes.status !== "fulfilled"
    ? "dim"
    : (ingestion.ok ? "ok" : ingestion.running ? "warn" : "bad");
  const providersTone = healthyProviders == null
    ? "dim"
    : (healthyProviders <= 0
      ? "bad"
      : (totalProviders > 0 && healthyProviders < totalProviders ? "warn" : "ok"));
  const barrierAllowed = barrier ? !!barrier.allowed : null;
  const barrierReason = String(
    (barrier && barrier.reason)
    || (asObject(barrier && barrier.execution_barrier).reason)
    || ""
  ).trim();
  const telemetryTone = telemetry
    ? ((telemetry.health && telemetry.health.ok) ? "ok" : "warn")
    : "dim";

  summaryGrid.innerHTML = buildStatGridMarkup([
    {
      label: "Pipeline",
      value: pipelineStatus,
      meta: ingestionRes.status === "fulfilled"
        ? (safeJoin([
            ingestion.running ? "running" : "not running",
            ingestion.active_child ? `child ${ingestion.active_child}` : "",
          ]) || "—")
        : "snapshot unavailable",
    },
    {
      label: "Visible Jobs",
      value: formatDecimal(ingestion.visible_jobs_running, 0),
      meta: ingestionRes.status === "fulfilled"
        ? (asArray(ingestion.stale_jobs).length
          ? `stale ${asArray(ingestion.stale_jobs).join(", ")}`
          : "no stale ingestion jobs")
        : "snapshot unavailable",
    },
    {
      label: "Fresh Rows",
      value: formatDecimal(ingestion.fresh_rows, 0),
      meta: ingestionRes.status === "fulfilled"
        ? `symbols ${formatDecimal(ingestion.fresh_symbols, 0)}`
        : "snapshot unavailable",
    },
    {
      label: "Price Age",
      value: formatAgeMs(priceAgeMs),
      meta: latestPriceTs ? `last price ${fmtTime(latestPriceTs)}` : "last price —",
    },
    {
      label: "Providers",
      value: totalProviders > 0
        ? `${intOr(healthyProviders, 0)}/${intOr(totalProviders, 0)}`
        : (healthyProviders == null ? "—" : `${intOr(healthyProviders, 0)}`),
      meta: telemetryProviders.total != null
        ? `runtime total ${intOr(telemetryProviders.total, 0)}`
        : "provider count from snapshot",
    },
    {
      label: "Execution Gate",
      value: barrierAllowed == null ? "—" : (barrierAllowed ? "ALLOWED" : "BLOCKED"),
      meta: barrierReason || "no active execution block",
    },
  ]);

  setPillTone("dataPipelinePill", pipelineTone, `pipeline ${pipelineStatus}`);
  setPillTone(
    "dataProvidersPill",
    providersTone,
    totalProviders > 0
      ? `providers ${intOr(healthyProviders, 0)}/${intOr(totalProviders, 0)}`
      : (healthyProviders == null ? "providers —" : `providers ${intOr(healthyProviders, 0)}`)
  );
  setPillTone(
    "dataFreshnessPill",
    freshnessTone(priceAgeMs, 30_000, 120_000),
    `price age ${formatAgeMs(priceAgeMs)}`,
    priceAgeMs,
    30_000,
    120_000
  );
  setPillTone(
    "dataBarrierPill",
    barrierAllowed == null ? "dim" : (barrierAllowed ? "ok" : "bad"),
    barrierAllowed == null ? "barrier —" : `barrier ${barrierAllowed ? "ALLOWED" : "BLOCKED"}`
  );
  setPillTone(
    "dataTelemetryPill",
    telemetryTone,
    telemetry ? `telemetry ${String(telemetry.system_state || "UNKNOWN")}` : "telemetry —"
  );
  setPillTone(
    "dataUpdatedPill",
    freshnessTone(updatedAgeMs, 60_000, 300_000),
    `updated ${formatAgeMs(updatedAgeMs)}`,
    updatedAgeMs,
    60_000,
    300_000
  );

  const healthNotes = [];
  if (barrierAllowed === false && barrierReason) {
    healthNotes.push(`execution blocked: ${barrierReason}`);
  }
  asArray(ingestion.reasons).slice(0, 5).forEach((reason) => {
    healthNotes.push(`ingestion: ${String(reason)}`);
  });
  if (!healthNotes.length && telemetry && telemetry.health && telemetry.health.ok === false) {
    asArray(telemetry.health.reasons).slice(0, 3).forEach((reason) => {
      healthNotes.push(`runtime: ${String(reason)}`);
    });
  }
  if (providerRes.status !== "fulfilled") {
    healthNotes.push("provider telemetry unavailable from the Python dashboard server snapshot");
  }
  renderNotes("dataHealthNotes", healthNotes, "No active ingestion blockers reported by the current snapshots.");

  const providersMetaText = totalProviders > 0
    ? `${intOr(healthyProviders, 0)}/${intOr(totalProviders, 0)} healthy`
    : (healthyProviders == null ? "provider telemetry unavailable" : "no providers reported");
  setPillTone("dataProvidersMeta", providersTone, providersMetaText);

  const providersBody = document.getElementById("dataProvidersBody");
  if (providersBody) {
    if (!providerEntries.length) {
      providersBody.innerHTML = `<tr class="table-row"><td colspan="5" class="metric-meta">(no provider rows reported)</td></tr>`;
    } else {
      providersBody.innerHTML = providerEntries.map((row) => `
        <tr class="table-row">
          <td class="mono">${escapeHTML(String(row.name || ""))}</td>
          <td><span class="${escapeHTML(pillClassNames(row.tone))}">${escapeHTML(row.statusText)}</span></td>
          <td><span class="${escapeHTML(`${pillClassNames(freshnessTone(row.ageMs, 30_000, 120_000))} ${stalenessClassNames(row.ageMs, 30_000, 120_000)}`.trim())}">${escapeHTML(formatAgeMs(row.ageMs))}</span></td>
          <td class="mono metric-meta">${row.updatedTs ? escapeHTML(fmtTime(row.updatedTs)) : "—"}</td>
          <td class="metric-meta">${escapeHTML(row.notes)}</td>
        </tr>
      `).join("");
    }
  }

  const providersFallback = document.getElementById("dataProvidersFallback");
  if (providersFallback) {
    const pipelineSummary = asObject(ingestion.summary);
    providersFallback.textContent = safeJoin([
      pipelineSummary.active_child ? `active child ${pipelineSummary.active_child}` : "",
      numOrNull(pipelineSummary.visible_jobs_running) != null ? `visible jobs ${formatDecimal(pipelineSummary.visible_jobs_running, 0)}` : "",
      numOrNull(providerTelemetry.child_pid) != null && Number(providerTelemetry.child_pid) > 0 ? `pid ${intOr(providerTelemetry.child_pid, 0)}` : "",
    ]) || "";
  }

  const runtimeGrid = document.getElementById("dataRuntimeGrid");
  if (runtimeGrid) {
    runtimeGrid.innerHTML = buildStatGridMarkup([
      {
        label: "CPU",
        value: telemetry ? `${formatDecimal(telemetry.cpu_percent, 1)}%` : "—",
        meta: telemetry ? `threads ${formatDecimal(telemetry.thread_count, 0)}` : "telemetry unavailable",
      },
      {
        label: "Memory",
        value: telemetry ? `${formatDecimal(telemetry.process_rss_mb, 0)}MB` : "—",
        meta: telemetry ? `${formatDecimal(telemetry.memory_percent, 1)}% of host` : "telemetry unavailable",
      },
      {
        label: "DB Size",
        value: telemetry ? `${formatDecimal(telemetry.db_size_mb, 1)}MB` : "—",
        meta: telemetry ? `state ${String(telemetry.system_state || "UNKNOWN")}` : "telemetry unavailable",
      },
      {
        label: "Alerts / 1h",
        value: telemetry ? formatDecimal(asObject(telemetry.alerts).last_hour, 0) : "—",
        meta: telemetry ? `crit open ${formatDecimal(asObject(telemetry.alerts).critical_open, 0)}` : "telemetry unavailable",
      },
      {
        label: "Fills",
        value: telemetry ? formatDecimal(asObject(telemetry.execution).n_fills, 0) : "—",
        meta: telemetry
          ? `last fill ${formatAgeMs(ageMsFromTimestamp(asObject(telemetry.execution).last_fill_ts_ms))}`
          : "telemetry unavailable",
      },
      {
        label: "Supervisor",
        value: telemetry ? formatDecimal(asObject(telemetry.supervisor).n_jobs, 0) : "—",
        meta: telemetry
          ? (asObject(telemetry.supervisor).delegated ? "delegated" : "local")
          : "telemetry unavailable",
      },
    ]);
  }

  const runtimeNotes = [];
  if (telemetry) {
    runtimeNotes.push(`system state: ${String(telemetry.system_state || "UNKNOWN")}`);
    if (telemetry.vol_target && telemetry.vol_target.enabled) {
      runtimeNotes.push(`vol target enabled: target ${formatDecimal(telemetry.vol_target.target_vol, 4)} scale ${formatDecimal(telemetry.vol_target.scale, 2)}x`);
    } else {
      runtimeNotes.push("vol target: off");
    }
  }
  if (barrierAllowed === false) {
    runtimeNotes.push(`execution barrier reason: ${barrierReason || "blocked"}`);
  } else if (barrierAllowed === true) {
    runtimeNotes.push("execution barrier clear");
  }
  if (telemetryRes.status !== "fulfilled") {
    runtimeNotes.push("runtime telemetry unavailable");
  }
  renderNotes("dataRuntimeNotes", runtimeNotes, "No runtime anomalies reported.");
}

async function loadPositionsExposureScreen() {
  const exposureGrid = document.getElementById("positionsExposureGrid");
  if (!exposureGrid) return;

  const [uiMetricsRes, portfolioRes, riskPortfolioRes, riskSummaryRes, brokerRes, terminalRes] = await Promise.allSettled([
    fetchJSON("/api/ui/metrics", { allowBusinessFalse: true }),
    fetchJSON("/api/portfolio"),
    fetchJSON("/api/risk/portfolio"),
    fetchJSON("/api/risk/summary"),
    fetchJSON("/api/broker"),
    fetchJSON("/api/terminal/positions"),
  ]);

  const uiMetrics = uiMetricsRes.status === "fulfilled" ? normalizeUiMetricsPayload(uiMetricsRes.value) : null;
  const canonicalExposure = uiMetrics ? canonicalExposureValues(uiMetrics) : null;
  const useCanonicalMetrics = !!(uiMetrics && uiMetrics.ok);
  const portfolio = portfolioRes.status === "fulfilled" ? portfolioRes.value : null;
  const riskPortfolio = riskPortfolioRes.status === "fulfilled" ? riskPortfolioRes.value : null;
  const riskSummary = riskSummaryRes.status === "fulfilled" ? riskSummaryRes.value : null;
  const broker = brokerRes.status === "fulfilled" ? brokerRes.value : null;
  const terminal = terminalRes.status === "fulfilled" ? terminalRes.value : null;

  const stateRows = asArray(portfolio && portfolio.state);
  const orderRows = asArray(portfolio && portfolio.orders);
  const diagnostics = asObject(portfolio && portfolio.diagnostics);
  const riskHistory = asArray(riskPortfolio && riskPortfolio.history);
  const riskLatest = asObject(riskHistory[0]);
  const brokerPositions = asArray(broker && broker.positions);
  const terminalRows = asArray(terminal && terminal.rows);
  const liveSymbolCount = new Set(
    [...brokerPositions, ...terminalRows]
      .map((row) => String((row || {}).symbol || "").trim().toUpperCase())
      .filter(Boolean)
  ).size;

  const legacyGrossExposure = numOrNull(riskSummary && riskSummary.gross_exposure)
    ?? numOrNull(riskLatest.gross)
    ?? numOrNull(asObject(riskPortfolio && riskPortfolio.summary).gross);
  const legacyNetExposure = numOrNull(riskSummary && riskSummary.net_exposure)
    ?? numOrNull(riskLatest.net)
    ?? numOrNull(asObject(riskPortfolio && riskPortfolio.summary).net);
  const legacyDrawdown = numOrNull(riskSummary && riskSummary.max_drawdown_pct)
    ?? numOrNull(riskLatest.drawdown);
  const grossExposure = useCanonicalMetrics ? canonicalExposure.gross : legacyGrossExposure;
  const netExposure = useCanonicalMetrics ? canonicalExposure.net : legacyNetExposure;
  const drawdown = useCanonicalMetrics ? canonicalExposure.drawdown : legacyDrawdown;
  const barrierPayload = useCanonicalMetrics
    ? asObject(canonicalExposure.barrier)
    : asObject(riskSummary && riskSummary.execution_barrier);
  const barrierAllowed = useCanonicalMetrics
    ? (
      barrierPayload.allowed != null
        ? !!barrierPayload.allowed
        : (canonicalExposure.riskBlocked == null ? null : !canonicalExposure.riskBlocked)
    )
    : (
      barrierPayload.allowed != null
        ? !!barrierPayload.allowed
        : (riskPortfolio ? !riskPortfolio.blocked : null)
    );
  const barrierReason = String(barrierPayload.reason || "").trim();
  const accountCash = useCanonicalMetrics ? canonicalExposure.cash : numOrNull(asObject(broker && broker.account).cash);
  const accountEquity = useCanonicalMetrics ? canonicalExposure.equity : numOrNull(asObject(broker && broker.account).equity);
  const riskState = useCanonicalMetrics
    ? String(canonicalExposure.riskStatus || "UNKNOWN").toUpperCase()
    : String((riskPortfolio && riskPortfolio.status) || "UNKNOWN").toUpperCase();

  const portfolioUpdatedTs = stateRows.reduce(
    (max, row) => Math.max(max, intOr(row && row.updated_ts_ms, 0)),
    intOr(diagnostics.ts_ms, 0)
  ) || null;
  const liveUpdatedTs = Math.max(
    brokerPositions.reduce((max, row) => Math.max(max, intOr(row && row.updated_ts_ms, 0)), 0),
    terminalRows.reduce((max, row) => Math.max(max, intOr(row && row.updated_ts_ms, 0)), 0)
  ) || null;
  const riskUpdatedTs = useCanonicalMetrics
    ? pickTimestamp(canonicalExposure.sourceTsMs)
    : pickTimestamp(riskSummary && riskSummary.ts_ms, riskPortfolio && riskPortfolio.ts_ms, riskLatest.ts_ms);
  const portfolioAgeMs = ageMsFromTimestamp(portfolioUpdatedTs);
  const liveAgeMs = ageMsFromTimestamp(liveUpdatedTs);
  const riskAgeMs = ageMsFromTimestamp(riskUpdatedTs);
  const freshnessAges = [portfolioAgeMs, liveAgeMs, riskAgeMs].filter((value) => value != null);
  const maxFreshnessAge = freshnessAges.length
    ? freshnessAges.reduce((max, value) => Math.max(max, Number(value)), 0)
    : null;

  setPillTone(
    "positionsGrossPill",
    grossExposure == null ? "dim" : (grossExposure > 1.0 ? "warn" : "ok"),
    `gross ${formatPercent(grossExposure)}`
  );
  setPillTone(
    "positionsNetPill",
    netExposure == null ? "dim" : (Math.abs(netExposure) > 0.5 ? "warn" : "ok"),
    `net ${netExposure == null ? "—" : formatSigned(netExposure * 100, 2, "%")}`
  );
  setPillTone(
    "positionsDrawdownPill",
    drawdown == null ? "dim" : (drawdown > 0.1 ? "bad" : drawdown > 0.05 ? "warn" : "ok"),
    `drawdown ${formatPercent(drawdown)}`
  );
  setPillTone(
    "positionsBarrierPill",
    barrierAllowed == null ? "dim" : (barrierAllowed ? "ok" : "bad"),
    barrierAllowed == null ? "barrier —" : `barrier ${barrierAllowed ? "ALLOWED" : "BLOCKED"}`
  );
  setPillTone(
    "positionsFreshnessPill",
    freshnessTone(maxFreshnessAge, 60_000, 300_000),
    useCanonicalMetrics
      ? `canonical ${formatAgeMs(riskAgeMs)} · target ${formatAgeMs(portfolioAgeMs)}`
      : `legacy fallback · target ${formatAgeMs(portfolioAgeMs)} · live ${formatAgeMs(liveAgeMs)}`,
    maxFreshnessAge,
    60_000,
    300_000
  );

  exposureGrid.innerHTML = buildStatGridMarkup([
    {
      label: "Target Rows",
      value: formatDecimal(stateRows.length, 0),
      meta: portfolioUpdatedTs ? `updated ${fmtTime(portfolioUpdatedTs)}` : "portfolio snapshot unavailable",
    },
    {
      label: "Live Rows",
      value: formatDecimal(liveSymbolCount, 0),
      meta: safeJoin([
        `broker ${formatDecimal(brokerPositions.length, 0)}`,
        `terminal ${formatDecimal(terminalRows.length, 0)}`,
      ]),
    },
    {
      label: "Gross Exposure",
      value: formatPercent(grossExposure),
      meta: useCanonicalMetrics
        ? `canonical risk ${formatAgeMs(riskAgeMs)}`
        : (riskUpdatedTs ? `risk ${formatAgeMs(riskAgeMs)}` : "risk snapshot unavailable"),
    },
    {
      label: "Net Exposure",
      value: netExposure == null ? "—" : formatSigned(netExposure * 100, 2, "%"),
      meta: barrierReason || "no active execution block",
    },
    {
      label: "Max Drawdown",
      value: formatPercent(drawdown),
      meta: riskLatest.ts_ms ? `snapshot ${fmtTime(riskLatest.ts_ms)}` : "latest Monte Carlo summary",
    },
    {
      label: "Risk State",
      value: riskState,
      meta: barrierAllowed == null
        ? "risk state unavailable"
        : (barrierAllowed ? "not blocked" : "blocked"),
    },
    {
      label: "Account Equity",
      value: formatCurrencyValue(accountEquity),
      meta: useCanonicalMetrics
        ? `canonical account ${formatAgeMs(ageMsFromTimestamp(canonicalExposure.accountSource && canonicalExposure.accountSource.ts_ms))}`
        : "legacy broker account",
    },
    {
      label: "Cash",
      value: formatCurrencyValue(accountCash),
      meta: useCanonicalMetrics
        ? String((canonicalExposure.accountSource && canonicalExposure.accountSource.endpoint) || "/api/broker")
        : "legacy broker account",
    },
  ]);

  const exposureNotes = [];
  if (useCanonicalMetrics) {
    exposureNotes.push("top-level PnL, exposure, account, and risk values are sourced from /api/ui/metrics");
    exposureNotes.push(...canonicalSourceNotes(uiMetrics, ["risk_summary", "broker", "portfolio"]));
  } else if (uiMetricsRes.status !== "fulfilled") {
    exposureNotes.push(`canonical UI metrics unavailable; legacy exposure fallback shown (${describeUiError(uiMetricsRes.reason)})`);
  }
  if (stateRows.length !== liveSymbolCount && (stateRows.length || brokerPositions.length || terminalRows.length)) {
    exposureNotes.push(`target/live symbol counts differ: targets ${stateRows.length}, live ${liveSymbolCount}, broker ${brokerPositions.length}, terminal ${terminalRows.length}`);
  }
  if (barrierAllowed === false) {
    exposureNotes.push(`execution blocked: ${barrierReason || "risk or execution gate is active"}`);
  }
  if (riskPortfolio && riskPortfolio.ready === false) {
    exposureNotes.push(`portfolio risk snapshot not ready (${String(riskPortfolio.status || "unknown")})`);
  }
  if (portfolioRes.status !== "fulfilled") {
    exposureNotes.push("portfolio snapshot unavailable");
  }
  if (brokerRes.status !== "fulfilled" || terminalRes.status !== "fulfilled") {
    exposureNotes.push("live broker inventory is partially unavailable");
  }
  const selectedPositionsHint = _symbolContextEmptyHint(
    [...stateRows, ...orderRows, ...brokerPositions, ...terminalRows],
    "position rows"
  );
  if (selectedPositionsHint) exposureNotes.push(selectedPositionsHint);
  renderNotes("positionsExposureNotes", exposureNotes, "Targets, broker inventory, and risk snapshots are aligned closely enough for review.");
  setPanelState("positionsExposureSummaryCard", {
    state: useCanonicalMetrics
      ? (canonicalExposure.degraded ? "stale" : "fresh")
      : "stale",
    reason: useCanonicalMetrics
      ? `Canonical exposure loaded from ${canonicalExposure.sourceLabel}.`
      : "Canonical UI metrics unavailable; legacy exposure calculations are marked as fallback.",
  });

  setPillTone(
    "positionsTargetsMeta",
    freshnessTone(portfolioAgeMs, 60_000, 300_000),
    stateRows.length ? `${stateRows.length} targets · ${formatAgeMs(portfolioAgeMs)}` : "no targets",
    portfolioAgeMs,
    60_000,
    300_000
  );
  const targetsBody = document.getElementById("positionsTargetsBody");
  if (targetsBody) {
    if (!stateRows.length) {
      targetsBody.innerHTML = `<tr class="table-row"><td colspan="8" class="metric-meta">(no portfolio targets reported)</td></tr>`;
    } else {
      targetsBody.innerHTML = stateRows.map((row) => {
        const size = numOrNull(row.size);
        const unrealizedPnl = numOrNull(row.unrealized_pnl);
        const realizedPnl = numOrNull(row.realized_pnl);
        const sizeText = size == null ? "—" : formatDecimal(size, Math.abs(size) >= 100 ? 2 : 6);
        const unrealizedTone = pnlToneClass(unrealizedPnl);
        const realizedTone = pnlToneClass(realizedPnl);
        return `
          <tr class="${_tableRowClassForSymbol(row.symbol)}">
            <td class="mono">${escapeHTML(String(row.symbol || ""))}</td>
            <td class="mono">${escapeHTML(String(row.side || ""))}</td>
            <td class="mono table-cell-num">${escapeHTML(sizeText)}</td>
            <td class="mono table-cell-num">${escapeHTML(formatPercent(row.weight))}</td>
            <td class="mono table-cell-num ${unrealizedTone}">${escapeHTML(formatSignedCurrencyValue(unrealizedPnl))}</td>
            <td class="mono table-cell-num ${realizedTone}">${escapeHTML(formatSignedCurrencyValue(realizedPnl))}</td>
            <td class="mono metric-meta">${row.opened_ts_ms ? escapeHTML(fmtTime(row.opened_ts_ms)) : "—"}</td>
            <td class="mono metric-meta">${row.updated_ts_ms ? escapeHTML(fmtTime(row.updated_ts_ms)) : "—"}</td>
          </tr>
        `;
      }).join("");
    }
  }

  const ordersBody = document.getElementById("positionsOrdersBody");
  if (ordersBody) {
    if (!orderRows.length) {
      ordersBody.innerHTML = `<tr class="table-row"><td colspan="6" class="metric-meta">(no recent portfolio order intents)</td></tr>`;
    } else {
      ordersBody.innerHTML = orderRows.slice(0, 20).map((row) => {
        const lookup = decisionLookupForOrderIntent(row, "positions_order_intents");
        const selectedClass = _symbolContextClassFor(row && row.symbol);
        const rowAttrs = hasDecisionLookup(lookup) ? _decisionLookupAttr(lookup, selectedClass) : _plainTableRowClass(selectedClass);
        return `
          <tr${rowAttrs}>
            <td class="mono metric-meta">${row.ts_ms ? escapeHTML(fmtTime(row.ts_ms)) : "—"}</td>
            <td class="mono">${escapeHTML(String(row.symbol || ""))}</td>
            <td class="mono">${escapeHTML(String(row.action || ""))}</td>
            <td class="mono">${escapeHTML(`${row.from_side || ""} ${formatPercent(row.from_weight)}`)}</td>
            <td class="mono">${escapeHTML(`${row.to_side || ""} ${formatPercent(row.to_weight)}`)}</td>
            <td class="mono table-cell-num">${escapeHTML(formatPercent(row.delta_weight))}</td>
          </tr>
        `;
      }).join("");
    }
  }

  const liveMap = new Map();
  brokerPositions.forEach((row) => {
    const symbol = String((row || {}).symbol || "").trim().toUpperCase();
    if (!symbol) return;
    liveMap.set(symbol, {
      symbol,
      brokerQty: numOrNull(row.qty),
      terminalQty: null,
      avgPx: numOrNull(row.avg_px),
      updatedTs: pickTimestamp(row.updated_ts_ms),
    });
  });
  terminalRows.forEach((row) => {
    const symbol = String((row || {}).symbol || "").trim().toUpperCase();
    if (!symbol) return;
    const current = liveMap.get(symbol) || {
      symbol,
      brokerQty: null,
      terminalQty: null,
      avgPx: null,
      updatedTs: null,
    };
    current.terminalQty = numOrNull(row.qty);
    if (current.avgPx == null) current.avgPx = numOrNull(row.avg_px);
    current.updatedTs = Math.max(intOr(current.updatedTs, 0), intOr(row.updated_ts_ms, 0)) || null;
    liveMap.set(symbol, current);
  });
  const liveRows = Array.from(liveMap.values()).sort((a, b) => {
    const aTs = intOr(a.updatedTs, 0);
    const bTs = intOr(b.updatedTs, 0);
    if (aTs !== bTs) return bTs - aTs;
    return String(a.symbol).localeCompare(String(b.symbol));
  });

  setPillTone(
    "positionsLiveMeta",
    freshnessTone(liveAgeMs, 60_000, 300_000),
    liveRows.length ? `${liveRows.length} live rows · ${formatAgeMs(liveAgeMs)}` : "no live rows",
    liveAgeMs,
    60_000,
    300_000
  );
  const liveBody = document.getElementById("positionsLiveBody");
  if (liveBody) {
    if (!liveRows.length) {
      liveBody.innerHTML = `<tr class="table-row"><td colspan="5" class="metric-meta">(no live broker positions reported)</td></tr>`;
    } else {
      liveBody.innerHTML = liveRows.map((row) => `
        <tr class="${_tableRowClassForSymbol(row.symbol)}">
          <td class="mono">${escapeHTML(String(row.symbol || ""))}</td>
          <td class="mono table-cell-num">${escapeHTML(row.brokerQty == null ? "—" : formatDecimal(row.brokerQty, 6))}</td>
          <td class="mono table-cell-num">${escapeHTML(row.terminalQty == null ? "—" : formatDecimal(row.terminalQty, 6))}</td>
          <td class="mono table-cell-num">${escapeHTML(row.avgPx == null ? "—" : formatDecimal(row.avgPx, 4))}</td>
          <td class="mono metric-meta">${row.updatedTs ? escapeHTML(fmtTime(row.updatedTs)) : "—"}</td>
        </tr>
      `).join("");
    }
  }

  const pairRows = asArray(diagnostics.position_pairs).length
    ? asArray(diagnostics.position_pairs).map((row) => ({
        pair: `${row.symbol_a || ""} ↔ ${row.symbol_b || ""}`,
        corr: numOrNull(row.corr),
        overlap: numOrNull(row.abs_weight_product),
        risk: numOrNull(row.same_direction_risk),
      }))
    : asArray(diagnostics.model_pairs).map((row) => ({
        pair: `${row.model_id_a || ""} ↔ ${row.model_id_b || ""}`,
        corr: numOrNull(row.corr),
        overlap: numOrNull(row.weight_product),
        risk: numOrNull(row.same_direction_overlap),
      }));
  const pairsTs = pickTimestamp(diagnostics.position_pairs_ts_ms, diagnostics.model_pairs_ts_ms, diagnostics.ts_ms);
  setPillTone(
    "positionsDiagnosticsMeta",
    freshnessTone(ageMsFromTimestamp(pairsTs), 300_000, 1_800_000),
    pairRows.length ? `${pairRows.length} pairs · ${formatAgeMs(ageMsFromTimestamp(pairsTs))}` : "no pairs",
    ageMsFromTimestamp(pairsTs),
    300_000,
    1_800_000
  );
  const pairsBody = document.getElementById("positionsPairsBody");
  if (pairsBody) {
    if (!pairRows.length) {
      pairsBody.innerHTML = `<tr class="table-row"><td colspan="4" class="metric-meta">(no correlation diagnostics persisted yet)</td></tr>`;
    } else {
      pairsBody.innerHTML = pairRows.slice(0, 10).map((row) => `
        <tr class="${["table-row", _symbolContextClassForText(row.pair)].filter(Boolean).join(" ")}">
          <td class="mono">${escapeHTML(String(row.pair || ""))}</td>
          <td class="mono table-cell-num">${escapeHTML(row.corr == null ? "—" : formatDecimal(row.corr, 3))}</td>
          <td class="mono table-cell-num">${escapeHTML(row.overlap == null ? "—" : formatPercent(row.overlap))}</td>
          <td class="mono table-cell-num">${escapeHTML(row.risk == null ? "—" : formatPercent(row.risk))}</td>
        </tr>
      `).join("");
    }
  }

  const riskNotes = [];
  if (useCanonicalMetrics) {
    riskNotes.push(`canonical risk summary: ${riskState} gross ${formatPercent(grossExposure)} net ${netExposure == null ? "—" : formatSigned(netExposure * 100, 2, "%")} drawdown ${formatPercent(drawdown)}`);
    riskNotes.push(...canonicalSourceNotes(uiMetrics, ["risk_summary", "portfolio_risk"]));
  } else if (uiMetricsRes.status !== "fulfilled") {
    riskNotes.push("canonical risk summary unavailable; diagnostics below use legacy risk endpoints");
  }
  if (riskPortfolio) {
    riskNotes.push(`risk status: ${String(riskPortfolio.status || "unknown").toUpperCase()} (${riskPortfolio.blocked ? "blocked" : "not blocked"})`);
  }
  if (riskLatest && Object.keys(riskLatest).length) {
    const latestNet = numOrNull(riskLatest.net);
    riskNotes.push(`latest snapshot gross ${formatPercent(riskLatest.gross)} net ${latestNet == null ? "—" : formatSigned(latestNet * 100, 2, "%")} drawdown ${formatPercent(riskLatest.drawdown)}`);
  }
  const totalRiskMeta = asObject(diagnostics.total_risk);
  if (Object.keys(totalRiskMeta).length) {
    riskNotes.push(`total risk pre ${formatPercent(totalRiskMeta.total_risk_pre)} post ${formatPercent(totalRiskMeta.total_risk_post)} limit ${formatPercent(totalRiskMeta.limit)}`);
  }
  if (pairsTs) {
    riskNotes.push(`correlation diagnostics updated ${formatAgeMs(ageMsFromTimestamp(pairsTs))} ago`);
  }
  renderNotes("positionsDiagnosticsNotes", riskNotes, "No persisted exposure diagnostics are available yet.");
}

function parseJsonObject(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value !== "string" || !value.trim()) return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function formatCurrencyValue(value, digits = 2) {
  const n = numOrNull(value);
  return n == null ? "—" : `$${fmtNum(n, digits)}`;
}

function formatSignedCurrencyValue(value, digits = 2) {
  const n = numOrNull(value);
  if (n == null) return "—";
  if (n > 0) return `+$${fmtNum(n, digits)}`;
  if (n < 0) return `-$${fmtNum(Math.abs(n), digits)}`;
  return `$${fmtNum(0, digits)}`;
}

function pnlToneClass(value) {
  const n = numOrNull(value);
  if (n == null || n === 0) return "";
  return n > 0 ? "pnl-positive" : "pnl-negative";
}

function setPnlPillValue(el, label, value) {
  if (!el) return;
  const n = numOrNull(value);
  const nextText = `${label} ${n == null ? "--" : formatSignedCurrencyValue(n)}`;
  el.classList.remove("dim", "neutral", "pnl-positive", "pnl-negative", "status-neutral");
  if (n == null || n === 0) {
    el.classList.add("neutral");
    el.classList.add("dim");
    el.classList.add("status-neutral");
  } else {
    el.classList.add(n > 0 ? "pnl-positive" : "pnl-negative");
  }
  el.innerText = nextText;
  flashOnContentChange(el, nextText, true);
}

function normalizeExecutionSide(value) {
  const token = String(value || "").trim().toUpperCase();
  if (!token) return "";
  if (token === "LONG" || token.includes("BUY")) return "BUY";
  if (token === "SHORT" || token.includes("SELL")) return "SELL";
  return token;
}

function pickFromObjects(objects, keys) {
  for (const raw of asArray(objects)) {
    const obj = asObject(raw);
    for (const key of asArray(keys)) {
      if (obj[key] != null && obj[key] !== "") return obj[key];
    }
  }
  return null;
}

function buildExecutionOrderRows(payload) {
  const data = asObject(payload && payload.data);
  const finalTokens = ["FILLED", "CANCELLED", "CANCELED", "REJECTED", "EXPIRED", "DONE", "CLOSED", "COMPLETE", "COMPLETED"];

  const brokerRows = asArray(data.broker).map((row) => {
    const meta = parseJsonObject(row && row.meta_json);
    const layers = [
      row,
      meta,
      asObject(meta.order),
      asObject(meta.payload),
      asObject(meta.request),
      asObject(meta.args),
    ];
    const state = String(row && row.state || "UNKNOWN").trim().toUpperCase() || "UNKNOWN";
    const side = normalizeExecutionSide(pickFromObjects(layers, ["side", "action", "to_side", "from_side", "direction"]));
    const qty = numOrNull(pickFromObjects(layers, ["qty", "quantity", "shares", "size"]));
    const notional = numOrNull(pickFromObjects(layers, ["notional", "order_notional", "cash_value"]));
    const sourceAlertId = pickFromObjects(layers, ["source_alert_id", "alert_id"]);
    const portfolioOrderId = pickFromObjects(layers, ["portfolio_orders_id", "portfolio_order_id", "source_order_id"]);
    const decisionId = pickFromObjects(layers, ["decision_id"]);
    const clientOrderId = pickFromObjects(layers, ["client_order_id", "order_uid", "source_order_id"]);
    return {
      kind: "broker",
      symbol: String((row && row.symbol) || "").trim().toUpperCase(),
      side: side || "—",
      size: qty != null
        ? formatDecimal(Math.abs(qty), Math.abs(qty) >= 100 ? 0 : 4)
        : (notional != null ? formatCurrencyValue(Math.abs(notional), 0) : "—"),
      status: state,
      updatedTs: pickTimestamp(row && row.updated_ts_ms, row && row.created_ts_ms, row && row.ts_ms),
      active: !finalTokens.some((token) => state === token || state.includes(token)),
      sizeSource: qty != null ? "qty" : (notional != null ? "notional" : ""),
      decision_id: decisionId,
      source_alert_id: sourceAlertId,
      portfolio_orders_id: portfolioOrderId,
      client_order_id: clientOrderId,
    };
  });

  const portfolioRows = asArray(data.portfolio).map((row) => {
    const deltaWeight = numOrNull(row && row.delta_weight);
    const toWeight = numOrNull(row && row.to_weight);
    const action = String((row && row.action) || "").trim().toUpperCase() || "PENDING";
    const side = normalizeExecutionSide(row && row.to_side) || normalizeExecutionSide(action) || normalizeExecutionSide(row && row.from_side);
    return {
      kind: "portfolio",
      symbol: String((row && row.symbol) || "").trim().toUpperCase(),
      side: side || "—",
      size: deltaWeight != null
        ? formatPercent(Math.abs(deltaWeight))
        : (toWeight != null ? formatPercent(Math.abs(toWeight)) : "—"),
      status: `INTENT ${action}`,
      updatedTs: pickTimestamp(row && row.ts_ms, row && row.updated_ts_ms),
      active: true,
      sizeSource: (deltaWeight != null || toWeight != null) ? "weight" : "",
      decision_id: row && row.decision_id,
      source_alert_id: row && row.source_alert_id,
      portfolio_orders_id: row && (row.id || row.source_order_id || row.portfolio_orders_id),
      client_order_id: row && row.client_order_id,
    };
  });

  const rows = [...brokerRows.filter((row) => row.active), ...portfolioRows]
    .sort((a, b) => intOr(b.updatedTs, 0) - intOr(a.updatedTs, 0))
    .slice(0, 40);

  return {
    rows,
    brokerActive: brokerRows.filter((row) => row.active).length,
    portfolioActive: portfolioRows.length,
    usedWeightFallback: portfolioRows.some((row) => row.sizeSource === "weight"),
    usedNotionalFallback: brokerRows.some((row) => row.active && row.sizeSource === "notional"),
  };
}

function renderSuppressedTrades(payload, requestStatus = "fulfilled") {
  const metaEl = document.getElementById("suppressedTradesMeta");
  const bodyEl = document.getElementById("suppressedTradesBody");
  if (!metaEl || !bodyEl) return;

  if (requestStatus !== "fulfilled") {
    setPillTone("suppressedTradesMeta", "dim", "unavailable");
    bodyEl.innerHTML = `<tr class="table-row"><td colspan="4" class="metric-meta">Suppression ledger unavailable.</td></tr>`;
    syncDashboardTableControls("suppressedTrades", buildTableView([], SUPPRESSED_TRADE_TABLE_COLUMNS));
    setPanelState("suppressedTradesCard", {
      state: "error",
      reason: "Suppressed trade rows could not be loaded from the attribution ledger.",
    });
    return;
  }

  const records = asArray(payload && payload.records);
  const suppressed = records
    .filter((row) => {
      const decisionJson = asObject(row && row.decision_json);
      return !!(
        String((row && row.suppression_reason) || "").trim() ||
        String(decisionJson.blocked_by || decisionJson.reason || "").trim()
      );
    })
    .map((row) => {
      const decisionJson = asObject(row && row.decision_json);
      const reason = String((row && row.suppression_reason) || decisionJson.blocked_by || decisionJson.reason || "suppressed");
      const lineage = safeJoin([
        row && row.source_alert_id ? `alert ${row.source_alert_id}` : "",
        row && row.id ? `ledger ${row.id}` : "",
        row && row.client_order_id ? `order ${row.client_order_id}` : "",
      ]) || "lineage unavailable";
      return {
        raw: row,
        symbol: String((row && row.symbol) || "UNKNOWN").trim().toUpperCase(),
        reason,
        lineage,
        ts_ms: row && row.ts_ms,
        lookup: decisionLookupForLedger(row, "suppressed_trades"),
      };
    });

  const latestTs = suppressed.reduce((max, row) => Math.max(max, intOr(row && row.ts_ms, 0)), 0) || null;
  setPillTone(
    "suppressedTradesMeta",
    suppressed.length ? "warn" : "dim",
    suppressed.length ? `${formatDecimal(suppressed.length, 0)} suppressed` : "none",
    latestTs ? ageMsFromTimestamp(latestTs) : null,
    300_000,
    1_800_000
  );

  const renderSuppressedRows = () => renderDashboardTableView({
    tableId: "suppressedTrades",
    bodyId: "suppressedTradesBody",
    columns: SUPPRESSED_TRADE_TABLE_COLUMNS,
    rows: suppressed,
    emptyText: "(no suppressed trades in recent attribution rows)",
    filteredEmptyText: "No suppressed trades match the current filter.",
    rowToHtml: (row) => {
      const lookup = row && row.lookup;
      const selectedClass = _symbolContextClassFor(row && row.symbol);
      const rowAttrs = hasDecisionLookup(lookup) ? _decisionLookupAttr(lookup, selectedClass) : _plainTableRowClass(selectedClass);
      return `
        <tr${rowAttrs}>
          <td class="mono metric-meta">${row && row.ts_ms ? escapeHTML(fmtTime(row.ts_ms)) : "—"}</td>
          <td class="mono">${escapeHTML(row && row.symbol ? row.symbol : "UNKNOWN")}</td>
          <td>${escapeHTML(row && row.reason ? row.reason : "suppressed")}</td>
          <td class="mono metric-meta">${escapeHTML(row && row.lineage ? row.lineage : "lineage unavailable")}</td>
        </tr>
      `;
    },
  });
  DASHBOARD_TABLE_RENDERERS.set("suppressedTrades", renderSuppressedRows);
  const view = renderSuppressedRows();

  if (!suppressed.length) {
    setPanelState("suppressedTradesCard", {
      state: "empty",
      reason: "The attribution ledger is reachable, but no recent suppression rows were returned.",
    });
    return;
  }

  setPanelState("suppressedTradesCard", {
    state: ageMsFromTimestamp(latestTs) >= 1_800_000 ? "stale" : "fresh",
    reason: `Loaded ${view.filteredRowsCount}/${suppressed.length} suppressed trade rows from the attribution ledger.`,
  });
}

async function loadDashboardExecutionScreen() {
  const snapshotGrid = document.getElementById("executionSnapshotGrid");
  if (!snapshotGrid) return;

  const [snapshotRes, ordersRes, fillsRes, metricsRes, overlaysRes, suppressionRes] = await Promise.allSettled([
    fetchJSON("/api/terminal/snapshot"),
    fetchJSON("/api/terminal/orders"),
    fetchJSON("/api/terminal/fills"),
    fetchJSON("/api/execution/metrics"),
    fetchJSON("/api/execution/overlays"),
    fetchJSON("/api/audit/records?table=trade_attribution_ledger&limit=50", { allowBusinessFalse: true }),
  ]);

  const snapshot = snapshotRes.status === "fulfilled" ? snapshotRes.value : null;
  const ordersPayload = ordersRes.status === "fulfilled" ? ordersRes.value : null;
  const fillsPayload = fillsRes.status === "fulfilled" ? fillsRes.value : null;
  const metrics = metricsRes.status === "fulfilled" ? metricsRes.value : null;
  const overlays = overlaysRes.status === "fulfilled" ? overlaysRes.value : null;
  const suppressionPayload = suppressionRes.status === "fulfilled" ? suppressionRes.value : null;

  renderSuppressedTrades(suppressionPayload, suppressionRes.status);

  const account = asObject(asObject(snapshot && snapshot.equity).account);
  const snapshotPositions = asArray(snapshot && snapshot.positions);
  const positionUpdatedTs = snapshotPositions.reduce(
    (max, row) => Math.max(max, intOr(row && row.updated_ts_ms, 0)),
    0
  ) || null;
  const snapshotTs = pickTimestamp(snapshot && snapshot.ts_ms, account.updated_ts_ms, positionUpdatedTs);
  const snapshotAgeMs = ageMsFromTimestamp(snapshotTs);
  const positionAgeMs = ageMsFromTimestamp(positionUpdatedTs);
  const watchCount = asArray(snapshot && snapshot.watchlist).length;
  const latencyMs = numOrNull(snapshot && snapshot.latency_ms);

  setPillTone(
    "executionAccountPill",
    account.equity == null ? "dim" : "ok",
    `equity ${formatCurrencyValue(account.equity)}`
  );
  setPillTone(
    "executionPositionPill",
    snapshotPositions.length ? "ok" : "dim",
    `${formatDecimal(snapshotPositions.length, 0)} positions`
  );
  setPillTone(
    "executionSnapshotPill",
    freshnessTone(snapshotAgeMs, 60_000, 300_000),
    `snapshot ${formatAgeMs(snapshotAgeMs)}${latencyMs == null ? "" : ` · ${formatDecimal(latencyMs, 0)}ms`}`,
    snapshotAgeMs,
    60_000,
    300_000
  );

  snapshotGrid.innerHTML = buildStatGridMarkup([
    {
      label: "Cash",
      value: formatCurrencyValue(account.cash),
      meta: account.updated_ts_ms ? `account ${fmtTime(account.updated_ts_ms)}` : "account snapshot unavailable",
    },
    {
      label: "Equity",
      value: formatCurrencyValue(account.equity),
      meta: latencyMs == null ? "latency unavailable" : `snapshot latency ${formatDecimal(latencyMs, 0)}ms`,
    },
    {
      label: "Watchlist",
      value: formatDecimal(watchCount, 0),
      meta: snapshotTs ? `snapshot ${fmtTime(snapshotTs)}` : "snapshot unavailable",
    },
    {
      label: "Live Positions",
      value: formatDecimal(snapshotPositions.length, 0),
      meta: positionUpdatedTs ? `latest ${formatAgeMs(positionAgeMs)} ago` : "no position updates",
    },
  ]);

  if (!snapshotPositions.length) {
    renderEmptyTableBody(
      "executionSnapshotBody",
      4,
      snapshotRes.status === "fulfilled" ? "No terminal positions." : "Snapshot unavailable."
    );
  } else {
    const snapshotBody = document.getElementById("executionSnapshotBody");
    if (snapshotBody) {
      snapshotBody.innerHTML = snapshotPositions.slice(0, 25).map((row) => `
        <tr class="${_tableRowClassForSymbol(row && row.symbol)}">
          <td class="mono">${escapeHTML(String((row && row.symbol) || "").trim().toUpperCase() || "—")}</td>
          <td class="mono table-cell-num">${escapeHTML(formatDecimal(row && row.qty, 4))}</td>
          <td class="mono table-cell-num">${escapeHTML(formatDecimal(row && row.avg_px, 4))}</td>
          <td class="mono metric-meta">${escapeHTML(fmtTime(row && row.updated_ts_ms))}</td>
        </tr>
      `).join("");
    }
  }

  const snapshotNotes = [];
  if (snapshotRes.status !== "fulfilled") {
    snapshotNotes.push("terminal snapshot unavailable");
  } else {
    if (latencyMs != null) snapshotNotes.push(`snapshot latency ${formatDecimal(latencyMs, 0)}ms`);
    if (watchCount) snapshotNotes.push(`watchlist ${formatDecimal(watchCount, 0)} symbols`);
    if (!snapshotPositions.length) snapshotNotes.push("no live positions returned by terminal snapshot");
    if (positionUpdatedTs) snapshotNotes.push(`latest position update ${formatAgeMs(positionAgeMs)} ago`);
  }
  const selectedExecutionSnapshotHint = _symbolContextEmptyHint(snapshotPositions, "execution snapshot positions");
  if (selectedExecutionSnapshotHint) snapshotNotes.push(selectedExecutionSnapshotHint);
  renderNotes("executionSnapshotNotes", snapshotNotes, "Execution snapshot is waiting on live terminal data.");

  const orderRows = buildExecutionOrderRows(ordersPayload);
  const ordersMeta = document.getElementById("executionOrdersMeta");
  if (ordersMeta) {
    const orderTone = ordersRes.status !== "fulfilled"
      ? "dim"
      : (orderRows.rows.length ? "ok" : "dim");
    setPillTone(
      "executionOrdersMeta",
      orderTone,
      ordersRes.status !== "fulfilled"
        ? "unavailable"
        : `${formatDecimal(orderRows.rows.length, 0)} active`
    );
  }
  const renderExecutionOrdersRows = () => renderDashboardTableView({
    tableId: "executionOrders",
    bodyId: "executionOrdersBody",
    columns: EXECUTION_ORDER_TABLE_COLUMNS,
  rows: orderRows.rows,
  emptyText: ordersRes.status === "fulfilled" ? "No open orders." : "Orders unavailable.",
  filteredEmptyText: "No open orders match the current filter.",
  colspan: 4,
  rowToHtml: (row) => {
        const lookup = decisionLookupForOrderIntent(row, "execution_open_orders");
        const selectedClass = _symbolContextClassFor(row && row.symbol);
        const rowAttrs = hasDecisionLookup(lookup) ? _decisionLookupAttr(lookup, selectedClass) : _plainTableRowClass(selectedClass);
        return `
          <tr${rowAttrs}>
            <td class="mono">${escapeHTML(row.symbol || "—")}</td>
            <td>${escapeHTML(row.side || "—")}</td>
            <td class="mono table-cell-num">${escapeHTML(row.size || "—")}</td>
            <td>${escapeHTML(row.status || "—")}</td>
          </tr>
        `;
      },
  });
  DASHBOARD_TABLE_RENDERERS.set("executionOrders", renderExecutionOrdersRows);
  renderExecutionOrdersRows();
  const ordersFallback = document.getElementById("executionOrdersFallback");
  if (ordersFallback) {
    ordersFallback.textContent = ordersRes.status !== "fulfilled"
      ? "Order state is temporarily unavailable."
      : safeJoin([
          orderRows.brokerActive ? `broker ${formatDecimal(orderRows.brokerActive, 0)} active` : "",
          orderRows.portfolioActive ? `portfolio intents ${formatDecimal(orderRows.portfolioActive, 0)}` : "",
          orderRows.usedWeightFallback ? "portfolio size falls back to weight deltas" : "",
          orderRows.usedNotionalFallback ? "some broker rows fall back to notional when qty is absent" : "",
        ]) || "No active order notes.";
  }

  const fills = asArray(fillsPayload && fillsPayload.rows);
  const latestFillTs = fills.reduce((max, row) => Math.max(max, intOr(row && row.ts_ms, 0)), 0) || null;
  const fillAgeMs = ageMsFromTimestamp(latestFillTs);
  setPillTone(
    "executionFillsMeta",
    fillsRes.status !== "fulfilled"
      ? "dim"
      : freshnessTone(fillAgeMs, 60_000, 300_000),
    fillsRes.status !== "fulfilled"
      ? "unavailable"
      : `${formatDecimal(fills.length, 0)} fills`,
    fillsRes.status !== "fulfilled" ? null : fillAgeMs,
    60_000,
    300_000
  );
  const renderExecutionFillsRows = () => renderDashboardTableView({
    tableId: "executionFills",
    bodyId: "executionFillsBody",
    columns: EXECUTION_FILL_TABLE_COLUMNS,
    rows: fills,
    emptyText: fillsRes.status === "fulfilled" ? "No recent fills." : "Fills unavailable.",
    filteredEmptyText: "No recent fills match the current filter.",
    rowToHtml: (row) => {
        const lookup = normalizeDecisionLookup({
          sourceAlertId: row && row.source_alert_id,
          portfolioOrderId: row && row.portfolio_orders_id,
          clientOrderId: row && row.source_order_id,
          surface: "execution_fills",
        });
        const selectedClass = _symbolContextClassFor(row && row.symbol);
        const rowAttrs = hasDecisionLookup(lookup) ? _decisionLookupAttr(lookup, selectedClass) : _plainTableRowClass(selectedClass);
        return `
          <tr${rowAttrs}>
            <td class="mono">${escapeHTML(String((row && row.symbol) || "").trim().toUpperCase() || "—")}</td>
            <td class="mono table-cell-num">${escapeHTML(formatDecimal(row && row.px, 4))}</td>
            <td class="mono table-cell-num">${escapeHTML(formatDecimal(row && row.qty, 4))}</td>
            <td class="mono metric-meta">${escapeHTML(fmtTime(row && row.ts_ms))}</td>
          </tr>
        `;
      },
  });
  DASHBOARD_TABLE_RENDERERS.set("executionFills", renderExecutionFillsRows);
  renderExecutionFillsRows();

  const overlaySources = asObject(overlays && overlays.sources);
  const overlayRows = Object.entries(overlaySources).map(([name, raw]) => {
    const row = asObject(raw);
    return {
      name,
      rows: intOr(row.rows, 0),
      lastTs: pickTimestamp(row.last_ts_ms),
    };
  });
  const overlayCount = overlayRows.reduce((sum, row) => sum + intOr(row.rows, 0), 0);
  const overlayLatestTs = overlayRows.reduce((max, row) => Math.max(max, intOr(row.lastTs, 0)), 0) || null;

  const metricsOk = !!(metrics && metrics.ok);
  const fillsCount = numOrNull(metrics && metrics.n_fills);
  const avgSlippageBps = numOrNull(metrics && metrics.avg_slippage_bps);
  const avgSlippageLegacy = numOrNull(metrics && metrics.avg_slippage);
  const totalFees = numOrNull(metrics && metrics.total_fees);
  const avgFillLatencyMs = numOrNull(metrics && metrics.avg_time_to_fill_ms);
  const avgSpreadBps = numOrNull(metrics && metrics.avg_spread_at_entry_bps);
  const byStrategy = asArray(metrics && metrics.by_strategy);
  const topStrategy = byStrategy
    .slice()
    .sort((a, b) => intOr(b && b.n_fills, 0) - intOr(a && a.n_fills, 0))[0];
  const costTone = avgSlippageBps == null
    ? "dim"
    : (Math.abs(avgSlippageBps) >= 10 ? "bad" : Math.abs(avgSlippageBps) >= 3 ? "warn" : "ok");

  setPillTone(
    "executionMetricsPill",
    !metricsOk ? "dim" : ((fillsCount || 0) > 0 ? "ok" : "dim"),
    !metricsOk ? "metrics unavailable" : `fills ${formatDecimal(fillsCount, 0)}`
  );
  setPillTone(
    "executionCostPill",
    costTone,
    avgSlippageBps != null
      ? `slip ${formatDecimal(avgSlippageBps, 2)} bps`
      : (avgSlippageLegacy != null ? `slip ${formatDecimal(avgSlippageLegacy, 4)}` : "slippage —")
  );
  setPillTone(
    "executionOverlayPill",
    overlaysRes.status !== "fulfilled" ? "dim" : freshnessTone(ageMsFromTimestamp(overlayLatestTs), 60_000, 300_000),
    overlaysRes.status !== "fulfilled" ? "overlays unavailable" : `overlays ${formatDecimal(overlayCount, 0)}`
  );
  setPillTone(
    "executionMetricsFreshnessPill",
    freshnessTone(fillAgeMs, 60_000, 300_000),
    `fills ${formatAgeMs(fillAgeMs)}`,
    fillAgeMs,
    60_000,
    300_000
  );

  const metricsGrid = document.getElementById("executionMetricsGrid");
  if (metricsGrid) {
    metricsGrid.innerHTML = buildStatGridMarkup([
      {
        label: "Fills",
        value: metricsOk ? formatDecimal(fillsCount, 0) : "—",
        meta: metricsOk ? `${formatDecimal(byStrategy.length, 0)} strategies` : "summary unavailable",
      },
      {
        label: "Avg Slippage",
        value: avgSlippageBps != null
          ? `${formatDecimal(avgSlippageBps, 2)} bps`
          : (avgSlippageLegacy != null ? formatDecimal(avgSlippageLegacy, 4) : "—"),
        meta: metricsOk ? `fees ${formatCurrencyValue(totalFees)}` : "summary unavailable",
      },
      {
        label: "Fill Latency",
        value: avgFillLatencyMs == null ? "—" : `${formatDecimal(avgFillLatencyMs, 0)} ms`,
        meta: latestFillTs ? `last fill ${fmtTime(latestFillTs)}` : "no recent fill timestamp",
      },
      {
        label: "Avg Spread",
        value: avgSpreadBps == null ? "—" : `${formatDecimal(avgSpreadBps, 2)} bps`,
        meta: safeJoin([
          `expected ${formatCurrencyValue(metrics && metrics.avg_expected_fill_price, 4)}`,
          `actual ${formatCurrencyValue(metrics && metrics.avg_actual_fill_price, 4)}`,
        ]) || "price context unavailable",
      },
      {
        label: "Overlay Rows",
        value: formatDecimal(overlayCount, 0),
        meta: overlayLatestTs ? `updated ${fmtTime(overlayLatestTs)}` : "overlay snapshot unavailable",
      },
      {
        label: "Snapshot Freshness",
        value: formatAgeMs(snapshotAgeMs),
        meta: snapshotTs ? `snapshot ${fmtTime(snapshotTs)}` : "snapshot unavailable",
      },
    ]);
  }

  const metricNotes = [];
  if (metricsRes.status !== "fulfilled") {
    metricNotes.push("execution metrics unavailable");
  } else if (!metricsOk) {
    metricNotes.push(String(metrics && metrics.error || "execution metrics returned no usable payload"));
  }
  if (latestFillTs) {
    metricNotes.push(`last fill ${formatAgeMs(fillAgeMs)} ago`);
  } else if (fillsRes.status === "fulfilled") {
    metricNotes.push("no recent fills captured yet");
  }
  if (topStrategy && topStrategy.strategy_name) {
    const strategySlip = numOrNull(topStrategy.avg_slippage_bps);
    metricNotes.push(
      `top strategy ${topStrategy.strategy_name} · ${formatDecimal(topStrategy.n_fills, 0)} fills${strategySlip == null ? "" : ` · avg slip ${formatDecimal(strategySlip, 2)} bps`}`
    );
  }
  if (overlayRows.length) {
    metricNotes.push(
      `overlay sources ${overlayRows.map((row) => `${row.name}:${formatDecimal(row.rows, 0)}`).join(", ")}`
    );
  } else if (overlaysRes.status !== "fulfilled") {
    metricNotes.push("execution overlays unavailable");
  }
  const selectedExecutionHint = _symbolContextEmptyHint([...orderRows.rows, ...fills], "execution rows");
  if (selectedExecutionHint) metricNotes.push(selectedExecutionHint);
  renderNotes("executionMetricsNotes", metricNotes, "Execution metrics are waiting on fills and analytics data.");
}

async function loadHealth(preloaded) {
  const hasOwn = (obj, key) => Object.prototype.hasOwnProperty.call(obj || {}, key);
  const args = preloaded && typeof preloaded === "object" && (
    hasOwn(preloaded, "health")
    || hasOwn(preloaded, "readiness")
    || hasOwn(preloaded, "sharedFailures")
  )
    ? preloaded
    : { health: preloaded };

  const providedHealth = hasOwn(args, "health");
  const failures = normalizeFailureItems(args.sharedFailures || window.__LAST_REFRESH_FAILURES__ || []);
  const healthRouteFailed = failures.some((item) => item.key === "health");
  const readiness = hasOwn(args, "readiness") ? args.readiness : (window.__LAST_READINESS__ || null);

  let h = providedHealth ? args.health : null;
  if (!providedHealth) {
    try {
      h = await fetchJSON("/api/health", { allowBusinessFalse: true });
    } catch (error) {
      h = null;
      failures.push(createFailureItem("health", "/api/health", error));
    }
  }

  const btn = document.getElementById("btnRunPipeline");
  const safeHealth = normalizeOperatorHealthPayload(h) || asObject(h);
  if (safeHealth && Object.keys(safeHealth).length) {
    window.__LAST_HEALTH__ = safeHealth;
  }

  let ingestion = {};
  let ingestionError = null;
  try {
    ingestion = extractIngestionStatus(await fetchJSON("/api/ingestion/status"));
  } catch (error) {
    ingestion = {};
    ingestionError = error;
  }

  if (!safeHealth || !Object.keys(safeHealth).length) {
    setPillTone("healthPrices", "bad", "prices unavailable");
    setPillTone("healthLabels", "bad", "labels unavailable");
    setPillTone("healthModel", "bad", "model health unavailable");
    updateDashboardLiveState({}, { errorKey: "health", error: failures[0] || "health route unavailable" });
    setPanelState("systemHealthCard", {
      state: "error",
      reason: `System health unavailable: ${describeUiError(failures[0] || "health route unavailable")}`,
    });
    if (btn) {
      btn.disabled = true;
      btn.title = `Pipeline unavailable: ${describeUiError(failures[0] || "health route unavailable")}`;
    }
    return;
  }

  const pricesOk = !!(safeHealth.prices && safeHealth.prices.ok);
  const labelsOk = !!(safeHealth.labels && safeHealth.labels.ok);
  const modelOk  = !!(safeHealth.model && safeHealth.model.ok);
  const providersOk = intOr(ingestion.healthy_providers, 0) > 0;
  const ingestionRunning = !!(ingestion.running || ingestion.job_visible);
  const healthSnapshotTs = pickTimestamp(
    safeHealth.ts_ms,
    asObject(safeHealth.timestamps).ts_ms
  );
  const healthSnapshotAgeMs = ageMsFromTimestamp(healthSnapshotTs);

  let pricesLabel = pricesOk ? `prices ok (${safeHealth.prices.age_s}s)` : "prices stale";
  let pricesTone = pricesOk ? "ok" : "bad";
  if (Object.keys(ingestion).length) {
    const child = ingestion.active_child || "—";
    const providers = `${ingestion.healthy_providers || 0}`;
    const rows = `${ingestion.fresh_rows || 0}`;
    const ageMs = Number(ingestion.price_age_ms || 0);
    const ageLabel = Number.isFinite(ageMs) && ageMs > 0 ? formatAgeMs(ageMs) : "—";
    pricesLabel = `ingestion ${ingestion.status || (ingestionRunning ? "RUNNING" : "STOPPED")} · child ${child} · providers ${providers} · rows ${rows} · age ${ageLabel}`;
    pricesTone = pricesOk && providersOk
      ? freshnessTone(ageMs, 60_000, 300_000)
      : (ingestionRunning ? "warn" : "bad");
  } else if (ingestionError) {
    pricesLabel = `ingestion status unavailable · ${describeUiError(ingestionError)}`;
    pricesTone = healthRouteFailed ? "warn" : "bad";
  }

  if (healthRouteFailed) {
    pricesTone = "warn";
    pricesLabel = `health snapshot stale · last good ${healthSnapshotTs ? `${formatAgeMs(healthSnapshotAgeMs)} ago` : "timestamp unavailable"}`;
  }

  setPillTone(
    "healthPrices",
    pricesTone,
    pricesLabel,
    healthSnapshotAgeMs,
    60_000,
    300_000
  );

  setPillTone(
    "healthLabels",
    healthRouteFailed ? "warn" : (labelsOk ? "ok" : "bad"),
    healthRouteFailed
      ? `labels snapshot stale · last good ${healthSnapshotTs ? `${formatAgeMs(healthSnapshotAgeMs)} ago` : "timestamp unavailable"}`
      : `labels ${safeHealth.labels ? safeHealth.labels.count : "?"}`,
    healthSnapshotAgeMs,
    60_000,
    300_000
  );

  setPillTone(
    "healthModel",
    healthRouteFailed ? "warn" : (modelOk ? "ok" : "bad"),
    healthRouteFailed
      ? `model snapshot stale · last good ${healthSnapshotTs ? `${formatAgeMs(healthSnapshotAgeMs)} ago` : "timestamp unavailable"}`
      : `model n=${safeHealth.model ? safeHealth.model.support_n : "?"}`,
    healthSnapshotAgeMs,
    60_000,
    300_000
  );

  const readinessBlocked = readiness && readiness.ready === false;
  const barrierBlocked = (window.__LAST_EXECUTION_BARRIER__ || {}).allowed === false;
  if (btn) {
    btn.disabled = !!(
      healthRouteFailed ||
      !pricesOk ||
      !labelsOk ||
      !providersOk ||
      !ingestionRunning ||
      readinessBlocked ||
      barrierBlocked
    );
    btn.title = btn.disabled
      ? [
          healthRouteFailed ? "health snapshot stale" : "",
          !pricesOk ? "prices not fresh" : "",
          !labelsOk ? "labels not ready" : "",
          !providersOk ? "providers unavailable" : "",
          !ingestionRunning ? "ingestion not running" : "",
          readinessBlocked ? "readiness blocked" : "",
          barrierBlocked ? "execution blocked" : "",
        ].filter(Boolean).join(" · ")
      : "Run full pipeline";
  }

  updateDashboardLiveState({
    health: safeHealth,
    readiness,
    failures,
  }, {
    sourceTs: healthSnapshotTs || Date.now(),
  });
}

async function loadExecutionOverlays() {
  const summaryEl = document.getElementById("execOverlaysSummary");
  const raw = document.getElementById("execOverlaysRaw");
  if (!raw || !summaryEl) return;

  try {
    const j = await fetchJSON("/api/execution/overlays");
    if (!j || !j.ok) throw new Error((j && j.error) || "execution overlays unavailable");

    const keys = Object.keys(asObject(j));
    const sourceCounts = keys
      .map((key) => {
        const value = j[key];
        if (Array.isArray(value)) return `${key}: ${value.length}`;
        if (value && typeof value === "object" && Array.isArray(value.rows)) return `${key}: ${value.rows.length}`;
        return "";
      })
      .filter(Boolean);
    const latestTs = extractCollectionRealtimeTs(j);

    renderStructuredSummary(summaryEl, [
      {
        label: "Sections",
        value: String(keys.length),
        meta: keys.length ? keys.slice(0, 4).join(", ") : "No overlay sections reported.",
      },
      {
        label: "Source rows",
        value: String(sourceCounts.length),
        meta: sourceCounts.length ? sourceCounts.slice(0, 4).join(" • ") : "No overlay row counts were exposed.",
      },
      {
        label: "Latest update",
        value: latestTs ? fmtTime(latestTs) : "—",
        meta: latestTs ? `${formatAgeMs(ageMsFromTimestamp(latestTs))} ago` : "No overlay timestamp reported.",
      },
    ], {
      emptyText: "Execution overlays returned no structured sections.",
      rawTarget: raw,
      rawPayload: j,
    });
    updateDashboardLiveState({ executionOverlays: j }, { sourceTs: latestTs || Date.now() });
    setPanelState("execOverlaysPanel", {
      state: latestTs && ageMsFromTimestamp(latestTs) >= 300_000 ? "stale" : "fresh",
      reason: latestTs
        ? `Execution overlays loaded. Latest snapshot ${formatAgeMs(ageMsFromTimestamp(latestTs))} old.`
        : "Execution overlays loaded without a usable timestamp.",
    });
  } catch (e) {
    renderStructuredSummary(summaryEl, [], {
      emptyText: e && e.message ? e.message : String(e),
      rawTarget: raw,
      rawPayload: { error: e && e.message ? e.message : String(e) },
    });
    updateDashboardLiveState({}, { errorKey: "executionOverlays", error: e });
    setPanelState("execOverlaysPanel", {
      state: "error",
      reason: `Execution overlays unavailable: ${describeUiError(e)}`,
    });
  }
}

async function loadStrategyMetrics() {
  try {
    const rows = await fetchJSON("/api/strategy/metrics");
    const tbody = document.querySelector("#strategyMetrics tbody");
    if (!tbody) return;

    tbody.innerHTML = "";
    for (const r of rows || []) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.strategy}</td>
        <td>${r.window_days}</td>
        <td>${Number(r.net_calmar).toFixed(3)}</td>
        <td>${Number(r.sharpe).toFixed(3)}</td>
        <td>${Number(r.turnover).toFixed(3)}</td>
        <td class="mono">${fmtTime(r.ts_ms)}</td>
      `;
      tbody.appendChild(tr);
    }

    tbody.scrollTop = 0;
  } catch (e) {}
}

async function loadTemporalShadowEval() {
  const tbody = document.querySelector("#temporalShadowEval tbody");
  if (!tbody) return;

  let rows = [];
  try {
    rows = await fetchJSON("/api/temporal/shadow_eval?limit=200");
    if (!Array.isArray(rows)) rows = [];
  } catch {
    rows = [];
  }

  tbody.innerHTML = "";
  for (const r of rows) {
    const why =
  r.detail && Array.isArray(r.detail.reasons)
    ? r.detail.reasons.join(", ")
    : (r.reason || "");
    const pass = !!r.pass_all;

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHTML(r.symbol || "")}</td>
      <td>${Number(r.horizon_s || 0)}</td>
      <td>${Number(r.n || 0)}</td>
      <td>${Number(r.rmse || 0).toFixed(3)}</td>
      <td>${Number(r.baseline_rmse || 0).toFixed(3)}</td>
      <td>${Number(r.rmse_improvement || 0).toFixed(3)}</td>
      <td>${Number(r.directional_acc || 0).toFixed(3)}</td>
      <td>${Number(r.baseline_directional_acc || 0).toFixed(3)}</td>
      <td>${Number(r.diracc_delta || 0).toFixed(3)}</td>
      <td>${r.capital_efficiency == null ? "—" : Number(r.capital_efficiency).toFixed(3)}</td>
      <td>${r.drawdown_contribution == null ? "—" : Number(r.drawdown_contribution).toFixed(3)}</td>
      <td>${r.avg_slippage_impact == null ? "—" : Number(r.avg_slippage_impact).toFixed(3)}</td>
      <td>${r.safety_score == null ? "—" : Number(r.safety_score).toFixed(3)}</td>
      <td>${pass ? "✅" : "❌"}</td>
      <td>${escapeHTML(why)}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function loadPromotionAudit() {
  const tbody = document.querySelector("#promotionAudit tbody");
  if (!tbody) return;

  let rows = [];
  try {
    rows = await fetchJSON("/api/promotion/audit?limit=200");
    if (!Array.isArray(rows)) rows = [];
  } catch {
    rows = [];
  }

  tbody.innerHTML = "";
  for (const r of rows) {
    const ts = fmtTime(r.ts_ms);
    const model = String(r.model_name || "");
    const action = String(r.action || "");
    const reg = (r.regime === null || r.regime === undefined) ? "" : String(r.regime);
    const causalScores = r.causal_scores || (r.reason && r.reason.causal_scores) || {};
    const causal = Object.entries(causalScores)
      .slice(0, 5)
      .map(([feature, score]) => `${feature}:${score === null || score === undefined ? "" : Number(score).toFixed(2)}`)
      .join(", ");
    const why = JSON.stringify(r.reason || {}).slice(0, 240);

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHTML(ts)}</td>
      <td>${escapeHTML(model)}</td>
      <td>${escapeHTML(action)}</td>
      <td>${escapeHTML(reg)}</td>
      <td><code>${escapeHTML(causal || "")}</code></td>
      <td><code>${escapeHTML(why)}</code></td>
    `;
    tbody.appendChild(tr);
  }
}


async function loadCausalScores() {
  const tbody = document.querySelector("#causalScores tbody");
  if (!tbody) return;

  let rows = [];
  try {
    rows = await fetchJSON("/api/causal/scores?limit=200");
    if (!Array.isArray(rows)) rows = [];
  } catch {
    rows = [];
  }

  tbody.innerHTML = "";
  for (const r of rows) {
    const score = r.score === null || r.score === undefined ? "" : Number(r.score).toFixed(3);
    const grangerP = r.granger_p === null || r.granger_p === undefined ? "" : Number(r.granger_p).toExponential(2);
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHTML(String(r.feature || ""))}</td>
      <td>${escapeHTML(String(r.target || ""))}</td>
      <td>${escapeHTML(String(r.window || ""))}</td>
      <td>${escapeHTML(score)}</td>
      <td>${escapeHTML(grangerP)}</td>
      <td>${escapeHTML(String(r.granger_lag || 0))}</td>
      <td>${escapeHTML(String(r.decision || ""))}</td>
    `;
    tbody.appendChild(tr);
  }
}


async function refreshCalibCurves() {
  const hEl = document.getElementById("calibHorizon");
  const kEl = document.getElementById("calibKind");
  const pre = document.getElementById("calibRaw");
  const canvas = document.getElementById("calibCanvas");
  if (!hEl || !kEl) return;

  const horizon_s = Number(hEl.value || 3600);
  const model_kind = String(kEl.value || "ridge");

  let out = null;
  try {
    out = await fetchJSON(`/api/embed_conf_calib?horizon_s=${encodeURIComponent(horizon_s)}&model_kind=${encodeURIComponent(model_kind)}`);
  } catch {
    out = null;
  }

if (pre) pre.textContent = out ? JSON.stringify(out, null, 2) : "(no calib data)";

if (
  canvas &&
  out &&
  out.ok &&
  Array.isArray(out.points)
) {
  drawCalibration(canvas, out.points);
}
}

async function loadTemporalEval() {
  try {
    const j = await fetchJSON("/api/temporal/eval");
    const tbody = document.querySelector("#temporalEval tbody");
    if (!tbody || !j || !j.rows) return;

    tbody.innerHTML = "";
    for (const r of j.rows) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="mono">${r.horizon_s}s</td>
        <td class="mono">${r.n}</td>
        <td class="mono">${r.rmse.toFixed(3)}</td>
        <td class="mono">${(100 * r.directional_acc).toFixed(1)}%</td>
      `;
      tbody.appendChild(tr);
    }

    tbody.scrollTop = 0;
  } catch {}
}

async function loadModelMetrics() {
  try {
    const rows = await fetchJSON("/api/model/metrics?model=default");
    const tbody = document.querySelector("#modelMetrics tbody");
    if (!tbody) return;

    tbody.innerHTML = "";
    for (const r of (rows || [])) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.symbol}</td>
        <td>${r.horizon_s}s</td>
        <td>${Number(r.r2 || 0).toFixed(3)}</td>
        <td>${Number(r.direction_acc || 0).toFixed(3)}</td>
        <td>${Number(r.ece || 0).toFixed(3)}</td>
        <td>${Number(r.avg_conf || 0).toFixed(3)}</td>
        <td>${Number(r.abs_err_p50 || 0).toFixed(3)}</td>
        <td>${Number(r.abs_err_p90 || 0).toFixed(3)}</td>
        <td>${Number(r.n || 0)}</td>
        <td class="mono">${fmtTime(r.ts_ms)}</td>
      `;
      tbody.appendChild(tr);
    }

    tbody.scrollTop = 0;
  } catch (e) {
    // ignore
  }
}

function normalizeModelRegistryPayload(payload) {
  const root = payload && typeof payload === "object" ? payload : {};
  const rows = Array.isArray(root.history)
    ? root.history
    : (Array.isArray(root.rows) ? root.rows : []);
  const byStage = (stage) => rows.find((row) => String((row || {}).stage || "").toLowerCase() === stage) || {};
  return {
    rows,
    champion: root.champion && Object.keys(root.champion).length ? root.champion : byStage("champion"),
    challenger: root.challenger && Object.keys(root.challenger).length ? root.challenger : byStage("challenger"),
  };
}

function renderPromotionGate(payload) {
  const metaEl = document.getElementById("promotionGateMeta");
  const summaryEl = document.getElementById("promotionGateSummary");
  const comparisonBody = document.getElementById("promotionGateComparisonBody");
  const checklistBody = document.getElementById("promotionGateChecklistBody");
  const validationEl = document.getElementById("promotionGateValidation");
  const previewEl = document.getElementById("promotionRollbackPreview");
  const rawEl = document.getElementById("promotionGateRaw");
  if (!summaryEl || !comparisonBody || !checklistBody) return;

  const gate = normalizePromotionGatePayload(payload);
  window.__LAST_PROMOTION_GATE__ = gate.raw;
  if (rawEl) rawEl.textContent = JSON.stringify(gate.raw || payload || {}, null, 2);

  const statusLabel = gate.status.enabled === false
    ? "OFF"
    : gate.status.allowed === true ? "ALLOWED" : gate.status.allowed === false ? "BLOCKED" : "UNKNOWN";
  if (metaEl) {
    metaEl.textContent = `${gate.modelName || "model"} / ${gate.regime || "global"} ${statusLabel}`;
    metaEl.className = buildPillClassName(
      metaEl,
      gate.status.allowed === true ? "ok" : gate.status.enabled === false ? "neutral" : "crit"
    );
  }

  const rollback = gate.actions.rollback && typeof gate.actions.rollback === "object" ? gate.actions.rollback : {};
  const forcePromote = gate.actions.force_promote && typeof gate.actions.force_promote === "object" ? gate.actions.force_promote : {};
  renderStructuredSummary(summaryEl, [
    {
      label: "Promotion",
      value: statusLabel,
      meta: Array.isArray(gate.status.blockers) && gate.status.blockers.length
        ? `Blockers: ${gate.status.blockers.slice(0, 4).join(", ")}`
        : "No blocker list returned by the promotion status route.",
    },
    {
      label: "Champion",
      value: modelLabel(gate.champion),
      meta: gate.champion && gate.champion.updated_ts_ms ? `Updated ${fmtTime(gate.champion.updated_ts_ms)}` : "not available",
    },
    {
      label: "Challenger",
      value: modelLabel(gate.challenger),
      meta: gate.challenger && gate.challenger.updated_ts_ms ? `Updated ${fmtTime(gate.challenger.updated_ts_ms)}` : "not available",
    },
    {
      label: "Cooldown",
      value: summarizeCooldown(gate.cooldown),
      meta: gate.cooldown.last_promo_ts_ms ? `Last promotion ${fmtTime(gate.cooldown.last_promo_ts_ms)}` : "No cooldown timestamp available.",
    },
    {
      label: "Rollback",
      value: rollback.available ? "Available" : "Unavailable",
      meta: rollback.available
        ? "Requires justification and explicit confirmation."
        : "No current champion plus retired rollback target was returned.",
    },
    {
      label: "Force promote",
      value: forcePromote.available ? "Available" : "Unavailable",
      meta: forcePromote.reason || "No audit-safe force promotion endpoint is registered.",
    },
  ]);

  comparisonBody.innerHTML = "";
  const metrics = gate.comparisonMetrics.length ? gate.comparisonMetrics : [];
  if (!metrics.length) {
    comparisonBody.innerHTML = `<tr class="table-row"><td colspan="5" class="metric-meta">No comparison metrics returned.</td></tr>`;
  } else {
    for (const metric of metrics) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHTML(metric.label || metric.key || "")}</td>
        <td class="mono">${escapeHTML(formatPromotionGateValue(metric.champion))}</td>
        <td class="mono">${escapeHTML(formatPromotionGateValue(metric.challenger))}</td>
        <td class="mono">${escapeHTML(formatPromotionGateValue(metric.delta))}</td>
        <td>${escapeHTML(String(metric.direction || "not available"))}</td>
      `;
      comparisonBody.appendChild(tr);
    }
  }

  checklistBody.innerHTML = "";
  if (!gate.checklist.length) {
    checklistBody.innerHTML = `<tr class="table-row"><td colspan="4" class="metric-meta">No gate checklist returned.</td></tr>`;
  } else {
    for (const item of gate.checklist) {
      const state = String(item.state || "unavailable");
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHTML(item.label || item.key || "")}</td>
        <td><span class="${escapeHTML(buildPillClassName(null, promotionGateStateTone(state)))}">${escapeHTML(formatGateState(state))}</span></td>
        <td class="mono">${escapeHTML(formatPromotionGateValue(item.observed))}</td>
        <td>${escapeHTML(item.expected || "not available")}</td>
      `;
      checklistBody.appendChild(tr);
    }
  }

  if (validationEl) {
    const replayStatus = gate.validation.replay_status || {};
    const replayValidation = gate.validation.replay_validation || {};
    const shadowScores = Array.isArray(gate.validation.shadow_scores) ? gate.validation.shadow_scores : [];
    renderStructuredSummary(validationEl, [
      {
        label: "Replay status",
        value: String(replayStatus.status || (replayStatus.fresh === true ? "fresh" : replayStatus.fresh === false ? "stale" : "not available")),
        meta: replayStatus.ts_ms ? `Updated ${fmtTime(replayStatus.ts_ms)}` : formatPromotionGateValue(replayStatus),
      },
      {
        label: "Replay models",
        value: String(Object.keys((replayValidation && replayValidation.models) || {}).length || "not available"),
        meta: "Replay validation model entries returned by backend metadata.",
      },
      {
        label: "Shadow scores",
        value: String(shadowScores.length || "not available"),
        meta: shadowScores[0] ? formatPromotionGateValue(shadowScores[0]) : "No shadow score rows available.",
      },
    ]);
  }

  if (previewEl) {
    previewEl.textContent = rollback.available
      ? buildRollbackConsequencePreview(gate.raw)
      : "Rollback unavailable: no rollback target was returned by the promotion gate data.";
  }

  setPanelState("promotionGateCard", {
    state: gate.ok ? "fresh" : "error",
    reason: gate.ok
      ? `Promotion gate loaded for ${gate.modelName || "model"} with ${gate.checklist.length} checklist rows.`
      : "Promotion gate returned an error payload.",
  });
}

async function loadPromotionGate() {
  const summaryEl = document.getElementById("promotionGateSummary");
  if (!summaryEl) return;
  try {
    const payload = await fetchJSON("/api/promotion/explain", { allowBusinessFalse: true });
    renderPromotionGate(payload && payload.gate ? payload.gate : payload);
  } catch (e) {
    renderStructuredSummary(summaryEl, [], {
      emptyText: `Promotion gate unavailable: ${e.message}`,
      rawTarget: "promotionGateRaw",
      rawPayload: { error: e && e.message ? e.message : String(e) },
    });
    const metaEl = document.getElementById("promotionGateMeta");
    if (metaEl) {
      metaEl.textContent = "unavailable";
      metaEl.className = buildPillClassName(metaEl, "neutral");
    }
    setPanelState("promotionGateCard", {
      state: "error",
      reason: `Promotion gate unavailable: ${describeUiError(e)}`,
    });
  }
}

async function loadModelRegistry() {
  const body = document.getElementById("modelRegistryBody");
  const chPill = document.getElementById("championPill");
  const clPill = document.getElementById("challengerPill");
  if (!body || !chPill || !clPill) return;

  try {
    const j = await fetchJSON("/api/model/registry?limit=25");
    if (!j || !j.ok) return;

    const registry = normalizeModelRegistryPayload(j);
    const champ = registry.champion || {};
    const chall = registry.challenger || {};

    const champRmse = champ.metrics ? champ.metrics.rmse : null;
    const challRmse = chall.metrics ? chall.metrics.rmse : null;

    chPill.textContent = `champion: ${champ.model_kind || "?"} rmse=${fmtNum(champRmse)}`;
    clPill.textContent = `challenger: ${chall.model_kind || "?"} rmse=${fmtNum(challRmse)}`;

    chPill.className = buildPillClassName(chPill, _isExecutionDegraded() ? "warn" : "ok");
    clPill.className = buildPillClassName(clPill, "neutral");

    body.innerHTML = "";
    for (const r of (registry.rows || [])) {
      const m = r.metrics || {};
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.stage || ""}</td>
        <td class="mono">${r.model_kind || ""}</td>
        <td class="mono">${fmtNum(m.rmse)}</td>
        <td class="mono">${m.directional_acc !== undefined ? (100*Number(m.directional_acc)).toFixed(2)+"%" : ""}</td>
        <td class="mono">${m.n_eval || ""}</td>
        <td class="mono">${fmtTime(m.eval_ts_ms || r.model_ts_ms || 0)}</td>
        <td class="mono">${fmtTime(r.created_ts_ms || 0)}</td>
      `;
      body.appendChild(tr);
    }
  } catch (e) {
    // ignore
  }
}

async function loadModelDiagnostics() {

  const el = document.getElementById("modelDiagnostics");
  if (!el) return;
  try {
    const d = await fetchJSON("/api/model/diagnostics");
    let out = "";
    out += "=== REGIME PRIORS ===\n";
    const rp = d && d.regime_priors ? d.regime_priors : {};
    for (const k of Object.keys(rp)) {
      out += `\n${k}\n`;
      for (const r of rp[k]) out += `  ${r.regime}: n=${r.n} mean_z=${Number(r.mean_z).toFixed(3)}\n`;
    }
    out += "\n=== GLOBAL PRIORS ===\n";
    for (const g of (d && d.global_priors) ? d.global_priors : []) {
      out += `${g.symbol} h=${g.horizon_s} n=${g.n} mean_z=${Number(g.mean_z).toFixed(3)}\n`;
    }
    out += "\n=== SPILLOVERS ===\n";
    const sp = d && d.spillovers ? d.spillovers : {};
    for (const k of Object.keys(sp)) {
      out += `\n${k}\n`;
      for (const s of sp[k]) out += `  <- ${s.driver}: beta=${Number(s.beta).toFixed(3)} n=${s.n}\n`;
    }
    el.textContent = out || "(no diagnostics yet)";
  } catch (e) {
    el.textContent = `[error] ${e.message}`;
  }
}

// ---------- Relevance Stats ----------
async function loadRelevanceStats() {
  const body = document.getElementById("relevanceTableBody");
  const meta = document.getElementById("relevanceMeta");
  const diffBox = document.getElementById("relevanceDiff");
  if (!body || !meta || !diffBox) return;

  try {
    const res = await fetchJSON("/api/relevance/stats");
    if (!res || !res.ok) {
      meta.textContent = "disabled";
      meta.className = buildPillClassName(meta, "neutral");
      body.innerHTML = "";
      diffBox.textContent = res && res.error ? res.error : "not available";
      return;
    }

    meta.textContent = res.cached ? "cached" : "live";
    meta.className = buildPillClassName(meta, res.cached ? "neutral" : "ok");

    const rows = _parseRelevanceStats(res.stats);

    // dominance thresholds (relative, not magic numbers)
    const maxRel = Math.max(...rows.map(r => r.relevance || 0), 0);
    const maxZ   = Math.max(...rows.map(r => r.mean_abs_z || 0), 0);

    body.innerHTML = "";
    rows
      .sort((a, b) => (b.relevance || 0) - (a.relevance || 0))
      .forEach(r => {
        const dom =
          (r.relevance >= 0.9 * maxRel && r.relevance > 0) ||
          (r.mean_abs_z >= 0.9 * maxZ && r.mean_abs_z > 0);

        body.insertAdjacentHTML("beforeend", `
          <tr class="${dom ? "health-ok" : ""}">
            <td class="mono">${r.symbol}</td>
            <td class="mono">${r.horizon}</td>
            <td>${Number.isFinite(r.relevance) ? r.relevance.toFixed(3) : "?"}</td>
            <td>${Number.isFinite(r.mean_abs_z) ? r.mean_abs_z.toFixed(3) : "?"}</td>
            <td>${Number.isFinite(r.n) ? r.n : "?"}</td>
          </tr>
        `);
      });

    // ---- diff vs previous snapshot ----
    const prevRaw = localStorage.getItem(_RELEVANCE_SNAPSHOT_KEY);
    const prev = prevRaw ? _safeParseJSON(prevRaw) : null;

    if (prev) {
      const diff = _diffRelevance(prev, res.stats);
      diffBox.textContent = diff.length
        ? diff.slice(0, 100).map(d => `- ${d}`).join("\n")
        : "(no changes)";
    } else {
      diffBox.textContent = "(first snapshot)";
    }

    localStorage.setItem(_RELEVANCE_SNAPSHOT_KEY, JSON.stringify(res.stats));
  } catch (e) {
    meta.textContent = "error";
    meta.className = buildPillClassName(meta, "crit");
    body.innerHTML = "";
    diffBox.textContent = e.message;
  }
}

async function loadConfidenceMass() {
  const wrap = document.getElementById("confidenceMass");
  if (!wrap) return;
  try {
    const d = await fetchJSON("/api/confidence_mass");
    const bins = (d && d.bins) ? d.bins : [];
    wrap.innerHTML = "";
    if (!bins.length) {
      wrap.textContent = "(no confidence data yet)";
      return;
    }

    const maxN = Math.max(...bins.map(b => b.count || 0), 1);
    for (const b of bins) {
      const row = document.createElement("div");
      row.style.display = "flex";
      row.style.alignItems = "center";
      row.style.gap = "8px";
      row.style.margin = "4px 0";

      const label = document.createElement("div");
      label.className = "mono small";
      label.style.width = "88px";
      label.textContent = `${b.lo.toFixed(1)}–${b.hi.toFixed(1)}`;

      const bar = document.createElement("div");
      bar.style.flex = "1";
      bar.style.height = "10px";
      bar.style.border = "1px solid #30363d";
      bar.style.borderRadius = "999px";
      bar.style.overflow = "hidden";
      bar.style.background = "#0e1117";

      const fill = document.createElement("div");
      fill.style.height = "100%";
      fill.style.width = barWidth((100 * (b.count || 0)) / maxN);
      fill.style.background = "#2ea043";
      bar.appendChild(fill);

      const n = document.createElement("div");
      n.className = "mono small";
      n.style.width = "48px";
      n.style.textAlign = "right";
      n.textContent = String(b.count || 0);

      row.appendChild(label);
      row.appendChild(bar);
      row.appendChild(n);
      wrap.appendChild(row);
    }
  } catch (e) {
    wrap.textContent = `[error] ${e.message}`;
  }
}

// ------            -- ------------------------------------------------------
// Social (read-only) panels
// ------            -- ------------------------------------------------------
async function loadJobHistory() {

  const panel = document.getElementById("jobHistoryPanel");
  const el = document.getElementById("jobHistory");
  if (!panel || !el) return;
  if (panel.style.display === "none") return;

  try {
    const d = await fetchJSON(`/api/jobs/history?name=${encodeURIComponent(selectedJob)}&limit=200`);
    const items = (d && d.ok && Array.isArray(d.rows)) ? d.rows : [];
    let out = "";
    for (const it of items) {
      const t = fmtTime(it.ts_ms);
      const name = it.job_name || "";
      const ev = it.event || "";
      const rc = (it.exit_code === null || it.exit_code === undefined) ? "" : ` rc=${it.exit_code}`;
      const det = it.detail ? ` — ${it.detail}` : "";
      out += `${t}  ${name}  ${ev}${rc}${det}\n`;
    }
    el.textContent = out || "(no history yet)";
  } catch (e) {
    el.textContent = `[error] ${e.message}`;
  }
}

async function loadCrashAnalytics() {
  const el = document.getElementById("jobHistory");
  if (!el) return;

  try {
    const j = await fetchJSON("/api/crash_analytics?limit=50");
    if (!j || !j.ok || !Array.isArray(j.rows)) return;

    const lines = [];
    lines.push("=== Crash Analytics ===");
    for (const r of j.rows) {
      lines.push(
        `${fmtTime(r.ts_ms)} | ${r.job_name} | rc=${r.exit_code} | ${r.reason || ""}`
      );
    }

    el.textContent += "\n\n" + lines.join("\n");

  } catch (_) {}
}

async function loadConfidenceTrends() {
  const panel = document.getElementById("confidenceTrendPanel");
  const el = document.getElementById("confidenceTrends");
  if (!panel || !el) return;
  if (panel.style.display === "none") return;

  try {
    const d = await fetchJSON("/api/alerts/timeline?limit=120");
    const rows = normalizeAlertsPayload(d);

    const bySym = {};
    for (const r of rows) {
      const sym = r.symbol || "UNK";
      const c = Number(r.confidence);
      if (!Number.isFinite(c)) continue;
      (bySym[sym] ||= []).push(c);
    }

    let out = "";
    for (const sym of Object.keys(bySym).sort()) {
      const arr = bySym[sym];
      const last10 = arr.slice(0, 10);
      const prev10 = arr.slice(10, 20);
      const avg = (xs) => xs.length ? xs.reduce((a,b)=>a+b,0)/xs.length : 0;

      const a1 = avg(last10);
      const a2 = avg(prev10);
      const trend = (a1 > a2 + 0.03) ? "↑" : (a1 < a2 - 0.03) ? "↓" : "→";

      out += `${sym}: avg_conf=${a1.toFixed(2)} ${trend} (n=${arr.length})\n`;
    }

    el.textContent = out || "(no alerts yet)";
  } catch (e) {
    el.textContent = `[error] ${e.message}`;
  }
}

function _notificationValidationSummary(row) {
  const errors = Array.isArray(row && row.validation_errors) ? row.validation_errors.filter(Boolean) : [];
  return errors.length ? errors.join(", ") : "none";
}

function _notificationLastTestSummary(row) {
  const lastTest = row && row.last_test && typeof row.last_test === "object" ? row.last_test : null;
  if (!lastTest || !Number.isFinite(Number(lastTest.ts_ms)) || Number(lastTest.ts_ms) <= 0) {
    return "No operator test recorded";
  }
  const outcome = lastTest.ok ? "PASS" : "FAIL";
  const detail = lastTest.ok
    ? (lastTest.message || "notification_test_sent")
    : (lastTest.error || lastTest.message || "test_failed");
  return `${outcome} • ${fmtTime(Number(lastTest.ts_ms))} • ${detail}`;
}

function _runtimeHealthAlertSummary(runtimeAlert) {
  const alert = runtimeAlert && typeof runtimeAlert === "object" ? runtimeAlert : null;
  if (!alert) {
    return {
      banner: "No runtime health transitions recorded yet.",
      bannerClass: "status",
      meta: "Health-monitor alert history will appear here after the first degraded or recovered transition.",
    };
  }

  const state = String(alert.state || "").trim().toLowerCase();
  const severity = String(alert.severity || "").trim().toUpperCase();
  const reasonCodes = Array.isArray(alert.reason_codes) ? alert.reason_codes.filter(Boolean) : [];
  const headline = String(alert.headline || "").trim() || (state === "healthy" ? "Runtime health recovered" : "Runtime health degraded");
  const updated = Number(alert.updated_ts_ms || 0);
  const when = updated > 0 ? fmtTime(updated) : "unknown time";
  const summary = String(alert.summary || "").trim();

  if (state === "healthy") {
    return {
      banner: `${headline} • ${when}`,
      bannerClass: "status ok",
      meta: summary || "Last health-monitor transition is clear.",
    };
  }

  return {
    banner: `${headline} • ${severity || "WARN"} • ${when}`,
    bannerClass: severity === "CRITICAL" ? "status bad" : "status warn",
    meta: reasonCodes.length ? reasonCodes.slice(0, 5).join(", ") : (summary || "No blocker codes recorded."),
  };
}

function renderNotificationStatusPanel(payload = null, errorMessage = "") {
  const banner = document.getElementById("notificationStatusBanner");
  const body = document.getElementById("notificationStatusTableBody");
  const runtimeHealthBanner = document.getElementById("runtimeHealthAlertBanner");
  const runtimeHealthMeta = document.getElementById("runtimeHealthAlertMeta");
  if (!banner || !body) return;

  if (runtimeHealthBanner && runtimeHealthMeta) {
    const runtimeSummary = _runtimeHealthAlertSummary(payload && payload.runtime_health_alert);
    runtimeHealthBanner.textContent = runtimeSummary.banner;
    runtimeHealthBanner.className = runtimeSummary.bannerClass;
    runtimeHealthMeta.textContent = runtimeSummary.meta;
  }

  const channels = payload && Array.isArray(payload.channels) ? payload.channels : [];
  const configuredCount = channels.filter((row) => !!(row && row.configured)).length;
  const enabledCount = channels.filter((row) => !!(row && row.enabled)).length;
  const invalidCount = channels.filter((row) => Array.isArray(row && row.validation_errors) && row.validation_errors.length).length;

  if (errorMessage) {
    banner.textContent = channels.length
      ? `Notification status unavailable. Showing last known state. ${errorMessage}`
      : `Notification status unavailable. ${errorMessage}`;
    banner.className = "status bad";
  } else if (!channels.length) {
    banner.textContent = "Notification status is unavailable or unconfigured.";
    banner.className = "status";
  } else if (enabledCount > 0 && invalidCount === 0) {
    banner.textContent = `Notification channels ready: ${enabledCount}/${channels.length} enabled. Tests are operator-initiated only.`;
    banner.className = "status ok";
  } else if (configuredCount > 0) {
    banner.textContent = `Notification config needs attention: ${enabledCount}/${channels.length} enabled, ${invalidCount} invalid.`;
    banner.className = "status bad";
  } else {
    banner.textContent = "No notification channels are configured. Dashboard stays in degraded-safe mode.";
    banner.className = "status";
  }

  if (!channels.length) {
    body.innerHTML = `
      <tr class="table-row">
        <td colspan="6" class="metric-meta">${escapeHTML(errorMessage || "Notification status is not available.")}</td>
      </tr>
    `;
    return;
  }

  body.innerHTML = channels.map((row) => {
    const channel = String(row && row.channel || "unknown").trim().toLowerCase();
    const transport = String(row && row.transport || "").trim();
    const provider = String(row && row.provider || "").trim();
    const configuredPill = row && row.configured
      ? '<span class="pill ok">yes</span>'
      : '<span class="pill dim">no</span>';
    const enabledPill = row && row.enabled
      ? '<span class="pill ok">yes</span>'
      : (Array.isArray(row && row.validation_errors) && row.validation_errors.length
          ? '<span class="pill warn">no</span>'
          : '<span class="pill dim">no</span>');
    const validationText = _notificationValidationSummary(row);
    const lastTestText = _notificationLastTestSummary(row);
    const pending = _notificationTestPending.has(channel);
    const actionHtml = row && row.supports_test
      ? `
        <button class="btn btnSmall" data-notification-test="${escapeHTML(channel)}" ${pending ? "disabled" : ""}>
          ${pending ? "Sending..." : "Send Test"}
        </button>
        <div class="metric-meta" style="margin-top:6px;">Explicit operator action only.</div>
      `
      : `
        <span class="metric-meta">Test unavailable</span>
        <div class="metric-meta" style="margin-top:6px;">${escapeHTML(validationText === "none" ? "Channel is not configured." : validationText)}</div>
      `;

    const metaParts = [transport];
    if (provider && provider !== transport) metaParts.push(provider);

    return `
      <tr class="table-row">
        <td>
          <div class="mono">${escapeHTML(channel)}</div>
          ${metaParts.filter(Boolean).length ? `<div class="metric-meta">${escapeHTML(metaParts.filter(Boolean).join(" • "))}</div>` : ""}
        </td>
        <td>${configuredPill}</td>
        <td>${enabledPill}</td>
        <td>${escapeHTML(validationText)}</td>
        <td>${escapeHTML(lastTestText)}</td>
        <td>${actionHtml}</td>
      </tr>
    `;
  }).join("");
}

async function loadNotificationStatus() {
  const banner = document.getElementById("notificationStatusBanner");
  const body = document.getElementById("notificationStatusTableBody");
  if (!banner || !body) return;

  try {
    const payload = await fetchJSON("/api/notifications/status");
    _lastNotificationStatus = payload;
    updateDashboardLiveState({ notificationStatus: payload }, { sourceTs: extractRealtimeTs(payload, Date.now()) });
    renderNotificationStatusPanel(payload);
    setPanelState("notificationStatusCard", {
      state: "fresh",
      reason: "Notification channel status and runtime health alert state loaded successfully.",
    });
  } catch (e) {
    updateDashboardLiveState({}, { errorKey: "notificationStatus", error: e });
    renderNotificationStatusPanel(
      _lastNotificationStatus,
      e && e.message ? e.message : String(e),
    );
    setPanelState("notificationStatusCard", {
      state: _lastNotificationStatus ? "stale" : "error",
      reason: _lastNotificationStatus
        ? `Showing the last known notification status because refresh failed: ${describeUiError(e)}`
        : `Notification status unavailable: ${describeUiError(e)}`,
    });
  }
}

async function sendNotificationTest(channel) {
  const normalizedChannel = String(channel || "").trim().toLowerCase();
  if (!normalizedChannel || _notificationTestPending.has(normalizedChannel)) return;

  _notificationTestPending.add(normalizedChannel);
  renderNotificationStatusPanel(_lastNotificationStatus);

  try {
    const result = await postJSONAllowBusinessFalse("/api/notifications/test", {
      channel: normalizedChannel,
      actor: "operator",
      source: "dashboard",
    });

    if (result && result.ok) {
      toast(`${normalizedChannel} test sent.`, "ok", 3000);
    } else {
      const reason = result && result.error ? String(result.error) : "test_unavailable";
      toast(`${normalizedChannel} test unavailable: ${reason}`, "warn", 4000);
    }
  } catch (e) {
    toast(`Notification test failed: ${e.message || e}`, "bad", 4000);
  } finally {
    _notificationTestPending.delete(normalizedChannel);
    await loadNotificationStatus();
  }
}


/* -----------------------------
   Actions
----------------------------- */


async function loadSupervisorStatus() {
  const pill = document.getElementById("supervisorPill");
  const raw  = document.getElementById("supervisorRaw");
  if (!pill || !raw) return;

  try {
    const j = await fetchJSON("/api/supervisor/status");
    if (!j || !j.ok) throw new Error((j && j.error) || "supervisor unavailable");

    pill.className = buildPillClassName(pill, j.enabled ? "ok" : "neutral");
    pill.textContent = j.enabled ? "supervisor: ON" : "supervisor: OFF";
    raw.textContent = JSON.stringify(j, null, 2);
  } catch (e) {
    pill.className = buildPillClassName(pill, "crit");
    pill.textContent = "supervisor: error";
    raw.textContent = e.message || String(e);
  }
}

// -----------------------------
// Structured Readiness + Telemetry
// -----------------------------
async function loadStructuredReadiness() {
  const el = document.getElementById("systemStateText");
  const banner = document.getElementById("systemStateBanner");
  if (!el) return;

  try {
    const state = await fetchJSON("/api/system/state", { allowBusinessFalse: true });
    if (!state) return;

    const lines = [];
    lines.push(`state: ${state.state}`);
    lines.push(`ok: ${state.ok}`);
    lines.push(`ts_ms: ${state.ts_ms}`);
    lines.push("");

    if (Array.isArray(state.reasons) && state.reasons.length) {
      lines.push("reasons:");
      for (const r of state.reasons) {
        lines.push(`  - ${r}`);
      }
      lines.push("");
    }

    if (state.jobs) {
      lines.push("running_daemons:");
      for (const j of (state.jobs.running_daemons || [])) {
        lines.push(`  - ${j}`);
      }

      lines.push("running_oneshots:");
      for (const j of (state.jobs.running_oneshots || [])) {
        lines.push(`  - ${j}`);
      }
    }

    renderSystemState(state, el, banner);
    updateDashboardLiveState({ systemState: state }, { sourceTs: extractRealtimeTs(state, Date.now()) });
    setPanelState("systemStateCard", {
      state: ageMsFromTimestamp(extractRealtimeTs(state, 0)) >= 300_000 ? "stale" : "fresh",
      reason: state.state === "LIVE"
        ? "System state is live and readable."
        : `System state is ${String(state.state || "unknown").toLowerCase()}.`,
    });

    // ---------- Institutional Banner State ----------
    if (banner) {
      banner.textContent = state.state || "UNKNOWN";

      banner.className = buildPillClassName(
        banner,
        state.state === "LIVE"
          ? "ok"
          : state.state === "DEGRADED"
            ? "warn"
            : state.state === "KILL_SWITCH"
              ? "crit"
              : "neutral"
      );
    }

  } catch (e) {
    el.textContent = `[readiness error] ${e.message || e}`;
    setPanelState("systemStateCard", {
      state: "error",
      reason: `System-state snapshot unavailable: ${describeUiError(e)}`,
    });
  }
}

async function loadOperatorStartupPanel() {
  const checklistEl = document.getElementById("operatorStartupChecklist");
  if (!checklistEl) return;

  let readiness = null;
  let health = null;
  let systemState = null;
  let broker = null;

  try {
    readiness = await fetchJSON("/api/readiness", { allowBusinessFalse: true });
  } catch {
    try {
      readiness = await fetchJSON("/api/operator/readiness", { allowBusinessFalse: true });
    } catch {
      readiness = null;
    }
  }

  try {
    health = await fetchJSON("/api/health", { allowBusinessFalse: true });
  } catch {
    health = null;
  }

  try {
    systemState = await fetchJSON("/api/system/state", { allowBusinessFalse: true });
  } catch {
    systemState = null;
  }

  try {
    broker = await fetchJSON("/api/broker");
  } catch {
    broker = null;
  }

  if (readiness) window.__LAST_READINESS__ = readiness;
  if (health) window.__LAST_HEALTH__ = health;
  if (systemState) window.__LAST_SYSTEM_STATE__ = systemState;

  const startup = buildStartupDiagnostics({
    readiness,
    health,
    systemState,
    broker
  });

  renderOperatorStartupPanel(startup);
  updateDashboardLiveState({
    readiness,
    health,
    systemState,
  }, {
    sourceTs: pickTimestamp(
      readiness && readiness.ts_ms,
      health && health.ts_ms,
      systemState && systemState.ts_ms
    ) || Date.now(),
  });
  setPanelState("operatorStartupCard", {
    state: startup.ready ? "fresh" : "stale",
    reason: startup.ready
      ? "All startup readiness checks passed."
      : (startup.blockers && startup.blockers.length
        ? `Startup readiness blocked: ${startup.blockers.slice(0, 2).join(" • ")}`
        : "Startup readiness is still waiting on required services."),
  });
}

async function loadPnL(preloaded = null, options = {}) {
  const totalEl = document.getElementById("pnlTotal");
  const uEl = document.getElementById("pnlUnrealized");
  const rEl = document.getElementById("pnlRealized");

  // UI card not present => skip silently
  if (!totalEl || !uEl || !rEl) return;

  const requestTs = coerceRealtimeTs(options.sourceTs) || Date.now();

  try {
    let j = preloaded ? normalizePnLPayload(preloaded) : null;
    let usedLegacyFallback = false;
    let canonicalFetchError = null;

    if (!preloaded) {
      try {
        j = normalizeCanonicalPnLAsPayload(await fetchCanonicalUiMetrics());
      } catch (e) {
        canonicalFetchError = e;
        usedLegacyFallback = true;
        j = normalizePnLPayload(await fetchJSON("/api/pnl"));
      }
    }

    if (requestTs < _latestPnLStateTs) {
      return;
    }
    if (!j || !j.ok || !j.data) {
      if (preloaded) return;
      setPnlPillValue(totalEl, "Today", null);
      setPnlPillValue(uEl, "Unrealized", null);
      setPnlPillValue(rEl, "Realized", null);
      updateDashboardLiveState({}, { errorKey: "pnl", error: "pnl route returned no usable payload" });
      setPanelState("livePnlCard", {
        state: "empty",
        reason: j && j.__canonicalPnl
          ? `Canonical PnL source missing: ${j.__canonicalPnl.sourceLabel}.`
          : "PnL route returned no usable portfolio totals.",
      });
      return;
    }
    _latestPnLStateTs = requestTs;

    const d = j.data || {};
    const total = Number(d.total);
    const unr = Number(d.unrealized);
    const rea = Number(d.realized);
    const canonicalPnl = j.__canonicalPnl || null;
    const canonicalMetrics = j.__canonical || null;
    const displayTs = coerceRealtimeTs(j.ts_ms, requestTs) || requestTs;

    setPnlPillValue(totalEl, "Today", total);
    setPnlPillValue(uEl, "Unrealized", unr);
    setPnlPillValue(rEl, "Realized", rea);
    updateDashboardLiveState({
      pnl: d,
      uiMetrics: canonicalMetrics,
    }, { sourceTs: displayTs });
    const pnlState = usedLegacyFallback
      ? "stale"
      : (canonicalPnl && (canonicalPnl.stale || canonicalPnl.degraded) ? "stale" : "fresh");
    const sourceDetail = canonicalPnl
      ? canonicalPnl.sourceLabel
      : (usedLegacyFallback ? "legacy PnL endpoint fallback" : "/api/pnl");
    setPanelState("livePnlCard", {
      state: pnlState,
      reason: usedLegacyFallback
        ? `Canonical UI metrics unavailable; legacy /api/pnl fallback shown. Today ${Number.isFinite(total) ? total.toFixed(2) : "—"} • ${describeUiError(canonicalFetchError)}.`
        : `Canonical PnL loaded from ${sourceDetail}. Today ${Number.isFinite(total) ? total.toFixed(2) : "—"} • updated ${formatAgeMs(Math.max(0, Date.now() - displayTs))} ago.`,
    });
  } catch (e) {
    if (preloaded) return;
    setPnlPillValue(totalEl, "Today", null);
    setPnlPillValue(uEl, "Unrealized", null);
    setPnlPillValue(rEl, "Realized", null);
    totalEl.title = e && e.message ? e.message : "pnl_load_failed";
    updateDashboardLiveState({}, { errorKey: "pnl", error: e });
    setPanelState("livePnlCard", {
      state: "error",
      reason: `PnL unavailable: ${describeUiError(e)}`,
    });
  }
}

async function jobAction(name, action) {
  const normalizedName = String(name || "").trim();
  const normalizedAction = String(action || "").trim().toLowerCase();
  if (!normalizedName || !["start", "stop"].includes(normalizedAction)) {
    toast("Unsupported job action", "warn", 2600);
    return;
  }

  setSelectedJob(normalizedName);

  if (hardBlockIfReadOnly({ actionName: `${normalizedAction} job ${normalizedName}`, toastFn: toast })) {
    return;
  }

  if (!confirmJobAction(normalizedName, normalizedAction)) {
    toast("Job action cancelled", "warn", 2200);
    return;
  }

  if (normalizedAction === "start") {
    await postJSON(`/api/jobs/start?name=${encodeURIComponent(normalizedName)}`, { name: normalizedName });
  } else if (normalizedAction === "stop") {
    await postJSON(`/api/jobs/stop?name=${encodeURIComponent(normalizedName)}`, { name: normalizedName });
  }

  await refresh();
  applyReadOnlyBanner();
}

async function loadSystemStatusHeader(preloaded = {}) {
  const headerEl = document.querySelector("#system-status-header");
  if (!headerEl) return;
  const requestTs = coerceRealtimeTs(preloaded.__sourceTs, preloaded.sourceTs) || Date.now();
  const hasOwn = (key) => Object.prototype.hasOwnProperty.call(preloaded || {}, key);

  const pick = (...values) => {
    for (const value of values) {
      if (value !== undefined && value !== null && value !== "") {
        return value;
      }
    }
    return null;
  };

  const pickNumber = (...values) => {
    for (const value of values) {
      const n = Number(value);
      if (Number.isFinite(n)) return n;
    }
    return null;
  };

  let health = hasOwn("health") ? preloaded.health : null;
  let systemState = hasOwn("systemState") ? preloaded.systemState : null;
  let ingestion = hasOwn("ingestion") ? preloaded.ingestion : null;
  let supervisor = hasOwn("supervisor") ? preloaded.supervisor : null;
  let executionBarrier = hasOwn("executionBarrier") ? preloaded.executionBarrier : null;
  let broker = hasOwn("broker") ? preloaded.broker : null;
  let uiMetrics = hasOwn("uiMetrics") ? preloaded.uiMetrics : null;

  if (!hasOwn("health")) {
    try {
      health = await fetchJSON("/api/health", { allowBusinessFalse: true });
    } catch {
      health = null;
    }
  }

  if (!hasOwn("systemState")) {
    try {
      systemState = await fetchJSON("/api/system/state", { allowBusinessFalse: true });
    } catch {
      systemState = null;
    }
  }

  if (!hasOwn("ingestion")) {
    try {
      ingestion = await fetchJSON("/api/ingestion/status");
    } catch {
      ingestion = null;
    }
  }

  if (!hasOwn("supervisor")) {
    try {
      supervisor = await fetchJSON("/api/supervisor/status");
    } catch {
      supervisor = null;
    }
  }

  if (!hasOwn("executionBarrier")) {
    try {
      executionBarrier = await fetchJSON("/api/execution/barrier", { allowBusinessFalse: true });
    } catch {
      executionBarrier = null;
    }
  }

  if (!hasOwn("broker")) {
    try {
      broker = await fetchJSON("/api/broker");
    } catch {
      broker = null;
    }
  }

  if (!hasOwn("uiMetrics")) {
    try {
      uiMetrics = await fetchCanonicalUiMetrics();
    } catch {
      uiMetrics = null;
    }
  } else if (uiMetrics) {
    uiMetrics = normalizeUiMetricsPayload(uiMetrics);
  }

  if (health) {
    _lastHealth = health;
    window.__LAST_HEALTH__ = health;
  }
  if (systemState) {
    window.__LAST_SYSTEM_STATE__ = systemState;
  }
  if (executionBarrier) {
    window.__LAST_EXECUTION_BARRIER__ = executionBarrier;
    syncJobActionSafetyState();
    applyReadOnlyBanner();
  }

  const unresolvedAlerts = Array.isArray(_lastAlerts)
    ? _lastAlerts.filter(a => a && !a.resolved).length
    : null;

  const healthyProviders = pickNumber(
    ingestion && ingestion.healthy_providers,
    ingestion && ingestion.summary && ingestion.summary.healthy_providers
  );

  const freshRows = pickNumber(
    ingestion && ingestion.fresh_rows,
    ingestion && ingestion.summary && ingestion.summary.fresh_rows
  );

  const priceAgeMs = pickNumber(
    ingestion && ingestion.price_age_ms,
    ingestion && ingestion.summary && ingestion.summary.price_age_ms,
    health && health.prices && health.prices.age_ms,
    health && health.prices && Number.isFinite(Number(health.prices.age_s)) ? Number(health.prices.age_s) * 1000 : null
  );

  const dataStatus = (() => {
    const explicit = pick(
      ingestion && ingestion.status,
      ingestion && ingestion.state,
      ingestion && ingestion.summary && ingestion.summary.status
    );
    if (explicit) return explicit;
    if (ingestion && ingestion.ok === false) return "DISCONNECTED";
    if ((healthyProviders || 0) > 0 && (freshRows || 0) > 0) return "CONNECTED";
    if ((ingestion && ingestion.running) || (ingestion && ingestion.job_visible)) return "DEGRADED";
    return "DISCONNECTED";
  })();

  const engineState = (() => {
    const explicit = pick(
      systemState && systemState.state,
      systemState && systemState.system_state,
      supervisor && supervisor.state,
      supervisor && supervisor.status
    );
    if (explicit) return explicit;
    if (supervisor && supervisor.ok && supervisor.enabled) return "RUNNING";
    if (systemState && systemState.ok) return "RUNNING";
    return "STOPPED";
  })();

  const tradingMode = pick(
    executionBarrier && executionBarrier.mode,
    executionBarrier && executionBarrier.execution_mode,
    systemState && systemState.mode,
    systemState && systemState.execution_mode
  ) || "UNKNOWN";

  const executionEnabled = !!pick(
    executionBarrier && executionBarrier.allowed,
    executionBarrier && executionBarrier.execution_enabled,
    executionBarrier && executionBarrier.enabled
  );

  const brokerConnectivity = (() => {
    if (broker && broker.ok === true) return "CONNECTED";
    if (broker && broker.ok === false) return "DISCONNECTED";
    if (broker && (broker.account || Array.isArray(broker.positions) || Array.isArray(broker.fills))) return "CONNECTED";
    return "UNKNOWN";
  })();

  const alertCount = pickNumber(
    unresolvedAlerts,
    health && health.alert_count,
    health && health.alerts && health.alerts.count
  );

  const canonicalPnl = uiMetrics ? canonicalPnlValues(uiMetrics) : null;
  const canonicalExposure = uiMetrics ? canonicalExposureValues(uiMetrics) : null;

  const normalized = {
    data_status: dataStatus,
    engine_state: engineState,
    trading_mode: tradingMode,
    execution_enabled: executionEnabled,
    market_data_latency_ms: priceAgeMs,
    broker_connectivity: brokerConnectivity,
    alert_count: alertCount,
    healthy_providers: healthyProviders,
    fresh_rows: freshRows,
    today_pnl: canonicalPnl && !canonicalPnl.missing ? canonicalPnl.today : null,
    gross_exposure: canonicalExposure && !canonicalExposure.missing ? canonicalExposure.gross : null,
    ui_metrics_degraded: uiMetrics ? !!uiMetrics.degraded : true,
  };

  if (requestTs < _latestSystemStatusTs) {
    return;
  }
  _latestSystemStatusTs = requestTs;
  window.__LAST_SYSTEM_STATUS_HEADER__ = normalized;
  updateDashboardLiveState({
    health,
    systemState,
    executionBarrier,
    uiMetrics,
  }, { sourceTs: requestTs });
  renderSystemStatusHeader(normalized, headerEl);
  renderTopLevelHealthScore();
}

async function loadExecutionBarrier() {
  const pill = document.getElementById("execBarrierPill");
  const raw  = document.getElementById("execBarrierRaw");
  if (!pill || !raw) return;

  const root = document.documentElement;

  try {
    const j = await fetchJSON("/api/execution/barrier", { allowBusinessFalse: true });
    if (!j || typeof j !== "object") throw new Error("barrier unavailable");
    window.__LAST_EXECUTION_BARRIER__ = j;
    syncJobActionSafetyState();
    applyReadOnlyBanner();
    updateDashboardLiveState({ executionBarrier: j }, { sourceTs: extractRealtimeTs(j, Date.now()) });

    pill.className = buildPillClassName(pill, j.allowed ? "ok" : "crit");
    pill.textContent = j.allowed ? "execution: ALLOWED" : "execution: BLOCKED";
    raw.textContent = JSON.stringify(j, null, 2);

    root.style.setProperty("--exec-blocked", j.allowed ? "0" : "1");
    updateDecisionHeader();

  } catch (e) {
    window.__LAST_EXECUTION_BARRIER__ = { ok: false, allowed: false, real_trading_allowed: false };
    syncJobActionSafetyState();
    applyReadOnlyBanner();
    updateDashboardLiveState({ executionBarrier: window.__LAST_EXECUTION_BARRIER__ }, { sourceTs: Date.now() });
    pill.className = buildPillClassName(pill, "crit");
    pill.textContent = "execution: error";
    raw.textContent = e.message || String(e);
    root.style.setProperty("--exec-blocked", "1"); // fail closed
    updateDecisionHeader();
  }
}

function renderTrainingStatusPanel(trainingStatus, error = null) {
  const statusEl = document.getElementById("trainingStatus");
  const detailEl = document.getElementById("trainingDetails");
  if (!statusEl && !detailEl) return;

  if (error) {
    if (statusEl) {
      setStatusTone(statusEl, "bad", "UNAVAILABLE");
    }
    if (detailEl) {
      detailEl.textContent = `Training status route unavailable.\n- ${describeUiError(error)}`;
    }
    return;
  }

  const payload = asObject(trainingStatus);
  const mode = String(payload.mode || "unknown").trim() || "unknown";
  const issues = collectStructuredIssues(payload, 6);

  if (statusEl) {
    setStatusTone(
      statusEl,
      payload.allowed === true ? "ok" : "warn",
      payload.allowed === true ? "ENABLED" : `BLOCKED (${mode})`
    );
  }

  if (detailEl) {
    const lines = [
      `Mode: ${mode}`,
      `Allowed: ${payload.allowed === true ? "yes" : "no"}`,
    ];
    if (payload.ts_ms) {
      lines.push(`Updated: ${fmtTime(payload.ts_ms)}`);
    }
    if (issues.length) {
      lines.push("Blockers:");
      issues.forEach((item) => lines.push(`- ${item}`));
    } else if (payload.allowed !== true) {
      lines.push("Blockers:");
      lines.push("- Training gate did not report an explicit allow state.");
    } else {
      lines.push("No active training blockers reported.");
    }
    detailEl.textContent = lines.join("\n");
  }
}

async function refresh() {
const refreshStartedAt = Date.now();
let systemState = null;
let health = null;
let readiness = null;
let executionBarrier = null;
let stressPayload = null;
let trainingStatus = null;
let trainingError = null;
const sharedFailures = [];

const [healthRes, systemStateRes, readinessRes, barrierRes, stressRes, trainingRes] = await Promise.allSettled([
  fetchJSON("/api/health", { allowBusinessFalse: true }),
  fetchJSON("/api/system/state", { allowBusinessFalse: true }),
  fetchJSON("/api/readiness", { allowBusinessFalse: true }),
  fetchJSON("/api/execution/barrier", { allowBusinessFalse: true }),
  fetchJSON("/api/market_stress"),
  fetchJSON("/api/training_status", { allowBusinessFalse: true }),
]);

if (healthRes.status === "fulfilled") {
  health = healthRes.value;
  const healthTs = extractRealtimeTs(health, refreshStartedAt);
  if (healthTs >= _latestHealthStateTs) {
    _latestHealthStateTs = healthTs;
    _lastHealth = health || null;
    window.__LAST_HEALTH__ = _lastHealth;
  }
} else {
  sharedFailures.push(createFailureItem("health", "/api/health", healthRes.reason));
}

if (systemStateRes.status === "fulfilled") {
  systemState = systemStateRes.value;
  window.__LAST_SYSTEM_STATE__ = systemState;
} else {
  sharedFailures.push(createFailureItem("system_state", "/api/system/state", systemStateRes.reason));
}

if (readinessRes.status === "fulfilled") {
  readiness = readinessRes.value;
  window.__LAST_READINESS__ = readiness;
} else {
  sharedFailures.push(createFailureItem("readiness", "/api/readiness", readinessRes.reason));
}

if (barrierRes.status === "fulfilled") {
  executionBarrier = barrierRes.value;
  window.__LAST_EXECUTION_BARRIER__ = executionBarrier;
} else {
  sharedFailures.push(createFailureItem("execution_barrier", "/api/execution/barrier", barrierRes.reason));
}

if (stressRes.status === "fulfilled") {
  stressPayload = stressRes.value;
  window.__LAST_MARKET_STRESS__ = stressPayload;
}

if (trainingRes.status === "fulfilled") {
  trainingStatus = trainingRes.value;
} else {
  trainingError = trainingRes.reason;
}

window.__LAST_REFRESH_FAILURES__ = sharedFailures;
updateDashboardLiveState({
  health: health || _lastHealth || null,
  readiness,
  systemState,
  executionBarrier,
  stressPayload,
  failures: sharedFailures,
}, {
  sourceTs: refreshStartedAt,
});
renderDashboardSystemStatus(health || _lastHealth || null, systemState || window.__LAST_SYSTEM_STATE__ || null);
renderTrainingStatusPanel(trainingStatus, trainingError);

// Auto-snapshot if CRIT alert detected
if (Array.isArray(_lastAlerts)) {
  const crit = _lastAlerts.find(a => a.severity === "CRIT" && !a.resolved);

  if (crit && !sessionStorage.getItem("auto_snapshot_crit")) {
    sessionStorage.setItem("auto_snapshot_crit", "pending");

    buildSnapshotBundle(getSnapshotBundleState())
      .then(bundle => {
        console.warn("AUTO SNAPSHOT (CRIT)", bundle);
        sessionStorage.setItem("auto_snapshot_crit", "1");
      })
      .catch(() => {
        sessionStorage.removeItem("auto_snapshot_crit");
      });
  }
}

// ✅ PATCH 2 — Reset auto-snapshot flag if CRIT cleared
if (Array.isArray(_lastAlerts)) {
  const stillCrit = _lastAlerts.some(a => a.severity === "CRIT" && !a.resolved);
  if (!stillCrit) {
    sessionStorage.removeItem("auto_snapshot_crit");
  }
}

  if (_pauseRefresh === true) {
    return;
  }

await refreshActiveScreenData({
  systemState,
  health,
  readiness,
  executionBarrier,
  stressPayload,
  sharedFailures,
});

  // If we paused promotions due to exec degradation, try to resume after recovery
await maybeAutoResumePromotionsAfterRecovery({
  operatorMode: OPERATOR_MODE
});

renderTelemetryStripFromDashboard();
renderTopLevelHealthScore();

} // <-- END refresh()

function renderPromotionReasonSummary(statusPayload, error = null) {
  const reasonEl = document.getElementById("promotionReason");
  if (!reasonEl) return;

  if (error) {
    reasonEl.textContent = `Promotion status route unavailable.\n- ${describeUiError(error)}`;
    return;
  }

  const payload = asObject(statusPayload);
  const reason = asObject(payload.reason);
  const blockers = [
    ...asArray(reason.blockers),
    ...collectStructuredIssues(reason, 6),
  ].filter(Boolean);

  const lines = [
    payload.enabled === false
      ? "Promotions are switched off."
      : (payload.allowed === true ? "Promotions are allowed." : "Promotions remain blocked."),
  ];

  if (payload.updated_ts_ms) {
    lines.push(`Updated: ${fmtTime(payload.updated_ts_ms)}`);
  }

  if (blockers.length) {
    lines.push("Gate detail:");
    blockers.slice(0, 6).forEach((item) => lines.push(`- ${item}`));
  } else if (payload.allowed !== true) {
    lines.push("Gate detail:");
    lines.push("- No explicit blocker list was returned by the promotion guard.");
  } else {
    lines.push("No active promotion blockers reported.");
  }

  reasonEl.textContent = lines.join("\n");
}

async function loadPromotionStatus() {
  const pill = document.getElementById("promotionPill");
  const reasonEl = document.getElementById("promotionReason");
  if (!pill || !reasonEl) return;

  try {
    if (_isExecutionDegraded()) {
      // mark that we intentionally paused due to exec degradation
      localStorage.setItem("promo_paused_due_to_exec_v1", "1");
    }

    const j = await fetchJSON("/api/promotion/status");
    if (!j || !j.ok) {
      window.__LAST_PROMOTION_STATUS__ = null;
      pill.textContent = "promotion: unavailable";
      pill.className = buildPillClassName(pill, "neutral");
      renderPromotionReasonSummary(null, j && j.error ? j.error : "promotion status unavailable");
      updateDecisionHeader();
      renderTelemetryStripFromDashboard();
      return;
    }

    window.__LAST_PROMOTION_STATUS__ = j;

    const promotionEnabled = j.enabled === true;
    const promotionAllowed = j.allowed === true;

    if (!promotionEnabled) {
      pill.textContent = "promotion: OFF";
      pill.className = buildPillClassName(pill, "neutral");
    } else if (!promotionAllowed) {
      pill.textContent = "promotion: BLOCKED";
      pill.className = buildPillClassName(pill, "crit");
    } else {
      pill.textContent = "promotion: ALLOWED";
      pill.className = buildPillClassName(pill, "ok");
    }

    renderPromotionReasonSummary(j);
    updateDecisionHeader();
    renderTelemetryStripFromDashboard();
  } catch (e) {
    window.__LAST_PROMOTION_STATUS__ = null;
    pill.textContent = "promotion: unavailable";
    pill.className = buildPillClassName(pill, "neutral");
    renderPromotionReasonSummary(null, e);
    updateDecisionHeader();
    renderTelemetryStripFromDashboard();
  }
}

function wirePromotionButtons() {
  const btnToggle = document.getElementById("btnTogglePromotions");
  const btnRollback = document.getElementById("btnRollbackChampion");

  if (btnToggle && !btnToggle._boundPromotionToggle) {
    btnToggle._boundPromotionToggle = true;
    btnToggle.addEventListener("click", async () => {
      if (hardBlockIfReadOnly({ actionName: "toggle promotions", toastFn: toast })) return;

      // STEP 5: HARD manipulation kill-switch (UI enforcement)
      if (hardBlockActionIfManipulated({
        actionName: "toggle promotions",
        symbol: "GLOBAL",
        expertUnlocked: EXPERT_UNLOCK,
        toastFn: toast
      })) return;

      if (!requireConfirmIfDegraded({
        executionDegraded: _isExecutionDegraded(),
        operatorMode: OPERATOR_MODE,
        message:
          "Execution degradation detected.\n\nToggling promotions during degradation may increase risk.\n\nProceed anyway?"
      })) return;

      btnToggle.disabled = true;
      try {
        await handlePromotionToggle({
          operatorMode: OPERATOR_MODE,
          expertUnlocked: EXPERT_UNLOCK
        });
        await loadPromotionStatus();
      } catch (e) {
        const el = document.getElementById("console");
        if (el) el.textContent += `[ui] toggle promotions ERROR: ${e.message}\n`;
        toast(`Toggle promotions failed: ${e.message}`, "bad", 4000);
      } finally {
        btnToggle.disabled = false;
      }
    });
  }

  if (btnRollback && !btnRollback._boundPromotionRollback) {
    btnRollback._boundPromotionRollback = true;
    btnRollback.addEventListener("click", async () => {
      if (hardBlockIfReadOnly({ actionName: "champion rollback", toastFn: toast })) return;

      // STEP 5: HARD manipulation kill-switch (UI enforcement)
      if (hardBlockActionIfManipulated({
        actionName: "champion rollback",
        symbol: "GLOBAL",
        expertUnlocked: EXPERT_UNLOCK,
        toastFn: toast
      })) return;

      if (!requireConfirmIfDegraded({
        executionDegraded: _isExecutionDegraded(),
        operatorMode: OPERATOR_MODE,
        message:
          "Execution degradation detected.\n\nRollback during degradation may be unsafe.\n\nProceed anyway?"
      })) return;

      try {
        if (!window.__LAST_PROMOTION_GATE__) {
          const explain = await fetchJSON("/api/promotion/explain", { allowBusinessFalse: true });
          renderPromotionGate(explain && explain.gate ? explain.gate : explain);
        }
        const gate = normalizePromotionGatePayload(window.__LAST_PROMOTION_GATE__ || {});
        const rollback = gate.actions.rollback && typeof gate.actions.rollback === "object" ? gate.actions.rollback : {};
        if (!rollback.available) {
          toast("Rollback unavailable: no retired rollback target was returned", "warn", 4000);
          return;
        }

        const previewText = buildRollbackConsequencePreview(gate.raw);
        if (!window.confirm(`${previewText}\n\nContinue to rollback justification?`)) return;

        const justification = window.prompt("Enter rollback justification for the promotion audit log:");
        const validation = validatePromotionActionInput({ justification });
        if (!validation.ok) {
          toast(`Rollback requires a justification of at least ${validation.minLength} characters`, "warn", 4200);
          return;
        }

        const typedConfirm = window.prompt('Type ROLLBACK_CHAMPION to confirm rollback:');
        if (String(typedConfirm || "").trim() !== "ROLLBACK_CHAMPION") {
          toast("Rollback cancelled: confirmation token did not match", "warn", 3200);
          return;
        }

        btnRollback.disabled = true;
        const payload = buildPromotionActionPayload({
          action: "rollback",
          confirm: "ROLLBACK_CHAMPION",
          justification: validation.justification,
          preview: rollback.preview || {},
          source: "dashboard",
        });
        const res = await fetchJSON("/api/champion/rollback", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        if (!res || !res.ok) throw new Error((res && res.error) || "rollback failed");

        const el = document.getElementById("console");
        if (el) el.textContent += `[ui] rollback ok -> ${JSON.stringify(res.champion || {})}\n`;

        toast("Rollback complete", "ok", 3000);
        window.__LAST_PROMOTION_GATE__ = null;
        await loadPromotionGate();
        await loadModelRegistry();
      } catch (e) {
        const el = document.getElementById("console");
        if (el) el.textContent += `[ui] rollback ERROR: ${e.message}\n`;
        toast(`Rollback failed: ${e.message}`, "bad", 4000);
      } finally {
        btnRollback.disabled = false;
      }
    });
  }
}

function wireCollapsibles() {
  document.querySelectorAll(".collapsible h2").forEach(h => {
    if (h._boundCollapsible) return;
    h._boundCollapsible = true;

    h.addEventListener("click", () => {
      const parent = h.parentElement;
      if (parent && parent.classList) {
        parent.classList.toggle("collapsed");
      }
    });
  });
}

// -----------------------------
// Pro Charting (Live Market)
// -----------------------------
function _applyProChartsUI() {
  const st = getProChartsState();

  const card = document.getElementById("proChartsCard");
  if (card) card.style.display = st.enabled ? "block" : "none";

  const meta = document.getElementById("proChartsMeta");
  if (meta) meta.textContent = st.enabled ? "enabled" : "disabled";

  const cb = document.getElementById("proChartsEnable");
  if (cb) cb.checked = !!st.enabled;

  const selTf = document.getElementById("proChartsTf");
  if (selTf) selTf.value = st.tf || "1m";

  const selType = document.getElementById("proChartsType");
  if (selType) selType.value = st.type || "candle";

  applyProChartsVisibility("proChartsCard");
}

/* -----------------------------
   Boot (single authoritative entrypoint)
----------------------------- */

function wireUI() {
  wireDecisionBarClicks();
  wireCollapsibles();
  wireDecisionDrilldownActivation();
  wirePromotionButtons();
  wirePromotionExplainUI();
  wireLogViewerControls();
  wireJobActionButtons();
  wireDashboardTableControls();

  const whyCloseBtn = document.getElementById("btnCloseWhy");
  if (whyCloseBtn && !whyCloseBtn._boundWhyClose) {
    whyCloseBtn._boundWhyClose = true;
    whyCloseBtn.addEventListener("click", closeWhyModal);
  }

  const promoCloseBtn = document.getElementById("btnClosePromoWhy");
  if (promoCloseBtn && !promoCloseBtn._boundPromoWhyClose) {
    promoCloseBtn._boundPromoWhyClose = true;
    promoCloseBtn.addEventListener("click", closePromoWhyModal);
  }

  const decisionCloseBtn = document.getElementById("btnCloseDecisionModal");
  if (decisionCloseBtn && !decisionCloseBtn._boundDecisionClose) {
    decisionCloseBtn._boundDecisionClose = true;
    decisionCloseBtn.addEventListener("click", closeDecisionModal);
  }

  const decisionTerminalBtn = document.getElementById("btnDecisionOpenTerminal");
  if (decisionTerminalBtn && !decisionTerminalBtn._boundDecisionTerminal) {
    decisionTerminalBtn._boundDecisionTerminal = true;
    decisionTerminalBtn.addEventListener("click", openDecisionContextInTerminal);
  }

  const decisionOperatorBtn = document.getElementById("btnDecisionOpenOperator");
  if (decisionOperatorBtn && !decisionOperatorBtn._boundDecisionOperator) {
    decisionOperatorBtn._boundDecisionOperator = true;
    decisionOperatorBtn.addEventListener("click", openDecisionContextInOperator);
  }

  const decisionModal = document.getElementById("decisionModal");
  if (decisionModal && !decisionModal._boundDecisionBackdrop) {
    decisionModal._boundDecisionBackdrop = true;
    decisionModal.addEventListener("click", (e) => {
      if (e && e.target === decisionModal) {
        closeDecisionModal();
      }
    });
  }

  const executionAdvisoryCloseBtn = document.getElementById("btnCloseExecutionAdvisoryModal");
  if (executionAdvisoryCloseBtn && !executionAdvisoryCloseBtn._boundExecAdvClose) {
    executionAdvisoryCloseBtn._boundExecAdvClose = true;
    executionAdvisoryCloseBtn.addEventListener("click", closeExecutionAdvisoryModal);
  }

  const advisoryTerminalBtn = document.getElementById("btnAdvisoryOpenTerminal");
  if (advisoryTerminalBtn && !advisoryTerminalBtn._boundAdvisoryTerminal) {
    advisoryTerminalBtn._boundAdvisoryTerminal = true;
    advisoryTerminalBtn.addEventListener("click", openAdvisoryContextInTerminal);
  }

  const advisoryOperatorBtn = document.getElementById("btnAdvisoryOpenOperator");
  if (advisoryOperatorBtn && !advisoryOperatorBtn._boundAdvisoryOperator) {
    advisoryOperatorBtn._boundAdvisoryOperator = true;
    advisoryOperatorBtn.addEventListener("click", openAdvisoryContextInOperator);
  }

  const executionAdvisoryModal = document.getElementById("executionAdvisoryModal");
  if (executionAdvisoryModal && !executionAdvisoryModal._boundExecAdvBackdrop) {
    executionAdvisoryModal._boundExecAdvBackdrop = true;
    executionAdvisoryModal.addEventListener("click", (e) => {
      if (e && e.target === executionAdvisoryModal) {
        closeExecutionAdvisoryModal();
      }
    });
  }

  const drawerClose = document.getElementById("btnCloseIncident");
  if (drawerClose && !drawerClose._boundIncidentClose) {
    drawerClose._boundIncidentClose = true;
    drawerClose.addEventListener("click", closeIncidentDrawer);
  }

  const notificationTableBody = document.getElementById("notificationStatusTableBody");
  if (notificationTableBody && !notificationTableBody._boundNotificationTest) {
    notificationTableBody._boundNotificationTest = true;
    notificationTableBody.addEventListener("click", (event) => {
      const button = event && event.target && typeof event.target.closest === "function"
        ? event.target.closest("button[data-notification-test]")
        : null;
      if (!button) return;
      event.preventDefault();
      const channel = String(button.getAttribute("data-notification-test") || "").trim().toLowerCase();
      void sendNotificationTest(channel);
    });
  }

  // -----------------------------
  // Size policy training button
  // -----------------------------
  const btnSP = document.getElementById("btnTrainSizePolicy");
  if (btnSP && !btnSP._boundTrainSizePolicy) {
    btnSP._boundTrainSizePolicy = true;
    btnSP.addEventListener("click", async () => {
      if (hardBlockIfReadOnly({ actionName: "train size policy", toastFn: toast })) return;

      btnSP.disabled = true;
      try {
        const el = document.getElementById("console");
        if (el) el.textContent += "[ui] training size policy...\n";

        const res = await fetchJSON("/api/strategy/size_policy/train", {
          method: "POST",
          body: JSON.stringify({ confirm: "TRAIN_SIZE_POLICY" }),
        });
        if (!res || !res.ok) {
          throw new Error((res && res.error) || "train_size_policy failed");
        }

        if (el) el.textContent += "[ui] size policy train job started\n";
        setTimeout(loadSizePolicyUI, 1000);
      } catch (e) {
        const el = document.getElementById("console");
        if (el) el.textContent += `[ui] size policy ERROR: ${e.message}\n`;
      } finally {
        btnSP.disabled = false;
      }
    });
  }

  // -----------------------------
// Global Filter Wiring
// -----------------------------
const rangeEl = document.getElementById("globalRange");
const sevEl = document.getElementById("globalSev");
const symEl = document.getElementById("globalSymbol");
const changedEl = document.getElementById("globalChangedOnly");

const onFilterChange = () => {
  if (typeof _debouncedRender === "function") {
    _debouncedRender();
  } else {
    loadAlerts();
  }
};

[rangeEl, sevEl, changedEl].forEach(el => {
  if (el && !el._boundFilter) {
    el._boundFilter = true;
    el.addEventListener("change", onFilterChange);
  }
});

if (symEl && !symEl._boundSymbolContext) {
  symEl._boundSymbolContext = true;
  const onSymbolFilterChange = () => {
    updateSymbolContextFromInput("global_filter");
    onFilterChange();
  };
  symEl.addEventListener("change", onSymbolFilterChange);
  symEl.addEventListener("keydown", (event) => {
    if (event && event.key === "Enter") {
      onSymbolFilterChange();
    }
  });
}

  // -----------------------------
  // Operator summary quick actions
  // -----------------------------
  const btnOpRefresh = document.getElementById("btnOpRefresh");
  if (btnOpRefresh && !btnOpRefresh._boundOpRefresh) {
    btnOpRefresh._boundOpRefresh = true;
    btnOpRefresh.addEventListener("click", async () => {
      try {
        const b = document.getElementById("btnRefresh");
        if (b) b.click();
        else await refresh();
      } catch {}
    });
  }

  const btnOpFix = document.getElementById("btnOpFixIssues");
  if (btnOpFix && !btnOpFix._boundOpFix) {
    btnOpFix._boundOpFix = true;
    btnOpFix.addEventListener("click", async () => {
      try {
        const b = document.getElementById("btnFixIssues");
        if (b) b.click();
        else if (typeof handleAutoFix === "function") await handleAutoFix({ toastFn: toast });
      } catch {}
    });
  }

  const btnOpAlerts = document.getElementById("btnOpJumpAlerts");
  if (btnOpAlerts && !btnOpAlerts._boundOpAlerts) {
    btnOpAlerts._boundOpAlerts = true;
    btnOpAlerts.addEventListener("click", () => {
      applyDashboardScreen("overview");
      const el = document.getElementById("incidentList") || document.getElementById("alertsHeatmap") || document.getElementById("alerts");
      if (el && typeof el.scrollIntoView === "function") {
        el.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  }

  const btnOpJobs = document.getElementById("btnOpJumpJobs");
  if (btnOpJobs && !btnOpJobs._boundOpJobs) {
    btnOpJobs._boundOpJobs = true;
    btnOpJobs.addEventListener("click", () => {
      applyDashboardScreen("operate");
      const el = document.getElementById("console") || document.getElementById("selectedJob");
      if (el && typeof el.scrollIntoView === "function") {
        el.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  }
  // -----------------------------
  // Unified Snapshot Button
  // -----------------------------
const btnCopySnapshot = document.getElementById("btnCopySnapshot");
if (btnCopySnapshot && !btnCopySnapshot._boundCopySnapshot) {
  btnCopySnapshot._boundCopySnapshot = true;
  btnCopySnapshot.addEventListener("click", async () => {
    await copySnapshotBundle(getSnapshotBundleState());
  });
}

// -----------------------------
// Pro Charts controls
// -----------------------------
const cbPro = document.getElementById("proChartsEnable");
if (cbPro && !cbPro._bound) {
  cbPro._bound = true;
  cbPro.addEventListener("change", async () => {
    setProChartsState({ enabled: !!cbPro.checked });
    await _refreshProCharts();
  });
}

const selTf = document.getElementById("proChartsTf");
if (selTf && !selTf._bound) {
  selTf._bound = true;
  selTf.addEventListener("change", async () => {
    setProChartsState({ tf: String(selTf.value || "1m") });
    await _refreshProCharts();
  });
}

const selType = document.getElementById("proChartsType");
if (selType && !selType._bound) {
  selType._bound = true;
  selType.addEventListener("change", async () => {
    setProChartsState({ type: String(selType.value || "candle") });
    await _refreshProCharts();
  });
}

const proSymEl = document.getElementById("globalSymbol");
if (proSymEl && !proSymEl._proChartBound) {
  proSymEl._proChartBound = true;
  proSymEl.addEventListener("change", async () => {
    await _refreshProCharts();
  });
}
}

function bootDashboard() {
  const bootStage = document.getElementById("bootStage");
  if (typeof _initDebouncedRender === "function") {
    _initDebouncedRender();
  }
  const setStage = (s) => { if (bootStage) bootStage.textContent = s; };

  setStage("BOOTING");

  applyDashboardLaunchParams();
  bindProChartSymbolWatcher();
  try {
    if (localStorage.getItem("proCharts.enabled") === null) {
      setProChartsState({ enabled: true });
    }
  } catch {}
  _applyProChartsUI();
  void _refreshProCharts();
  initReplayPanel({
    fetchJSON,
    root: document,
    getSymbol: () => _getActiveSymbol("SPY"),
  });

  if (typeof wireUI === "function") {
wireUI();
wireDashboardSymbolContext();
setStage("UI WIRED");

  }

  initMetricTooltips({ root: document });
  renderTopLevelHealthScore();
  renderRecommendedActionCard();

  setStage("VOICE READY");
  wireDashboardPersonaControls({
    root: document,
    onChange: () => {
      const persona = getActiveDashboardPersona();
      if (!isDashboardScreenAllowed(persona, ACTIVE_DASHBOARD_SCREEN)) {
        applyDashboardScreen(getDefaultDashboardScreen(persona), {
          syncHash: true,
          hashMode: "replace",
        });
        return;
      }
      applyDashboardPersonaView({
        root: document,
        screen: ACTIVE_DASHBOARD_SCREEN,
      });
    },
  });
  wireDashboardScreens();
  initCommandPalette({
    document,
    fetchJSON,
    screenLabels: DASHBOARD_SCREEN_LABELS,
    getActiveScreen: () => ACTIVE_DASHBOARD_SCREEN,
    navigateToScreen: commandPaletteNavigateToScreen,
    navigateToPanel: commandPaletteNavigateToPanel,
    focusSymbol: commandPaletteFocusSymbol,
    focusModel: commandPaletteFocusModel,
    openDecision: (decisionId) => openDecisionModal(decisionId),
    selectJob: commandPaletteSelectJob,
    runJobAction: jobAction,
    toast,
  });
  DASHBOARD_BOOTED = true;

  const globalSymbolInput = document.getElementById("globalSymbol");
  if (globalSymbolInput && !globalSymbolInput._boundSurfaceLinks) {
    globalSymbolInput._boundSurfaceLinks = true;
    globalSymbolInput.addEventListener("input", () => updateSurfaceLinks());
    globalSymbolInput.addEventListener("change", () => updateSurfaceLinks());
  }

  applyReadOnlyBanner();
  setStage("POLICY APPLIED");

  refresh().then(async () => {
    try {
      const st = await fetchJSON("/api/system/state", { allowBusinessFalse: true });
      setStage(st && st.state ? st.state : "UNKNOWN");
    } catch {
      setStage("ERROR");
    }

    try {
      await _refreshProCharts();
    } catch {}
    updateProChartsPanelState();

    if (_launchContext && _launchContext.decisionId) {
      const decisionId = _launchContext.decisionId;
      _launchContext.decisionId = "";
      try {
        await openDecisionModal(decisionId);
      } catch {}
    }
  });

startDashboardRefreshScheduler({
  refresh,
  loadAlerts,
  loadSystemStatusHeader: () => loadSystemStatusHeader({
    health: _lastHealth,
    systemState: window.__LAST_SYSTEM_STATE__ || null,
    executionBarrier: window.__LAST_EXECUTION_BARRIER__ || null,
  }),
  pauseFlagGetter: () => _pauseRefresh,
  getRealtimeState: getRealtimeSchedulerState,
  scheduleRefreshTasks
});

startOperatorRealtime();

}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootDashboard);
} else {
  bootDashboard();
}

function renderDockDetail(strategy) {
  const dock = document.getElementById("right-dock");
  if (!dock || !strategy) return;

  // Escape everything because this is innerHTML
  const safe = (v) => escapeHTML(v === null || v === undefined ? "" : String(v));

  dock.innerHTML = `
    <h3>${safe(strategy.name)}</h3>
    <div>Return: ${safe(strategy.return)}</div>
    <div>Drawdown: ${safe(strategy.drawdown)}</div>
    <div>Decay Sharpe: ${safe(strategy.decay_sharpe)}</div>
    <div>Slippage: ${safe(strategy.slippage_pct)}%</div>
    <div>Capital Efficiency: ${safe(strategy.cap_eff)}</div>
    <div>Regime Fit: ${safe(strategy.regime_fit)}%</div>
    <div>Promotion Streak: ${safe(strategy.promotion_streak)}</div>
  `;
}

function renderEventLog(events) {
  const log = document.getElementById("bottom-log");
  if (!log || !Array.isArray(events)) return;
  log.innerHTML = "";

  events.slice(-50).reverse().forEach(e => {
    const row = document.createElement("div");
    row.innerText = `${new Date(e.ts).toLocaleTimeString()} | ${e.type} | ${e.message}`;
    log.appendChild(row);
  });
}
