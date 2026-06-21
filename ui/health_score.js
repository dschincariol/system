/*
  FILE: ui/health_score.js

  Read-only top-level health score helpers for the dashboard.

  Scoring formula:
  - The score uses four factor groups with fixed weights that sum to 100:
      alerts    = 25
      runtime   = 30
      data      = 25
      execution = 20
  - Each factor is classified with existing repo signals and thresholds already
    surfaced elsewhere in the UI. We do not invent extra backend inputs.
  - Classification maps to factor credit:
      normal   = 100% of factor weight
      warning  = 60% of factor weight
      critical = 20% of factor weight
  - Missing factors contribute neither credit nor penalty.
  - Overall score = round(100 * earned_points / available_weight)

  This keeps the summary deterministic, transparent, and fail-soft when some
  upstream reads are unavailable. Coverage is surfaced separately so a high
  score from one or two available factors cannot be mistaken for full support.
*/

import { severityRank } from "./alerts.js";
import { classifyMetricValue } from "./metric_glossary.js";
import { unwrapHealthResponse } from "./runtime_status_summary.js";
import { statusAriaLabel, statusClassName, statusPillClasses, statusToken } from "./utils.js";

const FACTOR_WEIGHTS = Object.freeze({
  alerts: 25,
  runtime: 30,
  data: 25,
  execution: 20,
});

const FACTOR_ORDER = Object.freeze(["alerts", "runtime", "data", "execution"]);

const CLASSIFICATION_CREDIT = Object.freeze({
  normal: 1,
  warning: 0.6,
  critical: 0.2,
});

const LOW_COVERAGE_RATIO = 0.5;

function asFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function asBoolean(value) {
  if (typeof value === "boolean") return value;
  if (value === 1 || value === "1" || value === "true" || value === "TRUE") return true;
  if (value === 0 || value === "0" || value === "false" || value === "FALSE") return false;
  return null;
}

function asUpperToken(value) {
  const s = String(value == null ? "" : value).trim();
  return s ? s.toUpperCase() : "";
}

function uniqueParts(parts) {
  const out = [];
  const seen = new Set();
  for (const part of parts || []) {
    const text = String(part || "").trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    out.push(text);
  }
  return out;
}

function classificationCredit(classification) {
  return Object.prototype.hasOwnProperty.call(CLASSIFICATION_CREDIT, classification)
    ? CLASSIFICATION_CREDIT[classification]
    : null;
}

function worstClassification(values) {
  const classes = (values || []).filter(Boolean);
  if (classes.includes("critical")) return "critical";
  if (classes.includes("warning")) return "warning";
  if (classes.includes("normal")) return "normal";
  return "unknown";
}

function factorClassName(classification) {
  if (classification === "normal") return statusClassName("ok");
  if (classification === "warning") return statusClassName("warn");
  if (classification === "critical") return statusClassName("crit");
  return statusClassName("unavailable");
}

function makeUnavailableFactor(key, label, weight, detail = "Waiting for live data.") {
  return {
    key,
    label,
    weight,
    available: false,
    classification: "unknown",
    className: "unavailable",
    status: "unavailable",
    detail,
    points: 0,
  };
}

function makeFactor(key, label, weight, classification, status, detail) {
  const credit = classificationCredit(classification);
  if (credit === null) {
    return makeUnavailableFactor(key, label, weight, detail);
  }
  return {
    key,
    label,
    weight,
    available: true,
    classification,
    className: factorClassName(classification),
    status,
    detail,
    points: weight * credit,
  };
}

function normalizeHealth(healthPayload) {
  const health = unwrapHealthResponse(healthPayload);
  return health && typeof health === "object" ? health : {};
}

function normalizeReadiness(readinessPayload) {
  if (readinessPayload && readinessPayload.readiness && typeof readinessPayload.readiness === "object") {
    return readinessPayload.readiness;
  }
  if (readinessPayload && typeof readinessPayload === "object") {
    return readinessPayload;
  }
  return {};
}

function summarizeAlerts(alerts, systemStatus = {}) {
  const weight = FACTOR_WEIGHTS.alerts;

  if (!Array.isArray(alerts)) {
    const alertCount = asFiniteNumber(systemStatus && systemStatus.alert_count);
    if (alertCount === null) {
      return makeUnavailableFactor("alerts", "Alerts", weight);
    }

    const classification = classifyMetricValue("alert_count", alertCount);
    const status = classification === "critical"
      ? "heavy queue"
      : classification === "warning"
        ? "active queue"
        : "quiet";
    return makeFactor(
      "alerts",
      "Alerts",
      weight,
      classification,
      status,
      `${Math.max(0, Math.round(alertCount))} active`
    );
  }

  const active = alerts.filter((row) => row && row.resolved !== true);
  let critCount = 0;
  let warnCount = 0;

  for (const row of active) {
    const rank = severityRank(row && row.severity);
    if (rank >= 3) critCount += 1;
    else if (rank >= 2) warnCount += 1;
  }

  let classification = "normal";
  let status = "quiet";
  if (critCount > 0) {
    classification = "critical";
    status = "critical active";
  } else if (warnCount > 0 || active.length > 0) {
    classification = "warning";
    status = warnCount > 0 ? "warnings active" : "attention needed";
  }

  const detail = uniqueParts([
    `${active.length} active`,
    `${critCount} critical`,
    `${warnCount} warning`,
  ]).join(" · ");

  return makeFactor("alerts", "Alerts", weight, classification, status, detail);
}

function summarizeRuntime(systemState, healthPayload, readinessPayload) {
  const weight = FACTOR_WEIGHTS.runtime;
  const health = normalizeHealth(healthPayload);
  const readiness = normalizeReadiness(readinessPayload);
  const classes = [];
  const details = [];

  const rawSystemStateToken = asUpperToken(systemState && systemState.state);
  const systemStateToken = rawSystemStateToken === "KILL_SWITCH" ? "HALTED" : rawSystemStateToken;
  if (systemStateToken) {
    const cls = classifyMetricValue("system_status", systemStateToken);
    if (cls !== "unknown") classes.push(cls);
    details.push(`system ${systemStateToken.toLowerCase()}`);
  }

  const ready = asBoolean(readiness.ready);
  if (ready !== null) {
    classes.push(classifyMetricValue("startup_ready", ready));
    details.push(`readiness ${ready ? "ready" : "not ready"}`);
  } else {
    const readinessOk = asBoolean(readiness.ok);
    if (readinessOk !== null) {
      classes.push(classifyMetricValue("startup_ready", readinessOk));
      details.push(`readiness ${readinessOk ? "ok" : "not ready"}`);
    }
  }

  const healthOk = asBoolean(health.ok);
  if (healthOk !== null) {
    classes.push(healthOk ? "normal" : "warning");
    details.push(`health ${healthOk ? "ok" : "degraded"}`);
  }

  const classification = worstClassification(classes);
  if (classification === "unknown") {
    return makeUnavailableFactor("runtime", "Runtime", weight);
  }

  const status = classification === "critical"
    ? "blocked"
    : classification === "warning"
      ? "degraded"
      : "healthy";

  return makeFactor("runtime", "Runtime", weight, classification, status, uniqueParts(details).join(" · "));
}

function summarizeData(systemStatus = {}, healthPayload) {
  const weight = FACTOR_WEIGHTS.data;
  const health = normalizeHealth(healthPayload);
  const classes = [];
  const details = [];

  const dataStatus = asUpperToken(systemStatus && systemStatus.data_status);
  if (dataStatus) {
    const cls = classifyMetricValue("data_status", dataStatus);
    if (cls !== "unknown") classes.push(cls);
    details.push(`flow ${dataStatus.toLowerCase()}`);
  }

  const latencyMs = asFiniteNumber(systemStatus && systemStatus.market_data_latency_ms);
  if (latencyMs !== null) {
    const cls = classifyMetricValue("market_data_latency_ms", latencyMs);
    if (cls !== "unknown") classes.push(cls);
    details.push(`${Math.round(latencyMs)} ms`);
  }

  const healthyProviders = asFiniteNumber(systemStatus && systemStatus.healthy_providers);
  if (healthyProviders !== null) {
    const cls = classifyMetricValue("healthy_providers", healthyProviders);
    if (cls !== "unknown") classes.push(cls);
    details.push(`${Math.max(0, Math.round(healthyProviders))} provider${Math.round(healthyProviders) === 1 ? "" : "s"}`);
  }

  const pricesOk = asBoolean(health && health.prices && health.prices.ok);
  if (pricesOk !== null) {
    classes.push(classifyMetricValue("prices_ok", pricesOk));
    details.push(pricesOk ? "prices current" : "prices stale");
  }

  const classification = worstClassification(classes);
  if (classification === "unknown") {
    return makeUnavailableFactor("data", "Data", weight);
  }

  const status = classification === "critical"
    ? "stale"
    : classification === "warning"
      ? "watch"
      : "current";

  return makeFactor("data", "Data", weight, classification, status, uniqueParts(details).join(" · "));
}

function summarizeExecution(executionBarrier, executionDegraded, systemStatus = {}) {
  const weight = FACTOR_WEIGHTS.execution;
  const barrierAllowed = asBoolean(executionBarrier && executionBarrier.allowed);
  const executionEnabled = asBoolean(systemStatus && systemStatus.execution_enabled);
  const degraded = executionDegraded === true;

  let token = "";
  if (barrierAllowed === false || executionEnabled === false) {
    token = "BLOCKED";
  } else if (degraded) {
    token = "DEGRADED";
  } else if (barrierAllowed === true || executionEnabled === true) {
    token = "ALLOWED";
  }

  if (!token) {
    return makeUnavailableFactor("execution", "Execution", weight);
  }

  const classification = classifyMetricValue("execution_status", token);
  const mode = asUpperToken(executionBarrier && executionBarrier.mode);
  const reason = String(executionBarrier && executionBarrier.reason || "").trim();
  const detail = uniqueParts([
    `gate ${token.toLowerCase()}`,
    mode ? `mode ${mode.toLowerCase()}` : "",
    degraded && token !== "BLOCKED" ? "execution degradation active" : "",
    reason,
  ]).join(" · ");

  const status = token === "BLOCKED"
    ? "blocked"
    : token === "DEGRADED"
      ? "degraded"
      : "available";

  return makeFactor("execution", "Execution", weight, classification, status, detail);
}

function coverageState(availableCount, totalCount) {
  const total = Math.max(0, Number(totalCount) || 0);
  const available = Math.max(0, Number(availableCount) || 0);
  const ratio = total > 0 ? available / total : 0;

  if (available === 0 || total === 0) {
    return {
      ratio: 0,
      level: "none",
      className: "unavailable",
      label: "no coverage",
      partial: true,
      low: true,
    };
  }
  if (available < total && ratio <= LOW_COVERAGE_RATIO) {
    return {
      ratio,
      level: "low",
      className: "high",
      label: "low coverage",
      partial: true,
      low: true,
    };
  }
  if (available < total) {
    return {
      ratio,
      level: "partial",
      className: "warn",
      label: "partial coverage",
      partial: true,
      low: false,
    };
  }
  return {
    ratio: 1,
    level: "full",
    className: "ok",
    label: "full coverage",
    partial: false,
    low: false,
  };
}

function overallBadge(worst, coverage) {
  const state = coverage || coverageState(0, FACTOR_ORDER.length);
  if (state.level === "none") return { className: "unavailable", label: "waiting" };
  if (worst === "critical") return { className: "crit", label: "degraded" };
  if (worst === "warning") return { className: "warn", label: "watch" };
  if (state.partial) return { className: state.className, label: state.label };
  return { className: "ok", label: "stable" };
}

function overallSummary(score, worst, availableCount, totalCount, coverage) {
  if (availableCount === 0 || score === null) {
    return "Waiting for enough live health inputs to compute the summary.";
  }
  if (worst === "critical") {
    return "One or more core health factors are failing or blocked. Use the detailed panels below as the source of truth.";
  }
  if (worst === "warning") {
    return "Some core health factors are degraded, but the dashboard still has partial visibility into the system.";
  }
  if (coverage && coverage.low) {
    return `Only ${availableCount}/${totalCount} health factors are available. The score uses available factors only and needs more inputs before it can represent full system health.`;
  }
  if (coverage && coverage.partial) {
    return `Score is based on ${availableCount}/${totalCount} available health factors. Missing factors are not counted as failures, but coverage is not complete.`;
  }
  if (availableCount < totalCount) {
    return "Available health factors look stable. Remaining factors will appear as their inputs arrive.";
  }
  return "Core health factors are currently stable across alerts, runtime, data, and execution.";
}

export function computeHealthScore({
  alerts = null,
  health = null,
  readiness = null,
  systemState = null,
  systemStatus = null,
  executionBarrier = null,
  executionDegraded = false,
} = {}) {
  const statusHeader = systemStatus && typeof systemStatus === "object" ? systemStatus : {};
  const factors = {
    alerts: summarizeAlerts(alerts, statusHeader),
    runtime: summarizeRuntime(systemState, health, readiness),
    data: summarizeData(statusHeader, health),
    execution: summarizeExecution(executionBarrier, executionDegraded, statusHeader),
  };

  const orderedFactors = FACTOR_ORDER.map((key) => factors[key]);
  const availableFactors = orderedFactors.filter((factor) => factor && factor.available);
  const availableWeight = availableFactors.reduce((sum, factor) => sum + factor.weight, 0);
  const earnedPoints = availableFactors.reduce((sum, factor) => sum + factor.points, 0);
  const score = availableWeight > 0 ? Math.round((earnedPoints / availableWeight) * 100) : null;
  const worst = worstClassification(availableFactors.map((factor) => factor.classification));
  const coverage = coverageState(availableFactors.length, orderedFactors.length);
  const badge = overallBadge(worst, coverage);

  return {
    score,
    factors: orderedFactors,
    availableWeight,
    earnedPoints,
    availableCount: availableFactors.length,
    totalCount: orderedFactors.length,
    coverageRatio: coverage.ratio,
    coverageText: `${availableFactors.length}/${orderedFactors.length} factors`,
    coverageLevel: coverage.level,
    coverageClassName: coverage.className,
    coverageLabel: coverage.label,
    partialCoverage: coverage.partial,
    lowCoverage: coverage.low,
    badgeClassName: badge.className,
    badgeLabel: badge.label,
    summary: overallSummary(score, worst, availableFactors.length, orderedFactors.length, coverage),
  };
}

function updateHealthScoreCoverageClass(cardEl, coverageLevel) {
  const classes = [
    "is-health-coverage-none",
    "is-health-coverage-low",
    "is-health-coverage-partial",
    "is-health-coverage-full",
  ];
  const level = ["none", "low", "partial", "full"].includes(coverageLevel) ? coverageLevel : "none";

  if (cardEl.classList && typeof cardEl.classList.remove === "function") {
    cardEl.classList.remove(...classes);
    cardEl.classList.add(`is-health-coverage-${level}`);
  } else {
    const existing = String(cardEl.className || "")
      .split(/\s+/)
      .filter((part) => part && !classes.includes(part));
    existing.push(`is-health-coverage-${level}`);
    cardEl.className = existing.join(" ");
  }
  if (cardEl.dataset) cardEl.dataset.coverage = level;
}

function buildFactorNode(factor) {
  const item = document.createElement("div");
  const token = statusToken(factor.className || "unavailable");
  item.className = `healthScoreFactor ${token.className}`;
  item.dataset.status = token.key;
  item.setAttribute("aria-label", statusAriaLabel(token.key, `${factor.label}: ${factor.status || "unavailable"}`));

  const name = document.createElement("div");
  name.className = "healthScoreFactorName";
  name.textContent = factor.label;
  item.appendChild(name);

  const status = document.createElement("div");
  status.className = "healthScoreFactorState";
  status.textContent = factor.status || "unavailable";
  item.appendChild(status);

  const detail = document.createElement("div");
  detail.className = "healthScoreFactorDetail";
  detail.textContent = factor.detail || "Waiting for live data.";
  item.appendChild(detail);

  return item;
}

export function renderHealthScoreSummary(
  scorecard,
  {
    cardEl = document.getElementById("healthScoreBar"),
    valueEl = document.getElementById("healthScoreValue"),
    badgeEl = document.getElementById("healthScoreBadge"),
    coverageEl = document.getElementById("healthScoreCoverage"),
    summaryEl = document.getElementById("healthScoreSummary"),
    factorsEl = document.getElementById("healthScoreFactors"),
  } = {}
) {
  if (!cardEl || !valueEl || !badgeEl || !coverageEl || !summaryEl || !factorsEl) return;

  const safe = scorecard || computeHealthScore({});
  updateHealthScoreCoverageClass(cardEl, safe.coverageLevel || "none");
  valueEl.textContent = safe.score === null ? "—" : String(safe.score);
  const badgeToken = statusToken(safe.badgeClassName || "unavailable");
  badgeEl.className = statusPillClasses(badgeToken.key);
  badgeEl.dataset.status = badgeToken.key;
  badgeEl.setAttribute("aria-label", statusAriaLabel(badgeToken.key, safe.badgeLabel || "waiting"));
  badgeEl.textContent = safe.badgeLabel || "waiting";
  const coverageToken = statusToken(safe.coverageClassName || "unavailable");
  coverageEl.className = `healthScoreCoverage ${statusPillClasses(coverageToken.key)} mono`;
  if (coverageEl.dataset) {
    coverageEl.dataset.status = coverageToken.key;
    coverageEl.dataset.coverage = safe.coverageLevel || "none";
  }
  coverageEl.setAttribute(
    "aria-label",
    statusAriaLabel(coverageToken.key, `${safe.coverageLabel || "no coverage"}: ${safe.coverageText || "0/4 factors"}`)
  );
  coverageEl.title = safe.partialCoverage
    ? "Health score is normalized over available factors only; missing factors are shown as coverage."
    : "All health score factors are currently available.";
  coverageEl.textContent = safe.coverageText || "0/4 factors";
  summaryEl.textContent = safe.summary || "Waiting for enough live health inputs to compute the summary.";
  factorsEl.replaceChildren(...(safe.factors || []).map(buildFactorNode));
}
