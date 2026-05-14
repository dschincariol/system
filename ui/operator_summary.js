/*
  FILE: ui/operator_summary.js

  Operator-summary card helpers for the main dashboard. This module condenses
  system-state, market-stress, and health signals into high-level pills and
  labels for the top summary surface.
*/

import { summarizeRuntimeStatus } from "./runtime_status_summary.js";
import { setMetricValueAttribute } from "./tooltip.js";

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function pickTimestamp(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function formatAgeMs(ageMs) {
  const n = Number(ageMs);
  if (!Number.isFinite(n) || n < 0) return "—";
  if (n < 1000) return `${Math.round(n)}ms`;
  if (n < 60_000) return `${Math.round(n / 1000)}s`;
  if (n < 3_600_000) return `${Math.round(n / 60_000)}m`;
  return `${(n / 3_600_000).toFixed(n < 36_000_000 ? 1 : 0)}h`;
}

function normalizeFailures(items) {
  return asArray(items)
    .map((item) => {
      if (!item) return null;
      if (typeof item === "string") {
        return { label: "route", message: item };
      }
      return {
        label: String(item.label || item.key || "route").trim(),
        message: String(item.message || item.error || "").trim() || "request_failed",
      };
    })
    .filter(Boolean);
}

function collectIssues(payload, limit = 6) {
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

  const visit = (value) => {
    const source = asObject(value);
    asArray(source.issues).forEach((item) => {
      if (item && typeof item === "object") push(item.message || item.detail || item.code);
      else push(item);
    });
    asArray(source.reasons).forEach((item) => {
      if (item && typeof item === "object") push(item.message || item.detail || item.code);
      else push(item);
    });
    asArray(source.waiting_on).forEach((item) => push(item, "waiting on "));
  };

  visit(payload);
  visit(asObject(asObject(payload).readiness));
  return out;
}

function _pillSet(id, cls, text, metricValue = null) {
  const el = document.getElementById(id);
  if (!el) return;
  el.className = "pill " + (cls || "dim");
  el.textContent = text;
  setMetricValueAttribute(el, metricValue);
}

export async function loadOperatorSummary(fetchJSON, preloaded = {}) {

  const card = document.getElementById("operatorSummaryCard");
  if (!card) return;

  const hasOwn = (key) => Object.prototype.hasOwnProperty.call(preloaded || {}, key);
  let system = hasOwn("systemState") ? preloaded.systemState : (window.__LAST_SYSTEM_STATE__ || null);
  let stress = hasOwn("stressPayload") ? preloaded.stressPayload : (window.__LAST_MARKET_STRESS__ || null);
  let barrier = hasOwn("executionBarrier") ? preloaded.executionBarrier : (window.__LAST_EXECUTION_BARRIER__ || null);
  let health = hasOwn("health") ? preloaded.health : (window.__LAST_HEALTH__ || null);
  let readiness = hasOwn("readiness") ? preloaded.readiness : (window.__LAST_READINESS__ || null);
  const failures = normalizeFailures(
    hasOwn("sharedFailures") ? preloaded.sharedFailures : (window.__LAST_REFRESH_FAILURES__ || [])
  );

  if (!hasOwn("systemState") && !system) {
    try {
      system = await fetchJSON("/api/system/state", { allowBusinessFalse: true });
    } catch (error) {
      failures.push({ label: "/api/system/state", message: String(error && error.message ? error.message : error) });
    }
  }

  if (!hasOwn("stressPayload") && !stress) {
    try { stress = await fetchJSON("/api/market_stress"); } catch {}
  }
  if (!hasOwn("executionBarrier") && !barrier) {
    try { barrier = await fetchJSON("/api/execution/barrier", { allowBusinessFalse: true }); } catch (error) {
      failures.push({ label: "/api/execution/barrier", message: String(error && error.message ? error.message : error) });
    }
  }
  if (!hasOwn("health") && !health) {
    try { health = await fetchJSON("/api/health", { allowBusinessFalse: true }); } catch (error) {
      failures.push({ label: "/api/health", message: String(error && error.message ? error.message : error) });
    }
  }
  if (!hasOwn("readiness") && !readiness) {
    try { readiness = await fetchJSON("/api/readiness", { allowBusinessFalse: true }); } catch (error) {
      failures.push({ label: "/api/readiness", message: String(error && error.message ? error.message : error) });
    }
  }

  if (system) window.__LAST_SYSTEM_STATE__ = system;
  if (stress) window.__LAST_MARKET_STRESS__ = stress;
  if (barrier) window.__LAST_EXECUTION_BARRIER__ = barrier;
  if (health) window.__LAST_HEALTH__ = health;
  if (readiness) window.__LAST_READINESS__ = readiness;

  const summary = summarizeRuntimeStatus({
    systemState: system,
    stressPayload: stress,
    barrierPayload: barrier,
    healthPayload: health,
    readinessPayload: readiness,
  });

  _pillSet("opSystemPill", summary.pills.system.cls, "System: " + summary.pills.system.label, summary.pills.system.label);
  _pillSet("opExecPill", summary.pills.trading.cls, "Trading: " + summary.pills.trading.label, summary.pills.trading.label);
  _pillSet("opTrainPill", summary.pills.training.cls, "Training: " + summary.pills.training.label, summary.pills.training.label);
  _pillSet("opStressPill", stress ? summary.pills.market.cls : (failures.length ? "warn" : "dim"), "Market: " + (stress ? summary.pills.market.label : "unavailable"), summary.pills.market.label);
  _pillSet("opMoodPill", summary.pills.mood.cls, "Mood: " + summary.pills.mood.label, summary.pills.mood.label);

  const latestTs = pickTimestamp(
    system && system.ts_ms,
    stress && stress.ts_ms,
    barrier && barrier.ts_ms,
    health && health.ts_ms,
    readiness && readiness.ts_ms,
    asObject(readiness).readiness && asObject(asObject(readiness).readiness).ts_ms
  );
  const latestAgeMs = latestTs == null ? null : Math.max(0, Date.now() - latestTs);
  _pillSet(
    "opUpdatedPill",
    failures.length ? "bad" : (latestAgeMs != null && latestAgeMs >= 300_000 ? "warn" : "dim"),
    failures.length
      ? `Updated: stale ${latestAgeMs == null ? "—" : `${formatAgeMs(latestAgeMs)} ago`}`
      : (latestTs == null ? "Updated: unavailable" : `Updated: backend ${formatAgeMs(latestAgeMs)} ago`)
  );

  const headlineEl = document.getElementById("opHeadline");
  const meaningEl  = document.getElementById("opMeaning");

  if (!headlineEl || !meaningEl) return;

  if (failures.length) {
    headlineEl.textContent = "Critical backend state is unavailable";
    meaningEl.textContent = failures.slice(0, 2).map((item) => `${item.label}: ${item.message}`).join(" | ");
  } else if (!system) {
    headlineEl.textContent = "System status unavailable";
    meaningEl.textContent = "Could not load the current system state.";
  } else {
    headlineEl.textContent = summary.headline;
    meaningEl.textContent = summary.meaning;
  }

  const nextEl = document.getElementById("opNextList");
  if (nextEl) {
    const nextItems = failures.length
      ? [
          "Treat the overview as degraded until the failed route recovers.",
          "Open Operate and verify readiness, execution barrier, and notification state together.",
          "Avoid risk-increasing actions while health or readiness routes are unavailable.",
        ]
      : (summary.next || []);
    nextEl.innerHTML = nextItems
      .map((item) => `<li>${String(item)}</li>`)
      .join("");
  }

  const blockersEl = document.getElementById("opBlockersList");
  if (blockersEl) {
    const blockers = [
      ...failures.map((item) => `route failure: ${item.label} (${item.message})`),
      ...asArray(summary.blockers),
      ...collectIssues(readiness, 4).map((item) => `readiness: ${item}`),
    ];
    blockersEl.innerHTML = (blockers.length ? blockers : ["No critical blockers detected."])
      .map((item) => `<li>${String(item)}</li>`)
      .join("");
  }

  try {
    const setPanelState = typeof window !== "undefined" ? window.__setDashboardPanelState__ : null;
    if (typeof setPanelState === "function") {
      setPanelState("operatorSummaryCard", {
        state: failures.length ? "error" : (latestAgeMs != null && latestAgeMs >= 300_000 ? "stale" : "fresh"),
        reason: failures.length
          ? failures.slice(0, 2).map((item) => `${item.label}: ${item.message}`).join(" • ")
          : (latestTs == null
            ? "Operator summary is visible without a backend timestamp."
            : `Operator summary refreshed ${formatAgeMs(latestAgeMs)} ago.`),
      });
    }
  } catch {}
}
