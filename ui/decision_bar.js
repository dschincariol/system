"use strict";

/*
  ui/decision_bar.js
  Decision bar rendering + derivation (Phase 7)
  Extracted from ui/dashboard.js
*/

import { setMetricValueAttribute } from "./tooltip.js";
import { statusAriaLabel, statusPillClasses, statusToken } from "./utils.js";
import { normalizeSeverity, severityAtLeast } from "./alerts.js";

let _getLastAlerts = () => [];
let _getLastHealth = () => null;
let _getLastSystemState = () => null;
let _getLastExecutionBarrier = () => null;
let _getLastPromotionStatus = () => null;
let _isExecutionDegraded = () => false;

let _wired = false;

export function initDecisionBarEngine(deps) {
  _getLastAlerts = (deps && deps.getLastAlerts) ? deps.getLastAlerts : (() => []);
  _getLastHealth = (deps && deps.getLastHealth) ? deps.getLastHealth : (() => null);
  _getLastSystemState = (deps && deps.getLastSystemState) ? deps.getLastSystemState : (() => null);
  _getLastExecutionBarrier = (deps && deps.getLastExecutionBarrier) ? deps.getLastExecutionBarrier : (() => null);
  _getLastPromotionStatus = (deps && deps.getLastPromotionStatus) ? deps.getLastPromotionStatus : (() => null);
  _isExecutionDegraded = (deps && deps.isExecutionDegraded) ? deps.isExecutionDegraded : (() => false);

  // wire once (prevents duplicate listeners)
  wireDecisionBarClicks();
}

function _setPill(id, text, cls, metricValue = null) {
  const el = document.getElementById(id);
  if (!el) return;
  const token = statusToken(cls || "neutral");
  el.textContent = text;
  el.className = `${statusPillClasses(token.key)} clickable`;
  el.dataset.status = token.key;
  el.setAttribute("aria-label", statusAriaLabel(token.key, text));
  setMetricValueAttribute(el, metricValue);
}

function _jumpToCard(titleContains) {
  const cards = Array.from(document.querySelectorAll(".card"));
  const hit = cards.find((c) => (c.querySelector("h2")?.textContent || "").includes(titleContains));
  if (hit) hit.scrollIntoView({ behavior: "smooth", block: "start" });
}

export function wireDecisionBarClicks() {
  if (_wired) return;
  _wired = true;

  document.getElementById("pillSystem")?.addEventListener("click", ()=>_jumpToCard("System Health"));
  document.getElementById("pillCrit")?.addEventListener("click", ()=>_jumpToCard("Alerts"));
  document.getElementById("pillWarn")?.addEventListener("click", ()=>_jumpToCard("Alerts"));
  document.getElementById("pillData")?.addEventListener("click", ()=>_jumpToCard("System Health"));
  document.getElementById("pillModel")?.addEventListener("click", ()=>_jumpToCard("Promotions"));
  document.getElementById("pillExec")?.addEventListener("click", ()=>_jumpToCard("Execution"));
}

export function updateDecisionBarFromState(state) {
  // state: { system, crit, warn, data, model, exec, updated }
  const critKnown = Number.isFinite(Number(state.crit));
  const warnKnown = Number.isFinite(Number(state.warn));
  _setPill("pillSystem", `SYSTEM: ${state.system}`, state.system === "CRIT" ? "crit" : state.system === "WARN" ? "warn" : state.system === "OK" ? "ok" : "unavailable", state.system);
  _setPill("pillCrit", `CRIT: ${critKnown ? state.crit : "N/A"}`, !critKnown ? "unavailable" : state.crit > 0 ? "crit" : "neutral", critKnown ? state.crit : null);
  _setPill("pillWarn", `WARN+: ${warnKnown ? state.warn : "N/A"}`, !warnKnown ? "unavailable" : state.warn > 0 ? "warn" : "neutral", warnKnown ? state.warn : null);
  _setPill("pillData", `Data: ${state.data}`, state.data === "BAD" ? "crit" : state.data === "WARN" ? "warn" : state.data === "OK" ? "ok" : "unavailable", state.data);
  _setPill("pillModel", `Model: ${state.model}`, state.model === "BLOCKED" ? "blocked" : state.model === "OK" ? "ok" : "unavailable", state.model);
  _setPill("pillExec", `Exec: ${state.exec}`, state.exec === "BLOCKED" ? "blocked" : state.exec === "DEGRADED" ? "warn" : state.exec === "OK" ? "ok" : "unavailable", state.exec);

  const up = document.getElementById("pillUpdated");
  if (up) up.textContent = `Updated: ${state.updated}`;
}

function _toBoolOrNull(value) {
  return typeof value === "boolean" ? value : null;
}

function _deriveSystemStatus(systemState) {
  const state = String(
    (systemState && (systemState.state || systemState.system_state || systemState.status)) || ""
  ).trim().toUpperCase();

  if (state === "LIVE" || state === "RUNNING") return "OK";
  if (state === "DEGRADED" || state === "STARTING" || state === "BOOTING" || state === "WARMING_UP") return "WARN";
  if (!state) return "N/A";
  return "CRIT";
}

function _deriveDataStatus(health) {
  const checks = [
    _toBoolOrNull(health && health.prices && health.prices.ok),
    _toBoolOrNull(health && health.labels && health.labels.ok),
    _toBoolOrNull(health && health.providers && health.providers.ok),
  ];
  const explicit = checks.filter((value) => value !== null);

  if (!explicit.length) return "N/A";
  if (explicit.includes(false)) return "BAD";
  if (checks.includes(null)) return "WARN";
  return "OK";
}

function _deriveModelStatus(promotionStatus) {
  if (!promotionStatus || typeof promotionStatus !== "object") return "N/A";
  if (promotionStatus.enabled === true && promotionStatus.allowed === true) return "OK";
  if (promotionStatus.enabled === false || promotionStatus.allowed === false) return "BLOCKED";
  return "N/A";
}

function _deriveExecStatus(barrier, alerts) {
  if (barrier && barrier.allowed === false) return "BLOCKED";
  const hasExecutionAlert = (alerts || []).some((alert) => {
    if (!alert || alert.resolved) return false;
    const symbol = String(alert.symbol || "").trim().toUpperCase();
    return symbol === "EXECUTION" && severityAtLeast(alert.severity, "WARN");
  });
  if (hasExecutionAlert) return "DEGRADED";
  if (barrier && barrier.allowed === true) return "OK";
  return "N/A";
}

function _pickLatestTs(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
}

function _formatAge(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n < 0) return "—";
  if (n < 1000) return `${Math.round(n)}ms`;
  if (n < 60_000) return `${Math.round(n / 1000)}s`;
  if (n < 3_600_000) return `${Math.round(n / 60_000)}m`;
  return `${(n / 3_600_000).toFixed(n < 36_000_000 ? 1 : 0)}h`;
}

function _deriveUpdatedLabel(explicitLabel, health, systemState, barrier, promotionStatus) {
  const trimmed = String(explicitLabel || "").trim();
  if (trimmed && trimmed.toLowerCase() !== "just now") return trimmed;

  const latestTs = Math.max(
    _pickLatestTs(health && health.ts_ms, health && health.timestamp_ms),
    _pickLatestTs(systemState && systemState.ts_ms, systemState && systemState.timestamp_ms),
    _pickLatestTs(barrier && barrier.ts_ms, barrier && barrier.timestamp_ms),
    _pickLatestTs(promotionStatus && promotionStatus.updated_ts_ms, promotionStatus && promotionStatus.ts_ms),
  );
  const failures = Array.isArray(window.__LAST_REFRESH_FAILURES__) ? window.__LAST_REFRESH_FAILURES__ : [];
  if (!latestTs) return failures.length ? "stale" : "unavailable";
  const ageMs = Math.max(0, Date.now() - latestTs);
  return failures.length ? `stale ${_formatAge(ageMs)} ago` : `backend ${_formatAge(ageMs)} ago`;
}

export function updateDecisionHeader(updatedLabel) {
  try {
    const _lastAlerts = _getLastAlerts() || [];
    const _lastHealth = _getLastHealth() || null;
    const _lastSystemState = _getLastSystemState() || null;
    const _lastExecutionBarrier = _getLastExecutionBarrier() || null;
    const _lastPromotionStatus = _getLastPromotionStatus() || null;
    const alertsUnavailable = !!(typeof window !== "undefined" && window.__LAST_ALERTS_FAILED__);

    const critN = alertsUnavailable ? null : (_lastAlerts || []).filter((a) => !a.resolved && normalizeSeverity(a && a.severity) === "CRIT").length;
    const warnN = alertsUnavailable
      ? null
      : (_lastAlerts || []).filter((a) => {
          return !a.resolved && severityAtLeast(a && a.severity, "WARN");
        }).length;

    updateDecisionBarFromState({
      system: _deriveSystemStatus(_lastSystemState),
      crit: critN,
      warn: warnN,
      data: _deriveDataStatus(_lastHealth),
      model: _deriveModelStatus(_lastPromotionStatus),
      exec: _deriveExecStatus(_lastExecutionBarrier, alertsUnavailable ? [] : _lastAlerts),
      updated: _deriveUpdatedLabel(
        updatedLabel,
        _lastHealth,
        _lastSystemState,
        _lastExecutionBarrier,
        _lastPromotionStatus
      ),
    });
  } catch {}
}
