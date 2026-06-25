"use strict";

/*
  ui/read_only_mode.js
  Read-only / Demo / Investor mode enforcement (Phase 10)

  This is a browser-local safety layer that mirrors backend execution-barrier
  state and persisted UI preferences so the dashboard can disable dangerous
  controls by default.
*/

const READ_ONLY_KEY = "ui_read_only_mode_v1";

function _serverReadOnly() {
  try {
    const barrier = window.__LAST_EXECUTION_BARRIER__;
    if (!barrier || typeof barrier !== "object") return true;
    if (barrier.ok === false) return true;
    return !(barrier.allowed === true || barrier.real_trading_allowed === true);
  } catch {
    return true;
  }
}

function _lsGet(key) {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function _lsSet(key, value) {
  try {
    localStorage.setItem(key, value);
    return true;
  } catch {
    return false;
  }
}

export function isReadOnlyMode() {
  return _lsGet(READ_ONLY_KEY) === "1" || _serverReadOnly();
}

export function setReadOnlyMode(on) {
  _lsSet(READ_ONLY_KEY, on ? "1" : "0");
}

export function applyReadOnlyBanner() {
  const banner = document.getElementById("readOnlyBanner");
  if (!banner) return;

  if (isReadOnlyMode()) {
    banner.style.display = "block";
    banner.textContent =
      "🔒 Read-only mode — Actions stay disabled until the server explicitly reports an executable state.";
  } else {
    banner.style.display = "none";
  }
}

export function hardBlockIfReadOnly({
  actionName,
  toastFn
}) {
  let freshnessBlock = null;
  try {
    freshnessBlock = window.__DASHBOARD_CONNECTION_SUMMARY__;
  } catch {
    freshnessBlock = null;
  }
  const blockedByFreshness = !!(freshnessBlock && freshnessBlock.safetyGuardActive);
  if (!isReadOnlyMode() && !blockedByFreshness) return false;

  const freshnessReasons = Array.isArray(freshnessBlock && freshnessBlock.safetyProblems)
    ? freshnessBlock.safetyProblems.map((row) => row && row.label).filter(Boolean).slice(0, 3)
    : [];
  const blockLabel = blockedByFreshness
    ? `Fresh safety data required${freshnessReasons.length ? `: ${freshnessReasons.join(", ")}` : ""}`
    : "Demo / Read-only mode";

  if (typeof toastFn === "function") {
    toastFn(
      `Blocked "${actionName}" — ${blockLabel}`,
      "warn",
      3500
    );
  }

  return true;
}
