"use strict";

/*
  ui/policy.js — Operator vs Expert policy engine
  Extracted from ui/dashboard.js (Phase 5)

  This module manages browser-side operator/expert interaction policy. It gates
  sensitive UI actions and applies visual state, while the server remains the
  final authority for real execution permissions.
*/

// -----------------------------
// Storage keys
// -----------------------------
export const OPERATOR_MODE_KEY = "operator_mode";
export const EXPERT_UNLOCK_KEY = "expert_unlock";

function _serverExecutionBarrier() {
  try {
    const barrier = window.__LAST_EXECUTION_BARRIER__;
    return barrier && typeof barrier === "object" ? barrier : null;
  } catch {
    return null;
  }
}

function _serverBlocksMutations() {
  const barrier = _serverExecutionBarrier();
  if (!barrier) return true;
  if (barrier.ok === false) return true;
  if (barrier.allowed === true || barrier.real_trading_allowed === true) return false;
  return true;
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

// -----------------------------
// State
// -----------------------------
export function loadPolicyState() {
  // Operator mode defaults ON once
  if (_lsGet(OPERATOR_MODE_KEY) == null) {
    _lsSet(OPERATOR_MODE_KEY, "1");
  }

  return {
    operatorMode: _lsGet(OPERATOR_MODE_KEY) !== "0",
    expertUnlocked: _lsGet(EXPERT_UNLOCK_KEY) === "1",
  };
}

export function saveOperatorMode(on) {
  _lsSet(OPERATOR_MODE_KEY, on ? "1" : "0");
}

export function saveExpertUnlock(on) {
  _lsSet(EXPERT_UNLOCK_KEY, on ? "1" : "0");
}

// -----------------------------
// DOM policy application
// -----------------------------
export function applyPolicyToDOM({ operatorMode, expertUnlocked }) {
  document.body.classList.toggle("mode-operator", !!operatorMode);
  document.body.classList.toggle("mode-expert", !operatorMode);
  document.body.classList.toggle("expert-unlocked", !!expertUnlocked);

  // legacy alerts table hidden for operators
  const legacy = document.getElementById("alerts");
  if (legacy) legacy.style.display = operatorMode ? "none" : "";

  const btnOp = document.getElementById("btnOperatorMode");
  if (btnOp) {
    btnOp.textContent =
      `👷 Operator Mode: ${operatorMode ? "ON" : "OFF"}`;
  }

  const btnEx = document.getElementById("btnExpertUnlock");
  if (btnEx) {
    btnEx.textContent =
      `🛡 Unlock Advanced: ${expertUnlocked ? "ON" : "OFF"}`;
  }
}

// -----------------------------
// Guards
// -----------------------------
export function requireExpertUnlock(expertUnlocked, message) {
  if (_serverBlocksMutations()) return false;
  if (expertUnlocked) return true;
  if (!message) return false;
  return false;
}

export function requireConfirmIfDegraded({
  executionDegraded,
  operatorMode,
  message
}) {
  if (_serverBlocksMutations()) return false;
  if (!executionDegraded) return true;
  if (operatorMode) return true;
  return false;
}
