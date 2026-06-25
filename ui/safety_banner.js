"use strict";

/*
  ui/safety_banner.js — Safety banner / manipulation kill-switch engine
  Extracted from ui/dashboard.js (Phase 6)

  Responsibilities:
    - Track manipulation blocks from alerts stream
    - Enforce HARD blocks for actions unless Expert-unlocked
*/

import { normalizeSeverity, severityAtLeast } from "./alerts.js";

const _MANIP_STATE_KEY = "ui_manip_killswitch_v1";

let _manipBlockedSyms = new Set();
let _manipReasons = []; // [{symbol, severity, why, id, ts_ms}]

// restore last known manipulation state (best-effort)
try {
  const raw = localStorage.getItem(_MANIP_STATE_KEY);
  if (raw) {
    const st = JSON.parse(raw);
    _manipBlockedSyms = new Set(st.blocked || []);
    _manipReasons = st.reasons || [];
  }
} catch {}

function _kwHit(s) {
  return /(bot|promo|promot|manip|coordinat|astroturf|pump|dump|raid|brigad|shill|sockpuppet|spam)/i.test(String(s || ""));
}

function _isManipulationAlert(r) {
  if (!r) return false;
  const sev = normalizeSeverity(r.severity);
  if (!severityAtLeast(sev, "WARN")) return false;

  // Quantitative evidence (preferred when available)
  if (typeof r.manip_risk === "number" && r.manip_risk >= 0.7) return true;
  if (typeof r.bot_likelihood === "number" && r.bot_likelihood >= 0.7) return true;
  if (typeof r.promo_likelihood === "number" && r.promo_likelihood >= 0.7) return true;

  // Fallback: keyword scan
  const t = `${r.event_title || ""} ${r.reason || ""} ${r.symbol || ""} ${r.rule_id || ""}`;
  return _kwHit(t);
}

export function updateManipulationStateFromAlerts(rows) {
  const blocked = new Set();
  const reasons = [];

  for (const r of (rows || [])) {
    if (r && r.resolved) continue;
    if (!_isManipulationAlert(r)) continue;

    const sym = String(r.symbol || "").toUpperCase() || "UNKNOWN";
    blocked.add(sym);

    reasons.push({
      symbol: sym,
      severity: normalizeSeverity(r.severity),
      why: String(r.reason || r.event_title || "manipulation risk"),
      id: r.id,
      ts_ms: r.ts_ms
    });
  }

  _manipBlockedSyms = blocked;
  _manipReasons = reasons.slice(0, 50);

  // persist (best-effort)
  try {
    localStorage.setItem(_MANIP_STATE_KEY, JSON.stringify({
      ts_ms: Date.now(),
      blocked: Array.from(_manipBlockedSyms),
      reasons: _manipReasons
    }));
  } catch {}
}

export function isManipulationBlocked(sym) {
  if (!_manipBlockedSyms || _manipBlockedSyms.size === 0) return false;
  const s = String(sym || "").toUpperCase();
  if (!s) return true; // global block if unknown
  return _manipBlockedSyms.has(s) || _manipBlockedSyms.has("GLOBAL") || _manipBlockedSyms.has("EXECUTION");
}

export function manipBlockSummary() {
  const syms = Array.from(_manipBlockedSyms || []);
  return syms.length ? syms.join(", ") : "(none)";
}

export function hardBlockActionIfManipulated({
  actionName,
  symbol,
  expertUnlocked,
  toastFn,
  consoleElId = "console"
}) {
  // HARD block unless explicitly Expert-unlocked
  if (expertUnlocked) return false;

  if (isManipulationBlocked(symbol || "")) {
    const msg =
      `HARD BLOCK (${actionName}) — manipulation risk flagged for: ${manipBlockSummary()}`;

    const el = document.getElementById(consoleElId);
    if (el) el.textContent += `[kill-switch] ${msg}\n`;

    if (typeof toastFn === "function") toastFn(msg, "bad", 5200);
    return true;
  }
  return false;
}

// Optional: expose state for diagnostics panels
export function getManipulationState() {
  return {
    blocked: Array.from(_manipBlockedSyms || []),
    reasons: _manipReasons || []
  };
}
