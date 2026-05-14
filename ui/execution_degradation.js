"use strict";

/*
  ui/execution_degradation.js
  Execution degradation detection + state engine (Phase 6C)
  Extracted verbatim from dashboard.js
*/

// -----------------------------
// Storage keys
// -----------------------------
export const EXEC_CONF_STATE_KEY = "exec_conf_state_v2";

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

function _loadExecState() {
  const raw = _lsGet(EXEC_CONF_STATE_KEY);
  if (!raw) return {};

  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

// -----------------------------
// Detection
// -----------------------------
export function detectExecutionDegradation(rows, toastFn) {
  if (!Array.isArray(rows) || !rows.length) return [];

  const state = _loadExecState();

  const now = Date.now();
  const alerts = [];

  const bySym = {};
  for (const r of rows) {
    if (
      Number(r.conf_lo) >= 0.75 &&
      Number(r.n || 0) >= 20 &&
      Number.isFinite(Number(r.mean_cost))
    ) {
      const sym = r.symbol || "GLOBAL";
      (bySym[sym] ||= []).push(r);
    }
  }

  for (const sym of Object.keys(bySym)) {
    const avgCost =
      bySym[sym].reduce((a, r) => a + Number(r.mean_cost), 0) /
      bySym[sym].length;

    const s = state[sym] || {
      baseline: avgCost,
      degraded_since: null,
      level: "OK",
      acked: false,
    };

    if (!Number.isFinite(s.baseline)) {
      s.baseline = avgCost;
      state[sym] = s;
      continue;
    }

    const worsenPct =
      (avgCost - s.baseline) / Math.abs(s.baseline || 1e-9);

    // ---- DEGRADED ----
    if (worsenPct > 0.3) {
      if (!s.degraded_since) s.degraded_since = now;

      const ageMin = (now - s.degraded_since) / 60000;

      s.level = (ageMin >= 30) ? "CRIT" : "WARN";
      s.acked = false;

      alerts.push({
        symbol: sym,
        level: s.level,
        prev: s.baseline,
        cur: avgCost,
        worsenPct,
        ageMin,
      });
    }

    // ---- RECOVERY ----
    else if (avgCost <= s.baseline * 1.05) {
      if (s.level !== "OK") {
        s.level = "OK";
        s.acked = true;
        s.degraded_since = null;

        if (typeof toastFn === "function") {
          toastFn(`Execution recovered for ${sym}`, "ok", 2500);
        }
      }

      s.baseline = s.baseline * 0.8 + avgCost * 0.2;
    }

    state[sym] = s;
  }

  _lsSet(EXEC_CONF_STATE_KEY, JSON.stringify(state));
  return alerts;
}

// -----------------------------
// Global state helpers
// -----------------------------
export function isExecutionDegraded() {
  const st = _loadExecState();
  return Object.values(st).some(
    s => s.level === "WARN" || s.level === "CRIT"
  );
}

// -----------------------------
// Alert emission
// -----------------------------
export function buildExecutionAlert(info) {
  return {
    id: `exec-degradation-${info.symbol}`,
    ts_ms: Date.now(),
    severity: info.level,
    symbol: info.symbol,
    horizon_s: "-",
    expected_z: null,
    confidence: 0.99,
    event_title:
      info.level === "CRIT"
        ? "Execution degradation persists (CRIT)"
        : "Execution degradation detected",
    resolved: false,
    acked: false,
    resolved_reason: "",
    acked_by: "",
    reason:
      `Avg cost ${info.prev.toFixed(6)} → ${info.cur.toFixed(6)} ` +
      `(+${(info.worsenPct * 100).toFixed(1)}%, ${info.ageMin.toFixed(1)}m)`
  };
}
