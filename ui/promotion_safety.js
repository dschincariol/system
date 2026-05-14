"use strict";

/*
  ui/promotion_safety.js
  Promotion safety + execution-recovery auto-resume engine (Phase 8)
  Extracted verbatim from dashboard.js
*/

const _PROMO_PAUSED_KEY = "promo_paused_due_to_exec_v1";

let _isExecutionDegraded = () => false;
let _hardBlockActionIfManipulated = () => false;
let _toast = null;
let _fetchJSON = null;
let _loadPromotionStatus = null;
let _loadSizePolicy = null;
let _refresh = null;
let _getManipBlockedSyms = () => new Set();

export function initPromotionSafetyEngine(deps) {
  _isExecutionDegraded = deps.isExecutionDegraded;
  _hardBlockActionIfManipulated = deps.hardBlockActionIfManipulated;
  _toast = deps.toast;
  _fetchJSON = deps.fetchJSON;
  _loadPromotionStatus = deps.loadPromotionStatus;
  _loadSizePolicy = deps.loadSizePolicy;
  _refresh = deps.refresh;
  _getManipBlockedSyms = deps.getManipBlockedSyms || (() => new Set());
}

// -----------------------------
// Auto-resume after execution recovery
// -----------------------------
export async function maybeAutoResumePromotionsAfterRecovery({
  operatorMode
}) {
  // Never auto-resume during manipulation risk
  if (_getManipBlockedSyms().size > 0) return;

  if (localStorage.getItem(_PROMO_PAUSED_KEY) !== "1") return;
  if (_isExecutionDegraded()) return;

  try {
    const st = await _fetchJSON("/api/promotion/status");
    if (!st || !st.ok) return;

    const enabledDb =
      (st && st.promotion_enabled_db)
        ? String(st.promotion_enabled_db)
        : "1";

    if (enabledDb === "1") {
      localStorage.removeItem(_PROMO_PAUSED_KEY);
      return;
    }

    if (!operatorMode) {
      const ok = confirm(
        "Execution has recovered.\n\nResume promotions automatically now?"
      );
      if (!ok) return;
    }

    const res = await _fetchJSON("/api/promotion/enable", {
      method: "POST",
      body: JSON.stringify({ on: "1", confirm: "PROMOTION" }),
    });
    if (res && res.ok) {
      _toast("Promotions resumed after execution recovery", "ok", 3500);
      localStorage.removeItem(_PROMO_PAUSED_KEY);
      await _loadPromotionStatus();
    }
  } catch {
    // ignore
  }
}

// -----------------------------
// Promotion toggle guard
// -----------------------------
export async function handlePromotionToggle({
  operatorMode,
  expertUnlocked
}) {
  if (_hardBlockActionIfManipulated({
    actionName: "toggle promotions",
    symbol: "GLOBAL",
    expertUnlocked,
    toastFn: _toast
  })) return;

  if (_isExecutionDegraded()) {
    localStorage.setItem(_PROMO_PAUSED_KEY, "1");
    _toast("Promotions paused due to execution degradation", "warn", 4000);
    return;
  }

  if (!operatorMode) {
    if (!confirm("Toggle promotions? This affects model promotion safety.")) {
      return;
    }
  }

  const st = await _fetchJSON("/api/promotion/status");
  if (st && st.current_champion && st.current_champion.safety_score !== undefined) {
  if (Number(st.current_champion.safety_score) <= 0) {
    _toast("Cannot enable promotions: champion safety score is negative", "error", 4000);
    return;
  }
}

  const enabledDb =
    (st && st.promotion_enabled_db) ? st.promotion_enabled_db : "1";
  const next = (enabledDb === "1") ? "0" : "1";

  const res = await _fetchJSON("/api/promotion/enable", {
    method: "POST",
    body: JSON.stringify({ on: next, confirm: "PROMOTION" }),
  });
  if (!res || !res.ok) {
    throw new Error((res && res.error) || "toggle failed");
  }

  await _loadPromotionStatus();
}

// -----------------------------
// Automatic fix button logic
// -----------------------------
export async function handleAutoFix({
  operatorMode
}) {
  if (!operatorMode) {
    const ok = confirm(
      "This will automatically attempt to fix startup issues:\n" +
      "- Initialize / migrate databases\n" +
      "- Rebuild labels\n" +
      "- Train size policy if missing\n\n" +
      "Proceed?"
    );
    if (!ok) return;
  }

  const el = document.getElementById("console");
  if (el) el.textContent += "[ui] running automatic fix...\n";

  const res = await _fetchJSON("/api/system/fix", {
    method: "POST",
    body: JSON.stringify({ confirm: "SYSTEM_FIX" }),
  });
  if (!res || !res.ok) {
    throw new Error(res?.error || "fix failed");
  }

  if (el) {
    el.textContent += "[ui] automatic fix complete\n";
    if (res.actions) {
      el.textContent += JSON.stringify(res.actions, null, 2) + "\n";
    }
  }

  _toast("Automatic fixes applied", "ok", 3500);

  await _refresh();
  await _loadPromotionStatus();
  await _loadSizePolicy();
}

// -----------------------------
// Safety Metric Extraction
// -----------------------------
export function extractPromotionSafetyMetrics(row) {
  if (!row) return null;

  return {
    capital_efficiency:
      row.capital_efficiency !== undefined
        ? Number(row.capital_efficiency)
        : undefined,

    drawdown_contribution:
      row.drawdown_contribution !== undefined
        ? Number(row.drawdown_contribution)
        : undefined,

    avg_slippage_impact:
      row.avg_slippage_impact !== undefined
        ? Number(row.avg_slippage_impact)
        : undefined,

    safety_score:
      row.safety_score !== undefined
        ? Number(row.safety_score)
        : undefined
  };
}

// -----------------------------
// Render Safety Metrics
// -----------------------------
export function renderPromotionSafetyMetrics(row) {
  const m = extractPromotionSafetyMetrics(row);
  if (!m) return "";

  let html = `<div class="promo-safety-metrics">`;

  if (m.capital_efficiency !== undefined) {
    const cls =
      m.capital_efficiency > 1
        ? "metric-good"
        : m.capital_efficiency > 0
        ? "metric-warn"
        : "metric-bad";

    html += `
      <div class="metric ${cls}">
        <label>Capital Efficiency</label>
        <span>${m.capital_efficiency.toFixed(3)}</span>
      </div>
    `;
  }

  if (m.drawdown_contribution !== undefined) {
    const cls =
      m.drawdown_contribution < 1
        ? "metric-good"
        : m.drawdown_contribution < 3
        ? "metric-warn"
        : "metric-bad";

    html += `
      <div class="metric ${cls}">
        <label>Drawdown Contribution</label>
        <span>${m.drawdown_contribution.toFixed(3)}</span>
      </div>
    `;
  }

  if (m.avg_slippage_impact !== undefined) {
    const cls =
      m.avg_slippage_impact < 0.5
        ? "metric-good"
        : m.avg_slippage_impact < 1.5
        ? "metric-warn"
        : "metric-bad";

    html += `
      <div class="metric ${cls}">
        <label>Slippage Impact</label>
        <span>${m.avg_slippage_impact.toFixed(3)}</span>
      </div>
    `;
  }

  if (m.safety_score !== undefined) {
    const cls =
      m.safety_score > 1
        ? "metric-good"
        : m.safety_score > 0
        ? "metric-warn"
        : "metric-bad";

    html += `
      <div class="metric ${cls}">
        <label>Safety Score</label>
        <span>${m.safety_score.toFixed(3)}</span>
      </div>
    `;
  }

  html += `</div>`;
  return html;
}
