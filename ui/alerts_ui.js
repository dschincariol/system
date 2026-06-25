/*
  FILE: ui/alerts_ui.js

  Dashboard alert rendering helpers. This module turns alert API payloads into
  grouped, human-readable cards and keeps alert presentation logic out of the
  main dashboard controller.
*/

import { esc, escapeHTML, fmtTime } from "./utils.js";
import {
  applyAlertLocalState,
  cellColor,
  normalizeAlert,
  normalizeAlertDetailPayload,
  normalizeAlertsPayload,
  normalizeSeverity,
} from "./alerts.js";

export function _scoreCell(rows, severityRank) {
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

const _ALERT_PERSONA_KEYS = [
  "alert_persona",
  "dashboard.persona",
  "ui.persona",
  "persona",
];
const _DEFAULT_ALERT_PERSONA = "operations";

const _ALERT_THRESHOLD_RULES = {
  info_z075_conf45: { minAbsZ: 0.75, minConf: 0.45, severity: "INFO" },
  warn_z1_conf55: { minAbsZ: 1.0, minConf: 0.55, severity: "WARN" },
  high_z15_conf60: { minAbsZ: 1.5, minConf: 0.60, severity: "HIGH" },
  crit_z2_conf70: { minAbsZ: 2.0, minConf: 0.70, severity: "CRIT" },
};

const _ALERT_EXPLANATIONS = {
  equity_recon: {
    meaning: {
      operations: "Broker and system equity are out of line. Treat the live book as unreconciled until fills and prices match again.",
      fund_manager: "Live equity no longer matches the system view, so current PnL and exposure may be unreliable until reconciled.",
    },
    posture: {
      operations: "Pause execution and reconcile",
      fund_manager: "Hold risk changes until reconciled",
    },
    why: {
      operations: "This alert family is raised for broker-versus-backtest mismatch, not for a soft model warning.",
      fund_manager: "The recommendation is driven by a live state mismatch, not by a discretionary portfolio view.",
    },
    noChange: {
      operations: "Further orders can compound the mismatch and make later reconciliation harder.",
      fund_manager: "Risk decisions may be made off an unreliable equity base, which can hide slippage or booking errors.",
    },
    safeIgnore: {
      operations: "No — live state integrity needs reconciliation first.",
      fund_manager: "No — current PnL and exposure may be unreliable until reconciled.",
    },
    steps: {
      operations: [
        "Stop broker_apply_orders to prevent compounding errors.",
        "Open broker snapshot and compare it to the latest backtest run, including timestamps and prices.",
        "Inspect recent fills for abnormal slippage, fees, or missing price updates.",
        "After reconciliation, acknowledge the alert and resume with reduced sizing.",
      ],
      fund_manager: [
        "Pause new risk changes until broker and system equity line up again.",
        "Compare broker snapshot, latest backtest run, and recent fills for missing or mispriced activity.",
        "Resume only after the mismatch is explained and sizing can restart conservatively.",
      ],
    },
  },
  equity_drift_sustained: {
    meaning: {
      operations: "Live equity drift has stayed elevated across multiple observations, which points to execution or pricing mismatch rather than a one-off noisy fill.",
      fund_manager: "Realized performance is drifting away from the modeled path over time, so current edge may not be translating cleanly into live PnL.",
    },
    posture: {
      operations: "Investigate execution quality before scaling risk",
      fund_manager: "Reduce trust in live edge until drift is explained",
    },
    why: {
      operations: "The drift is sustained, which makes execution quality and pricing inputs the first things to verify.",
      fund_manager: "Persistent drift is harder to dismiss as noise than a single bad fill or transient mark.",
    },
    noChange: {
      operations: "Persistent drift can keep eroding realized performance and break attribution between the model and live book.",
      fund_manager: "Unexplained drift can turn a seemingly healthy strategy into weaker realized returns than the dashboard suggests.",
    },
    safeIgnore: {
      operations: "No — sustained drift is not a one-off blip.",
      fund_manager: "No — persistent drift changes how much confidence to place in realized performance.",
    },
    steps: {
      operations: [
        "Check execution_metrics for sudden changes in fees, slippage, or cost bps.",
        "Check poll_prices health and confirm the pricing source is stable.",
        "If drift persists, pause execution and re-run portfolio_backtest to verify assumptions.",
      ],
      fund_manager: [
        "Review execution cost and pricing quality before trusting recent realized returns.",
        "Compare live drift to the latest backtest assumptions and recent fills.",
        "Avoid adding risk until the source of drift is understood.",
      ],
    },
  },
  execution: {
    meaning: {
      operations: "This alert is about execution quality rather than a pure model signal. Slippage, latency, or broker path health may be degrading realized edge.",
      fund_manager: "Execution conditions are degrading, so modeled edge may not convert cleanly into realized PnL.",
    },
    posture: {
      operations: "Check execution path and data freshness",
      fund_manager: "Reduce trust in live fill quality",
    },
    why: {
      operations: "Execution alerts usually reflect realized trading conditions, which can invalidate otherwise normal signals.",
      fund_manager: "A good signal is not enough if live fills, fees, or latency are eroding the realized outcome.",
    },
    noChange: {
      operations: "Poor fills or stale prices can keep dragging realized performance until the path stabilizes.",
      fund_manager: "Realized returns can continue to lag modeled returns even if the strategy signal remains valid.",
    },
    safeIgnore: {
      operations: "No — execution degradation affects realized edge directly.",
      fund_manager: "No — weak execution quality can quietly overwhelm signal quality.",
    },
    steps: {
      operations: [
        "Check broker connectivity, latency, and fill slippage.",
        "Confirm data freshness for prices, labels, and related health signals.",
        "If degradation persists, avoid risk-increasing changes until execution quality stabilizes.",
      ],
      fund_manager: [
        "Review slippage, fees, and latency before trusting live edge.",
        "Confirm the market data path is fresh and stable.",
        "Avoid increasing size until execution quality is back within normal bounds.",
      ],
    },
  },
  info_z075_conf45: {
    meaning: {
      operations: (ctx) => `This crossed the repo's informational signal threshold: ${_thresholdSummary(ctx.ruleKey)}. It is a watch item, not an execution block.${_signalSummary(ctx)}`,
      fund_manager: (ctx) => `This crossed the lowest configured signal threshold: ${_thresholdSummary(ctx.ruleKey)}. It is useful context, but not a stand-alone portfolio action.${_signalSummary(ctx)}`,
    },
    posture: {
      operations: "Observe and watch for clustering",
      fund_manager: "Watchlist only",
    },
    why: {
      operations: "Only the lowest configured threshold was met, so this should guide monitoring unless it repeats or escalates.",
      fund_manager: "The signal cleared a low bar for expected move and confidence, so it should inform context rather than drive sizing.",
    },
    noChange: {
      operations: (ctx) => `Most informational alerts self-resolve. Risk rises mainly if the same symbol and horizon keep firing${ctx.horizonLabel ? ` over the ${ctx.horizonLabel} horizon` : ""}.`,
      fund_manager: "Usually no material impact. Repeated follow-on alerts matter more than a single isolated info event.",
    },
    safeIgnore: {
      operations: "Usually yes, if it is isolated and no higher-severity follow-on alert appears.",
      fund_manager: "Usually yes, unless the same symbol starts clustering or severity escalates.",
    },
    steps: {
      operations: [
        "Check whether the same symbol or horizon is repeating.",
        "Use the Why modal for signal context and priors if the alert clusters.",
        "No workflow change is needed unless severity or frequency increases.",
      ],
      fund_manager: [
        "Track whether the symbol escalates into warning-tier alerts.",
        "Use this as context, not as a stand-alone reason to change sizing.",
      ],
    },
  },
  warn_z1_conf55: {
    meaning: {
      operations: (ctx) => `This crossed the configured warning threshold: ${_thresholdSummary(ctx.ruleKey)}. Review context before trusting the signal or expanding risk.${_signalSummary(ctx)}`,
      fund_manager: (ctx) => `This is a warning-tier signal under the repo's configured alert rules: ${_thresholdSummary(ctx.ruleKey)}. Re-check sizing and market context before leaning into it.${_signalSummary(ctx)}`,
    },
    posture: {
      operations: "Monitor closely",
      fund_manager: "Review before adding risk",
    },
    why: {
      operations: "The signal cleared the repo's warning threshold for both expected move magnitude and confidence.",
      fund_manager: "This is above watchlist level and deserves review before it influences portfolio changes.",
    },
    noChange: {
      operations: "Repeated warning alerts can roll into higher-severity alerts or weaker entries and exits if conditions stay unchanged.",
      fund_manager: "If similar warnings keep firing, realized risk can rise before the dashboard clearly shows performance damage.",
    },
    safeIgnore: {
      operations: "Only briefly, and only if the same symbol and horizon are not repeating.",
      fund_manager: "Not safely if the same symbol keeps reappearing or confidence rises further.",
    },
    steps: {
      operations: [
        "Check /api/health for stale prices, events, or predictions.",
        "Review drift dashboards and recent validation stability.",
        "If warnings persist, reduce sizing or tighten confidence gates temporarily.",
      ],
      fund_manager: [
        "Review health and drift before increasing exposure.",
        "Check whether the same symbol and horizon are repeating.",
        "Avoid increasing size until the warning clears or is explained.",
      ],
    },
  },
  high_z15_conf60: {
    meaning: {
      operations: (ctx) => `This crossed the elevated threshold above routine warnings: ${_thresholdSummary(ctx.ruleKey)}. Validate data and model state before leaning on it.${_signalSummary(ctx)}`,
      fund_manager: (ctx) => `This is stronger than a routine warning under the repo's configured rules: ${_thresholdSummary(ctx.ruleKey)}. Reduce willingness to add risk until the context is confirmed.${_signalSummary(ctx)}`,
    },
    posture: {
      operations: "Validate before aggressive execution",
      fund_manager: "Size down until verified",
    },
    why: {
      operations: "The model cleared the higher configured threshold for both move magnitude and confidence.",
      fund_manager: "This is meaningfully stronger than a normal warning and deserves an explicit review before risk is expanded.",
    },
    noChange: {
      operations: "If the signal is wrong or system conditions are degraded, realized PnL can diverge quickly from modeled edge.",
      fund_manager: "Ignoring elevated alerts can amplify exposure to conditions where the strategy no longer behaves as modeled.",
    },
    safeIgnore: {
      operations: "No — elevated threshold breaches warrant review.",
      fund_manager: "No — treat this as a real risk-control input, not background noise.",
    },
    steps: {
      operations: [
        "Check model metrics and validation for the affected symbol and horizon.",
        "Review recent job history for failures or repeated restarts.",
        "Consider pausing execution for the affected universe until resolved.",
      ],
      fund_manager: [
        "Review recent model stability and execution quality before trusting the signal.",
        "Avoid increasing risk until validation and operating health look clean again.",
      ],
    },
  },
  crit_z2_conf70: {
    meaning: {
      operations: (ctx) => `This crossed the repo's critical threshold: ${_thresholdSummary(ctx.ruleKey)}. Treat it as immediate review territory, not passive monitoring.${_signalSummary(ctx)}`,
      fund_manager: (ctx) => `This is the highest configured alert tier: ${_thresholdSummary(ctx.ruleKey)}. Avoid adding risk until the condition is understood.${_signalSummary(ctx)}`,
    },
    posture: {
      operations: "Pause or tightly gate execution",
      fund_manager: "Hold new risk and reassess",
    },
    why: {
      operations: "The signal cleared the highest configured threshold for both expected move magnitude and confidence.",
      fund_manager: "This is the repo's strongest configured signal alert and should be treated as a hard review point.",
    },
    noChange: {
      operations: (ctx) => `Downstream impact can arrive quickly${ctx.horizonLabel ? ` within the stated ${ctx.horizonLabel} horizon` : ""} if the condition is real.`,
      fund_manager: "Allowing risk to expand into a critical condition can turn a bad regime change or model problem into an immediate PnL event.",
    },
    safeIgnore: {
      operations: "No.",
      fund_manager: "No.",
    },
    steps: {
      operations: [
        "Pause or disable execution until the root cause is identified.",
        "Verify data freshness, drift, and recent model promotions or rollbacks.",
        "Resume only after the issue is resolved, with reduced sizing and close monitoring.",
      ],
      fund_manager: [
        "Do not add risk until the alert is explained.",
        "Review health, drift, and recent model changes immediately.",
        "Resume conservatively only after the condition is understood.",
      ],
    },
  },
};

const _ALERT_SEVERITY_DEFAULTS = {
  INFO: {
    meaning: {
      operations: "Monitor only. No action is required unless the alert starts repeating or escalates.",
      fund_manager: "This is informational context. It should not drive a portfolio change by itself.",
    },
    posture: {
      operations: "Observe only",
      fund_manager: "Observe only",
    },
    why: {
      operations: "The alert is below warning severity and is best treated as monitoring context.",
      fund_manager: "This is below warning severity and should stay contextual unless it clusters or escalates.",
    },
    noChange: {
      operations: "No material impact is expected from a single isolated info alert.",
      fund_manager: "No material portfolio impact is expected from a single isolated info alert.",
    },
    safeIgnore: {
      operations: "Usually yes, if it remains isolated.",
      fund_manager: "Usually yes, if it remains isolated.",
    },
    steps: {
      operations: [
        "Verify the alert matches expected news or event flow.",
        "No changes are needed unless alerts become frequent or drift increases.",
      ],
      fund_manager: [
        "Use it as context only.",
        "Revisit only if similar alerts start clustering.",
      ],
    },
  },
  WARN: {
    meaning: {
      operations: "Investigate. Confirm data freshness and model stability before normalizing the condition.",
      fund_manager: "This warning deserves review before it influences new risk decisions.",
    },
    posture: {
      operations: "Monitor closely",
      fund_manager: "Review before adding risk",
    },
    why: {
      operations: "Warning alerts sit above normal noise and often justify a targeted health and drift review.",
      fund_manager: "This is no longer background noise and should be reviewed before portfolio changes are made.",
    },
    noChange: {
      operations: "If the warning persists, system or market conditions can degrade further and spill into execution quality.",
      fund_manager: "Repeated warnings can turn into worse realized outcomes before portfolio metrics fully catch up.",
    },
    safeIgnore: {
      operations: "Only if it is isolated and clearly not repeating.",
      fund_manager: "Only if it is isolated and clearly not repeating.",
    },
    steps: {
      operations: [
        "Check /api/health for stale prices, events, and predictions.",
        "Review drift dashboards and recent validation scores.",
        "If warnings persist, reduce sizing or raise confidence thresholds temporarily.",
      ],
      fund_manager: [
        "Review health and drift before increasing exposure.",
        "Avoid adding size until the warning is explained.",
      ],
    },
  },
  HIGH: {
    meaning: {
      operations: "Elevated risk. Validate model performance and recent changes before acting.",
      fund_manager: "Risk is elevated enough that new portfolio changes should wait for validation.",
    },
    posture: {
      operations: "Validate before acting",
      fund_manager: "Hold risk changes until verified",
    },
    why: {
      operations: "This is above routine warning level and deserves explicit review.",
      fund_manager: "This is materially stronger than a routine warning and should alter risk appetite until verified.",
    },
    noChange: {
      operations: "If conditions stay degraded, realized performance can diverge quickly from modeled expectations.",
      fund_manager: "Ignoring elevated alerts can expand risk into a regime the strategy is not handling well.",
    },
    safeIgnore: {
      operations: "No.",
      fund_manager: "No.",
    },
    steps: {
      operations: [
        "Check model metrics and validation for the affected symbol and horizon.",
        "Review recent job history for failures or repeated restarts.",
        "Consider pausing execution for the affected universe until resolved.",
      ],
      fund_manager: [
        "Do not add risk until validation and health are checked.",
        "Resume changes only after the alert is understood.",
      ],
    },
  },
  CRIT: {
    meaning: {
      operations: "High risk. Pause execution and investigate immediately.",
      fund_manager: "This is a high-risk condition. Avoid adding risk until it is understood.",
    },
    posture: {
      operations: "Act now",
      fund_manager: "Hold new risk now",
    },
    why: {
      operations: "Critical alerts indicate a condition serious enough to justify immediate operating intervention.",
      fund_manager: "Critical alerts should block discretionary risk increases until the root cause is known.",
    },
    noChange: {
      operations: "Likely escalation or downstream impact if the underlying condition remains live.",
      fund_manager: "Allowing the condition to persist can turn directly into avoidable PnL and risk-management damage.",
    },
    safeIgnore: {
      operations: "No.",
      fund_manager: "No.",
    },
    steps: {
      operations: [
        "Pause or disable execution until the root cause is identified.",
        "Verify data freshness, drift, and recent model promotions or rollbacks.",
        "Resolve the underlying issue, then resume with reduced sizing and close monitoring.",
      ],
      fund_manager: [
        "Avoid adding risk until the condition is explained.",
        "Review health, drift, and recent model changes immediately.",
      ],
    },
  },
};

function _readAlertPersona() {
  try {
    for (const key of _ALERT_PERSONA_KEYS) {
      const raw = localStorage.getItem(key);
      const value = String(raw || "").trim().toLowerCase().replace(/[\s-]+/g, "_");
      if (value === "fund_manager" || value === "operations") return value;
    }
  } catch {}
  return _DEFAULT_ALERT_PERSONA;
}

function _safeMetric(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function _alertRuleId(r) {
  return String(r?.rule_id || "").trim();
}

function _alertRuleKey(r) {
  return _alertRuleId(r).toLowerCase();
}

function _alertSeverityKey(r) {
  return normalizeSeverity(r?.severity_raw || r?.level || r?.severity);
}

function _formatHorizonLabel(value) {
  const s = Number(value);
  if (!Number.isFinite(s) || s <= 0) return "";
  if (s < 60) return `${Math.round(s)}s`;
  if (s % 3600 === 0) return `${Math.round(s / 3600)}h`;
  if (s % 60 === 0) return `${Math.round(s / 60)}m`;
  if (s < 3600) return `${(s / 60).toFixed(1)}m`;
  return `${(s / 3600).toFixed(1)}h`;
}

function _buildAlertContext(r) {
  return {
    ruleId: _alertRuleId(r),
    ruleKey: _alertRuleKey(r),
    severity: _alertSeverityKey(r),
    symbolKey: String(r?.symbol || "").trim().toUpperCase(),
    horizonS: _safeMetric(r?.horizon_s),
    horizonLabel: _formatHorizonLabel(r?.horizon_s),
    absZ: (() => {
      const z = _safeMetric(r?.expected_z);
      return z == null ? null : Math.abs(z);
    })(),
    conf: _safeMetric(r?.confidence),
    confRaw: _safeMetric(r?.confidence_raw),
    predictionStrength: _safeMetric(r?.prediction_strength),
    status: String(r?.status || "").trim().toLowerCase(),
    acked: !!(r && r.acked),
    reason: String(r?.reason || "").trim(),
  };
}

function _thresholdSummary(ruleKey) {
  const meta = _ALERT_THRESHOLD_RULES[ruleKey];
  if (!meta) return "configured alert threshold";
  return `abs(expected_z) >= ${meta.minAbsZ.toFixed(2)} and confidence >= ${meta.minConf.toFixed(2)}`;
}

function _signalSummary(ctx) {
  const parts = [];
  if (ctx.horizonLabel) parts.push(`horizon ${ctx.horizonLabel}`);
  if (ctx.absZ != null) parts.push(`|z| ${ctx.absZ.toFixed(2)}`);
  if (ctx.conf != null) parts.push(`confidence ${ctx.conf.toFixed(2)}`);
  if (!parts.length) return "";
  return ` Current context: ${parts.join(", ")}.`;
}

function _pickVariant(value, persona, ctx) {
  let selected = value;
  if (selected && typeof selected === "object" && !Array.isArray(selected)) {
    selected =
      selected[persona] ??
      selected[_DEFAULT_ALERT_PERSONA] ??
      selected.operations ??
      selected.fund_manager ??
      "";
  }
  if (typeof selected === "function") selected = selected(ctx);
  return String(selected || "").trim();
}

function _pickList(value, persona, ctx) {
  let selected = value;
  if (selected && typeof selected === "object" && !Array.isArray(selected)) {
    selected =
      selected[persona] ??
      selected[_DEFAULT_ALERT_PERSONA] ??
      selected.operations ??
      selected.fund_manager ??
      [];
  }
  if (typeof selected === "function") selected = selected(ctx);
  return Array.isArray(selected)
    ? selected.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
}

function _resolveAlertDefinition(ctx) {
  if (ctx.ruleKey && _ALERT_EXPLANATIONS[ctx.ruleKey]) return _ALERT_EXPLANATIONS[ctx.ruleKey];
  if (ctx.symbolKey === "EXECUTION") return _ALERT_EXPLANATIONS.execution;
  return _ALERT_SEVERITY_DEFAULTS[ctx.severity] || _ALERT_SEVERITY_DEFAULTS.INFO;
}

function _alertWhyPosture(r) {
  const persona = _readAlertPersona();
  const ctx = _buildAlertContext(r);
  const entry = _resolveAlertDefinition(ctx);
  const base = _pickVariant(entry?.why, persona, ctx) || _pickVariant(entry?.meaning, persona, ctx);
  const cleanedReason = ctx.reason.replace(/[.\s]+$/g, "");
  if (cleanedReason) return `Current reason: ${cleanedReason}. ${base}`;
  return base;
}

export function _meaningForAlert(r) {
  const persona = _readAlertPersona();
  const ctx = _buildAlertContext(r);
  const entry = _resolveAlertDefinition(ctx);
  return _pickVariant(entry?.meaning, persona, ctx) || _pickVariant(_ALERT_SEVERITY_DEFAULTS.INFO.meaning, persona, ctx);
}

export function _recommendedPosture(r) {
  const persona = _readAlertPersona();
  const ctx = _buildAlertContext(r);
  const entry = _resolveAlertDefinition(ctx);
  return _pickVariant(entry?.posture, persona, ctx) || "Observe only";
}

export function _decisionConfidence(r) {
  const ctx = _buildAlertContext(r);

  if (ctx.ruleKey === "equity_recon") {
    return "High evidence: recommendation is based on an observed broker-versus-system mismatch.";
  }
  if (ctx.ruleKey === "equity_drift_sustained") {
    return "High evidence: sustained drift across observations is stronger than a one-off fill anomaly.";
  }
  if (ctx.symbolKey === "EXECUTION" && ctx.conf == null && ctx.absZ == null) {
    return "Operational evidence: use recent fills, slippage, fees, and latency as the confidence base.";
  }

  const evidence = [];
  if (ctx.conf != null) evidence.push(`confidence ${ctx.conf.toFixed(2)}`);
  if (ctx.confRaw != null) evidence.push(`raw ${ctx.confRaw.toFixed(2)}`);
  if (ctx.predictionStrength != null) evidence.push(`strength ${ctx.predictionStrength.toFixed(2)}`);
  if (ctx.absZ != null) evidence.push(`|z| ${ctx.absZ.toFixed(2)}`);

  if (!evidence.length) return "Context only: no structured confidence fields were attached to this alert.";

  let band = "Early evidence";
  if ((ctx.conf ?? 0) >= 0.70 && (ctx.absZ ?? 0) >= 2.0) band = "High evidence";
  else if ((ctx.conf ?? 0) >= 0.60 && (ctx.absZ ?? 0) >= 1.5) band = "Elevated evidence";
  else if ((ctx.conf ?? 0) >= 0.55 && (ctx.absZ ?? 0) >= 1.0) band = "Moderate evidence";

  return `${band}: ${evidence.join(", ")}.`;
}

export function _safeToIgnore(r) {
  const persona = _readAlertPersona();
  const ctx = _buildAlertContext(r);
  const entry = _resolveAlertDefinition(ctx);

  if (ctx.status === "resolved") return "Already resolved.";
  if (ctx.acked) return "Not yet — acknowledged only. Keep watching until it clears.";

  return _pickVariant(entry?.safeIgnore, persona, ctx) || "No.";
}

export function _ifNothingChanges(r) {
  const persona = _readAlertPersona();
  const ctx = _buildAlertContext(r);
  const entry = _resolveAlertDefinition(ctx);
  return _pickVariant(entry?.noChange, persona, ctx) || "No material impact expected.";
}

export function _stepsForAlert(r) {
  const persona = _readAlertPersona();
  const ctx = _buildAlertContext(r);
  const entry = _resolveAlertDefinition(ctx);
  const steps = _pickList(entry?.steps, persona, ctx);
  if (steps.length) return steps;

  return [
    "Open Why for supporting context.",
    "Confirm data freshness and drift status.",
    "If the alert repeats, investigate the affected symbol and execution path.",
  ];
}

export function _renderSteps(list) {
  return `<ol style="margin:0; padding-left: 18px;">${(list || []).map(s => `<li>${esc(s)}</li>`).join("")}</ol>`;
}

export function _findSimilarAlerts(row, all) {
  return (all || [])
    .filter(a =>
      a.id !== row.id &&
      a.symbol === row.symbol &&
      a.severity === row.severity
    )
    .slice(0, 3);
}

let _alertsReloadTimer = null;

function _overlayAlertState(row, deps = {}) {
  const {
    isAckedLocal,
    isResolvedLocal,
  } = deps || {};
  return applyAlertLocalState([row], {
    isAcked: isAckedLocal,
    isResolved: isResolvedLocal,
  })[0] || normalizeAlert(row);
}

function _drawerStateMeta(alertRow) {
  const alert = normalizeAlert(alertRow) || {};
  if (alert.status === "resolved") {
    return { text: "RESOLVED", cls: "ok" };
  }
  if (alert.shelved) {
    return { text: "SHELVED", cls: "warn" };
  }
  if (alert.ack_expired || alert.retriggered || alert.lifecycle_state === "retriggered") {
    return { text: "RE-TRIGGERED", cls: cellColor(alert).cls };
  }
  if (alert.acked) {
    return { text: "ACKED", cls: cellColor(alert).cls };
  }
  return { text: alert.severity || "INFO", cls: cellColor(alert).cls };
}

function _setDrawerActionStatus(host, text = "", tone = "muted") {
  if (!host) return;
  host.textContent = text || "";
  host.className = [
    "metric-meta",
    "drawer-action-status",
    tone === "ok"
      ? "status-ok"
      : tone === "warn"
        ? "status-warn"
        : tone === "bad"
          ? "status-crit"
          : "status-neutral",
  ].join(" ");
}

function _scheduleAlertsReload(reloadAlerts, delayMs = 1000) {
  if (typeof reloadAlerts !== "function") return;
  clearTimeout(_alertsReloadTimer);
  _alertsReloadTimer = setTimeout(() => {
    void reloadAlerts();
  }, Math.max(0, Number(delayMs) || 0));
}

function _lifecycleLabel(state) {
  const key = String(state || "").trim().toLowerCase();
  return {
    triggered: "Triggered",
    acknowledged: "Acknowledged",
    shelved: "Shelved",
    retriggered: "Re-triggered",
    shelve_expired: "Shelving expired",
    resolved: "Resolved",
  }[key] || (key ? key.replace(/_/g, " ") : "Updated");
}

function _lifecycleTone(state, severity = "") {
  const key = String(state || "").trim().toLowerCase();
  if (key === "resolved") return "ok";
  if (key === "shelved" || key === "acknowledged") return "warn";
  if (key === "retriggered" || key === "shelve_expired") {
    return normalizeSeverity(severity) === "CRIT" ? "crit" : "high";
  }
  return cellColor({ severity }).cls;
}

function _hasLifecycleState(events, state) {
  const target = String(state || "").trim().toLowerCase();
  return events.some((item) => String(item && item.state || "").trim().toLowerCase() === target);
}

function _eventFromAlert(_alert, state, ts, reason = "", actor = "", source = "dashboard") {
  return {
    ts_ms: Number.isFinite(Number(ts)) && Number(ts) > 0 ? Number(ts) : null,
    state,
    actor,
    reason,
    source,
  };
}

export function normalizeAlertLifecycle(row, nowMs = Date.now()) {
  const alert = normalizeAlert(row) || {};
  const events = Array.isArray(alert.lifecycle) ? alert.lifecycle.map((item) => ({ ...item })) : [];
  if (!_hasLifecycleState(events, "triggered")) {
    events.push(_eventFromAlert(alert, "triggered", alert.ts_ms || alert.ts, alert.message || alert.event_title || "alert triggered", "system", "alerts"));
  }
  if (alert.acked && !_hasLifecycleState(events, "acknowledged")) {
    events.push(_eventFromAlert(alert, "acknowledged", alert.acked_ts_ms, alert.ack_reason || "acknowledged", alert.acked_by || "operator", alert.ack_source || "dashboard"));
  }
  if ((alert.ack_expired || alert.retriggered) && !_hasLifecycleState(events, "retriggered")) {
    events.push(_eventFromAlert(alert, "retriggered", alert.ack_expires_ts_ms || nowMs, "ack timeout expired before resolution", alert.acked_by || "", "alert_lifecycle"));
  }
  if (alert.shelved && !_hasLifecycleState(events, "shelved")) {
    events.push(_eventFromAlert(alert, "shelved", alert.shelved_ts_ms, alert.shelve_reason || "shelved", alert.shelved_by || "operator", alert.shelve_source || "dashboard"));
  }
  if (alert.shelve_expired && !_hasLifecycleState(events, "shelve_expired")) {
    events.push(_eventFromAlert(alert, "shelve_expired", alert.shelve_expires_ts_ms || nowMs, "shelving expired before resolution", alert.shelved_by || "", "alert_lifecycle"));
  }
  if (alert.status === "resolved" && !_hasLifecycleState(events, "resolved")) {
    events.push(_eventFromAlert(alert, "resolved", alert.resolved_ts_ms || nowMs, alert.resolved_reason || "resolved", alert.resolved_by || "operator", alert.resolve_source || "dashboard"));
  }
  return events
    .map((item) => ({
      ...item,
      state: String(item.state || item.lifecycle_state || "updated").trim().toLowerCase(),
      ts_ms: Number.isFinite(Number(item.ts_ms)) && Number(item.ts_ms) > 0 ? Number(item.ts_ms) : null,
      actor: String(item.actor || item.owner || ""),
      reason: String(item.reason || ""),
      source: String(item.source || ""),
    }))
    .sort((a, b) => Number(a.ts_ms || 0) - Number(b.ts_ms || 0));
}

export function alertLifecycleSummary(row, nowMs = Date.now()) {
  const alert = normalizeAlert(row) || {};
  if (alert.status === "resolved") {
    return `Resolved${alert.resolved_reason ? `: ${alert.resolved_reason}` : ""}.`;
  }
  if (alert.shelved) {
    const until = alert.shelve_expires_ts_ms ? fmtTime(alert.shelve_expires_ts_ms) : "expiry unavailable";
    return `Shelved until ${until}${alert.shelve_reason ? `: ${alert.shelve_reason}` : ""}.`;
  }
  if (alert.ack_expired || alert.retriggered) {
    return "Acknowledgement timed out while unresolved; escalation resumes now.";
  }
  if (alert.acked) {
    const until = alert.ack_expires_ts_ms ? fmtTime(alert.ack_expires_ts_ms) : "timeout unavailable";
    return `Acknowledged until ${until}; unresolved alerts re-escalate after the timeout.`;
  }
  const next = Number(alert.next_escalation_ts_ms || 0);
  if (Number.isFinite(next) && next > nowMs) {
    return `Triggered; next escalation window starts ${fmtTime(next)}.`;
  }
  return "Triggered; severity-aware notifications are active.";
}

export function notificationPolicySummary(row, nowMs = Date.now()) {
  const alert = normalizeAlert(row) || {};
  const policy = alert.notification_policy && typeof alert.notification_policy === "object"
    ? alert.notification_policy
    : {};
  const rateLimitMs = Number(policy.rate_limit_ms);
  const rate = Number.isFinite(rateLimitMs) && rateLimitMs > 0
    ? `${Math.round(rateLimitMs / 60000)}m rate limit`
    : "rate limit unavailable";
  const next = Number(policy.next_escalation_ts_ms || alert.next_escalation_ts_ms || 0);
  const nextText = Number.isFinite(next) && next > 0
    ? (next <= nowMs ? "next escalation is due now" : `next escalation ${fmtTime(next)}`)
    : "next escalation not scheduled";
  const suppressed = policy.suppressed || alert.shelved || alert.status === "resolved"
    ? "suppressed"
    : "active";
  const explanation = String(policy.explanation || alertLifecycleSummary(alert, nowMs));
  return `${normalizeSeverity(policy.severity || alert.severity)} notifications ${suppressed}; ${rate}; ${nextText}. ${explanation}`;
}

function _renderAlertLifecycle(row) {
  const alert = normalizeAlert(row) || {};
  const events = normalizeAlertLifecycle(alert);
  if (!events.length) return "<div class='metric-meta'>No lifecycle events recorded.</div>";
  return `
    <ol class="alertLifecycleList">
      ${events.map((item) => {
        const tone = _lifecycleTone(item.state, alert.severity);
        const time = item.ts_ms ? fmtTime(item.ts_ms) : "time unavailable";
        const actor = item.actor ? ` by ${item.actor}` : "";
        const reason = item.reason ? `<div class="metric-meta">${esc(item.reason)}</div>` : "";
        return `
          <li class="alertLifecycleItem" data-state="${esc(item.state)}">
            <span class="alertLifecycleMarker ${tone}" aria-hidden="true">${esc(_lifecycleLabel(item.state).slice(0, 1).toUpperCase())}</span>
            <div>
              <div><strong>${esc(_lifecycleLabel(item.state))}</strong><span class="metric-meta"> ${esc(time)}${esc(actor)}</span></div>
              ${reason}
            </div>
          </li>
        `;
      }).join("")}
    </ol>
  `;
}

export async function openIncidentDrawer(row, deps) {
  const {
    fetchJSON,
    getLastAlerts,
    postUiInteraction,
    ackAlert,
    shelveAlert,
    resolveAlert,
    reloadAlerts,
    isAckedLocal,
    isResolvedLocal,
  } = deps;

  const fallbackRow = _overlayAlertState(row, { isAckedLocal, isResolvedLocal });
  if (!fallbackRow) return;

  window.__ACTIVE_INCIDENT__ = fallbackRow;

  const overlay = document.getElementById("incidentOverlay");
  if (!overlay) return;

  let alertRow = fallbackRow;
  let ex = null;

  try {
    const res = await fetchJSON(`/api/alerts/by_id?id=${encodeURIComponent(fallbackRow.id)}`);
    const detail = normalizeAlertDetailPayload(res, fallbackRow);
    if (detail) {
      alertRow = detail;
      try {
        ex = alertRow.explain_json ? JSON.parse(alertRow.explain_json) : null;
      } catch {
        ex = null;
      }
    }
  } catch {}

  const title = document.getElementById("drawerTitle");
  const sub = document.getElementById("drawerSubtitle");
  const meaning = document.getElementById("drawerMeaning");
  const steps = document.getElementById("drawerSteps");
  const facts = document.getElementById("drawerFacts");
  const raw = document.getElementById("drawerRaw");
  const posture = document.getElementById("drawerPosture");
  const decision = document.getElementById("drawerDecisionConfidence");
  const ignore = document.getElementById("drawerSafeIgnore");
  const future = document.getElementById("drawerNoChange");
  const whyPosture = document.getElementById("drawerWhyPosture");
  const statePill = document.getElementById("drawerAlertState");
  const ackBtn = document.getElementById("btnIncidentAck");
  const shelveBtn = document.getElementById("btnIncidentShelve");
  const resolveBtn = document.getElementById("btnIncidentResolve");
  const actionStatus = document.getElementById("drawerActionStatus");
  const lifecycleEl = document.getElementById("drawerLifecycle");
  const notificationEl = document.getElementById("drawerNotificationPolicy");
  const simEl = document.getElementById("drawerSimilar");
  let actionPending = false;

  const renderDrawer = () => {
    alertRow = _overlayAlertState(alertRow, { isAckedLocal, isResolvedLocal }) || alertRow;
    window.__ACTIVE_INCIDENT__ = alertRow;

    const titleSymbol = alertRow.symbol || "SYSTEM";
    const titleMessage = alertRow.message || alertRow.event_title || "Alert";
    const tsLabel = alertRow.ts ? fmtTime(alertRow.ts) : "timestamp unavailable";
    const stateMeta = _drawerStateMeta(alertRow);

    if (title) title.textContent = `${alertRow.severity} • ${titleSymbol}`;
    if (sub) {
      sub.textContent = `${titleMessage} • ${tsLabel}${alertRow.status === "resolved" ? " • resolved" : ""}`;
    }
    if (statePill) {
      statePill.className = `pill ${stateMeta.cls}`;
      statePill.textContent = stateMeta.text;
    }
    if (meaning) meaning.textContent = _meaningForAlert(alertRow);
    if (steps) steps.innerHTML = _renderSteps(_stepsForAlert(alertRow));

    if (posture) posture.textContent = _recommendedPosture(alertRow);
    if (decision) decision.textContent = _decisionConfidence(alertRow);
    if (ignore) ignore.textContent = _safeToIgnore(alertRow);
    if (future) future.textContent = _ifNothingChanges(alertRow);
    if (whyPosture) {
      whyPosture.textContent = _alertWhyPosture(alertRow);
    }
    if (lifecycleEl) {
      lifecycleEl.innerHTML = _renderAlertLifecycle(alertRow);
    }
    if (notificationEl) {
      notificationEl.textContent = notificationPolicySummary(alertRow);
    }

    if (facts) {
      const z = Number(alertRow.expected_z);
      const conf = Number(alertRow.confidence);
      const confRaw = Number(alertRow.confidence_raw);
      const strength = Number(alertRow.prediction_strength);
      const ageMin = Number.isFinite(Number(alertRow.ts))
        ? Math.max(0, Math.floor((Date.now() - Number(alertRow.ts)) / 60000))
        : null;
      const nextEscalation = Number(alertRow.next_escalation_ts_ms || 0);
      const shelfExpiry = Number(alertRow.shelve_expires_ts_ms || 0);

      facts.innerHTML = `
        <div class="kvK">symbol</div><div class="kvV">${esc(titleSymbol)}</div>
        <div class="kvK">severity</div><div class="kvV">${esc(alertRow.severity)}</div>
        <div class="kvK">rule_id</div><div class="kvV">${esc(alertRow.rule_id || "—")}</div>
        <div class="kvK">status</div><div class="kvV">${esc(alertRow.status)}</div>
        <div class="kvK">lifecycle</div><div class="kvV">${esc(alertLifecycleSummary(alertRow))}</div>
        <div class="kvK">next escalation</div><div class="kvV">${Number.isFinite(nextEscalation) && nextEscalation > 0 ? esc(fmtTime(nextEscalation)) : "—"}</div>
        <div class="kvK">shelving expiry</div><div class="kvV">${Number.isFinite(shelfExpiry) && shelfExpiry > 0 ? esc(fmtTime(shelfExpiry)) : "—"}</div>
        <div class="kvK">horizon_s</div><div class="kvV">${esc(alertRow.horizon_s)}</div>
        <div class="kvK">expected_z</div><div class="kvV">${Number.isFinite(z) ? z.toFixed(3) : "—"}</div>
        <div class="kvK">confidence</div><div class="kvV">${Number.isFinite(conf) ? conf.toFixed(2) : "—"}</div>
        <div class="kvK">confidence_raw</div><div class="kvV">${Number.isFinite(confRaw) ? confRaw.toFixed(2) : "—"}</div>
        <div class="kvK">prediction_strength</div><div class="kvV">${Number.isFinite(strength) ? strength.toFixed(3) : "—"}</div>
        <div class="kvK">time</div><div class="kvV">${esc(tsLabel)}</div>
        <div class="kvK">age</div><div class="kvV">${ageMin == null ? "—" : `${ageMin}m`}</div>
        <div class="kvK">message</div><div class="kvV">${esc(titleMessage)}</div>
        <div class="kvK">reason</div><div class="kvV">${esc(alertRow.reason || "")}</div>
      `;
    }

    const similar = _findSimilarAlerts(alertRow, getLastAlerts());
    if (simEl) {
      simEl.innerHTML = similar.length
        ? similar.map(s =>
            `<div class="metric-meta mono">${fmtTime(s.ts || s.ts_ms)} • z=${Number.isFinite(Number(s.expected_z)) ? Number(s.expected_z).toFixed(2) : "—"} • c=${Number.isFinite(Number(s.confidence)) ? Number(s.confidence).toFixed(2) : "—"} • raw=${Number.isFinite(Number(s.confidence_raw)) ? Number(s.confidence_raw).toFixed(2) : "—"} • ps=${Number.isFinite(Number(s.prediction_strength)) ? Number(s.prediction_strength).toFixed(2) : "—"}</div>`
          ).join("")
        : "<div class='metric-meta'>No similar recent incidents</div>";
    }

    if (ackBtn) {
      ackBtn.disabled = actionPending || alertRow.status === "resolved" || !!alertRow.acked || !!alertRow.shelved;
      ackBtn.textContent = alertRow.status === "resolved"
        ? "Resolved"
        : alertRow.shelved
          ? "Shelved"
        : alertRow.acked
          ? "Acknowledged"
          : "Acknowledge";
    }
    if (shelveBtn) {
      shelveBtn.disabled = actionPending || alertRow.status === "resolved" || !!alertRow.shelved;
      shelveBtn.textContent = alertRow.status === "resolved"
        ? "Resolved"
        : alertRow.shelved
          ? "Shelved"
          : "Shelve";
    }
    if (resolveBtn) {
      resolveBtn.disabled = actionPending || alertRow.status === "resolved";
      resolveBtn.textContent = alertRow.status === "resolved" ? "Resolved" : "Resolve";
    }
  };

  if (raw) {
    if (ex) raw.textContent = JSON.stringify(ex, null, 2);
    else raw.textContent = "(no explain_json on this alert)";
  }

  _setDrawerActionStatus(actionStatus, "");
  renderDrawer();

  if (ackBtn) {
    ackBtn.onclick = async () => {
      if (actionPending || typeof ackAlert !== "function" || alertRow.status === "resolved" || alertRow.acked || alertRow.shelved) return;
      actionPending = true;
      _setDrawerActionStatus(actionStatus, "Saving acknowledgement…");
      renderDrawer();
      const result = await ackAlert(alertRow);
      if (result && result.ok) {
        alertRow = {
          ...alertRow,
          acked: true,
          acked_by: result.persistence === "local" ? "local" : (alertRow.acked_by || "operator"),
          acked_ts_ms: result.acked_ts_ms || alertRow.acked_ts_ms || Date.now(),
          ack_expires_ts_ms: result.expires_ts_ms || alertRow.ack_expires_ts_ms || null,
          ack_expired: false,
          retriggered: false,
          lifecycle_state: "acknowledged",
          next_escalation_ts_ms: result.expires_ts_ms || alertRow.next_escalation_ts_ms || null,
        };
        renderDrawer();
        _setDrawerActionStatus(
          actionStatus,
          result.persistence === "local"
            ? "Server unavailable. Acknowledged locally for this browser."
            : "Acknowledged. Syncing dashboard…",
          result.persistence === "local" ? "warn" : "ok"
        );
        _scheduleAlertsReload(reloadAlerts, result.persistence === "local" ? 80 : 1000);
      } else if (!(result && result.blocked)) {
        _setDrawerActionStatus(actionStatus, result && result.error ? result.error : "Acknowledge failed.", "bad");
      }
      actionPending = false;
      renderDrawer();
    };
  }

  if (shelveBtn) {
    shelveBtn.onclick = async () => {
      if (actionPending || typeof shelveAlert !== "function" || alertRow.status === "resolved" || alertRow.shelved) return;
      actionPending = true;
      _setDrawerActionStatus(actionStatus, "Saving shelving state…");
      renderDrawer();
      const result = await shelveAlert(alertRow);
      if (result && result.ok) {
        alertRow = {
          ...alertRow,
          shelved: true,
          shelved_by: result.persistence === "local" ? "local" : (alertRow.shelved_by || "operator"),
          shelved_ts_ms: result.shelved_ts_ms || alertRow.shelved_ts_ms || Date.now(),
          shelve_expires_ts_ms: result.expires_ts_ms || alertRow.shelve_expires_ts_ms || null,
          shelve_reason: result.reason || alertRow.shelve_reason || "shelved in dashboard",
          lifecycle_state: "shelved",
          next_escalation_ts_ms: result.expires_ts_ms || alertRow.next_escalation_ts_ms || null,
        };
        renderDrawer();
        _setDrawerActionStatus(
          actionStatus,
          result.persistence === "local"
            ? "Server unavailable. Shelved locally for this browser."
            : "Shelved. Syncing dashboard…",
          result.persistence === "local" ? "warn" : "ok"
        );
        _scheduleAlertsReload(reloadAlerts, result.persistence === "local" ? 80 : 1000);
      } else if (!(result && result.blocked)) {
        _setDrawerActionStatus(actionStatus, result && result.error ? result.error : "Shelve failed.", "bad");
      }
      actionPending = false;
      renderDrawer();
    };
  }

  if (resolveBtn) {
    resolveBtn.onclick = async () => {
      if (actionPending || typeof resolveAlert !== "function" || alertRow.status === "resolved") return;
      actionPending = true;
      _setDrawerActionStatus(actionStatus, "Saving resolution…");
      renderDrawer();
      const result = await resolveAlert(alertRow);
      if (result && result.ok) {
        alertRow = {
          ...alertRow,
          status: "resolved",
          resolved: true,
          resolved_reason: result.persistence === "local"
            ? "local fallback"
            : (alertRow.resolved_reason || "resolved in dashboard"),
          resolved_ts_ms: result.resolved_ts_ms || alertRow.resolved_ts_ms || Date.now(),
          lifecycle_state: "resolved",
        };
        renderDrawer();
        _setDrawerActionStatus(
          actionStatus,
          result.persistence === "local"
            ? "Server unavailable. Resolution saved locally for this browser."
            : "Resolved. Syncing dashboard…",
          result.persistence === "local" ? "warn" : "ok"
        );
        _scheduleAlertsReload(reloadAlerts, result.persistence === "local" ? 80 : 1000);
      } else if (!(result && result.blocked)) {
        _setDrawerActionStatus(actionStatus, result && result.error ? result.error : "Resolve failed.", "bad");
      }
      actionPending = false;
      renderDrawer();
    };
  }

  try {
    const sendInteraction =
      (typeof postUiInteraction === "function")
        ? postUiInteraction
        : (typeof window !== "undefined" && typeof window.__postUiInteraction === "function"
            ? window.__postUiInteraction
            : null);
    if (sendInteraction) {
      await sendInteraction({
        alert_id: alertRow.id,
        interaction_type: "alert_open",
        detail: {
          panel: "incident_drawer",
          severity: alertRow.severity || "",
          symbol: alertRow.symbol || "",
        }
      });
    }
  } catch {}

  overlay.style.display = "block";
}

export function closeIncidentDrawer() {
  const overlay = document.getElementById("incidentOverlay");
  if (overlay) overlay.style.display = "none";
  try {
    if (typeof window !== "undefined") {
      window.__ACTIVE_INCIDENT__ = null;
    }
  } catch {}
}

export async function loadAlertsUI(deps) {
  const {
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
    ackAlert,
    shelveAlert,
    resolveAlert,
    reloadAlerts,
    OPERATOR_MODE,
    openWhyModal,
    setLastAlerts,
    getLastAlerts,
    updateDecisionHeader
  } = deps;

  const data = await fetchJSON("/api/alerts/timeline?limit=50");
  const rows = applyAlertLocalState(
    normalizeAlertsPayload(data),
    {
      isAcked: _isAckedLocal,
      isResolved: _isResolvedLocal,
    }
  );

  setLastAlerts(Array.isArray(rows) ? rows : []);

  updateManipulationStateFromAlerts(getLastAlerts());

  const filtered = filterAlerts(
    getLastAlerts() || [],
    _getGlobalFilters(),
    {
      isAcked: _isAckedLocal,
      isResolved: _isResolvedLocal,
      isSnoozed: _isSnoozedLocal
    }
  );

  renderHeatmap(
    document.getElementById("alertsHeatmap"),
    filtered,
    (sym) => {
      const globalSymbolEl = document.getElementById("globalSymbol");
      if (globalSymbolEl) {
        globalSymbolEl.value = sym;
        try {
          globalSymbolEl.dispatchEvent(new Event("change", { bubbles: true }));
        } catch {}
      }
    }
  );

  renderIncidentQueue(
    document.getElementById("incidentList"),
    filtered,
    {
      onOpen: (row) => openIncidentDrawer(row, {
        fetchJSON,
        getLastAlerts,
        postUiInteraction,
        ackAlert,
        shelveAlert,
        resolveAlert,
        reloadAlerts,
        isAckedLocal: _isAckedLocal,
        isResolvedLocal: _isResolvedLocal,
      })
    }
  );

  const tbody = document.querySelector("#alerts tbody");
  if (!tbody) return;

  tbody.innerHTML = "";

  if (!filtered.length) {
    tbody.innerHTML = `<tr class="table-row"><td colspan="9" class="metric-meta">No alerts in the selected window.</td></tr>`;
    updateDecisionHeader();
    return;
  }

  for (const r of filtered) {
    const tr = document.createElement("tr");
    const ts = Number(r.ts);
    const ageMin = Number.isFinite(ts) ? Math.max(0, Math.floor((Date.now() - ts) / 60000)) : null;

    const z = Number(r.expected_z);
    const conf = Number(r.confidence);
    const color = cellColor(r);
    const symbolLabel = r.symbol === "EXECUTION" ? "Execution" : (r.symbol || "—");

    const impactWord =
      (Math.abs(z) >= 2.5) ? "Very strong" :
      (Math.abs(z) >= 1.5) ? "Strong" :
      (Math.abs(z) >= 0.8) ? "Moderate" :
      "Weak";

    const confWord =
      (conf >= 0.85) ? "High" :
      (conf >= 0.65) ? "Medium" :
      "Low";

    tr.className = "table-row clickable-row";
    tr.tabIndex = 0;
    tr.innerHTML = `
      <td class="mono metric-meta">${Number.isFinite(ts) ? fmtTime(ts) : "—"}</td>
      <td>
<div class="pill ${
  r.status === "resolved"
    ? "ok"
    : color.cls
}">
  ${r.status === "resolved"
    ? "RESOLVED"
    : r.acked
      ? "ACKED"
      : r.severity}
</div>
      </td>
      <td>${escapeHTML(symbolLabel)}</td>
      <td class="table-cell-num">${r.horizon_s != null && String(r.horizon_s).trim() !== "" ? `${escapeHTML(String(r.horizon_s))}s` : "—"}</td>
      <td class="table-cell-num">${OPERATOR_MODE ? impactWord : (Number.isFinite(z) ? z.toFixed(2) : "—")}</td>
      <td class="table-cell-num">${OPERATOR_MODE ? confWord : (Number.isFinite(conf) ? conf.toFixed(2) : "—")}</td>
      <td>
        ${escapeHTML(r.message || r.event_title || "Alert")}
        ${r.reason ? `<div class="metric-meta">${esc(r.reason)}</div>` : ""}
      </td>
      <td class="mono metric-meta">
${ageMin == null ? "—" : `${ageMin}m ago`}${
  r.status === "resolved"
    ? ` • RESOLVED${r.resolved_reason ? ` (${r.resolved_reason})` : ""}`
    : r.acked
      ? ` • ACKED by ${r.acked_by || "?"}`
      : ""
}
</td>
      <td>
        <button class="btn btnSmall" data-why="${escapeHTML(String(r.id))}">Why</button>
      </td>
    `;

    tr.addEventListener("click", () => {
      void openIncidentDrawer(r, {
        fetchJSON,
        getLastAlerts,
        postUiInteraction,
        ackAlert,
        shelveAlert,
        resolveAlert,
        reloadAlerts,
        isAckedLocal: _isAckedLocal,
        isResolvedLocal: _isResolvedLocal,
      });
    });
    tr.addEventListener("keydown", (event) => {
      if (!event) return;
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        void openIncidentDrawer(r, {
          fetchJSON,
          getLastAlerts,
          postUiInteraction,
          ackAlert,
          shelveAlert,
          resolveAlert,
          reloadAlerts,
          isAckedLocal: _isAckedLocal,
          isResolvedLocal: _isResolvedLocal,
        });
      }
    });
    tbody.appendChild(tr);
  }

  if (tbody) tbody.scrollTop = 0;

  tbody.querySelectorAll("button[data-why]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const id = String(btn.getAttribute("data-why") || "");
      const row = getLastAlerts().find(a => String(a.id) === id);
      if (row) openWhyModal(row);
    });
  });

  updateDecisionHeader();
}
