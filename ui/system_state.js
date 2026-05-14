/*
  FILE: ui/system_state.js

  System-state header renderer for the dashboard. This module converts raw
  runtime status payloads into a compact visual headline and keeps duplicate
  header nodes from drifting out of sync.
*/

export function renderSystemStatusHeader(status, el = document.getElementById("system-status-header")) {
  const allHeaderNodes = Array.from(document.querySelectorAll("#system-status-header"));
  if (!el) {
    el = allHeaderNodes[0] || null;
  }
  if (!el) return;

  if (allHeaderNodes.length > 1) {
    for (const extra of allHeaderNodes.slice(1)) {
      if (extra && extra.parentNode) {
        extra.parentNode.removeChild(extra);
      }
    }
  }

  const safeStatus = status || {};

  const asUpper = (...values) => {
    for (const value of values) {
      if (value === undefined || value === null) continue;
      const s = String(value).trim();
      if (s) return s.toUpperCase();
    }
    return "UNKNOWN";
  };

  const asNumber = (...values) => {
    for (const value of values) {
      const n = Number(value);
      if (Number.isFinite(n)) return n;
    }
    return null;
  };

  const asBool = (...values) => {
    for (const value of values) {
      if (typeof value === "boolean") return value;
      if (value === 1 || value === "1" || value === "true" || value === "TRUE") return true;
      if (value === 0 || value === "0" || value === "false" || value === "FALSE") return false;
    }
    return false;
  };

  const dataStatus = asUpper(
    safeStatus.data_status,
    safeStatus.dataState,
    safeStatus.ingestion_status,
    safeStatus.ingestionState
  );

  const engineState = asUpper(
    safeStatus.engine_state,
    safeStatus.engineState,
    safeStatus.system_state,
    safeStatus.systemState
  );

  const tradingMode = asUpper(
    safeStatus.trading_mode,
    safeStatus.tradingMode,
    safeStatus.mode,
    safeStatus.execution_mode,
    safeStatus.executionMode
  );

  const executionEnabled = asBool(
    safeStatus.execution_enabled,
    safeStatus.executionEnabled,
    safeStatus.allowed,
    safeStatus.enabled
  );

  const marketDataLatencyMs = asNumber(
    safeStatus.market_data_latency_ms,
    safeStatus.marketDataLatencyMs,
    safeStatus.latency_ms,
    safeStatus.latencyMs,
    safeStatus.price_age_ms,
    safeStatus.prices_age_ms
  );

  const brokerConnectivity = asUpper(
    safeStatus.broker_connectivity,
    safeStatus.brokerConnectivity,
    safeStatus.broker_status,
    safeStatus.brokerStatus
  );

  const alertCountNum = asNumber(
    safeStatus.alert_count,
    safeStatus.alertCount,
    safeStatus.alerts,
    safeStatus.unresolved_alerts
  );

  const alertCount = alertCountNum === null ? "—" : String(Math.max(0, Math.round(alertCountNum)));

  const todayPnl = asNumber(
    safeStatus.today_pnl,
    safeStatus.todayPnl,
    safeStatus.daily_pnl,
    safeStatus.day_pnl
  );

  const grossExposure = asNumber(
    safeStatus.gross_exposure,
    safeStatus.grossExposure
  );

  const uiMetricsDegraded = asBool(
    safeStatus.ui_metrics_degraded,
    safeStatus.uiMetricsDegraded
  );

  const dataClass =
    dataStatus === "RUNNING" || dataStatus === "CONNECTED" || dataStatus === "OK"
      ? "status_ok"
      : dataStatus === "DEGRADED" || dataStatus === "WARMING_UP" || dataStatus === "UNKNOWN"
        ? "status_warning"
        : "status_error";

  const engineClass =
    engineState === "LIVE" || engineState === "RUNNING" || engineState === "OK"
      ? "status_ok"
      : engineState === "DEGRADED" || engineState === "WARMING_UP" || engineState === "BOOTING" || engineState === "UNKNOWN"
        ? "status_warning"
        : "status_error";

  const modeClass =
    tradingMode === "LIVE"
      ? "status_ok"
      : tradingMode === "SAFE" || tradingMode === "SHADOW" || tradingMode === "UNKNOWN"
        ? "status_warning"
        : "status_error";

  const executionClass = executionEnabled ? "status_ok" : "status_warning";

  const latencyValue =
    marketDataLatencyMs !== null
      ? `${Math.round(marketDataLatencyMs)} ms`
      : "—";

  const latencyClass =
    marketDataLatencyMs === null
      ? "status_warning"
      : marketDataLatencyMs <= 1000
        ? "status_ok"
        : marketDataLatencyMs <= 5000
          ? "status_warning"
          : "status_error";

  const brokerClass =
    brokerConnectivity === "CONNECTED" || brokerConnectivity === "OK"
      ? "status_ok"
      : brokerConnectivity === "DEGRADED" || brokerConnectivity === "UNKNOWN"
        ? "status_warning"
        : "status_error";

  const alertsClass =
    alertCountNum === null
      ? "status_warning"
      : alertCountNum === 0
        ? "status_ok"
        : alertCountNum <= 3
          ? "status_warning"
          : "status_error";

  const pnlValue =
    todayPnl === null
      ? "—"
      : `${todayPnl > 0 ? "+" : todayPnl < 0 ? "-" : ""}$${Math.abs(todayPnl).toFixed(2)}`;

  const pnlClass =
    todayPnl === null || uiMetricsDegraded
      ? "status_warning"
      : todayPnl >= 0
        ? "status_ok"
        : "status_error";

  const exposureValue =
    grossExposure === null
      ? "—"
      : `${(grossExposure * 100).toFixed(1)}%`;

  const exposureClass =
    grossExposure === null || uiMetricsDegraded
      ? "status_warning"
      : grossExposure <= 1
        ? "status_ok"
        : "status_error";

  el.innerHTML = `
    <div class="systemStatusBar" role="status" aria-live="polite">
      <div class="systemStatusItem ${dataClass}">
        <span class="systemStatusLabel">DATA</span>
        <span class="systemStatusValue">${dataStatus}</span>
      </div>
      <div class="systemStatusItem ${engineClass}">
        <span class="systemStatusLabel">ENGINE</span>
        <span class="systemStatusValue">${engineState}</span>
      </div>
      <div class="systemStatusItem ${modeClass}">
        <span class="systemStatusLabel">MODE</span>
        <span class="systemStatusValue">${tradingMode}</span>
      </div>
      <div class="systemStatusItem ${executionClass}">
        <span class="systemStatusLabel">EXECUTION</span>
        <span class="systemStatusValue">${executionEnabled ? "ENABLED" : "DISABLED"}</span>
      </div>
      <div class="systemStatusItem ${latencyClass}">
        <span class="systemStatusLabel">LATENCY</span>
        <span class="systemStatusValue">${latencyValue}</span>
      </div>
      <div class="systemStatusItem ${brokerClass}">
        <span class="systemStatusLabel">BROKER</span>
        <span class="systemStatusValue">${brokerConnectivity}</span>
      </div>
      <div class="systemStatusItem ${alertsClass}">
        <span class="systemStatusLabel">ALERTS</span>
        <span class="systemStatusValue">${alertCount}</span>
      </div>
      <div class="systemStatusItem ${pnlClass}">
        <span class="systemStatusLabel">TODAY PNL</span>
        <span class="systemStatusValue">${pnlValue}</span>
      </div>
      <div class="systemStatusItem ${exposureClass}">
        <span class="systemStatusLabel">GROSS EXP</span>
        <span class="systemStatusValue">${exposureValue}</span>
      </div>
    </div>
  `;
}

function _startupStepClass(ok, blocked = false) {
  if (blocked) return "startupStep startupStepBlocked";
  return ok ? "startupStep startupStepOk" : "startupStep startupStepPending";
}

function _startupPill(label, ok) {
  return `<span class="pill ${ok ? "ok" : "bad"}">${label}: ${ok ? "OK" : "WAIT"}</span>`;
}

export function renderOperatorStartupPanel(
  startup,
  {
    checklistEl = document.getElementById("operatorStartupChecklist"),
    summaryEl = document.getElementById("operatorStartupSummary"),
    headlineEl = document.getElementById("operatorStartupHeadline"),
    rawEl = document.getElementById("operatorStartupRaw"),
    progressEl = document.getElementById("operatorStartupProgressBar"),
  } = {}
) {
  if (!checklistEl || !summaryEl || !headlineEl || !rawEl || !progressEl) return;

  const safe = startup || {};
  const steps = Array.isArray(safe.steps) ? safe.steps : [];
  const blockers = Array.isArray(safe.blockers) ? safe.blockers.filter(Boolean) : [];
  const completed = steps.filter(s => !!(s && s.ok)).length;
  const total = steps.length || 8;
  const progressPct = Math.max(0, Math.min(100, Math.round((completed / total) * 100)));

  summaryEl.innerHTML = [
    _startupPill("DATA FEED", !!safe.data_feed_ok),
    _startupPill("MODELS", !!safe.models_ok),
    _startupPill("SIDECARS", !!safe.sidecars_ok),
    _startupPill("PORTFOLIO", !!safe.portfolio_runtime_ok),
    _startupPill("EXECUTION", !!safe.execution_health_ok),
    _startupPill("RISK", !!safe.risk_ok),
    _startupPill("BROKER", !!safe.broker_ok),
  ].join("");

  headlineEl.textContent = safe.ready
    ? "All readiness gates passed. Trading can be enabled."
    : (blockers.length
      ? `Startup readiness blocked — ${blockers.slice(0, 2).join(" | ")}`
      : `Startup readiness incomplete — ${completed}/${total} steps passed.`);

  progressEl.style.width = `${progressPct}%`;
  progressEl.className =
    "operatorStartupProgressBar " + (safe.ready ? "operatorStartupProgressBarOk" : "operatorStartupProgressBarWarn");

  checklistEl.innerHTML = steps.map((step, idx) => `
    <div class="${_startupStepClass(!!step.ok, !!step.blocked)}">
      <div class="startupStepIndex">STEP ${idx + 1}</div>
      <div class="startupStepBody">
        <div class="startupStepTitle">${step.label || "Unknown step"}</div>
        <div class="startupStepDetail">${step.detail || "—"}</div>
      </div>
      <div class="startupStepStatus">${step.ok ? "OK" : (step.blocked ? "BLOCKED" : "WAIT")}</div>
    </div>
  `).join("");

  rawEl.textContent = JSON.stringify(safe.raw || safe, null, 2);
}

export function renderSystemState(state, el, banner) {

  if (!el) return;

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
  }

  el.textContent = lines.join("\n");

  if (!banner) return;

  banner.textContent = state.state || "UNKNOWN";

  banner.className = "pill ";

  if (state.state === "LIVE") banner.className += "ok";
  else if (state.state === "DEGRADED") banner.className += "warn";
  else if (state.state === "KILL_SWITCH") banner.className += "crit";
  else banner.className += "dim";
}
