/*
  FILE: ui/runtime_status_summary.js

  Shared runtime-status summarizers used by the dashboard and operator console.
  These helpers translate raw health/readiness/system payloads into stable
  UI-facing pills, labels, and top-line operator guidance.
*/

function _pill(cls, label) {
  return { cls: cls || "dim", label: String(label || "—") };
}

function _safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function _dedupeStrings(values) {
  const seen = new Set();
  const out = [];
  _safeArray(values).forEach((value) => {
    const text = String(value || "").trim();
    if (!text || seen.has(text)) return;
    seen.add(text);
    out.push(text);
  });
  return out;
}

export function unwrapHealthResponse(payload) {
  if (payload && payload.body && typeof payload.body === "object") {
    return payload.body;
  }
  if (payload && typeof payload === "object") {
    return payload;
  }
  return {};
}

function _systemPill(systemState) {
  const state = String(systemState && systemState.state || "UNKNOWN");
  if (state === "LIVE") return _pill("ok", "LIVE");
  if (state === "DEGRADED") return _pill("warn", "DEGRADED");
  if (state === "KILL_SWITCH") return _pill("bad", "HALTED");
  return _pill("dim", state);
}

function _marketPill(stressPayload) {
  const score = Number(
    stressPayload &&
    stressPayload.ok &&
    stressPayload.stress &&
    stressPayload.stress.stress_score
  );
  if (!Number.isFinite(score)) return _pill("dim", "unknown");
  if (score >= 0.75) return _pill("bad", "high stress");
  if (score >= 0.55) return _pill("warn", "elevated stress");
  return _pill("ok", "normal");
}

function _trainingPill(health) {
  const training = health && health.training ? health.training : {};
  const mode = String(training.mode || "unknown").toUpperCase();
  if (training.allowed === true) return _pill("ok", mode === "UNKNOWN" ? "ACTIVE" : mode);
  if (mode === "OFF" || mode === "PAUSED") return _pill("dim", mode);
  if (mode !== "UNKNOWN") return _pill("warn", mode);
  return _pill("dim", "unknown");
}

function _tradingPill(barrier) {
  const allowed = barrier && barrier.allowed === true;
  return allowed
    ? _pill("ok", "ALLOWED")
    : _pill("bad", "BLOCKED");
}

function _moodPill({ barrier, readiness, health, stress }) {
  const allowed = barrier && barrier.allowed === true;
  const ready = readiness ? readiness.ready === true : null;
  const healthOk = health ? health.ok === true : null;
  const stressPill = _marketPill(stress);

  if (!allowed) return _pill("bad", "defensive");
  if (ready === false || healthOk === false) return _pill("warn", "guarded");
  if (stressPill.cls === "bad") return _pill("warn", "cautious");
  if (stressPill.cls === "warn") return _pill("warn", "watchful");
  return _pill("ok", "steady");
}

function _collectRuntimeBlockers({ health, readiness }) {
  const blockers = [];
  const timeseriesStorage = health && typeof health.timeseries_storage === "object" ? health.timeseries_storage : {};
  const featureStore = health && typeof health.feature_store === "object"
    ? health.feature_store
    : (timeseriesStorage && typeof timeseriesStorage.feature_store === "object" ? timeseriesStorage.feature_store : {});
  const portfolioRuntime = health && typeof health.portfolio_runtime === "object" ? health.portfolio_runtime : {};
  const executionDegraded = health && typeof health.execution_degraded === "object" ? health.execution_degraded : {};

  const timeseriesReasons = [];
  if (timeseriesStorage && timeseriesStorage.enabled && (!timeseriesStorage.ok || timeseriesStorage.degraded || timeseriesStorage.detail)) {
    if (timeseriesStorage.detail) timeseriesReasons.push(timeseriesStorage.detail);
    timeseriesReasons.push(..._safeArray(timeseriesStorage.degraded_reasons));
  }
  if (featureStore && featureStore.enabled && (!featureStore.ok || featureStore.degraded)) {
    featureStore.degraded_reasons && timeseriesReasons.push(..._safeArray(featureStore.degraded_reasons));
    if (!featureStore.ok && !_safeArray(featureStore.degraded_reasons).length) {
      timeseriesReasons.push("feature_store_not_ready");
    }
  }

  const portfolioReasons = [];
  if (portfolioRuntime && (portfolioRuntime.degraded || portfolioRuntime.ok === false)) {
    if (portfolioRuntime.detail) portfolioReasons.push(portfolioRuntime.detail);
    portfolioReasons.push(..._safeArray(portfolioRuntime.degraded_codes));
    _safeArray(portfolioRuntime.degraded_reasons).forEach((item) => {
      if (item && typeof item === "object") {
        portfolioReasons.push(item.code || item.reason || item.detail || "");
      } else {
        portfolioReasons.push(item);
      }
    });
  }

  const executionReasons = [];
  if (executionDegraded && executionDegraded.active) {
    executionReasons.push(..._safeArray(executionDegraded.reason_codes));
    if (executionDegraded.reason) executionReasons.push(executionDegraded.reason);
  }

  _dedupeStrings(timeseriesReasons).forEach((reason) => blockers.push(`Sidecars degraded: ${reason}`));
  _dedupeStrings(portfolioReasons).forEach((reason) => blockers.push(`Portfolio runtime degraded: ${reason}`));
  _dedupeStrings(executionReasons).forEach((reason) => blockers.push(`Execution degraded: ${reason}`));
  _safeArray(readiness && readiness.waiting_on).forEach((item) => blockers.push(`Readiness waiting on: ${item}`));

  return {
    blockers: _dedupeStrings(blockers),
    details: {
      sidecarsDegraded: _dedupeStrings(timeseriesReasons).length > 0,
      portfolioRuntimeDegraded: _dedupeStrings(portfolioReasons).length > 0,
      executionDegraded: _dedupeStrings(executionReasons).length > 0,
    },
  };
}

function _headlineSummary({ system, trading, readiness, health, market, training, blockers, blockerDetails }) {
  if (blockers.length && (blockerDetails.sidecarsDegraded || blockerDetails.portfolioRuntimeDegraded || blockerDetails.executionDegraded)) {
    return {
      headline: "Critical runtime blockers are active",
      meaning: blockers.slice(0, 3).join(" • "),
      next: [
        "Open the Operate screen and inspect the blocked startup steps.",
        "Review notification status and recent runtime health transitions.",
        "Keep trading blocked until the degraded runtime components recover."
      ]
    };
  }

  if (trading.cls === "bad") {
    return {
      headline: "Trading is blocked",
      meaning: "Order placement is disabled by safety controls.",
      next: [
        "Open the Operate screen and inspect the execution barrier.",
        "Review recent alerts before changing risk controls.",
        "Use the Operator Console only if a repair action is required."
      ]
    };
  }

  if (system.label === "DEGRADED" || (health && health.ok === false) || (readiness && readiness.ready === false)) {
    return {
      headline: "System is protecting itself",
      meaning: "Runtime checks detected instability or unmet readiness gates.",
      next: [
        "Review readiness blockers and service health.",
        "Check alerts and market stress before enabling more risk.",
        "Keep trading conservative until health returns to normal."
      ]
    };
  }

  if (market.cls === "bad" || market.cls === "warn") {
    return {
      headline: "System running with elevated caution",
      meaning: "Execution is available, but current market conditions are less favorable.",
      next: [
        "Monitor alerts and execution advisories.",
        "Prefer lower-risk interventions unless conditions improve.",
        "Watch stress and barrier state for further deterioration."
      ]
    };
  }

  return {
    headline: "System running normally",
    meaning: training.cls === "ok"
      ? "No immediate action required. Training and trading are both available."
      : "No immediate action required.",
    next: [
      "Check Alerts if anything looks unusual.",
      "Monitor market stress and execution advisories.",
      "Only unlock advanced controls if you need to intervene."
    ]
  };
}

export function summarizeRuntimeStatus({
  systemState = null,
  stressPayload = null,
  barrierPayload = null,
  healthPayload = null,
  readinessPayload = null,
  engineStatus = null,
} = {}) {
  const health = unwrapHealthResponse(healthPayload);
  const system = _systemPill(systemState || {});
  const trading = _tradingPill(barrierPayload || health.execution_barrier || {});
  const market = _marketPill(stressPayload || {});
  const training = _trainingPill(health);
  const mood = _moodPill({
    barrier: barrierPayload || health.execution_barrier || {},
    readiness: readinessPayload || null,
    health,
    stress: stressPayload || null,
  });
  const runtimeBlockers = _collectRuntimeBlockers({
    health,
    readiness: readinessPayload || null,
  });
  const summary = _headlineSummary({
    system,
    trading,
    readiness: readinessPayload || null,
    health,
    market,
    training,
    blockers: runtimeBlockers.blockers,
    blockerDetails: runtimeBlockers.details,
  });

  const readinessHealth = unwrapHealthResponse(readinessPayload && readinessPayload.health);
  const healthHttpOk = !!(readinessPayload && readinessPayload.health && typeof readinessPayload.health === "object");
  const healthBodyOk = readinessHealth.ok === true;
  const engineRunning = String(engineStatus && engineStatus.status || "") === "RUNNING";

  let healthIndicator = { state: "fail", text: "Health: n/a" };
  if (healthBodyOk) {
    healthIndicator = {
      state: "ok",
      text: `Health: OK (${String(readinessHealth.status || "ready")})`
    };
  } else if (healthHttpOk || engineRunning) {
    healthIndicator = {
      state: "warn",
      text: healthHttpOk ? "Health: WARMING UP" : "Health: UNREACHABLE"
    };
  }

  let dataIndicator = { state: "fail", text: "Data: n/a" };
  if (!engineRunning) {
    dataIndicator = { state: "fail", text: "Data: n/a" };
  } else if (healthBodyOk) {
    dataIndicator = { state: "ok", text: "Data: FLOWING" };
  } else if (healthHttpOk) {
    dataIndicator = { state: "warn", text: "Data: WARMING UP" };
  } else {
    dataIndicator = { state: "warn", text: "Data: WAITING FOR DASHBOARD" };
  }

  return {
    pills: {
      system,
      trading,
      training,
      market,
      mood,
    },
    headline: summary.headline,
    meaning: summary.meaning,
    next: summary.next,
    indicators: {
      health: healthIndicator,
      data: dataIndicator,
    },
    blockers: runtimeBlockers.blockers,
    health,
  };
}
