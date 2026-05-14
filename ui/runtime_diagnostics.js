/*
  FILE: ui/runtime_diagnostics.js

  Shared diagnostic-model builders for dashboard and operator surfaces.
  These helpers transform raw API payloads into stable UI-ready summaries.
*/

import { unwrapHealthResponse } from "./runtime_status_summary.js";

export function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

export function asNumber(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

export function formatAgeSeconds(v) {
  if (v === null || v === undefined) return "n/a";
  const n = Number(v);
  if (!Number.isFinite(n)) return "n/a";
  if (n < 60) return `${n.toFixed(1)}s`;
  if (n < 3600) return `${(n / 60).toFixed(1)}m`;
  return `${(n / 3600).toFixed(2)}h`;
}

export function buildStartupDiagnostics({
  readiness = null,
  health = null,
  systemState = null,
  broker = null,
} = {}) {
  const runtime = readiness && readiness.runtime ? readiness.runtime : {};
  const runtimeHealth = runtime && runtime.health ? runtime.health : {};
  const prices = runtimeHealth.prices || (health && health.prices) || {};
  const providers = runtimeHealth.providers || (health && health.providers) || {};
  const labels = runtimeHealth.labels || (health && health.labels) || {};
  const model = runtimeHealth.model || (health && health.model) || {};
  const timeseriesStorage = runtimeHealth.timeseries_storage || (health && health.timeseries_storage) || {};
  const featureStore = runtimeHealth.feature_store
    || (health && health.feature_store)
    || (timeseriesStorage && timeseriesStorage.feature_store)
    || {};
  const portfolioRuntime = runtimeHealth.portfolio_runtime || (health && health.portfolio_runtime) || {};
  const executionDegraded = runtimeHealth.execution_degraded || (health && health.execution_degraded) || {};
  const executionBarrier = runtimeHealth.execution_barrier || (health && health.execution_barrier) || {};
  const brokerConnection = runtimeHealth.broker_connection || (health && health.broker_connection) || {};

  const dataFeedOk = !!(prices.ok && providers.ok);
  const modelsOk = !!(labels.ok && model.ok);
  const timeseriesOk = !timeseriesStorage.enabled || !!timeseriesStorage.ok;
  const featureStoreOk = !featureStore.enabled || (!!featureStore.ok && !featureStore.degraded);
  const sidecarsOk = !!(timeseriesOk && featureStoreOk);
  const portfolioRuntimeOk = !(portfolioRuntime.available !== false && (portfolioRuntime.degraded || portfolioRuntime.ok === false));
  const executionHealthOk = !(executionDegraded.active && String(executionDegraded.severity || "").toUpperCase() === "CRITICAL");
  const riskOk = !!executionBarrier.allowed;
  const brokerOk = !!(brokerConnection.ok || (broker && broker.ok));
  const enableTradingOk =
    dataFeedOk &&
    modelsOk &&
    sidecarsOk &&
    portfolioRuntimeOk &&
    executionHealthOk &&
    riskOk &&
    brokerOk &&
    !!(readiness && readiness.ready) &&
    !!(systemState && systemState.state === "LIVE");

  const blockers = [];
  if (!sidecarsOk) {
    const sidecarReasons = [
      timeseriesStorage.detail,
      ...safeArray(timeseriesStorage.degraded_reasons),
      ...safeArray(featureStore.degraded_reasons),
      (!featureStore.ok && featureStore.enabled && !safeArray(featureStore.degraded_reasons).length) ? "feature_store_not_ready" : "",
    ].filter(Boolean);
    blockers.push(`sidecars: ${sidecarReasons.join(", ") || "not_ready"}`);
  }
  if (!portfolioRuntimeOk) {
    const portfolioReasons = [
      portfolioRuntime.detail,
      ...safeArray(portfolioRuntime.degraded_codes),
      ...safeArray(portfolioRuntime.degraded_reasons).map((item) => (item && typeof item === "object")
        ? (item.code || item.reason || item.detail || "")
        : item),
    ].filter(Boolean);
    blockers.push(`portfolio_runtime: ${portfolioReasons.join(", ") || "degraded"}`);
  }
  if (!executionHealthOk) {
    const executionReasons = [
      ...safeArray(executionDegraded.reason_codes),
      executionDegraded.reason,
    ].filter(Boolean);
    blockers.push(`execution: ${executionReasons.join(", ") || "degraded"}`);
  }
  if (!riskOk) blockers.push(`risk: ${executionBarrier.reason || "execution_blocked"}`);
  if (!brokerOk) blockers.push(`broker: ${brokerConnection.state || "disconnected"}`);

  return {
    ready: enableTradingOk,
    data_feed_ok: dataFeedOk,
    models_ok: modelsOk,
    sidecars_ok: sidecarsOk,
    portfolio_runtime_ok: portfolioRuntimeOk,
    execution_health_ok: executionHealthOk,
    risk_ok: riskOk,
    broker_ok: brokerOk,
    blockers,
    steps: [
      {
        label: "Verify Data Feed",
        ok: dataFeedOk,
        detail: `prices_ok=${!!prices.ok} providers=${Number(providers.healthy || 0)}/${Number(providers.total || 0)} age_s=${prices.age_s ?? "—"}`
      },
      {
        label: "Verify Models",
        ok: modelsOk,
        detail: `labels_ok=${!!labels.ok} label_count=${labels.count ?? "—"} model_ok=${!!model.ok} support_n=${model.support_n ?? "—"}`
      },
      {
        label: "Verify Sidecars",
        ok: sidecarsOk,
        blocked: !sidecarsOk,
        detail: `timeseries_ok=${timeseriesOk} feature_store_ok=${featureStoreOk} reasons=${[
          timeseriesStorage.detail,
          ...safeArray(timeseriesStorage.degraded_reasons),
          ...safeArray(featureStore.degraded_reasons),
        ].filter(Boolean).join(", ") || "none"}`
      },
      {
        label: "Verify Portfolio Runtime",
        ok: portfolioRuntimeOk,
        blocked: !portfolioRuntimeOk,
        detail: `degraded=${!!portfolioRuntime.degraded} reasons=${[
          portfolioRuntime.detail,
          ...safeArray(portfolioRuntime.degraded_codes),
          ...safeArray(portfolioRuntime.degraded_reasons).map((item) => (item && typeof item === "object")
            ? (item.code || item.reason || item.detail || "")
            : item),
        ].filter(Boolean).join(", ") || "none"}`
      },
      {
        label: "Verify Risk",
        ok: riskOk,
        blocked: !riskOk,
        detail: `execution_allowed=${!!executionBarrier.allowed} reason=${executionBarrier.reason || "none"}`
      },
      {
        label: "Verify Broker",
        ok: brokerOk,
        blocked: !brokerOk,
        detail: `broker_health=${brokerConnection.state || (brokerOk ? "connected" : "disconnected")}`
      },
      {
        label: "Verify Execution Health",
        ok: executionHealthOk,
        blocked: !executionHealthOk,
        detail: `critical_degraded=${!!executionDegraded.active && String(executionDegraded.severity || "").toUpperCase() === "CRITICAL"} reasons=${[
          ...safeArray(executionDegraded.reason_codes),
          executionDegraded.reason,
        ].filter(Boolean).join(", ") || "none"}`
      },
      {
        label: "Enable Trading",
        ok: enableTradingOk,
        blocked: !enableTradingOk,
        detail: `readiness=${!!(readiness && readiness.ready)} system=${systemState && systemState.state ? systemState.state : "UNKNOWN"}`
      }
    ],
    raw: {
      readiness,
      health,
      systemState,
      broker
    }
  };
}

export function buildOperatorHealthPanelItems({
  health = null,
  readiness = null,
  services = null,
  watchdogs = null,
  providers = null,
  supervisor = null,
} = {}) {
  const healthBody = unwrapHealthResponse(health);
  const db = healthBody?.db || {};
  const prices = healthBody?.prices || {};
  const providersSummary = healthBody?.providers || {};
  const model = healthBody?.model || {};
  const labels = healthBody?.labels || {};
  const readinessReasons = safeArray(readiness?.reasons)
    .map((x) => x.code || x.message || "unknown")
    .join(", ") || "none";

  const serviceDetail = services?.managed
    ? `engine=${services?.engine?.active || "unknown"} operator=${services?.operator?.active || "unknown"}`
    : `engine=${services?.engine?.status || "unknown"} operator=${services?.operator?.status || "unknown"}`;

  const watchdogDetail = `provider_restart=${asNumber(watchdogs?.provider_monitor?.restart_count)} metrics_restart=${asNumber(watchdogs?.metrics_collector?.restart_count)} stale=${asNumber(watchdogs?.job_summary?.stale)}`;
  const providerDetail = `running=${providers?.providers?.running ? "yes" : "no"} age=${formatAgeSeconds(providers?.providers?.age_s)}`;
  const supervisorDetail = `jobs=${asNumber(supervisor?.counts?.total)} running=${asNumber(supervisor?.counts?.running)} stale=${asNumber(supervisor?.counts?.stale)}`;

  return [
    { label: "System Health", value: healthBody?.ok ? "healthy" : "degraded", state: healthBody?.ok ? "ok" : "fail" },
    { label: "Database Health", value: `quick_check=${db?.quick_check || "unknown"}`, state: db?.ok ? "ok" : "fail" },
    { label: "Ingestion Health", value: `prices_age=${formatAgeSeconds(prices?.age_s)} providers=${asNumber(providersSummary?.healthy)} / ${asNumber(providersSummary?.total)}`, state: (prices?.ok && providersSummary?.ok) ? "ok" : "fail" },
    { label: "Model Status", value: `support=${asNumber(model?.support_n)} labels=${asNumber(labels?.count)}`, state: model?.ok ? "ok" : "warn" },
    { label: "Trading Readiness", value: readiness?.ready ? "READY" : readinessReasons, state: readiness?.ready ? "ok" : "fail" },
    { label: "Service Status", value: serviceDetail, state: services?.ok ? "ok" : "warn" },
    { label: "Watchdog Telemetry", value: watchdogDetail, state: watchdogs?.ok ? "ok" : "warn" },
    { label: "Provider Telemetry", value: providerDetail, state: providers?.ok ? "ok" : "warn" },
    { label: "Supervisor Diagnostics", value: supervisorDetail, state: supervisor?.ok ? "ok" : "warn" }
  ];
}
