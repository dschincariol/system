/*
  FILE: ui/data_health.js

  Data Health screen controller for the main dashboard. Keep the fetch,
  normalize, and render steps separate so dashboard.js can remain the screen
  router instead of the owner of this screen's data contract.
*/

import {
  escapeHTML,
  fmtTime,
  formatAgeMs,
  formatDecimal,
  freshnessTone,
  numOrNull,
  pickTimestamp,
  safeJoin,
  statusAriaLabel,
  statusPillClasses,
  statusToken,
} from "./utils.js";
import {
  applyStalenessState,
  stalenessClassNames,
} from "./panel_state.js";

export const DATA_HEALTH_FEATURE_VISIBILITY_TARGETS = Object.freeze({
  structured: "structuredDocumentsVisibility",
  graph: "graphFeaturesVisibility",
});

export const DATA_HEALTH_ENDPOINTS = Object.freeze({
  ingestion: Object.freeze({ path: "/api/ingestion/status" }),
  feedStatus: Object.freeze({ path: "/api/feeds" }),
  telemetry: Object.freeze({ path: "/api/telemetry" }),
  barrier: Object.freeze({
    path: "/api/execution/barrier",
    options: Object.freeze({ allowBusinessFalse: true }),
  }),
  provider: Object.freeze({ path: "/api/operator/provider_telemetry" }),
  featureVisibility: Object.freeze({
    path: "/api/data/feature_visibility?limit=12",
    options: Object.freeze({ allowBusinessFalse: true }),
  }),
});

const FEATURE_VISIBILITY_UNAVAILABLE = Object.freeze({
  ok: false,
  structured_documents: Object.freeze({
    status: "unavailable",
    warnings: Object.freeze(["feature visibility route unavailable"]),
  }),
  graph_features: Object.freeze({
    status: "unavailable",
    warnings: Object.freeze(["feature visibility route unavailable"]),
  }),
});

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function intOr(value, fallback = 0) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

function ageMsFromTimestampAt(tsMs, nowMs) {
  const ts = numOrNull(tsMs);
  if (ts == null || ts <= 0) return null;
  const now = numOrNull(nowMs) ?? Date.now();
  return Math.max(0, now - ts);
}

function fulfilledValue(result, fallback = null) {
  return result && result.status === "fulfilled" ? result.value : fallback;
}

function isFulfilled(result) {
  return !!result && result.status === "fulfilled";
}

function buildSettled(value) {
  return { status: "fulfilled", value };
}

export function extractDataHealthIngestionStatus(payload) {
  const root = asObject(payload);
  const nested = asObject(root.ingestion);
  return Object.keys(nested).length ? nested : root;
}

export function extractDataHealthProviderTelemetry(payload) {
  const root = asObject(payload);
  const nested = asObject(root.provider_telemetry);
  return Object.keys(nested).length ? nested : root;
}

function normalizeProviderEntries({ providerTelemetry, ingestion, nowMs }) {
  const providerMap = asObject(
    Object.keys(asObject(providerTelemetry.providers)).length
      ? providerTelemetry.providers
      : ingestion.providers
  );
  return Object.entries(providerMap)
    .map(([name, raw]) => {
      const row = asObject(raw);
      const updatedTs = pickTimestamp(row.updated_ts_ms, row.last_price_ts_ms, row.ts_ms);
      const ageMs = numOrNull(row.price_age_ms) ?? ageMsFromTimestampAt(updatedTs, nowMs);
      let statusText = String(row.status || "").trim().toUpperCase();
      if (!statusText) {
        if (row.ok === false) statusText = "DEGRADED";
        else if (row.running === false) statusText = "STOPPED";
        else if (row.running === true || row.ok === true) statusText = "LIVE";
        else statusText = "UNKNOWN";
      }
      const tone = row.ok === false || statusText === "STOPPED"
        ? "crit"
        : freshnessTone(ageMs, 60_000, 300_000);
      const notes = safeJoin([
        row.error ? `error ${row.error}` : "",
        row.owner ? `owner ${row.owner}` : "",
        numOrNull(row.last_seq) != null && Number(row.last_seq) > 0 ? `seq ${intOr(row.last_seq, 0)}` : "",
      ]);
      return {
        name,
        statusText,
        tone,
        ageMs,
        updatedTs,
        notes: notes || "—",
      };
    })
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));
}

function buildStatGridMarkup(items) {
  return asArray(items)
    .map((item) => {
      const label = escapeHTML(String((item || {}).label || "—"));
      const value = escapeHTML(String((item || {}).value || "—"));
      const meta = escapeHTML(String((item || {}).meta || "—"));
      return `
        <div class="opsStat">
          <div class="opsStatLabel metric-label">${label}</div>
          <div class="opsStatValue metric-value">${value}</div>
          <div class="opsStatMeta metric-meta">${meta}</div>
        </div>
      `;
    })
    .join("");
}

function normalizePillTone(tone) {
  return statusToken(tone || "neutral").key;
}

function pillClassNames(tone) {
  const raw = String(tone || "dim").trim().toLowerCase();
  const normalized = normalizePillTone(raw);
  const classes = statusPillClasses(normalized).split(/\s+/).filter(Boolean);
  if (raw === "bad") classes.push("bad");
  if (raw === "err") classes.push("bad", "err");
  return Array.from(new Set(classes.filter(Boolean))).join(" ");
}

function structuralPillClasses(el) {
  if (!el) return [];
  return ["mono", "meta-pill-offset"].filter((cls) => el.classList.contains(cls));
}

function buildPillClassName(el, tone) {
  const token = statusToken(tone || "neutral");
  if (el) {
    el.dataset.status = token.key;
    el.setAttribute("aria-label", statusAriaLabel(token.key, el.textContent || ""));
  }
  return Array.from(new Set([
    ...pillClassNames(tone).split(/\s+/).filter(Boolean),
    ...structuralPillClasses(el),
  ])).join(" ");
}

function flashOnContentChange(el, value) {
  if (!el) return;
  if (el.dataset.flashOnChange !== "true") return;
  const nextValue = String(value ?? "");
  if (el.dataset.lastValue !== undefined && el.dataset.lastValue !== nextValue) {
    el.classList.remove("update-flash");
    void el.offsetWidth;
    el.classList.add("update-flash");
  }
  el.dataset.lastValue = nextValue;
}

function setPillTone(rootDocument, id, tone, text, staleAgeMs = null, staleWarnMs = 60_000, staleCritMs = 300_000) {
  const el = rootDocument.getElementById(id);
  if (!el) return;
  const nextText = String(text || "—");
  el.className = buildPillClassName(el, tone);
  el.textContent = nextText;
  el.setAttribute("aria-label", statusAriaLabel(tone, nextText));
  applyStalenessState(el, staleAgeMs, staleWarnMs, staleCritMs);
  flashOnContentChange(el, nextText);
}

function renderNotes(rootDocument, containerId, notes, emptyText = "No active issues reported.") {
  const el = rootDocument.getElementById(containerId);
  if (!el) return;
  const rows = asArray(notes).filter(Boolean);
  if (!rows.length) {
    el.innerHTML = `<div class="opsNote">${escapeHTML(emptyText)}</div>`;
    return;
  }
  el.innerHTML = rows
    .map((note) => `<div class="opsNote">${escapeHTML(String(note))}</div>`)
    .join("");
}

export async function fetchDataHealthScreen(fetchJSON) {
  const endpoints = DATA_HEALTH_ENDPOINTS;
  const [
    ingestion,
    feedStatus,
    telemetry,
    barrier,
    provider,
    featureVisibility,
  ] = await Promise.allSettled([
    fetchJSON(endpoints.ingestion.path),
    fetchJSON(endpoints.feedStatus.path),
    fetchJSON(endpoints.telemetry.path),
    fetchJSON(endpoints.barrier.path, endpoints.barrier.options),
    fetchJSON(endpoints.provider.path),
    fetchJSON(endpoints.featureVisibility.path, endpoints.featureVisibility.options),
  ]);

  return {
    ingestion,
    feedStatus,
    telemetry,
    barrier,
    provider,
    featureVisibility,
  };
}

export function normalizeDataHealthScreen(results = {}, { nowMs = Date.now() } = {}) {
  const settled = {
    ingestion: results.ingestion || buildSettled(results.ingestionPayload),
    feedStatus: results.feedStatus || buildSettled(results.feedStatusPayload),
    telemetry: results.telemetry || buildSettled(results.telemetryPayload),
    barrier: results.barrier || buildSettled(results.barrierPayload),
    provider: results.provider || buildSettled(results.providerPayload),
    featureVisibility: results.featureVisibility || buildSettled(results.featureVisibilityPayload),
  };

  const ingestionPayload = fulfilledValue(settled.ingestion);
  const feedStatus = fulfilledValue(settled.feedStatus);
  const telemetry = fulfilledValue(settled.telemetry);
  const barrier = fulfilledValue(settled.barrier);
  const providerPayload = fulfilledValue(settled.provider);
  const featureVisibility = isFulfilled(settled.featureVisibility)
    ? settled.featureVisibility.value
    : FEATURE_VISIBILITY_UNAVAILABLE;

  const ingestion = extractDataHealthIngestionStatus(ingestionPayload);
  const providerTelemetry = extractDataHealthProviderTelemetry(providerPayload);
  const telemetryProviders = asObject(telemetry && telemetry.providers);
  const providerEntries = normalizeProviderEntries({ providerTelemetry, ingestion, nowMs });
  const priceFreshness = asObject(feedStatus && feedStatus.price_freshness);
  const priceFreshnessAgeMs = numOrNull(priceFreshness.age_s) == null
    ? null
    : Math.max(0, Number(priceFreshness.age_s) * 1000);
  const priceFreshnessSource = String(priceFreshness.source || "").trim();
  const priceFreshnessStatus = String(priceFreshness.status || "").trim();
  const liveFeedStatus = String(
    priceFreshness.live_feed_status
    || (feedStatus && feedStatus.live_feed_status)
    || ingestion.live_feed_status
    || providerTelemetry.live_feed_status
    || ""
  ).trim();
  const hasLiveMarketFlag = Object.prototype.hasOwnProperty.call(priceFreshness, "live_market_data_ok")
    || Object.prototype.hasOwnProperty.call(asObject(feedStatus), "live_market_data_ok")
    || Object.prototype.hasOwnProperty.call(ingestion, "live_market_data_ok")
    || Object.prototype.hasOwnProperty.call(providerTelemetry, "live_market_data_ok");
  const liveMarketDataOk = hasLiveMarketFlag
    ? (
      priceFreshness.live_market_data_ok === true
      || (feedStatus && feedStatus.live_market_data_ok === true)
      || ingestion.live_market_data_ok === true
      || providerTelemetry.live_market_data_ok === true
    )
    : null;
  const missingCredentialEnvVars = asArray(
    priceFreshness.missing_credential_env_vars
    || (feedStatus && feedStatus.missing_credential_env_vars)
    || ingestion.missing_credential_env_vars
    || providerTelemetry.missing_credential_env_vars
  ).map((name) => String(name || "").trim()).filter(Boolean);
  const priceFreshnessLabel = safeJoin([
    priceFreshnessStatus || (priceFreshness.ok === true ? "fresh" : priceFreshness.ok === false ? "stale" : ""),
    priceFreshnessSource ? `source ${priceFreshnessSource}` : "",
    priceFreshness.simulated ? "simulated" : "",
    liveFeedStatus ? `live ${liveFeedStatus}` : "",
    missingCredentialEnvVars.length ? `missing ${missingCredentialEnvVars.join(", ")}` : "",
  ]);

  const healthyProviders = numOrNull(providerTelemetry.healthy_providers)
    ?? numOrNull(ingestion.healthy_providers)
    ?? numOrNull(telemetryProviders.healthy);
  const totalProviders = Math.max(
    providerEntries.length,
    numOrNull(telemetryProviders.total) ?? 0,
    numOrNull(healthyProviders) ?? 0
  );
  const priceAgeMs = priceFreshnessAgeMs ?? numOrNull(providerTelemetry.price_age_ms) ?? numOrNull(ingestion.price_age_ms);
  const latestPriceTs = pickTimestamp(
    priceFreshness.last_ts_ms,
    providerTelemetry.last_price_ts_ms,
    ingestion.last_price_ts_ms
  );
  const updatedTs = pickTimestamp(
    providerTelemetry.updated_ts_ms,
    ingestion.updated_ts_ms,
    telemetry && telemetry.ts_ms,
    barrier && barrier.ts_ms,
    latestPriceTs
  );
  const updatedAgeMs = ageMsFromTimestampAt(updatedTs, nowMs);
  const pipelineStatus = isFulfilled(settled.ingestion)
    ? String(ingestion.status || (ingestion.running ? "RUNNING" : "STOPPED") || "UNKNOWN")
    : "UNAVAILABLE";
  const pipelineTone = !isFulfilled(settled.ingestion)
    ? "dim"
    : (ingestion.ok ? "ok" : ingestion.running ? "warn" : "bad");
  const providersTone = liveMarketDataOk === false
    ? "warn"
    : healthyProviders == null
    ? "dim"
    : (healthyProviders <= 0
      ? "bad"
      : (totalProviders > 0 && healthyProviders < totalProviders ? "warn" : "ok"));
  const barrierAllowed = barrier ? !!barrier.allowed : null;
  const barrierReason = String(
    (barrier && barrier.reason)
    || (asObject(barrier && barrier.execution_barrier).reason)
    || ""
  ).trim();
  const telemetryTone = telemetry
    ? ((telemetry.health && telemetry.health.ok) ? "ok" : "warn")
    : "dim";

  const healthNotes = [];
  if (barrierAllowed === false && barrierReason) {
    healthNotes.push(`execution blocked: ${barrierReason}`);
  }
  asArray(ingestion.reasons).slice(0, 5).forEach((reason) => {
    healthNotes.push(`ingestion: ${String(reason)}`);
  });
  if (!healthNotes.length && telemetry && telemetry.health && telemetry.health.ok === false) {
    asArray(telemetry.health.reasons).slice(0, 3).forEach((reason) => {
      healthNotes.push(`runtime: ${String(reason)}`);
    });
  }
  if (!isFulfilled(settled.provider)) {
    healthNotes.push("provider telemetry unavailable from the Python dashboard server snapshot");
  }
  if (!isFulfilled(settled.featureVisibility)) {
    healthNotes.push("structured document and graph feature visibility unavailable");
  }
  if (liveMarketDataOk === false) {
    healthNotes.push(
      missingCredentialEnvVars.length
        ? `market data not live: missing ${missingCredentialEnvVars.join(", ")}`
        : `market data not live: ${liveFeedStatus || "simulated or fallback feed"}`
    );
  }

  const pipelineSummary = asObject(ingestion.summary);
  const providersFallback = safeJoin([
    pipelineSummary.active_child ? `active child ${pipelineSummary.active_child}` : "",
    numOrNull(pipelineSummary.visible_jobs_running) != null ? `visible jobs ${formatDecimal(pipelineSummary.visible_jobs_running, 0)}` : "",
    numOrNull(providerTelemetry.child_pid) != null && Number(providerTelemetry.child_pid) > 0 ? `pid ${intOr(providerTelemetry.child_pid, 0)}` : "",
  ]) || "";

  const runtimeNotes = [];
  if (telemetry) {
    runtimeNotes.push(`system state: ${String(telemetry.system_state || "UNKNOWN")}`);
    if (telemetry.vol_target && telemetry.vol_target.enabled) {
      runtimeNotes.push(`vol target enabled: target ${formatDecimal(telemetry.vol_target.target_vol, 4)} scale ${formatDecimal(telemetry.vol_target.scale, 2)}x`);
    } else {
      runtimeNotes.push("vol target: off");
    }
  }
  if (barrierAllowed === false) {
    runtimeNotes.push(`execution barrier reason: ${barrierReason || "blocked"}`);
  } else if (barrierAllowed === true) {
    runtimeNotes.push("execution barrier clear");
  }
  if (!isFulfilled(settled.telemetry)) {
    runtimeNotes.push("runtime telemetry unavailable");
  }

  return {
    featureVisibility,
    summaryStats: [
      {
        label: "Pipeline",
        value: pipelineStatus,
        meta: isFulfilled(settled.ingestion)
          ? (safeJoin([
              ingestion.running ? "running" : "not running",
              ingestion.active_child ? `child ${ingestion.active_child}` : "",
            ]) || "—")
          : "snapshot unavailable",
      },
      {
        label: "Visible Jobs",
        value: formatDecimal(ingestion.visible_jobs_running, 0),
        meta: isFulfilled(settled.ingestion)
          ? (asArray(ingestion.stale_jobs).length
            ? `stale ${asArray(ingestion.stale_jobs).join(", ")}`
            : "no stale ingestion jobs")
          : "snapshot unavailable",
      },
      {
        label: "Fresh Rows",
        value: formatDecimal(ingestion.fresh_rows, 0),
        meta: isFulfilled(settled.ingestion)
          ? `symbols ${formatDecimal(ingestion.fresh_symbols, 0)}`
          : "snapshot unavailable",
      },
      {
        label: "Price Age",
        value: formatAgeMs(priceAgeMs),
        meta: safeJoin([
          priceFreshnessLabel,
          latestPriceTs ? `last price ${fmtTime(latestPriceTs)}` : "last price —",
        ]),
      },
      {
        label: "Providers",
        value: totalProviders > 0
          ? `${intOr(healthyProviders, 0)}/${intOr(totalProviders, 0)}`
          : (healthyProviders == null ? "—" : `${intOr(healthyProviders, 0)}`),
        meta: telemetryProviders.total != null
          ? `runtime total ${intOr(telemetryProviders.total, 0)}`
          : "provider count from snapshot",
      },
      {
        label: "Execution Gate",
        value: barrierAllowed == null ? "—" : (barrierAllowed ? "ALLOWED" : "BLOCKED"),
        meta: barrierReason || "no active execution block",
      },
    ],
    pills: [
      {
        id: "dataPipelinePill",
        tone: pipelineTone,
        text: `pipeline ${pipelineStatus}`,
      },
      {
        id: "dataProvidersPill",
        tone: providersTone,
        text: totalProviders > 0
          ? `providers ${intOr(healthyProviders, 0)}/${intOr(totalProviders, 0)}`
          : (healthyProviders == null ? "providers —" : `providers ${intOr(healthyProviders, 0)}`),
      },
      {
        id: "dataFreshnessPill",
        tone: liveMarketDataOk === false
          ? "warn"
          : priceFreshnessStatus === "stale"
          ? "warn"
          : priceFreshnessStatus === "missing" || priceFreshnessStatus === "error"
            ? "bad"
            : freshnessTone(priceAgeMs, 30_000, 120_000),
        text: safeJoin([
          priceFreshnessStatus ? `prices ${priceFreshnessStatus}` : "price age",
          priceFreshnessSource ? `source ${priceFreshnessSource}` : "",
          priceFreshness.simulated ? "sim" : "",
          liveMarketDataOk === false && liveFeedStatus ? liveFeedStatus : "",
          liveMarketDataOk === false && missingCredentialEnvVars.length ? `missing ${missingCredentialEnvVars.join(", ")}` : "",
          formatAgeMs(priceAgeMs),
        ]),
        staleAgeMs: priceAgeMs,
        staleWarnMs: 30_000,
        staleCritMs: 120_000,
      },
      {
        id: "dataBarrierPill",
        tone: barrierAllowed == null ? "dim" : (barrierAllowed ? "ok" : "bad"),
        text: barrierAllowed == null ? "barrier —" : `barrier ${barrierAllowed ? "ALLOWED" : "BLOCKED"}`,
      },
      {
        id: "dataTelemetryPill",
        tone: telemetryTone,
        text: telemetry ? `telemetry ${String(telemetry.system_state || "UNKNOWN")}` : "telemetry —",
      },
      {
        id: "dataUpdatedPill",
        tone: freshnessTone(updatedAgeMs, 60_000, 300_000),
        text: `updated ${formatAgeMs(updatedAgeMs)}`,
        staleAgeMs: updatedAgeMs,
        staleWarnMs: 60_000,
        staleCritMs: 300_000,
      },
    ],
    healthNotes,
    providersMeta: {
      tone: providersTone,
      text: totalProviders > 0
        ? `${intOr(healthyProviders, 0)}/${intOr(totalProviders, 0)} healthy`
        : (healthyProviders == null ? "provider telemetry unavailable" : "no providers reported"),
    },
    providerEntries,
    providersFallback,
    runtimeStats: [
      {
        label: "CPU",
        value: telemetry ? `${formatDecimal(telemetry.cpu_percent, 1)}%` : "—",
        meta: telemetry ? `threads ${formatDecimal(telemetry.thread_count, 0)}` : "telemetry unavailable",
      },
      {
        label: "Memory",
        value: telemetry ? `${formatDecimal(telemetry.process_rss_mb, 0)}MB` : "—",
        meta: telemetry ? `${formatDecimal(telemetry.memory_percent, 1)}% of host` : "telemetry unavailable",
      },
      {
        label: "DB Size",
        value: telemetry ? `${formatDecimal(telemetry.db_size_mb, 1)}MB` : "—",
        meta: telemetry ? `state ${String(telemetry.system_state || "UNKNOWN")}` : "telemetry unavailable",
      },
      {
        label: "Alerts / 1h",
        value: telemetry ? formatDecimal(asObject(telemetry.alerts).last_hour, 0) : "—",
        meta: telemetry ? `crit open ${formatDecimal(asObject(telemetry.alerts).critical_open, 0)}` : "telemetry unavailable",
      },
      {
        label: "Fills",
        value: telemetry ? formatDecimal(asObject(telemetry.execution).n_fills, 0) : "—",
        meta: telemetry
          ? `last fill ${formatAgeMs(ageMsFromTimestampAt(asObject(telemetry.execution).last_fill_ts_ms, nowMs))}`
          : "telemetry unavailable",
      },
      {
        label: "Supervisor",
        value: telemetry ? formatDecimal(asObject(telemetry.supervisor).n_jobs, 0) : "—",
        meta: telemetry
          ? (asObject(telemetry.supervisor).delegated ? "delegated" : "local")
          : "telemetry unavailable",
      },
    ],
    runtimeNotes,
  };
}

export function renderDataHealthScreen(
  model,
  {
    document: rootDocument = document,
    renderFeatureVisibility,
  } = {}
) {
  if (!rootDocument) return;
  const summaryGrid = rootDocument.getElementById("dataHealthSummaryGrid");
  if (!summaryGrid) return;

  if (typeof renderFeatureVisibility === "function") {
    renderFeatureVisibility(DATA_HEALTH_FEATURE_VISIBILITY_TARGETS, model.featureVisibility);
  }

  summaryGrid.innerHTML = buildStatGridMarkup(model.summaryStats);
  asArray(model.pills).forEach((pill) => {
    setPillTone(
      rootDocument,
      pill.id,
      pill.tone,
      pill.text,
      pill.staleAgeMs ?? null,
      pill.staleWarnMs ?? 60_000,
      pill.staleCritMs ?? 300_000
    );
  });
  renderNotes(rootDocument, "dataHealthNotes", model.healthNotes, "No active ingestion blockers reported by the current snapshots.");
  setPillTone(rootDocument, "dataProvidersMeta", model.providersMeta.tone, model.providersMeta.text);

  const providersBody = rootDocument.getElementById("dataProvidersBody");
  if (providersBody) {
    if (!model.providerEntries.length) {
      providersBody.innerHTML = `<tr class="table-row"><td colspan="5" class="metric-meta">(no provider rows reported)</td></tr>`;
    } else {
      providersBody.innerHTML = model.providerEntries.map((row) => `
        <tr class="table-row">
          <td class="mono">${escapeHTML(String(row.name || ""))}</td>
          <td><span class="${escapeHTML(pillClassNames(row.tone))}">${escapeHTML(row.statusText)}</span></td>
          <td><span class="${escapeHTML(`${pillClassNames(freshnessTone(row.ageMs, 30_000, 120_000))} ${stalenessClassNames(row.ageMs, 30_000, 120_000)}`.trim())}">${escapeHTML(formatAgeMs(row.ageMs))}</span></td>
          <td class="mono metric-meta">${row.updatedTs ? escapeHTML(fmtTime(row.updatedTs)) : "—"}</td>
          <td class="metric-meta">${escapeHTML(row.notes)}</td>
        </tr>
      `).join("");
    }
  }

  const providersFallback = rootDocument.getElementById("dataProvidersFallback");
  if (providersFallback) {
    providersFallback.textContent = model.providersFallback || "";
  }

  const runtimeGrid = rootDocument.getElementById("dataRuntimeGrid");
  if (runtimeGrid) {
    runtimeGrid.innerHTML = buildStatGridMarkup(model.runtimeStats);
  }
  renderNotes(rootDocument, "dataRuntimeNotes", model.runtimeNotes, "No runtime anomalies reported.");
}

export async function loadDataHealthScreen({
  fetchJSON,
  document: rootDocument = document,
  renderFeatureVisibility,
} = {}) {
  if (!rootDocument || !rootDocument.getElementById("dataHealthSummaryGrid")) return null;
  if (typeof fetchJSON !== "function") {
    throw new TypeError("loadDataHealthScreen requires fetchJSON");
  }
  const results = await fetchDataHealthScreen(fetchJSON);
  const model = normalizeDataHealthScreen(results);
  renderDataHealthScreen(model, { document: rootDocument, renderFeatureVisibility });
  return model;
}
