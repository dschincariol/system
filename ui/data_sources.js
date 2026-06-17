/*
  ui/data_sources.js

  Browser controller for the data-source control plane. It renders the source
  inventory, runtime detail pane, source-specific logs, and a provider-aware
  editor backed by the `/api/data_sources/*` endpoints.
*/

import { requestConfirmation } from "./confirmation_modal.mjs";

const SESSION_STORAGE_KEY = "dataSourceControlPlaneSession";

const state = {
  sources: [],
  templates: [],
  runtime: {},
  auth: {},
  selectedKey: "",
  refreshTimer: null,
  editingKey: "",
  editingSource: null,
  session: {
    actor: "",
    token: "",
  },
};

function el(id) {
  return document.getElementById(id);
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"]/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
  }[ch] || ch));
}

function fmtTs(ts) {
  const n = Number(ts || 0);
  if (!n) return "Never";
  return new Date(n).toLocaleString();
}

function fmtAgeMs(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n < 0) return "n/a";
  if (n < 1000) return `${n}ms`;
  const s = n / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  if (s < 3600) return `${(s / 60).toFixed(1)}m`;
  return `${(s / 3600).toFixed(2)}h`;
}

function flash(message, isError = false) {
  const target = el("flash");
  target.textContent = String(message || "");
  target.classList.toggle("flash-error", !!isError);
}

function statusPill(source) {
  const status = String(source.status || "unknown").toLowerCase();
  if (status === "ok" || status === "tested") return `<span class="pill ok">${esc(status)}</span>`;
  if (status === "error" || status === "test_failed") return `<span class="pill err">${esc(status)}</span>`;
  return `<span class="pill dim">${esc(status)}</span>`;
}

function queryParam(name) {
  return new URLSearchParams(window.location.search).get(name) || "";
}

function saveSession() {
  window.localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(state.session));
  renderSession();
}

function loadSession() {
  try {
    const raw = window.localStorage.getItem(SESSION_STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") {
        state.session.actor = String(parsed.actor || "");
        state.session.token = String(parsed.token || "");
      }
    }
  } catch (_error) {
    state.session = { actor: "", token: "" };
  }
  const actorQuery = queryParam("actor").trim();
  const tokenQuery = queryParam("token").trim();
  if (actorQuery) state.session.actor = actorQuery;
  if (tokenQuery) state.session.token = tokenQuery;
}

function renderSession() {
  el("actorInput").value = state.session.actor;
  el("tokenInput").value = state.session.token;
  const tokenRequired = !!state.auth.token_required;
  const actorValue = state.session.actor.trim() || "operator";
  el("sessionNote").textContent = tokenRequired
    ? `Mutating requests require X-API-Token. Changes will be attributed to ${actorValue}.`
    : `Localhost mutations are allowed without a token. Changes will be attributed to ${actorValue}.`;
}

function captureSessionInputs() {
  const actorEl = el("actorInput");
  const tokenEl = el("tokenInput");
  if (actorEl) state.session.actor = String(actorEl.value || "").trim();
  if (tokenEl) state.session.token = String(tokenEl.value || "").trim();
}

async function request(url, options = {}) {
  captureSessionInputs();
  const headers = new Headers(options.headers || {});
  headers.set("Content-Type", "application/json");
  if (state.session.token.trim()) {
    headers.set("X-API-Token", state.session.token.trim());
  }
  const response = await fetch(url, {
    cache: "no-store",
    ...options,
    headers,
  });
  const payload = await response.json();
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || payload.detail || `request_failed:${response.status}`);
  }
  return payload;
}

function selectedSource() {
  return state.sources.find((row) => String(row.source_key) === String(state.selectedKey)) || null;
}

function templateByKey(templateKey) {
  return state.templates.find((row) => String(row.template_key) === String(templateKey)) || null;
}

function templateForSource(source) {
  if (!source) return null;
  return templateByKey(source.template_key || source.source_key || "");
}

const SOURCE_GUIDES = {
  polygon_ws: {
    category: "Market Data",
    summary: "Streams live market data from Polygon for the fastest price updates.",
    needs: ["A Polygon API key with WebSocket market data access."],
    setup: [
      "Use Edit Source.",
      "Enter the Polygon API key.",
      "Save the source, then run Test Connection.",
      "Enable the source if you want live streaming."
    ],
    whenEnabled: "The runtime can stream live market data and reduce delay on price updates."
  },
  polygon: {
    category: "Market Data",
    summary: "Polls Polygon REST snapshots as a market-data source and fallback feed.",
    needs: ["A Polygon API key with REST access."],
    setup: [
      "Use Edit Source.",
      "Enter the Polygon API key.",
      "Save the source, then run Test Connection.",
      "Leave this enabled if you want Polygon snapshot polling."
    ],
    whenEnabled: "The runtime can poll Polygon snapshot data for price and options workflows."
  },
  ibkr: {
    category: "Broker Connectivity",
    summary: "Connects to Interactive Brokers for streaming broker-backed market data.",
    needs: ["IBKR Gateway or TWS running.", "Host, port, and client ID that match your IBKR setup."],
    setup: [
      "Use Edit Source.",
      "Enter the host, port, and client ID.",
      "Run Test Connection to verify the socket is reachable.",
      "Enable only when the IBKR service is running."
    ],
    whenEnabled: "The runtime can connect to IBKR for broker-side market data."
  },
  yfinance: {
    category: "Market Data",
    summary: "Provides Yahoo Finance polling as a low-friction backup market-data source.",
    needs: ["No credentials are required."],
    setup: [
      "Enable the source if you want Yahoo polling available.",
      "Run Test Connection if you want a quick connectivity check."
    ],
    whenEnabled: "The runtime can use Yahoo Finance as a backup polling source."
  },
  ccxt: {
    category: "Market Data",
    summary: "Provides crypto price polling through CCXT.",
    needs: ["No credentials are required for the default public polling path."],
    setup: [
      "Enable the source if you want CCXT crypto price polling.",
      "Use Test Connection if you want to confirm public exchange reachability."
    ],
    whenEnabled: "The runtime can poll public crypto market data."
  },
  tradier: {
    category: "Options",
    summary: "Provides options-chain polling through Tradier.",
    needs: ["A Tradier API token."],
    setup: [
      "Use Edit Source.",
      "Enter the Tradier API token.",
      "Save and run Test Connection.",
      "Enable when you want options data from Tradier."
    ],
    whenEnabled: "The runtime can poll options expirations and option-chain data."
  },
  reddit: {
    category: "Social Sentiment",
    summary: "Polls Reddit to gather social sentiment from configured communities.",
    needs: ["A Reddit client ID.", "A Reddit client secret."],
    setup: [
      "Use Edit Source.",
      "Enter the Reddit client ID and client secret.",
      "Optional: adjust subreddits and user agent in Settings.",
      "Save and run Test Connection."
    ],
    whenEnabled: "The runtime can collect Reddit sentiment and discussion signals."
  },
  stocktwits: {
    category: "Social Sentiment",
    summary: "Polls Stocktwits trending and symbol streams.",
    needs: ["No stored credentials are required for the default public endpoint."],
    setup: [
      "Enable the source.",
      "Run Test Connection.",
      "If access is blocked, review the last error and decide whether to disable it."
    ],
    whenEnabled: "The runtime can gather public Stocktwits sentiment context."
  },
  company_news: {
    category: "News",
    summary: "Pulls company-specific news through Finnhub.",
    needs: ["A Finnhub API key."],
    setup: [
      "Use Edit Source.",
      "Enter the Finnhub API key.",
      "Optional: adjust symbol limit and lookback window.",
      "Save and run Test Connection."
    ],
    whenEnabled: "The runtime can ingest company-level news for tracked symbols."
  },
  transcripts: {
    category: "News",
    summary: "Fetches company transcripts through Financial Modeling Prep.",
    needs: ["An FMP API key."],
    setup: [
      "Use Edit Source.",
      "Enter the FMP API key.",
      "Save and run Test Connection."
    ],
    whenEnabled: "The runtime can ingest transcripts for supported symbols."
  },
  gdelt: {
    category: "News",
    summary: "Queries GDELT for broad market and macro news coverage.",
    needs: ["No credentials are required."],
    setup: [
      "Enable the source if you want GDELT news in the pipeline.",
      "Run Test Connection.",
      "If it rate limits, either wait or disable it."
    ],
    whenEnabled: "The runtime can pull broad market news and article references."
  },
  sec: {
    category: "Filings",
    summary: "Polls SEC filing data for tracked companies.",
    needs: ["A proper SEC user agent and contact details in Settings if you customize the source."],
    setup: [
      "Review the source settings.",
      "Run Test Connection.",
      "Enable only if the SEC path is healthy."
    ],
    whenEnabled: "The runtime can ingest SEC filings and filing-related events."
  },
  earnings: {
    category: "Calendar",
    summary: "Pulls upcoming earnings events through Financial Modeling Prep.",
    needs: ["An FMP API key."],
    setup: [
      "Use Edit Source.",
      "Enter the FMP API key.",
      "Save and run Test Connection."
    ],
    whenEnabled: "The runtime can ingest earnings calendar events."
  },
  weather_forecasts: {
    category: "Weather",
    summary: "Pulls weather forecasts for configured regions.",
    needs: ["No credentials are required for the default provider."],
    setup: [
      "Enable the source if you want weather forecasts.",
      "Run Test Connection."
    ],
    whenEnabled: "The runtime can ingest forecast data for weather-aware models."
  },
  weather_alerts: {
    category: "Weather",
    summary: "Pulls active weather alerts from the configured alerts provider.",
    needs: ["No credentials are required for the default provider."],
    setup: [
      "Enable the source if you want alert ingestion.",
      "Run Test Connection."
    ],
    whenEnabled: "The runtime can ingest active weather alerts."
  },
  macro: {
    category: "Macro",
    summary: "Builds macro factor snapshots used by the strategy layer.",
    needs: ["No credentials are required."],
    setup: [
      "Leave enabled unless you intentionally want to stop macro ingestion."
    ],
    whenEnabled: "The runtime can refresh macro factor data for models and dashboards."
  },
  model_feature_snapshots: {
    category: "Model Support",
    summary: "Captures feature snapshots used for diagnostics and model analysis.",
    needs: ["No credentials are required."],
    setup: [
      "Leave enabled unless you intentionally want to stop feature snapshots."
    ],
    whenEnabled: "The runtime can capture model feature snapshots for diagnostics."
  },
  rss_feed: {
    category: "News",
    summary: "A custom RSS feed you manage directly from this page.",
    needs: ["A name.", "A feed URL."],
    setup: [
      "Use Add RSS Feed.",
      "Enter the feed name and feed URL.",
      "Save and then use Test Connection."
    ],
    whenEnabled: "The runtime can ingest articles from that RSS feed."
  }
};

function sourceGuideKey(source) {
  const sourceKey = String(source?.source_key || "").trim().toLowerCase();
  const providerName = String(source?.provider_name || "").trim().toLowerCase();
  const templateKey = String(source?.template_key || "").trim().toLowerCase();
  if (sourceKey.startsWith("rss:")) return "rss_feed";
  return sourceKey || providerName || templateKey || "rss_feed";
}

function guideForSource(source) {
  const key = sourceGuideKey(source);
  return SOURCE_GUIDES[key] || {
    category: "Source",
    summary: "This source is managed from this page.",
    needs: ["Review the source state below."],
    setup: ["Select Edit Source, adjust settings or credentials, then run Test Connection."],
    whenEnabled: "The runtime will include this source in ingestion and health monitoring."
  };
}

function deriveSourceState(source, template) {
  const credentialFields = (template && template.credential_fields) || [];
  const credentialError = String(source?.credential_error || "").trim();
  const lastError = String(source?.last_error || "").trim();
  const status = String(source?.status || "").trim().toLowerCase();
  const enabled = !!source?.enabled;
  const credentialsRequired = credentialFields.length > 0;
  const credentialsConfigured = !!source?.credentials_configured;

  if (credentialError) {
    return {
      label: "Stored credentials need reset",
      tone: "err",
      priority: 0,
      detail: "This page can clear the unreadable stored credential blob. Reset credentials, then enter a fresh value if you want to keep the source enabled.",
      nextStep: "Reset corrupted credentials."
    };
  }

  if (credentialsRequired && !credentialsConfigured) {
    return {
      label: enabled ? "Needs setup" : "Disabled and not configured",
      tone: enabled ? "warn" : "dim",
      priority: enabled ? 1 : 5,
      detail: "This source needs credentials before it can connect successfully.",
      nextStep: enabled ? "Open Edit Source and enter the required credentials." : "Enter credentials before enabling this source."
    };
  }

  if (/not authorized|forbidden|401|403|unauthorized|invalid api|auth/i.test(lastError)) {
    return {
      label: "Credentials rejected",
      tone: "err",
      priority: 1,
      detail: "The provider rejected the saved credentials.",
      nextStep: "Replace credentials or disable the source."
    };
  }

  if (/429|rate limit|too many requests/i.test(lastError)) {
    return {
      label: "Rate limited",
      tone: "warn",
      priority: 2,
      detail: "The provider is reachable but is currently limiting requests.",
      nextStep: "Wait, reduce usage, or disable the source if it is optional."
    };
  }

  if (!enabled) {
    return {
      label: "Disabled",
      tone: "dim",
      priority: 6,
      detail: "This source is turned off and will not be used by the runtime.",
      nextStep: "Enable it only if you want this source active."
    };
  }

  if (status === "ok" || status === "tested") {
    return {
      label: "Connected",
      tone: "ok",
      priority: 4,
      detail: "This source is configured and the last known state is healthy.",
      nextStep: "No action needed."
    };
  }

  if (status === "error" || status === "test_failed") {
    return {
      label: "Needs attention",
      tone: "warn",
      priority: 2,
      detail: lastError || "The source reported an error.",
      nextStep: "Review the last error, then test or adjust the source."
    };
  }

  return {
    label: enabled ? "Configured" : "Disabled",
    tone: enabled ? "dim" : "dim",
    priority: 5,
    detail: "This source is configured but does not yet have a clear healthy state.",
    nextStep: "Run Test Connection to verify the setup."
  };
}

function statePill(stateInfo) {
  return `<span class="pill ${esc(stateInfo.tone || "dim")}">${esc(stateInfo.label || "Unknown")}</span>`;
}

function createTemplates() {
  return state.templates.filter((row) => row.allow_create);
}

function runtimeForSource(source) {
  const providerName = String(source?.provider_name || "").trim().toLowerCase();
  const providerTelemetry = ((state.runtime.provider_telemetry || {}).providers || {})[providerName] || null;
  const pipelineHealth = ((state.runtime.pipeline_health || {}).pipelines || {})[String(source?.job_name || "")] || null;
  return { providerTelemetry, pipelineHealth };
}

function renderMetrics(payload) {
  const sources = payload.sources || [];
  const enabled = sources.filter((row) => row.enabled).length;
  const healthy = sources.filter((row) => deriveSourceState(row, templateForSource(row)).tone === "ok").length;
  const errors = sources.reduce((sum, row) => sum + Number(row.error_count || 0), 0);
  el("metricTotal").textContent = String(sources.length);
  el("metricEnabled").textContent = String(enabled);
  el("metricHealthy").textContent = String(healthy);
  el("metricErrors").textContent = String(errors);
}

function selectSource(sourceKey) {
  state.selectedKey = String(sourceKey || "");
  renderTable();
  renderSourceCards();
  renderDetail();
  if (state.selectedKey) loadLogs();
}

function renderOverview() {
  const target = el("actionCenter");
  const quickGuide = el("quickGuide");
  const actionRows = state.sources
    .map((source) => ({
      source,
      guide: guideForSource(source),
      stateInfo: deriveSourceState(source, templateForSource(source)),
    }))
    .sort((a, b) => {
      if (a.stateInfo.priority !== b.stateInfo.priority) return a.stateInfo.priority - b.stateInfo.priority;
      return String(a.source.display_name || "").localeCompare(String(b.source.display_name || ""));
    });

  const urgent = actionRows.filter((row) => row.stateInfo.priority <= 2).slice(0, 4);
  const nextActions = urgent.length ? urgent : actionRows.slice(0, 3);
  target.innerHTML = nextActions.map((row) => `
    <div class="action-item">
      <div class="action-item-head">
        <div>
          <div class="action-item-title">${esc(row.source.display_name)}</div>
          <div class="action-item-body">${esc(row.guide.category)}</div>
        </div>
        ${statePill(row.stateInfo)}
      </div>
      <div class="action-item-body">${esc(row.stateInfo.nextStep)}</div>
      <div class="source-card-actions">
        <button class="btn-secondary action-open-btn" data-key="${esc(row.source.source_key)}">Open Source</button>
      </div>
    </div>
  `).join("") || `<div class="empty">No sources available.</div>`;
  target.querySelectorAll(".action-open-btn").forEach((button) => {
    button.addEventListener("click", () => selectSource(button.getAttribute("data-key") || ""));
  });

  quickGuide.innerHTML = `
    <div class="quick-list">
      <div class="quick-item">1. Look at the recommended next step on the left. The page highlights the most urgent source setup or recovery task first.</div>
      <div class="quick-item">2. Click a source card below to see plain-language setup instructions, test status, and the right recovery action.</div>
      <div class="quick-item">3. Use only this page for source credentials, source settings, testing, enabling, disabling, and resets.</div>
    </div>
  `;
}

function renderSourceCards() {
  const target = el("sourceCards");
  if (!target) return;
  if (!state.sources.length) {
    target.innerHTML = `<div class="empty">No sources configured.</div>`;
    return;
  }
  const orderedSources = [...state.sources].sort((a, b) => {
    const aState = deriveSourceState(a, templateForSource(a));
    const bState = deriveSourceState(b, templateForSource(b));
    if (aState.priority !== bState.priority) return aState.priority - bState.priority;
    return String(a.display_name || "").localeCompare(String(b.display_name || ""));
  });
  target.innerHTML = orderedSources.map((source) => {
    const guide = guideForSource(source);
    const stateInfo = deriveSourceState(source, templateForSource(source));
    return `
      <div class="source-card ${String(source.source_key) === String(state.selectedKey) ? "is-active" : ""}" data-key="${esc(source.source_key)}">
        <div class="source-card-head">
          <div>
            <div class="source-card-title">${esc(source.display_name)}</div>
            <div class="source-card-text">${esc(guide.category)}</div>
          </div>
          ${statePill(stateInfo)}
        </div>
        <div class="source-card-text">${esc(guide.summary)}</div>
        <div class="source-card-meta">
          <span class="pill dim">${source.enabled ? "Enabled" : "Disabled"}</span>
          <span class="pill dim">${esc(source.provider_name || source.source_type)}</span>
        </div>
        <div class="source-card-text">${esc(stateInfo.nextStep)}</div>
      </div>
    `;
  }).join("");
  target.querySelectorAll(".source-card").forEach((card) => {
    card.addEventListener("click", () => selectSource(card.getAttribute("data-key") || ""));
  });
}

function renderTable() {
  const body = el("sourcesBody");
  if (!state.sources.length) {
    body.innerHTML = `<tr><td colspan="7" class="empty">No sources configured.</td></tr>`;
    return;
  }
  body.innerHTML = state.sources.map((source) => `
    <tr data-key="${esc(source.source_key)}" class="${String(source.source_key) === String(state.selectedKey) ? "is-active" : ""}">
      <td>
        <div><strong>${esc(source.display_name)}</strong> ${source.builtin ? '<span class="pill dim">builtin</span>' : '<span class="pill ok">custom</span>'}</div>
        <div class="mono subline">${esc(source.source_key)}</div>
      </td>
      <td>${esc(source.source_type)}</td>
      <td class="mono">${esc(source.job_name)}</td>
      <td>${source.enabled ? '<span class="pill ok">enabled</span>' : '<span class="pill err">disabled</span>'}</td>
      <td>${statePill(deriveSourceState(source, templateForSource(source)))}</td>
      <td>${esc(source.error_count || 0)}</td>
      <td>${esc(fmtTs(source.updated_ts_ms))}</td>
    </tr>
  `).join("");
  body.querySelectorAll("tr[data-key]").forEach((row) => {
    row.addEventListener("click", () => {
      selectSource(row.getAttribute("data-key") || "");
    });
  });
}

function renderRuntimePanel(source) {
  const runtime = runtimeForSource(source);
  const provider = runtime.providerTelemetry;
  const pipeline = runtime.pipelineHealth;
  return `
    <div class="detail-block">
      <div class="detail-label">Provider Telemetry</div>
      <div class="detail-value">
        ${provider ? `
          Provider ok: ${provider.ok ? "yes" : "no"}<br>
          Score: ${Number(provider.score || 0).toFixed(3)}<br>
          Latency: ${esc(fmtAgeMs(provider.latency_ms))}<br>
          Age: ${esc(fmtAgeMs(provider.age_ms))}<br>
          Symbols: ${esc(provider.n_symbols || 0)}
        ` : "No live provider telemetry for this source."}
      </div>
    </div>
    <div class="detail-block">
      <div class="detail-label">Pipeline Health</div>
      <div class="detail-value">
        ${pipeline ? `
          Pipeline ok: ${pipeline.ok ? "yes" : "no"}<br>
          Updated: ${esc(fmtTs(pipeline.updated_ts_ms))}<br>
          Age: ${esc(fmtAgeMs(pipeline.age_ms))}<br>
          Failures: ${esc(pipeline.failure_count || 0)}
        ` : "No pipeline health snapshot for this job yet."}
      </div>
    </div>
  `;
}

function renderDetail() {
  const source = selectedSource();
  const pane = el("detailPane");
  if (!source) {
    pane.innerHTML = `<div class="empty">Select a source to inspect health, schema, telemetry, and logs.</div>`;
    return;
  }

  const template = templateForSource(source);
  const credentialFields = (template && template.credential_fields) || [];
  const supportsCredentialReset = credentialFields.length > 0;
  const credentialError = String(source.credential_error || "").trim();
  const credentialsStored = !!source.credentials_stored;
  const credentialsConfigured = !!source.credentials_configured;
  const resettableCredentials = credentialsConfigured || credentialsStored || !!credentialError;
  const guide = guideForSource(source);
  const stateInfo = deriveSourceState(source, template);
  pane.innerHTML = `
    <div class="detail-hero">
      <div class="detail-summary">
        <div class="detail-banner">
          ${statePill(stateInfo)}
          <span class="pill dim">${source.enabled ? "Enabled" : "Disabled"}</span>
          <span class="pill dim">${esc(guide.category)}</span>
        </div>
        <h3>${esc(source.display_name)}</h3>
        <p>${esc(guide.summary)}</p>
      </div>
    </div>
    <div class="detail-grid">
      <div class="detail-block">
        <div class="detail-label">What To Do Now</div>
        <div class="detail-value">${esc(stateInfo.nextStep)}<br><br>${esc(stateInfo.detail)}</div>
      </div>
      <div class="detail-block">
        <div class="detail-label">What You Need</div>
        <div class="detail-value">${(guide.needs || []).map((item) => `• ${esc(item)}`).join("<br>")}</div>
      </div>
      <div class="detail-block">
        <div class="detail-label">How To Set It Up</div>
        <div class="detail-value">${(guide.setup || []).map((item) => `• ${esc(item)}`).join("<br>")}</div>
      </div>
      <div class="detail-block">
        <div class="detail-label">What Happens When Enabled</div>
        <div class="detail-value">${esc(guide.whenEnabled || "The runtime will include this source in ingestion and health monitoring.")}</div>
      </div>
      <div class="detail-block">
        <div class="detail-label">Credential State</div>
        <div class="detail-value">
          Stored value present: ${credentialsStored ? "yes" : "no"}<br>
          Readable value present: ${credentialsConfigured ? "yes" : "no"}<br>
          Credential fields: ${supportsCredentialReset ? esc(credentialFields.map((field) => field.field).join(", ")) : "none"}${credentialError ? `<br>Stored value problem: <span class="mono">${esc(credentialError)}</span>` : ""}
        </div>
      </div>
      <div class="detail-block">
        <div class="detail-label">Recent Health</div>
        <div class="detail-value">Last success: ${esc(fmtTs(source.last_success_ts_ms))}<br>Last test: ${esc(fmtTs(source.last_test_ts_ms))}<br>Last error: ${esc(source.last_error || "none")}<br>Error count: ${esc(source.error_count || 0)}</div>
      </div>
      ${renderRuntimePanel(source)}
    </div>
    <div class="detail-actions">
      <button class="btn" id="detailEditBtn">Edit Source</button>
      <button class="btn-secondary" id="detailTestBtn">Test Connection</button>
      ${supportsCredentialReset
        ? `<button class="btn-secondary" id="detailResetCredsBtn"${resettableCredentials ? "" : " disabled"}>${credentialError ? "Reset Corrupted Credentials" : (resettableCredentials ? "Reset Credentials" : "No Stored Credentials")}</button>`
        : '<button class="btn-secondary" id="detailResetCredsBtn" disabled>No Credential Fields</button>'}
      ${source.enabled
        ? '<button class="btn-secondary" id="detailDisableBtn">Disable</button>'
        : '<button class="btn-secondary" id="detailEnableBtn">Enable</button>'}
      ${source.can_delete
        ? '<button class="btn-danger" id="detailDeleteBtn">Delete</button>'
        : '<button class="btn-secondary" id="detailDeleteBtn" disabled>Delete Locked</button>'}
    </div>
    <details class="advanced-block">
      <summary>Advanced Details</summary>
      <div class="advanced-block-body">
        <div class="detail-grid">
          <div class="detail-block">
            <div class="detail-label">Identity</div>
            <div class="detail-value"><strong>${esc(source.display_name)}</strong><br><span class="mono">${esc(source.source_key)}</span></div>
          </div>
          <div class="detail-block">
            <div class="detail-label">Routing</div>
            <div class="detail-value"><span class="mono">${esc(source.job_name)}</span><br>${esc(source.provider_name || source.source_type)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">Lifecycle</div>
            <div class="detail-value">Created: ${esc(fmtTs(source.created_ts_ms))}<br>Updated: ${esc(fmtTs(source.updated_ts_ms))}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">Masked Credentials</div>
            <pre class="mono">${esc(JSON.stringify(source.masked_credentials || {}, null, 2))}</pre>
          </div>
          <div class="detail-block">
            <div class="detail-label">Settings</div>
            <pre class="mono">${esc(JSON.stringify(source.settings || {}, null, 2))}</pre>
          </div>
          <div class="detail-block">
            <div class="detail-label">Template Policy</div>
            <div class="detail-value">
              ${source.builtin ? "Built-in provider: identity and routing are locked." : "Custom RSS feed: delete is allowed, routing remains fixed to ingest_now."}<br>
              Delete allowed: ${source.can_delete ? "yes" : "no"}<br>
              Credentials replace or clear: ${template && template.credential_fields && template.credential_fields.length ? "supported" : "not required"}
            </div>
          </div>
        </div>
      </div>
    </details>
    <div class="card card-inline">
      <div class="card-head card-inline-head">
        <h2>Source Logs</h2>
      </div>
      <div class="logs" id="detailLogs"><div class="empty">Loading logs...</div></div>
    </div>
  `;

  el("detailEditBtn").addEventListener("click", () => openModal(source));
  el("detailTestBtn").addEventListener("click", () => testSource(source.source_key));
  const resetCredsBtn = el("detailResetCredsBtn");
  if (supportsCredentialReset && resettableCredentials && resetCredsBtn) {
    resetCredsBtn.addEventListener("click", () => resetSourceCredentials(source, template));
  }
  const enableBtn = el("detailEnableBtn");
  const disableBtn = el("detailDisableBtn");
  if (enableBtn) enableBtn.addEventListener("click", () => toggleSource(source.source_key, true));
  if (disableBtn) disableBtn.addEventListener("click", () => toggleSource(source.source_key, false));
  const deleteBtn = el("detailDeleteBtn");
  if (source.can_delete) deleteBtn.addEventListener("click", () => deleteSource(source.source_key));
}

async function loadLogs() {
  const source = selectedSource();
  const target = el("detailLogs");
  if (!source || !target) return;
  target.innerHTML = `<div class="empty">Loading logs...</div>`;
  try {
    const payload = await request(`/api/data_sources/logs?source_key=${encodeURIComponent(source.source_key)}&limit=80`);
    const logs = payload.logs || [];
    if (!logs.length) {
      target.innerHTML = `<div class="empty">No logs for this source yet.</div>`;
      return;
    }
    target.innerHTML = logs.map((row) => `
      <div class="log-row">
        <div class="log-top">
          <span class="pill ${String(row.level || "").toLowerCase() === "error" ? "err" : "dim"}">${esc(row.level)}</span>
          <span class="pill dim mono">${esc(row.event_type)}</span>
          <span class="mono subline">${esc(fmtTs(row.ts_ms))}</span>
        </div>
        <div class="log-message">${esc(row.message)}</div>
      </div>
    `).join("");
  } catch (error) {
    target.innerHTML = `<div class="empty">${esc(error.message)}</div>`;
  }
}

async function refreshSources({ preserveSelection = true } = {}) {
  const payload = await request("/api/data_sources", { method: "GET" });
  state.sources = payload.sources || [];
  state.templates = payload.templates || [];
  state.runtime = payload.runtime || {};
  state.auth = payload.auth || {};
  renderSession();
  if (!preserveSelection || !state.sources.some((row) => String(row.source_key) === String(state.selectedKey))) {
    state.selectedKey = state.sources.length ? String(state.sources[0].source_key) : "";
  }
  renderMetrics(payload);
  renderOverview();
  renderSourceCards();
  renderTable();
  renderDetail();
  if (state.selectedKey) loadLogs();
}

function credentialFieldInputId(field) {
  return `credentialField__${field}`;
}

function clearCredentialInputId(field) {
  return `clearCredentialField__${field}`;
}

function settingFieldInputId(field) {
  return `settingField__${field}`;
}

function schemaSummary(template, source) {
  if (!template) return "No template metadata available.";
  const createDelete = template.allow_delete ? "Delete is allowed." : "Delete is locked; disable instead.";
  const routing = template.routing_locked ? "The runtime routing for this source is fixed automatically." : "Routing can be changed.";
  const identity = source?.builtin ? "This is a built-in source. Its identity is fixed." : "This custom RSS feed can be renamed or removed.";
  const creds = template.credential_fields.length
    ? "Credential values are stored securely in the database and can be replaced or cleared here."
    : "No credentials are required.";
  return `${identity} ${routing} ${createDelete} ${creds}`;
}

function renderCredentialEditors(template, source) {
  const fields = (template && template.credential_fields) || [];
  const target = el("credentialsFields");
  const clearTarget = el("clearCredentials");
  if (!fields.length) {
    target.innerHTML = `<div class="empty">No credentials are required for this template.</div>`;
    clearTarget.innerHTML = "";
    return;
  }
  target.innerHTML = fields.map((field) => `
    <label class="editor-field" for="${esc(credentialFieldInputId(field.field))}">
      <span>${esc(field.label)}</span>
      <input
        id="${esc(credentialFieldInputId(field.field))}"
        type="password"
        placeholder="${source?.masked_credentials?.[field.field] ? "Configured; leave blank to preserve" : "Enter new secret"}"
      >
      <span class="field-help">${source?.masked_credentials?.[field.field] ? "A stored value already exists." : "Stored securely in the database."}</span>
    </label>
  `).join("");
  clearTarget.innerHTML = fields.map((field) => `
    <label class="checkbox-row" for="${esc(clearCredentialInputId(field.field))}">
      <input id="${esc(clearCredentialInputId(field.field))}" type="checkbox">
      <span>Clear ${esc(field.label)}</span>
    </label>
  `).join("");
}

function renderSettingEditors(template, source) {
  const fields = (template && template.setting_fields) || [];
  const target = el("settingsFields");
  if (!fields.length) {
    target.innerHTML = `<div class="empty">No structured settings are defined for this template.</div>`;
    return;
  }
  const sourceSettings = (source && source.settings) || {};
  target.innerHTML = fields.map((field) => `
    <label class="editor-field" for="${esc(settingFieldInputId(field.field))}">
      <span>${esc(field.label)}</span>
      <input
        id="${esc(settingFieldInputId(field.field))}"
        type="${esc(field.type || "text")}"
        value="${esc(sourceSettings[field.field] ?? "")}"
        placeholder="${field.required ? "Required" : "Optional"}"
      >
      <span class="field-help">${field.required ? "Required for this source." : "Saved only for this source."}</span>
    </label>
  `).join("");
}

function renderTemplateOptions(source = null) {
  const select = el("templateKey");
  const options = source
    ? [templateForSource(source)].filter(Boolean)
    : createTemplates();
  select.innerHTML = options.map((template) => `
    <option value="${esc(template.template_key)}">${esc(template.display_name)}</option>
  `).join("");
  select.disabled = !!source;
}

function applyTemplate(templateKey, source = null) {
  const template = templateByKey(templateKey);
  if (!template) return;
  const sourceKeyInput = el("sourceKey");
  const isCreate = !source;
  state.editingSource = source;
  el("sourceType").value = template.source_type || "";
  el("providerName").value = template.provider_name || "";
  el("jobName").value = template.job_name || "";
  sourceKeyInput.readOnly = !isCreate;
  sourceKeyInput.placeholder = template.template_key === "rss_feed" ? "rss:reuters_top" : template.source_key || "";
  sourceKeyInput.value = source ? String(source.source_key || "") : "";
  el("displayName").value = source ? String(source.display_name || "") : String(template.display_name || "");
  el("enabled").checked = source ? !!source.enabled : true;
  el("replaceCredentials").checked = false;
  renderCredentialEditors(template, source);
  renderSettingEditors(template, source);
  el("schemaSummary").textContent = schemaSummary(template, source);
}

function openModal(source = null) {
  state.editingKey = source ? String(source.source_key || "") : "";
  state.editingSource = source;
  el("modalTitle").textContent = source ? "Edit Data Source" : "Create RSS Feed";
  renderTemplateOptions(source);
  const template = source ? templateForSource(source) : createTemplates()[0];
  if (!template) {
    flash("No creatable source templates are available.", true);
    return;
  }
  el("templateKey").value = String(template.template_key || "");
  applyTemplate(String(template.template_key || ""), source);
  el("modalShell").classList.add("is-open");
}

function closeModal() {
  el("modalShell").classList.remove("is-open");
}

function collectCredentials(template) {
  const credentials = {};
  const clear = [];
  for (const field of (template.credential_fields || [])) {
    const value = String(el(credentialFieldInputId(field.field)).value || "").trim();
    if (value) credentials[field.field] = value;
    if (el(clearCredentialInputId(field.field)).checked) clear.push(field.field);
  }
  return { credentials, clear };
}

function collectSettings(template) {
  const settings = {};
  for (const field of (template.setting_fields || [])) {
    const raw = String(el(settingFieldInputId(field.field)).value || "").trim();
    if (raw !== "") settings[field.field] = raw;
  }
  return settings;
}

async function saveSource(event) {
  event.preventDefault();
  const template = templateByKey(el("templateKey").value);
  if (!template) {
    flash("Missing source template.", true);
    return;
  }
  const sourceKey = String(el("sourceKey").value || "").trim();
  if (!state.editingKey && !sourceKey) {
    flash("Source key is required for new RSS feeds.", true);
    return;
  }
  const actor = state.session.actor.trim() || "operator";
  const settings = collectSettings(template);
  const { credentials, clear } = collectCredentials(template);
  const replaceCredentials = !!el("replaceCredentials").checked;
  const body = {
    actor,
    source_key: sourceKey,
    display_name: el("displayName").value.trim(),
    source_type: template.source_type,
    provider_name: template.provider_name,
    job_name: template.job_name,
    enabled: el("enabled").checked,
    settings,
    replace_credentials: replaceCredentials,
  };
  if (replaceCredentials || Object.keys(credentials).length) body.credentials = credentials;
  if (clear.length) body.clear_credential_fields = clear;
  const url = state.editingKey ? "/api/data_sources/update" : "/api/data_sources/create";
  await request(url, { method: "POST", body: JSON.stringify(body) });
  closeModal();
  flash(state.editingKey ? "Source updated." : "Source created.");
  await refreshSources({ preserveSelection: false });
}

async function toggleSource(sourceKey, enabled) {
  await request(enabled ? "/api/data_sources/enable" : "/api/data_sources/disable", {
    method: "POST",
    body: JSON.stringify({ source_key: sourceKey, actor: state.session.actor.trim() || "operator" }),
  });
  flash(enabled ? "Source enabled." : "Source disabled.");
  await refreshSources();
}

async function testSource(sourceKey) {
  const result = await request("/api/data_sources/test", {
    method: "POST",
    body: JSON.stringify({ source_key: sourceKey, actor: state.session.actor.trim() || "operator" }),
  });
  flash(result.message || result.error || "Test completed.");
  await refreshSources();
}

async function deleteSource(sourceKey) {
  const actor = state.session.actor.trim() || "operator";
  const confirmed = await requestConfirmation({
    title: "Delete data source",
    action: "Delete data source",
    target: sourceKey,
    consequence: "This removes the source configuration and reconciles ingestion jobs. Built-in sources may be protected by the server.",
    confirmText: "DELETE_SOURCE",
    requireReason: true,
    minReasonLength: 8,
    submitLabel: "Delete source",
    actor,
    source: "data_sources_ui",
  });
  if (!confirmed.ok) return;
  await request("/api/data_sources/delete", {
    method: "POST",
    body: JSON.stringify({
      source_key: sourceKey,
      actor,
      ...confirmed.payload,
    }),
  });
  state.selectedKey = "";
  flash("Source deleted.");
  await refreshSources({ preserveSelection: false });
}

async function resetSourceCredentials(source, template) {
  const fields = ((template && template.credential_fields) || [])
    .map((field) => String(field.field || "").trim())
    .filter(Boolean);
  if (!fields.length) {
    flash("This source has no credential fields to clear.", true);
    return;
  }
  const sourceKey = String(source?.source_key || "").trim();
  const credentialError = String(source?.credential_error || "").trim();
  if (!sourceKey) {
    flash("Missing source key.", true);
    return;
  }
  const actor = state.session.actor.trim() || "operator";
  const confirmed = await requestConfirmation({
    title: "Clear stored credentials",
    action: "Clear stored credentials",
    target: `${sourceKey}: ${fields.join(", ")}`,
    consequence: credentialError
      ? `Stored credentials will be cleared. Current decode error: ${credentialError}`
      : "Stored credentials will be cleared and the source will require new credentials before protected provider access works.",
    confirmText: "RESET_CREDENTIALS",
    requireReason: true,
    minReasonLength: 8,
    submitLabel: "Clear credentials",
    actor,
    source: "data_sources_ui",
  });
  if (!confirmed.ok) return;
  await request("/api/data_sources/update", {
    method: "POST",
    body: JSON.stringify({
      source_key: sourceKey,
      actor,
      clear_credential_fields: fields,
      ...confirmed.payload,
    }),
  });
  flash(`Stored credentials cleared for ${sourceKey}.`);
  await refreshSources();
}

function wireEvents() {
  el("addSourceBtn").addEventListener("click", () => openModal());
  el("refreshBtn").addEventListener("click", () => refreshSources().catch((error) => flash(error.message, true)));
  el("modalCloseBtn").addEventListener("click", closeModal);
  el("modalCancelBtn").addEventListener("click", closeModal);
  el("modalForm").addEventListener("submit", saveSource);
  el("modalShell").addEventListener("click", (event) => {
    if (event.target === el("modalShell")) closeModal();
  });
  el("templateKey").addEventListener("change", (event) => {
    applyTemplate(String(event.target.value || ""), null);
  });
  el("saveSessionBtn").addEventListener("click", () => {
    state.session.actor = String(el("actorInput").value || "").trim();
    state.session.token = String(el("tokenInput").value || "").trim();
    saveSession();
    flash("Session settings saved.");
  });
  el("clearTokenBtn").addEventListener("click", () => {
    state.session.token = "";
    saveSession();
    flash("API token cleared.");
  });
}

async function boot() {
  loadSession();
  wireEvents();
  renderSession();
  await refreshSources({ preserveSelection: false });
  state.refreshTimer = window.setInterval(() => {
    refreshSources().catch((error) => {
      flash(error.message, true);
    });
  }, 15000);
}

window.addEventListener("DOMContentLoaded", () => {
  boot().catch((error) => {
    flash(error.message, true);
  });
});
