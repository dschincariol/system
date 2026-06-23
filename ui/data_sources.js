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
  providerAccounts: [],
  providerAccountTemplates: [],
  runtime: {},
  auth: {},
  selectedKey: "",
  refreshTimer: null,
  editingKey: "",
  editingSource: null,
  editingAccountKey: "",
  lastTestResults: {},
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
  if (status === "test_degraded") return `<span class="pill warn">${esc(status)}</span>`;
  if (status === "error" || status === "test_failed") return `<span class="pill err">${esc(status)}</span>`;
  if (status === "test_unsupported") return `<span class="pill dim">${esc(status)}</span>`;
  return `<span class="pill dim">${esc(status)}</span>`;
}

function runnableStateLabel(value) {
  const runtimeState = String(value || "off").toLowerCase();
  const labels = {
    "off": "Off",
    "enabled-missing-credential": "Missing Credential",
    "enabled-credentialed-not-scheduled": "Not Scheduled",
    "scheduled-waiting": "Scheduled",
    "running": "Running",
    "degraded": "Degraded",
    "failed": "Failed",
    "healthy": "Healthy",
  };
  return labels[runtimeState] || runtimeState.replace(/-/g, " ");
}

function runnableStateTone(value) {
  const runtimeState = String(value || "off").toLowerCase();
  if (runtimeState === "healthy" || runtimeState === "running") return "ok";
  if (runtimeState === "failed" || runtimeState === "enabled-missing-credential") return "err";
  if (runtimeState === "degraded" || runtimeState === "scheduled-waiting") return "warn";
  return "dim";
}

function runnableStatePill(source) {
  const runtimeState = String(source?.runnable_state || "off").toLowerCase();
  return `<span class="pill ${esc(runnableStateTone(runtimeState))}">${esc(runnableStateLabel(runtimeState))}</span>`;
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
  const { allowApplicationError = false, ...fetchOptions } = options;
  const headers = new Headers(options.headers || {});
  headers.set("Content-Type", "application/json");
  if (state.session.token.trim()) {
    headers.set("X-API-Token", state.session.token.trim());
  }
  const response = await fetch(url, {
    cache: "no-store",
    ...fetchOptions,
    headers,
  });
  const payload = await response.json();
  const allowedBusinessRefusal = allowApplicationError
    && response.status >= 400
    && response.status < 500
    && payload
    && payload.ok === false;
  if ((!response.ok && !allowedBusinessRefusal) || (payload.ok === false && !allowApplicationError)) {
    const reason = payload.reason_code || payload.error || payload.detail || `request_failed:${response.status}`;
    const message = payload.message || payload.reason || reason;
    throw new Error(reason && reason !== message ? `${message} (${reason})` : message);
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

function providerAccountByKey(accountKey) {
  return state.providerAccounts.find((row) => String(row.account_key) === String(accountKey)) || null;
}

function providerAccountTemplateByKey(accountKey) {
  return state.providerAccountTemplates.find((row) => String(row.account_key) === String(accountKey)) || null;
}

function accountStatusPill(account) {
  const status = String(account?.status || "empty").toLowerCase();
  if (status === "configured") return `<span class="pill ok">configured</span>`;
  if (status === "error") return `<span class="pill err">error</span>`;
  return `<span class="pill dim">${esc(status || "empty")}</span>`;
}

function guideForSource(source) {
  const guide = templateForSource(source)?.guide || null;
  return guide || {
    category: "Source",
    summary: "This source is managed from this page.",
    needs: ["Review the source state below."],
    setup: ["Select Edit Source, adjust settings or credentials, then run Test Connection."],
    when_enabled: "The runtime will include this source in ingestion and health monitoring.",
    safety_warnings: [],
  };
}

function isFxDataSource(source, template = null) {
  const guide = (template && template.guide) || {};
  const parts = [
    source && source.source_key,
    source && source.source_type,
    source && source.provider_name,
    source && source.display_name,
    source && source.job_name,
    source && source.asset_class,
    template && template.template_key,
    template && template.category,
    guide.category,
    guide.summary,
    guide.when_enabled,
  ].map((value) => String(value || "").toLowerCase());
  return parts.some((value) => /(^|[^a-z0-9])(fx|forex|foreign exchange|oanda|currency pair)([^a-z0-9]|$)/.test(value));
}

function fxSourceBadge(source, template = null) {
  if (!isFxDataSource(source, template)) return "";
  return `<span class="pill fx">FX feed</span>`;
}

function fxFeedStatusPill(source) {
  const status = String(source?.status || source?.runnable_state || "unknown").toLowerCase();
  const tone = status === "ok" || status === "tested" || status === "healthy"
    ? "ok"
    : (status.includes("fail") || status === "error" ? "err" : "dim");
  return `<span class="pill ${tone}">FX status ${esc(status || "unknown")}</span>`;
}

function deriveSourceState(source, template) {
  const credentialFields = (template && template.credential_fields) || [];
  const credentialError = String(source?.credential_error || "").trim();
  const lastError = String(source?.last_error || "").trim();
  const status = String(source?.status || "").trim().toLowerCase();
  const runtimeState = String(source?.runnable_state || "").trim().toLowerCase();
  const missingEnvVars = (source?.missing_credential_env_vars || [])
    .map((name) => String(name || "").trim())
    .filter(Boolean);
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

  if (runtimeState === "enabled-missing-credential") {
    return {
      label: "Missing credential",
      tone: "err",
      priority: 0,
      detail: missingEnvVars.length
        ? `Missing runtime credential: ${missingEnvVars.join(", ")}.`
        : "A required runtime credential is missing or could not be projected.",
      nextStep: "Enter the required credential or disable the source."
    };
  }

  if (runtimeState === "enabled-credentialed-not-scheduled") {
    if (template && template.runtime_runnable === false) {
      return {
        label: "Read-only",
        tone: "dim",
        priority: 4,
        detail: "This broker-data source is enabled for read-only status visibility and is intentionally not scheduled as a runtime job.",
        nextStep: "Use Test Connection for account/data visibility; use broker execution controls for any trading authority."
      };
    }
    return {
      label: "Not scheduled",
      tone: "dim",
      priority: enabled ? 3 : 6,
      detail: "This source is enabled but is not currently part of the runnable job set.",
      nextStep: enabled ? "Review runtime policy and desired jobs." : "Enable it only if you want this source active."
    };
  }

  if (runtimeState === "scheduled-waiting") {
    return {
      label: "Scheduled",
      tone: "warn",
      priority: 3,
      detail: "The job is desired and waiting for runtime startup or first health evidence.",
      nextStep: "Wait for the supervisor to start the job or inspect runtime logs."
    };
  }

  if (runtimeState === "running") {
    return {
      label: "Running",
      tone: "ok",
      priority: 4,
      detail: "The supervised job is running; health evidence has not yet promoted it to healthy.",
      nextStep: "Monitor provider and pipeline health."
    };
  }

  if (runtimeState === "degraded") {
    return {
      label: "Degraded",
      tone: "warn",
      priority: 1,
      detail: lastError || "Runtime health is stale or degraded.",
      nextStep: "Inspect provider telemetry and source logs."
    };
  }

  if (runtimeState === "failed") {
    return {
      label: "Failed",
      tone: "err",
      priority: 1,
      detail: lastError || "Runtime health reports a failed job or pipeline.",
      nextStep: "Review the last error, then test or adjust the source."
    };
  }

  if (runtimeState === "healthy") {
    return {
      label: "Healthy",
      tone: "ok",
      priority: 4,
      detail: "This source is scheduled and has fresh healthy runtime evidence.",
      nextStep: "No action needed."
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

  if (status === "test_degraded") {
    return {
      label: "Degraded",
      tone: "warn",
      priority: 2,
      detail: lastError || "The latest test reached a fallback or partial provider path and was not counted as success.",
      nextStep: "Review the test evidence and provider guidance before enabling reliance on this source."
    };
  }

  if (status === "test_unsupported") {
    return {
      label: "Unsupported",
      tone: "dim",
      priority: 5,
      detail: lastError || "This source has no successful external connection probe.",
      nextStep: "Do not treat this source as connected; use runtime health or configure a supported provider test."
    };
  }

  if (status === "error" || status === "test_failed") {
    return {
      label: "Failed",
      tone: "err",
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
  const jobState = source?.job_runnable_state || ((state.runtime.jobs || {})[String(source?.job_name || "")] || null);
  return { providerTelemetry, pipelineHealth, jobState };
}

function renderMetrics(payload) {
  const sources = payload.sources || [];
  const enabled = sources.filter((row) => row.enabled).length;
  const healthy = sources.filter((row) => deriveSourceState(row, templateForSource(row)).tone === "ok").length;
  const runnable = sources.filter((row) => ["scheduled-waiting", "running", "healthy", "degraded"].includes(String(row.runnable_state || ""))).length;
  const errors = sources.reduce((sum, row) => sum + Number(row.error_count || 0), 0);
  el("metricTotal").textContent = String(sources.length);
  el("metricEnabled").textContent = `${enabled} / ${runnable}`;
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

function renderProviderAccounts() {
  const target = el("providerAccounts");
  if (!target) return;
  if (!state.providerAccounts.length) {
    target.innerHTML = `<div class="empty">No provider accounts are defined.</div>`;
    return;
  }
  target.innerHTML = state.providerAccounts.map((account) => {
    const schema = account.schema || providerAccountTemplateByKey(account.account_key) || {};
    const guide = account.guide || schema.guide || {};
    const configuredFields = Object.entries(account.configured_fields || {})
      .filter(([, configured]) => !!configured)
      .map(([field]) => field);
    const usedBy = (account.used_by || schema.used_by || [])
      .map((item) => item.display_name || item.source_key || item.job_name)
      .filter(Boolean);
    return `
      <div class="account-card" data-account-key="${esc(account.account_key)}">
        <div class="account-card-head">
          <div>
            <div class="source-card-title">${esc(account.display_name)}</div>
            <div class="source-card-text">${esc(guide.summary || account.provider_name || "")}</div>
          </div>
          ${accountStatusPill(account)}
        </div>
        <div class="source-card-meta">
          <span class="pill dim mono">${esc(account.provider_name)}</span>
          <span class="pill ${configuredFields.length ? "ok" : "dim"}">${configuredFields.length ? `${configuredFields.length} fields set` : "empty"}</span>
        </div>
        <div class="account-used">
          <div class="detail-label">Used By</div>
          <div class="account-used-list">${usedBy.map((name) => `<span class="pill dim">${esc(name)}</span>`).join("") || '<span class="pill dim">none</span>'}</div>
        </div>
        <div class="source-card-actions">
          <button class="btn-secondary account-edit-btn" type="button" data-account-key="${esc(account.account_key)}">Edit Account</button>
        </div>
      </div>
    `;
  }).join("");
  target.querySelectorAll(".account-edit-btn").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      openAccountModal(button.getAttribute("data-account-key") || "");
    });
  });
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
    const template = templateForSource(source);
    const guide = guideForSource(source);
    const stateInfo = deriveSourceState(source, template);
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
          ${fxSourceBadge(source, template)}
          <span class="pill dim">${source.enabled ? "Enabled" : "Disabled"}</span>
          ${runnableStatePill(source)}
          ${isFxDataSource(source, template) ? fxFeedStatusPill(source) : ""}
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
        <div><strong>${esc(source.display_name)}</strong> ${source.builtin ? '<span class="pill dim">builtin</span>' : '<span class="pill ok">custom</span>'} ${fxSourceBadge(source, templateForSource(source))}</div>
        <div class="mono subline">${esc(source.source_key)}</div>
      </td>
      <td>${esc(source.source_type)}</td>
      <td class="mono">${esc(source.job_name)}</td>
      <td>${source.enabled ? '<span class="pill ok">enabled</span>' : '<span class="pill err">disabled</span>'}</td>
      <td>${runnableStatePill(source)}</td>
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
  const jobState = runtime.jobState || {};
  return `
    <div class="detail-block">
      <div class="detail-label">Runnable Job State</div>
      <div class="detail-value">
        ${runnableStatePill({ runnable_state: jobState.state || source.runnable_state })}<br>
        Desired: ${jobState.desired ? "yes" : "no"}<br>
        Running: ${jobState.running ? "yes" : "no"}<br>
        Reason: ${esc(jobState.reason || source.runnable_state_reason || "n/a")}
      </div>
    </div>
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

function renderCredentialResolution(source) {
  const rows = source?.credential_resolution || [];
  if (!rows.length) return "No account-linked credential fields for this source.";
  return rows.map((row) => {
    const mode = String(row.mode || "missing");
    const tone = mode === "overridden" ? "warn" : (mode === "inherited" ? "ok" : (mode === "missing" ? "err" : "dim"));
    const owner = mode === "inherited"
      ? ` from ${row.account_display_name || row.account_key}`
      : (mode === "overridden" ? " by source override" : (mode === "runtime_external" ? " from runtime" : ""));
    return `<div class="credential-resolution-row">
      <span class="pill ${tone}">${esc(mode)}</span>
      <span class="mono">${esc(row.env_var)}</span>
      <span>${esc(owner)}</span>
    </div>`;
  }).join("");
}

function testStatusTone(result) {
  const status = String(result?.status || "").toLowerCase();
  if (status === "pass") return "ok";
  if (status === "fail") return "err";
  if (status === "degraded") return "warn";
  return "dim";
}

function renderEvidence(evidence) {
  const entries = Object.entries(evidence || {});
  if (!entries.length) return "No evidence returned.";
  return entries.map(([key, value]) => {
    const rendered = typeof value === "string" || typeof value === "number" || typeof value === "boolean"
      ? String(value)
      : JSON.stringify(value);
    return `<div class="evidence-row"><span class="mono">${esc(key)}</span><span>${esc(rendered)}</span></div>`;
  }).join("");
}

function contractTone(value) {
  const status = String(value || "").toLowerCase();
  if (status === "pass") return "ok";
  if (status === "warn") return "warn";
  if (status === "fail") return "err";
  return "dim";
}

function renderPopulateEvidencePanel(source) {
  const evidence = source?.populate_evidence || {};
  const contract = source?.data_contract || {};
  const contractStatus = String(evidence.contract_status || "missing").toLowerCase();
  const providerEvidence = evidence.provider_evidence || {};
  return `
    <div class="detail-block populate-evidence-block">
      <div class="detail-label">Populate Evidence</div>
      <div class="detail-value">
        <span class="pill ${contractTone(contractStatus)}">${esc(contractStatus)}</span>
        <span class="pill dim mono">${esc(contract.storage_table || evidence.storage_table || "no table")}</span><br>
        Rows: ${esc(evidence.row_count ?? 0)}<br>
        Latest: ${esc(fmtTs(evidence.latest_ts_ms))}<br>
        Latency: ${esc(fmtAgeMs(evidence.latency_ms))}<br>
        Stale/gap: ${esc(evidence.stale_gap_status || "not checked")}<br>
        Missing/nulls: ${esc(JSON.stringify(evidence.missing_null_counts || {}))}<br>
        Duplicate drops: ${esc(evidence.duplicate_drops || 0)}${evidence.error ? `<br>Error: ${esc(evidence.error)}` : ""}
        <div class="evidence-list">${renderEvidence(providerEvidence)}</div>
      </div>
    </div>
    <div class="detail-block">
      <div class="detail-label">Data Contract</div>
      <div class="detail-value">
        Shape: ${esc(contract.normalized_shape || "not defined")}<br>
        Required: ${esc((contract.required_fields || []).join(", ") || "none")}<br>
        Unique key: ${esc((contract.unique_key || []).join(", ") || "none")}<br>
        PIT: ${esc(contract.point_in_time_availability || "n/a")}<br>
        Consumer: ${esc(contract.consumer || "n/a")}
      </div>
    </div>
  `;
}

function renderTestResultPanel(source) {
  const result = state.lastTestResults[String(source?.source_key || "")];
  if (!result) return "";
  if (isFxDataSource(source, templateForSource(source))) {
    return renderFxTestResultPanel(result);
  }
  const tone = testStatusTone(result);
  const nextSteps = (result.next_steps || []).map((item) => `• ${esc(item)}`).join("<br>") || "No next steps returned.";
  return `
    <div class="detail-block test-result-block">
      <div class="detail-label">Latest Test Result</div>
      <div class="detail-value">
        <span class="pill ${tone}">${esc(result.status || "unknown")}</span>
        <span class="pill dim">${esc(result.classification || "unclassified")}</span><br>
        ${esc(result.message || result.error || "No message returned.")}
        <div class="evidence-list">${renderEvidence(result.evidence || {})}</div>
        <div class="next-steps">${nextSteps}</div>
      </div>
    </div>
  `;
}

function renderFxTestResultPanel(result) {
  const tone = testStatusTone(result);
  const safe = {};
  for (const key of ["status", "ok", "latency_ms", "latency", "detail", "message"]) {
    if (result && Object.prototype.hasOwnProperty.call(result, key)) {
      safe[key] = result[key];
    }
  }
  const evidence = result && result.evidence && typeof result.evidence === "object" ? result.evidence : {};
  for (const key of ["status", "ok", "latency_ms", "latency", "detail", "message"]) {
    if (Object.prototype.hasOwnProperty.call(evidence, key) && !Object.prototype.hasOwnProperty.call(safe, key)) {
      safe[key] = evidence[key];
    }
  }
  return `
    <div class="detail-block test-result-block fx-test-result">
      <div class="detail-label">FX Feed Connectivity</div>
      <div class="detail-value">
        <span class="pill ${tone}">${esc(result.status || "unknown")}</span><br>
        <div class="evidence-list">${renderEvidence(safe)}</div>
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
          ${runnableStatePill(source)}
          ${fxSourceBadge(source, template)}
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
        <div class="detail-value">${esc(guide.when_enabled || "The runtime will include this source in ingestion and health monitoring.")}</div>
      </div>
      <div class="detail-block">
        <div class="detail-label">Provider Reference</div>
        <div class="detail-value">
          ${guide.docs_url ? `<a href="${esc(guide.docs_url)}" target="_blank" rel="noopener noreferrer">Docs</a><br>` : ""}
          ${guide.signup_url ? `<a href="${esc(guide.signup_url)}" target="_blank" rel="noopener noreferrer">Signup</a><br>` : ""}
          ${guide.plan_note ? esc(guide.plan_note) : "No provider plan note supplied."}
          ${(guide.safety_warnings || []).length ? `<br><br>${(guide.safety_warnings || []).map((item) => `• ${esc(item)}`).join("<br>")}` : ""}
        </div>
      </div>
      <div class="detail-block">
        <div class="detail-label">Credential State</div>
        <div class="detail-value">
          Stored value present: ${credentialsStored ? "yes" : "no"}<br>
          Readable value present: ${credentialsConfigured ? "yes" : "no"}<br>
          Runtime credentialed: ${source.runtime_credentialed ? "yes" : "no"}<br>
          Missing runtime env: ${(source.missing_credential_env_vars || []).length ? esc((source.missing_credential_env_vars || []).join(", ")) : "none"}<br>
          Credential fields: ${supportsCredentialReset ? esc(credentialFields.map((field) => field.field).join(", ")) : "none"}${credentialError ? `<br>Stored value problem: <span class="mono">${esc(credentialError)}</span>` : ""}
          <div class="credential-resolution">${renderCredentialResolution(source)}</div>
        </div>
      </div>
      <div class="detail-block">
        <div class="detail-label">Recent Health</div>
        <div class="detail-value">Last success: ${esc(fmtTs(source.last_success_ts_ms))}<br>Last test: ${esc(fmtTs(source.last_test_ts_ms))}<br>Last error: ${esc(source.last_error || "none")}<br>Error count: ${esc(source.error_count || 0)}</div>
      </div>
      ${renderPopulateEvidencePanel(source)}
      ${renderTestResultPanel(source)}
      ${renderRuntimePanel(source)}
    </div>
    <div class="detail-actions">
      <button class="btn" id="detailEditBtn">Edit Source</button>
      <button class="btn-secondary" id="detailTestBtn">Test Connection</button>
      <button class="btn-secondary" id="detailPopulateBtn">Populate Now</button>
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
  el("detailPopulateBtn").addEventListener("click", () => populateSource(source.source_key));
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
    const logState = payload.runnable_state || source.runnable_state || "off";
    target.innerHTML = `
      <div class="log-state">${runnableStatePill({ runnable_state: logState })}</div>
    ` + logs.map((row) => `
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
  state.providerAccounts = payload.provider_accounts || [];
  state.providerAccountTemplates = payload.provider_account_templates || [];
  state.runtime = payload.runtime || {};
  state.auth = payload.auth || {};
  renderSession();
  if (!preserveSelection || !state.sources.some((row) => String(row.source_key) === String(state.selectedKey))) {
    state.selectedKey = state.sources.length ? String(state.sources[0].source_key) : "";
  }
  renderMetrics(payload);
  renderOverview();
  renderProviderAccounts();
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

function accountCredentialFieldInputId(field) {
  return `accountCredentialField__${field}`;
}

function clearAccountCredentialInputId(field) {
  return `clearAccountCredentialField__${field}`;
}

function credentialFieldErrorId(field) {
  return `credentialFieldError__${field}`;
}

function settingFieldErrorId(field) {
  return `settingFieldError__${field}`;
}

function accountCredentialFieldErrorId(field) {
  return `accountCredentialFieldError__${field}`;
}

function fieldDocsLinks(field) {
  const links = [];
  if (field.docs_url) {
    links.push(`<a href="${esc(field.docs_url)}" target="_blank" rel="noopener noreferrer">Docs</a>`);
  }
  if (field.signup_url) {
    links.push(`<a href="${esc(field.signup_url)}" target="_blank" rel="noopener noreferrer">Signup</a>`);
  }
  return links.length ? `<span class="field-links">${links.join(" ")}</span>` : "";
}

function fieldHelpHtml(field, configured = false) {
  const parts = [];
  if (configured) parts.push("A stored value already exists.");
  if (field.help_text) parts.push(field.help_text);
  if (field.env_var || field.env_name) parts.push(`Runtime env: ${field.env_var || field.env_name}.`);
  if (field.plan_note) parts.push(field.plan_note);
  if (field.validation_hint) parts.push(field.validation_hint);
  if (field.safety_warning) parts.push(field.safety_warning);
  return `
    <span class="field-help">${esc(parts.join(" "))}</span>
    ${fieldDocsLinks(field)}
  `;
}

function resolutionForField(source, field) {
  const envName = String(field?.env_var || field?.env_name || "").trim();
  return (source?.credential_resolution || []).find((row) => String(row.env_var || "") === envName) || null;
}

function credentialModeText(source, field) {
  const resolution = resolutionForField(source, field);
  const mode = String(resolution?.mode || "").trim();
  if (mode === "inherited") return `Inherited from ${resolution.account_display_name || resolution.account_key}.`;
  if (mode === "overridden") return "Source override is active.";
  if (mode === "runtime_external") return "Runtime external value is active.";
  return "";
}

function setFieldError(input, errorId, message) {
  if (!input) return;
  const errorEl = el(errorId);
  const hasError = !!String(message || "").trim();
  input.classList.toggle("is-invalid", hasError);
  input.setAttribute("aria-invalid", hasError ? "true" : "false");
  if (errorEl) errorEl.textContent = hasError ? String(message) : "";
  const wrapper = input.closest(".editor-field");
  if (wrapper) wrapper.classList.toggle("has-error", hasError);
}

function validateFieldInput(field, input, errorId, { requireValue = false, honorRequired = true } = {}) {
  if (!input) return true;
  const value = String(input.value || "").trim();
  const label = String(field.label || field.field || "Field");
  const hint = String(field.validation_hint || "Review the expected format.");
  if ((requireValue || (honorRequired && field.required)) && !value) {
    setFieldError(input, errorId, `${label} is required.`);
    return false;
  }
  const pattern = String(field.validation_regex || field.validation?.regex || "").trim();
  if (value && pattern) {
    try {
      if (!new RegExp(pattern).test(value)) {
        setFieldError(input, errorId, `${label}: ${hint}`);
        return false;
      }
    } catch (_error) {
      // Backend validation remains authoritative if a shipped regex is unsupported.
    }
  }
  setFieldError(input, errorId, "");
  return true;
}

function wireFieldValidation(template) {
  for (const field of (template.credential_fields || [])) {
    const input = el(credentialFieldInputId(field.field));
    if (!input) continue;
    input.addEventListener("input", () => {
      validateFieldInput(field, input, credentialFieldErrorId(field.field), {
        requireValue: false,
        honorRequired: false,
      });
    });
  }
  for (const field of (template.setting_fields || [])) {
    const input = el(settingFieldInputId(field.field));
    if (!input) continue;
    input.addEventListener("input", () => {
      validateFieldInput(field, input, settingFieldErrorId(field.field), {
        requireValue: !!field.required,
      });
    });
  }
}

function validateEditor(template) {
  let ok = true;
  for (const field of (template.credential_fields || [])) {
    const input = el(credentialFieldInputId(field.field));
    ok = validateFieldInput(field, input, credentialFieldErrorId(field.field), {
      requireValue: false,
      honorRequired: false,
    }) && ok;
  }
  for (const field of (template.setting_fields || [])) {
    const input = el(settingFieldInputId(field.field));
    ok = validateFieldInput(field, input, settingFieldErrorId(field.field), {
      requireValue: !!field.required,
    }) && ok;
  }
  return ok;
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
      <span>${esc(field.label)} ${field.required ? '<span class="field-required">Required</span>' : '<span class="field-optional">Optional</span>'}</span>
      <input
        id="${esc(credentialFieldInputId(field.field))}"
        type="${esc(field.input_type || field.type || "password")}"
        autocomplete="new-password"
        aria-describedby="${esc(credentialFieldErrorId(field.field))}"
        placeholder="${esc(source?.masked_credentials?.[field.field] ? "Override configured; leave blank to preserve" : (resolutionForField(source, field)?.mode === "inherited" ? "Inherited; enter only to override" : (field.placeholder || "Enter new secret")))}"
      >
      ${fieldHelpHtml(field, !!source?.masked_credentials?.[field.field])}
      ${credentialModeText(source, field) ? `<span class="field-help">${esc(credentialModeText(source, field))}</span>` : ""}
      <span class="field-error" id="${esc(credentialFieldErrorId(field.field))}" aria-live="polite"></span>
    </label>
  `).join("");
  clearTarget.innerHTML = fields.map((field) => `
    <label class="checkbox-row" for="${esc(clearCredentialInputId(field.field))}">
      <input id="${esc(clearCredentialInputId(field.field))}" type="checkbox">
      <span>Clear ${esc(field.label)}</span>
    </label>
  `).join("");
}

function renderAccountCredentialEditors(account, template) {
  const fields = (template && template.credential_fields) || [];
  const target = el("accountCredentialsFields");
  const clearTarget = el("clearAccountCredentials");
  if (!fields.length) {
    target.innerHTML = `<div class="empty">No account credential fields are defined.</div>`;
    clearTarget.innerHTML = "";
    return;
  }
  target.innerHTML = fields.map((field) => `
    <label class="editor-field" for="${esc(accountCredentialFieldInputId(field.field))}">
      <span>${esc(field.label)} ${field.required ? '<span class="field-required">Required</span>' : '<span class="field-optional">Optional</span>'}</span>
      <input
        id="${esc(accountCredentialFieldInputId(field.field))}"
        type="${esc(field.input_type || field.type || "password")}"
        autocomplete="new-password"
        aria-describedby="${esc(accountCredentialFieldErrorId(field.field))}"
        placeholder="${esc(account?.masked_credentials?.[field.field] ? "Configured; leave blank to preserve" : (field.placeholder || "Enter new value"))}"
      >
      ${fieldHelpHtml(field, !!account?.masked_credentials?.[field.field])}
      <span class="field-error" id="${esc(accountCredentialFieldErrorId(field.field))}" aria-live="polite"></span>
    </label>
  `).join("");
  clearTarget.innerHTML = fields.map((field) => `
    <label class="checkbox-row" for="${esc(clearAccountCredentialInputId(field.field))}">
      <input id="${esc(clearAccountCredentialInputId(field.field))}" type="checkbox">
      <span>Clear ${esc(field.label)}</span>
    </label>
  `).join("");
}

function wireAccountFieldValidation(template) {
  for (const field of (template.credential_fields || [])) {
    const input = el(accountCredentialFieldInputId(field.field));
    if (!input) continue;
    input.addEventListener("input", () => {
      validateFieldInput(field, input, accountCredentialFieldErrorId(field.field), {
        requireValue: false,
        honorRequired: false,
      });
    });
  }
}

function validateAccountEditor(template) {
  let ok = true;
  for (const field of (template.credential_fields || [])) {
    const input = el(accountCredentialFieldInputId(field.field));
    ok = validateFieldInput(field, input, accountCredentialFieldErrorId(field.field), {
      requireValue: false,
      honorRequired: false,
    }) && ok;
  }
  return ok;
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
      <span>${esc(field.label)} ${field.required ? '<span class="field-required">Required</span>' : '<span class="field-optional">Optional</span>'}</span>
      <input
        id="${esc(settingFieldInputId(field.field))}"
        type="${esc(field.input_type || field.type || "text")}"
        value="${esc(sourceSettings[field.field] ?? "")}"
        aria-describedby="${esc(settingFieldErrorId(field.field))}"
        placeholder="${esc(field.placeholder || (field.required ? "Required" : "Optional"))}"
      >
      ${fieldHelpHtml(field)}
      <span class="field-error" id="${esc(settingFieldErrorId(field.field))}" aria-live="polite"></span>
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
  wireFieldValidation(template);
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

function openAccountModal(accountKey) {
  const key = String(accountKey || "").trim();
  const account = providerAccountByKey(key);
  const template = providerAccountTemplateByKey(key) || account?.schema || null;
  if (!account || !template) {
    flash("Provider account metadata is unavailable.", true);
    return;
  }
  state.editingAccountKey = key;
  el("accountModalTitle").textContent = `Edit ${account.display_name} Account`;
  el("accountKey").value = key;
  el("accountProviderName").value = account.provider_name || template.provider_name || "";
  el("accountSchemaSummary").textContent = (account.guide || template.guide || {}).summary || "Shared provider credentials for dependent feeds.";
  renderAccountCredentialEditors(account, template);
  wireAccountFieldValidation(template);
  el("accountModalShell").classList.add("is-open");
}

function closeAccountModal() {
  el("accountModalShell").classList.remove("is-open");
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

function collectAccountCredentials(template) {
  const credentials = {};
  const clear = [];
  for (const field of (template.credential_fields || [])) {
    const value = String(el(accountCredentialFieldInputId(field.field)).value || "").trim();
    if (value) credentials[field.field] = value;
    if (el(clearAccountCredentialInputId(field.field)).checked) clear.push(field.field);
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

function buildSourceSaveRequest() {
  const template = templateByKey(el("templateKey").value);
  if (!template) {
    flash("Missing source template.", true);
    return null;
  }
  const sourceKey = String(el("sourceKey").value || "").trim();
  if (!state.editingKey && !sourceKey) {
    flash("Source key is required for new RSS feeds.", true);
    return null;
  }
  const actor = state.session.actor.trim() || "operator";
  if (!validateEditor(template)) {
    flash("Fix the highlighted source fields before saving.", true);
    return null;
  }
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
  return { body, url };
}

async function saveSource(event) {
  event.preventDefault();
  const requestSpec = buildSourceSaveRequest();
  if (!requestSpec) return;
  const { body, url } = requestSpec;
  await request(url, { method: "POST", body: JSON.stringify(body) });
  closeModal();
  flash(state.editingKey ? "Source updated." : "Source created.");
  await refreshSources({ preserveSelection: false });
}

async function testAndSaveSource(event) {
  event.preventDefault();
  const requestSpec = buildSourceSaveRequest();
  if (!requestSpec) return;
  const body = { ...requestSpec.body, create: !state.editingKey };
  const result = await request("/api/data_sources/test_save", {
    method: "POST",
    body: JSON.stringify(body),
    allowApplicationError: true,
  });
  if (!result.saved) {
    flash(result.error || "Test & Save failed before credentials were stored.", true);
    return;
  }
  closeModal();
  const test = result.test || {};
  const testedKey = String(result.source_key || body.source_key || "");
  if (testedKey) state.lastTestResults[testedKey] = test;
  const status = String(test.status || (test.ok ? "pass" : "fail"));
  flash(`${status}: ${test.message || test.error || "Test & Save completed."}`, status === "fail");
  await refreshSources({ preserveSelection: false });
}

async function saveProviderAccount(event) {
  event.preventDefault();
  const accountKey = String(el("accountKey").value || state.editingAccountKey || "").trim();
  const template = providerAccountTemplateByKey(accountKey) || providerAccountByKey(accountKey)?.schema || null;
  if (!accountKey || !template) {
    flash("Missing provider account template.", true);
    return;
  }
  if (!validateAccountEditor(template)) {
    flash("Fix the highlighted account fields before saving.", true);
    return;
  }
  const actor = state.session.actor.trim() || "operator";
  const { credentials, clear } = collectAccountCredentials(template);
  const body = {
    actor,
    account_key: accountKey,
    replace_credentials: !!el("replaceAccountCredentials").checked,
  };
  if (body.replace_credentials || Object.keys(credentials).length) body.credentials = credentials;
  if (clear.length) body.clear_credential_fields = clear;
  await request("/api/data_sources/accounts/update", { method: "POST", body: JSON.stringify(body) });
  closeAccountModal();
  flash("Provider account updated.");
  await refreshSources();
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
    allowApplicationError: true,
  });
  state.lastTestResults[String(sourceKey)] = result;
  const status = String(result.status || (result.ok ? "pass" : "fail"));
  const classification = String(result.classification || "");
  flash(`${status}: ${result.message || result.error || "Test completed."}${classification ? ` (${classification})` : ""}`, status === "fail");
  await refreshSources();
}

async function populateSource(sourceKey) {
  const result = await request("/api/data_sources/populate_now", {
    method: "POST",
    body: JSON.stringify({ source_key: sourceKey, actor: state.session.actor.trim() || "operator" }),
    allowApplicationError: true,
  });
  const evidence = result.populate_evidence || {};
  const status = String(evidence.contract_status || (result.ok ? "pass" : "fail"));
  flash(`populate ${status}: ${evidence.storage_table || result.error || "completed"}`, status === "fail");
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
  el("modalTestSaveBtn").addEventListener("click", testAndSaveSource);
  el("accountModalCloseBtn").addEventListener("click", closeAccountModal);
  el("accountModalCancelBtn").addEventListener("click", closeAccountModal);
  el("accountModalForm").addEventListener("submit", saveProviderAccount);
  el("modalShell").addEventListener("click", (event) => {
    if (event.target === el("modalShell")) closeModal();
  });
  el("accountModalShell").addEventListener("click", (event) => {
    if (event.target === el("accountModalShell")) closeAccountModal();
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
