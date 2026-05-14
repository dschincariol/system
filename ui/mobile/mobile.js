import {
  KILL_SWITCH_HOLD_MS,
  canFireKillSwitch,
  canStartKillSwitchHold,
  cleanText,
  describeEmergencyConsequences,
  formatAge,
  formatMoney,
  killSwitchIsActive,
  normalizeAlertRows,
  normalizeEndpointResult,
  normalizePnl,
  normalizePositionRows,
  numberOrNull,
  summarizeEmergencyResult,
} from "./mobile_helpers.mjs";

const ENDPOINTS = Object.freeze({
  status: "/api/status",
  health: "/api/health",
  readiness: "/api/readiness",
  watchdogs: "/api/operator/runtime_watchdogs",
  pnl: "/api/pnl",
  positions: "/api/terminal/positions",
  alerts: "/api/alerts/timeline?limit=20",
  notifications: "/api/notifications/status",
  feeds: "/api/feeds",
  providerTelemetry: "/api/operator/provider_telemetry",
  broker: "/api/broker",
  killSwitches: "/api/system/kill_switches",
  emergencyStop: "/api/operator/emergency_stop",
});

const POLL_MS = 5000;
const FETCH_TIMEOUT_MS = 10000;

const state = {
  endpoints: {},
  snapshot: {},
  pendingEmergency: false,
  holdStartMs: 0,
  holdFrame: 0,
  pollTimer: 0,
};

function $(id) {
  return document.getElementById(id);
}

function setText(id, value) {
  const el = $(id);
  if (el) el.textContent = cleanText(value);
}

function setClass(el, base, tone) {
  if (!el) return;
  const suffix = tone === "ok"
    ? "badge-ok"
    : tone === "warn"
      ? "badge-warn"
      : tone === "danger"
        ? "badge-danger"
        : "badge-muted";
  el.className = `${base} ${suffix}`;
}

function setBadge(id, text, tone = "muted") {
  const el = $(id);
  if (!el) return;
  el.textContent = cleanText(text);
  setClass(el, "badge", tone);
}

function endpointData(name) {
  const row = normalizeEndpointResult(state.endpoints[name] || {});
  return row.ok && row.data ? row.data : {};
}

async function fetchJson(path, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`fetch_timeout:${path}`)), FETCH_TIMEOUT_MS);
  try {
    const headers = new Headers(options.headers || {});
    if (options.body !== undefined && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
    const res = await fetch(path, {
      ...options,
      headers,
      cache: "no-store",
      signal: controller.signal,
    });
    const text = await res.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { ok: false, error: "invalid_json_response" };
    }
    if (!res.ok) {
      return {
        ok: false,
        data,
        error: cleanText(data && data.error, `${res.status} ${res.statusText}`),
        httpStatus: res.status,
      };
    }
    return { ok: data && data.ok !== false, data, error: data && data.ok === false ? data.error : null };
  } catch (error) {
    return { ok: false, data: null, error: cleanText(error && error.message, "request_failed") };
  } finally {
    clearTimeout(timer);
  }
}

async function readEndpoint(name, path) {
  const result = await fetchJson(path);
  return [name, result];
}

function renderEndpointBanner() {
  const entries = Object.entries(state.endpoints);
  const failed = entries.filter(([, result]) => !result.ok);
  const banner = $("endpointBanner");
  if (!banner) return;
  if (!entries.length) {
    banner.textContent = "Connecting to control-plane endpoints.";
    banner.className = "surfaceBanner";
    return;
  }
  if (!failed.length) {
    banner.textContent = "All critical mobile endpoints responded.";
    banner.className = "surfaceBanner ok";
    return;
  }
  const names = failed.map(([name]) => name).slice(0, 4).join(", ");
  banner.textContent = `${failed.length} endpoint failure${failed.length === 1 ? "" : "s"}: ${names}`;
  banner.className = failed.length >= 3 ? "surfaceBanner danger" : "surfaceBanner warn";
}

function renderSystem() {
  const status = endpointData("status");
  const health = endpointData("health");
  const readiness = endpointData("readiness");
  const watchdogs = endpointData("watchdogs");
  const killSwitches = endpointData("killSwitches");
  const executionAllowed = Boolean(status.execution_allowed ?? readiness.execution_allowed);
  const killActive = killSwitchIsActive(killSwitches);
  const systemStatus = cleanText(status.status || readiness.status || health.status, "unknown");
  const healthOk = health.ok === true;
  const ready = readiness.ready === true || readiness.ok === true;

  setText("systemState", systemStatus);
  setText("systemMeta", ready ? "ready" : "not ready");
  setText("tradingState", killActive ? "KILL" : executionAllowed ? "Allowed" : "Blocked");
  setText("tradingMeta", cleanText(status.mode || status.execution_mode || readiness.execution_mode, "mode unknown"));
  setText("readinessValue", ready ? "ready" : cleanText(readiness.status, "not ready"));
  setText("healthValue", healthOk ? "healthy" : cleanText(health.status || health.error, "degraded"));

  const provider = watchdogs.provider_monitor || {};
  const providerAge = provider.heartbeat_age_s !== undefined
    ? `${Number(provider.heartbeat_age_s).toFixed(0)}s heartbeat age`
    : formatAge(provider.heartbeat_ts_ms);
  setText("providerHeartbeatValue", provider.running ? providerAge : cleanText(provider.reason, "not running"));

  const ingestion = watchdogs.pipeline_watchdog_state?.ingestion_runtime || {};
  setText("ingestionValue", ingestion.running ? formatAge(ingestion.heartbeat_ts_ms) : cleanText(ingestion.reason, "not running"));

  const reasons = []
    .concat(Array.isArray(status.reasons) ? status.reasons : [])
    .concat(Array.isArray(readiness.reasons) ? readiness.reasons : [])
    .filter(Boolean)
    .slice(0, 6);
  setText("systemReasons", reasons.length ? reasons.join("\n") : "No active status reasons reported.");

  setBadge("heartbeatBadge", ready && healthOk ? "ok" : "degraded", ready && healthOk ? "ok" : "warn");
  setBadge("overallBadge", killActive ? "kill" : ready && healthOk ? "ok" : "degraded", killActive ? "danger" : ready && healthOk ? "ok" : "warn");
}

function renderPnl() {
  const result = normalizeEndpointResult(state.endpoints.pnl || {});
  const pnl = normalizePnl(result.data || {});
  const tone = pnl.ok && pnl.ready ? "ok" : "warn";
  setText("pnlTotal", formatMoney(pnl.total));
  setText("pnlMeta", pnl.ready ? "live snapshot" : "not ready");
  setText("pnlTotalDetail", formatMoney(pnl.total));
  setText("pnlUnrealized", formatMoney(pnl.unrealized));
  setText("pnlRealized", formatMoney(pnl.realized));
  setBadge("pnlBadge", pnl.ready ? "live" : "unavailable", tone);
}

function renderPositions() {
  const positions = normalizePositionRows(endpointData("positions"), endpointData("broker"));
  const host = $("positionsList");
  if (!host) return;
  setBadge("positionsBadge", `${positions.length} open`, positions.length ? "warn" : "ok");
  setText("brokerPositions", `${positions.length} open`);
  if (!positions.length) {
    host.textContent = "No open broker positions were returned.";
    return;
  }
  host.innerHTML = positions.slice(0, 12).map((row) => {
    const qty = Number(row.qty || 0).toLocaleString(undefined, { maximumFractionDigits: 4 });
    const avg = row.avgPx !== null && row.avgPx !== undefined
      ? Number(row.avgPx).toLocaleString(undefined, { maximumFractionDigits: 4 })
      : "-";
    return `
      <div class="dataRow">
        <strong>${escapeHtml(row.symbol)}</strong>
        <span>qty ${escapeHtml(qty)} at ${escapeHtml(avg)} | ${escapeHtml(formatAge(row.updatedTsMs))}</span>
      </div>
    `;
  }).join("");
}

function renderAlerts() {
  const alerts = normalizeAlertRows(endpointData("alerts"));
  const host = $("alertsList");
  if (!host) return;
  const critical = alerts.filter((row) => row.score >= 4).length;
  setText("alertCount", String(alerts.length));
  setText("alertMeta", critical ? `${critical} critical` : "active");
  setBadge("alertsBadge", `${alerts.length} active`, critical ? "danger" : alerts.length ? "warn" : "ok");
  if (!alerts.length) {
    host.textContent = "No active alerts were returned.";
    return;
  }
  host.innerHTML = alerts.slice(0, 10).map((row) => {
    const tone = row.score >= 4 ? "danger" : row.score >= 3 ? "warn" : "";
    const symbol = row.symbol ? `${row.symbol} | ` : "";
    return `
      <div class="dataRow ${tone}">
        <strong>${escapeHtml(row.severity)}</strong>
        <span>${escapeHtml(symbol + row.title)} | ${escapeHtml(formatAge(row.tsMs))}</span>
      </div>
    `;
  }).join("");
}

function renderBrokerFeeds() {
  const broker = endpointData("broker");
  const feeds = endpointData("feeds");
  const telemetry = endpointData("providerTelemetry");
  const notifications = endpointData("notifications");
  const account = broker.account || {};
  const equity = numberOrNull(account.equity);
  const feedRows = Array.isArray(feeds.rows)
    ? feeds.rows
    : Array.isArray(feeds.data)
      ? feeds.data
      : Array.isArray(feeds.feeds)
        ? feeds.feeds
        : [];
  const feedOk = feeds.ok !== false && (feeds.status ? String(feeds.status).toUpperCase() !== "DEGRADED" : true);
  const channels = notifications.channels && typeof notifications.channels === "object"
    ? Object.entries(notifications.channels)
    : [];
  const enabledChannels = channels.filter(([, value]) => value && value.enabled !== false);

  setText("brokerEquity", equity === null ? "-" : formatMoney(equity));
  setText("feedStatus", feedOk ? "available" : cleanText(feeds.status || feeds.error, "degraded"));
  setText("notificationStatus", channels.length ? `${enabledChannels.length}/${channels.length} channels` : "no channel data");
  setBadge("feedsBadge", feedOk ? "online" : "degraded", feedOk ? "ok" : "warn");

  const detailLines = [];
  if (feedRows.length) {
    detailLines.push(...feedRows.slice(0, 4).map((row) => {
      const name = cleanText(row.name || row.provider || row.source || row.key, "feed");
      const status = cleanText(row.status || row.state || row.health, "unknown");
      return `${name}: ${status}`;
    }));
  }
  if (telemetry.providers && typeof telemetry.providers === "object") {
    detailLines.push(`providers reported: ${Object.keys(telemetry.providers).length}`);
  }
  setText("feedDetails", detailLines.length ? detailLines.join("\n") : "No feed details returned.");
}

function renderEmergencyPreview() {
  state.snapshot = {
    status: endpointData("status"),
    pnl: endpointData("pnl"),
    positions: endpointData("positions"),
    broker: endpointData("broker"),
    killSwitches: endpointData("killSwitches"),
  };
  setText("consequencePreview", describeEmergencyConsequences(state.snapshot));
  const active = killSwitchIsActive(state.snapshot.killSwitches);
  setBadge("killSwitchBadge", active ? "active" : "confirmation required", active ? "danger" : "warn");
}

function renderAll() {
  renderEndpointBanner();
  renderSystem();
  renderPnl();
  renderPositions();
  renderAlerts();
  renderBrokerFeeds();
  renderEmergencyPreview();
  setText("lastUpdated", `Updated ${new Date().toLocaleTimeString()}`);
  updateHoldButton();
}

async function refreshAll() {
  const pairs = await Promise.all(Object.entries(ENDPOINTS)
    .filter(([name]) => name !== "emergencyStop")
    .map(([name, path]) => readEndpoint(name, path)));
  state.endpoints = Object.fromEntries(pairs);
  renderAll();
}

function updateHoldButton() {
  const input = $("killPhraseInput");
  const button = $("holdKillButton");
  const label = $("holdButtonLabel");
  if (!button || !label) return;
  const typedPhrase = input ? input.value : "";
  const canStart = canStartKillSwitchHold({ typedPhrase, pending: state.pendingEmergency });
  button.disabled = !canStart;
  if (state.pendingEmergency) {
    label.textContent = "Sending emergency stop";
  } else if (canStart) {
    label.textContent = `Hold for ${(KILL_SWITCH_HOLD_MS / 1000).toFixed(0)} seconds`;
  } else {
    label.textContent = "Type KILL to enable hold";
  }
}

function cancelHold() {
  if (state.holdFrame) cancelAnimationFrame(state.holdFrame);
  state.holdFrame = 0;
  state.holdStartMs = 0;
  const button = $("holdKillButton");
  if (button) button.style.setProperty("--hold-progress", "0%");
  updateHoldButton();
}

function tickHold() {
  const button = $("holdKillButton");
  const input = $("killPhraseInput");
  if (!button || !input || !state.holdStartMs) return;
  const elapsed = Date.now() - state.holdStartMs;
  const pct = Math.min(100, Math.max(0, (elapsed / KILL_SWITCH_HOLD_MS) * 100));
  button.style.setProperty("--hold-progress", `${pct}%`);
  if (elapsed >= KILL_SWITCH_HOLD_MS) {
    const typedPhrase = input.value;
    cancelHold();
    if (canFireKillSwitch({ typedPhrase, holdComplete: true, pending: state.pendingEmergency })) {
      void fireEmergencyStop();
    }
    return;
  }
  state.holdFrame = requestAnimationFrame(tickHold);
}

function startHold(event) {
  if (event) event.preventDefault();
  if (state.holdStartMs) return;
  const input = $("killPhraseInput");
  const typedPhrase = input ? input.value : "";
  if (!canStartKillSwitchHold({ typedPhrase, pending: state.pendingEmergency })) return;
  state.holdStartMs = Date.now();
  tickHold();
}

async function fireEmergencyStop() {
  if (state.pendingEmergency) return;
  state.pendingEmergency = true;
  updateHoldButton();
  const resultEl = $("emergencyResult");
  if (resultEl) {
    resultEl.textContent = "Sending emergency stop to backend.";
    resultEl.className = "noteBlock resultBlock";
  }
  const token = $("apiTokenInput") ? $("apiTokenInput").value.trim() : "";
  const headers = token ? { "X-API-Token": token } : {};
  const result = await fetchJson(ENDPOINTS.emergencyStop, {
    method: "POST",
    headers,
    body: JSON.stringify({
      actor: "mobile_operator",
      source: "mobile_pwa",
      confirmation: "KILL",
      confirmation_hold_ms: KILL_SWITCH_HOLD_MS,
      consequence_ack: true,
      requested_at_ms: Date.now(),
    }),
  });
  state.pendingEmergency = false;
  const payload = result.data && typeof result.data === "object"
    ? result.data
    : { ok: false, error: result.error };
  if (resultEl) {
    resultEl.textContent = summarizeEmergencyResult(payload);
    resultEl.className = `noteBlock resultBlock ${payload.ok ? "ok" : "danger"}`;
  }
  await refreshAll();
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[ch]);
}

function wireControls() {
  const refreshButton = $("refreshButton");
  if (refreshButton) refreshButton.addEventListener("click", () => void refreshAll());

  const phrase = $("killPhraseInput");
  if (phrase) phrase.addEventListener("input", updateHoldButton);

  const button = $("holdKillButton");
  if (button) {
    button.addEventListener("pointerdown", startHold);
    button.addEventListener("pointerup", cancelHold);
    button.addEventListener("pointerleave", cancelHold);
    button.addEventListener("pointercancel", cancelHold);
    button.addEventListener("keydown", (event) => {
      if (event.key === " " || event.key === "Enter") startHold(event);
    });
    button.addEventListener("keyup", cancelHold);
  }
}

function startPolling() {
  if (state.pollTimer) clearInterval(state.pollTimer);
  void refreshAll();
  state.pollTimer = setInterval(() => {
    if (!document.hidden) void refreshAll();
  }, POLL_MS);
}

async function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  try {
    const registration = await navigator.serviceWorker.register("./sw.js", { scope: "./" });
    await registration.update();
  } catch (error) {
    const banner = $("endpointBanner");
    if (banner && !banner.className.includes("danger")) {
      banner.textContent = `PWA service worker unavailable: ${cleanText(error && error.message, "registration_failed")}`;
      banner.className = "surfaceBanner warn";
    }
  }
}

function init() {
  wireControls();
  startPolling();
  void registerServiceWorker();
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) void refreshAll();
  });
}

if (typeof document !== "undefined") {
  init();
}
