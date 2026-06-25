// CREATE NEW FILE: public/ui/weather_widgets.js
// Weather widgets (dashboard-ready)
// Expects existing dashboard to call these and provide container elements.

let _weatherReqSeq = 0;
const WEATHER_FETCH_TIMEOUT_MS = 15000;

function _esc(value) {
  return String(value == null ? "" : value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function _formatValue(value) {
  if (value === undefined || value === null || value === "") return "—";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (Array.isArray(value)) return value.length ? value.map((item) => _formatValue(item)).join(", ") : "—";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function _pickTs(payload) {
  const candidates = [
    payload && payload.ts_ms,
    payload && payload.updated_ts_ms,
    payload && payload.snapshot_ts_ms,
    payload && payload.generated_ts_ms,
    payload && payload.event_ts_ms,
    payload && payload.asof_ts_ms,
  ];
  for (const candidate of candidates) {
    const n = Number(candidate);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
}

function _summaryRows(payload, preferredKeys = []) {
  const safe = payload && typeof payload === "object" ? payload : {};
  const preferred = preferredKeys
    .map((key) => [key, safe[key]])
    .filter(([, value]) => value !== undefined && value !== null && value !== "");
  const rest = Object.entries(safe)
    .filter(([key, value]) => !preferredKeys.includes(key) && (typeof value !== "object" || value === null))
    .filter(([, value]) => value !== undefined && value !== null && value !== "");
  return [...preferred, ...rest].slice(0, 6).map(([key, value]) => ({
    label: String(key).replace(/_/g, " "),
    value: key.endsWith("ts_ms") ? new Date(Number(value)).toLocaleString() : _formatValue(value),
    meta: "",
  }));
}

function _renderSummary(el, rows, rawPayload, state, reason) {
  if (!el) return;
  const safeRows = Array.isArray(rows) ? rows.filter(Boolean) : [];
  const rawText = rawPayload ? JSON.stringify(rawPayload, null, 2) : "";
  el.innerHTML = `
    <div class="panelStateRow">
      <span class="panelStateBadge is-${_esc(state || "empty")}">${_esc(String(state || "empty").toUpperCase())}</span>
      <span class="panelStateText">${_esc(reason || "")}</span>
    </div>
    <div class="structuredSummary">
      ${safeRows.length ? safeRows.map((row) => `
        <div class="structuredSummaryRow">
          <div class="structuredSummaryLabel">${_esc(row.label || "Field")}</div>
          <div class="structuredSummaryValue">${_esc(row.value || "—")}</div>
          <div class="structuredSummaryMeta">${_esc(row.meta || "")}</div>
        </div>
      `).join("") : `<div class="structuredSummaryMeta">No structured weather data available.</div>`}
    </div>
    <details class="rawToggle" style="margin-top:8px;">
      <summary>View Raw Payload</summary>
      <pre class="mono small" style="white-space:pre-wrap; margin:8px 0 0;">${_esc(rawText)}</pre>
    </details>
  `;
}

export async function loadWeatherWidgets({ symbol = "SPY", fetchJSON = null } = {}) {
  const root = document.getElementById("weather-widgets");
  if (!root) return;
  const activeSymbol = String(symbol || "").trim().toUpperCase() || "SPY";
  const sharedFetchJSON = typeof fetchJSON === "function" ? fetchJSON : null;

  const reqId = ++_weatherReqSeq;
  const weatherState = {
    snapshot: { state: "empty", reason: "Loading weather snapshot…" },
    alerts: { state: "empty", reason: "Loading weather alerts…" },
    effect: { state: "empty", reason: "Loading weather contribution…" },
  };

  root.innerHTML = `
    <div class="card">
      <div class="card-title">Weather Snapshot</div>
      <div id="wx-snap"></div>
    </div>
    <div class="card">
      <div class="card-title">Active Weather Alerts</div>
      <div id="wx-alerts"></div>
    </div>
    <div class="card">
      <div class="card-title">Weather Contribution (Base vs WX)</div>
      <div id="wx-effect"></div>
    </div>
  `;

  function _getSlot(id) {
    if (reqId !== _weatherReqSeq) return null;
    const nextRoot = document.getElementById("weather-widgets");
    if (!nextRoot || nextRoot !== root) return null;
    return document.getElementById(id);
  }

  function _updateWeatherPanelState() {
    const values = Object.values(weatherState);
    const state = values.some((item) => item.state === "error")
      ? "error"
      : values.some((item) => item.state === "stale")
        ? "stale"
        : values.every((item) => item.state === "empty")
          ? "empty"
          : "fresh";
    const reason = values
      .map((item) => item.reason)
      .filter(Boolean)
      .slice(0, 3)
      .join(" • ");
    try {
      if (typeof window.__setDashboardPanelState__ === "function") {
        window.__setDashboardPanelState__("weatherWidgetsCard", { state, reason });
      }
    } catch {}
  }

  async function jget(url) {
    if (sharedFetchJSON) {
      return sharedFetchJSON(url);
    }
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(new Error(`fetch_timeout:${url}`)), WEATHER_FETCH_TIMEOUT_MS);
    try {
      const r = await fetch(url, { cache: "no-store", signal: controller.signal });
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      const raw = await r.text();
      let j = null;
      try {
        j = raw ? JSON.parse(raw) : null;
      } catch {}
      if (!j || typeof j !== "object") throw new Error(`invalid_json_response: ${url}`);
      if (j.ok === false) throw new Error(String(j.error || `api_error: ${url}`));
      return j;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  _renderSummary(_getSlot("wx-snap"), [], { status: "loading" }, "empty", weatherState.snapshot.reason);
  _renderSummary(_getSlot("wx-alerts"), [], { status: "loading" }, "empty", weatherState.alerts.reason);
  _renderSummary(_getSlot("wx-effect"), [], { status: "loading" }, "empty", weatherState.effect.reason);
  _updateWeatherPanelState();

  await Promise.allSettled([
    (async () => {
      try {
        const snap = await jget(`/api/weather/snapshot?symbol=${encodeURIComponent(activeSymbol)}`);
        const ts = _pickTs(snap);
        const ageMs = ts > 0 ? Math.max(0, Date.now() - ts) : 0;
        weatherState.snapshot = {
          state: ts > 0 && ageMs >= 300_000 ? "stale" : "fresh",
          reason: ts > 0 ? `Snapshot ${Math.round(ageMs / 1000)}s old.` : "Snapshot loaded.",
        };
        _renderSummary(
          _getSlot("wx-snap"),
          _summaryRows(snap, ["symbol", "region", "condition", "risk", "ts_ms"]),
          snap,
          weatherState.snapshot.state,
          weatherState.snapshot.reason
        );
      } catch (e) {
        weatherState.snapshot = {
          state: "error",
          reason: String(e && e.message ? e.message : e),
        };
        _renderSummary(_getSlot("wx-snap"), [], { error: weatherState.snapshot.reason }, "error", weatherState.snapshot.reason);
      }
      _updateWeatherPanelState();
    })(),
    (async () => {
      try {
        const alerts = await jget("/api/weather/alerts");
        const rows = Array.isArray(alerts && alerts.rows) ? alerts.rows : Array.isArray(alerts) ? alerts : [];
        const ts = _pickTs(alerts) || rows.reduce((latest, row) => Math.max(latest, _pickTs(row)), 0);
        const ageMs = ts > 0 ? Math.max(0, Date.now() - ts) : 0;
        weatherState.alerts = {
          state: rows.length ? (ts > 0 && ageMs >= 300_000 ? "stale" : "fresh") : "empty",
          reason: rows.length ? `${rows.length} active weather alerts.` : "No active weather alerts were returned.",
        };
        const preview = rows.slice(0, 3).map((row, index) => ({
          label: `Alert ${index + 1}`,
          value: _formatValue(row && (row.title || row.alert || row.event || row.headline || row.type)),
          meta: _formatValue(row && (row.region || row.scope || row.severity || row.status)),
        }));
        _renderSummary(_getSlot("wx-alerts"), preview, alerts, weatherState.alerts.state, weatherState.alerts.reason);
      } catch (e) {
        weatherState.alerts = {
          state: "error",
          reason: String(e && e.message ? e.message : e),
        };
        _renderSummary(_getSlot("wx-alerts"), [], { error: weatherState.alerts.reason }, "error", weatherState.alerts.reason);
      }
      _updateWeatherPanelState();
    })(),
    (async () => {
      try {
        const eff = await jget("/api/weather/effect");
        const ts = _pickTs(eff);
        const ageMs = ts > 0 ? Math.max(0, Date.now() - ts) : 0;
        weatherState.effect = {
          state: Object.keys(eff || {}).length ? (ts > 0 && ageMs >= 300_000 ? "stale" : "fresh") : "empty",
          reason: Object.keys(eff || {}).length
            ? (ts > 0 ? `Weather effect ${Math.round(ageMs / 1000)}s old.` : "Weather effect loaded.")
            : "No weather contribution payload was returned.",
        };
        _renderSummary(
          _getSlot("wx-effect"),
          _summaryRows(eff, ["symbol", "base_score", "weather_score", "delta", "impact", "ts_ms"]),
          eff,
          weatherState.effect.state,
          weatherState.effect.reason
        );
      } catch (e) {
        weatherState.effect = {
          state: "error",
          reason: String(e && e.message ? e.message : e),
        };
        _renderSummary(_getSlot("wx-effect"), [], { error: weatherState.effect.reason }, "error", weatherState.effect.reason);
      }
      _updateWeatherPanelState();
    })(),
  ]);
}
