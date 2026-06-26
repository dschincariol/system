/*
  FILE: ui/social_panels.js

  Social-context panel helpers for the dashboard. This module fetches
  social/risk payloads and renders operator-readable summaries without coupling
  the rest of the dashboard to endpoint-specific parsing.
*/

import { apiFetch } from "./api_client.js";
import { esc, escapeHTML, fmtTime } from "./utils.js";

const FETCH_TIMEOUT_MS = 15000;

function _normalizeSymbol(value) {
  return String(value || "").trim().toUpperCase();
}

function _symbolFromOptions(options, fallback = "SPY") {
  const selected = options && typeof options === "object" && !Array.isArray(options)
    ? _normalizeSymbol(options.symbol)
    : "";
  if (selected) return selected;
  return _normalizeSymbol(document.getElementById("globalSymbol")?.value) || fallback;
}

async function _fetchWithTimeout(path, options = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(new Error(`fetch_timeout:${path}`)), FETCH_TIMEOUT_MS);
  try {
    return await apiFetch(path, {
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeoutId);
  }
}

async function fetchJSON(path) {
  const res = await _fetchWithTimeout(path, { cache: "no-store" });
  const txt = await res.text();
  let data = null;
  try {
    data = txt ? JSON.parse(txt) : null;
  } catch (e) {
    console.warn("JSON parse error", path, typeof txt === "string" ? txt.slice(0, 200) : "");
  }
  if (!res.ok) throw new Error(txt || res.statusText);
  if (!data || typeof data !== "object") throw new Error(`invalid_json_response: ${path}`);
  if (data.ok === false) throw new Error(String(data.error || `api_error: ${path}`));
  return data;
}

// --------------------------------------------------
// Social pressure
// --------------------------------------------------

export async function loadSocialPressure(options = {}) {
  const body = document.getElementById("socialPressureBody");
  if (!body) return;

  const sym = _symbolFromOptions(options, "SPY");

  try {
    const d = await fetchJSON(
      `/api/social/features?symbol=${encodeURIComponent(sym)}&limit=50`
    );
    const rows = d?.rows || [];

    body.innerHTML = "";

    for (const r of rows.slice(0, 20)) {
      body.insertAdjacentHTML(
        "beforeend",
        `
        <tr>
          <td class="mono">${fmtTime(r.bucket_ts_ms)}</td>
          <td class="mono">${Number(r.mention_rate_z).toFixed(2)}</td>
          <td class="mono">${Number(r.attention_shock).toFixed(2)}</td>
          <td class="mono">${Number(r.manip_risk).toFixed(2)}</td>
          <td class="mono">${Number(r.cross_platform_confirm).toFixed(2)}</td>
        </tr>
      `
      );
    }

    if (!rows.length) {
      body.innerHTML = `<tr class="table-row"><td colspan="5" class="small">(no social data for ${esc(sym)})</td></tr>`;
    }
  } catch {
    body.innerHTML = `<tr><td colspan="5" class="small">error loading social features</td></tr>`;
  }
}

// --------------------------------------------------
// Social regimes
// --------------------------------------------------

export async function loadSocialRegimes(options = {}) {
  const body = document.getElementById("socialRegimeBody");
  if (!body) return;

  const sym = _symbolFromOptions(options, "SPY");

  try {
    const d = await fetchJSON(
      `/api/social/regimes?symbol=${encodeURIComponent(sym)}&limit=50`
    );
    const rows = d?.rows || [];

    body.innerHTML = "";

    for (const r of rows.slice(0, 20)) {
      body.insertAdjacentHTML(
        "beforeend",
        `
        <tr>
          <td class="mono">${fmtTime(r.bucket_ts_ms)}</td>
          <td>${esc(r.regime)}</td>
          <td class="mono">${Number(r.regime_conf).toFixed(2)}</td>
        </tr>
      `
      );
    }

    if (!rows.length) {
      body.innerHTML = `<tr class="table-row"><td colspan="3" class="small">(no regimes for ${esc(sym)})</td></tr>`;
    }
  } catch {
    body.innerHTML = `<tr><td colspan="3" class="small">error loading regimes</td></tr>`;
  }
}

// --------------------------------------------------
// Social manipulation blocks
// --------------------------------------------------

export async function loadSocialBlocks(options = {}) {
  const body = document.getElementById("socialBlocksBody");
  if (!body) return;
  const selectedSymbol = _symbolFromOptions(options, "");

  try {
    const d = await fetchJSON(`/api/social/blocks?limit=20`);
    const rows = d?.rows || [];

    body.innerHTML = "";

    const visibleRows = rows.slice(0, 10);
    const hasSelectedRow = !!selectedSymbol && visibleRows.some((r) => _normalizeSymbol(r && r.symbol) === selectedSymbol);
    if (selectedSymbol && !hasSelectedRow) {
      body.insertAdjacentHTML(
        "beforeend",
        `<tr class="table-row"><td colspan="3" class="small">No recent social blocks for ${esc(selectedSymbol)}; showing global blocks.</td></tr>`
      );
    }

    for (const r of visibleRows) {
      const isSelected = selectedSymbol && _normalizeSymbol(r.symbol) === selectedSymbol;
      body.insertAdjacentHTML(
        "beforeend",
        `
        <tr class="table-row${isSelected ? " symbolContextMatch" : ""}">
          <td class="mono">${fmtTime(r.ts_ms)}</td>
          <td class="mono">${esc(r.symbol)}</td>
          <td class="small"><code>${escapeHTML(
            JSON.stringify(r.reason || {})
          )}</code></td>
        </tr>
      `
      );
    }

    if (!rows.length) {
      body.innerHTML = `<tr><td colspan="3" class="small">(no social blocks)</td></tr>`;
    }
  } catch {
    body.innerHTML = `<tr><td colspan="3" class="small">error loading blocks</td></tr>`;
  }
}
