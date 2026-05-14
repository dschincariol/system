/*
  FILE: ui/why_modal.js

  Alert snapshot and diff helpers for the dashboard "why" modal. This module
  stores prior alert payloads locally and computes human-readable field diffs so
  operators can inspect what changed between alert updates.
*/

import { esc, escapeHTML, fmtTime } from "./utils.js";

function _flattenScalarEntries(obj, preferredKeys = []) {
  const safe = obj && typeof obj === "object" ? obj : {};
  const preferred = preferredKeys
    .map((key) => [key, safe[key]])
    .filter(([, value]) => value !== undefined && value !== null && value !== "");
  const rest = Object.entries(safe)
    .filter(([key, value]) => !preferredKeys.includes(key) && (typeof value !== "object" || value === null))
    .filter(([, value]) => value !== undefined && value !== null && value !== "");
  return [...preferred, ...rest].slice(0, 8);
}

function _formatValue(value) {
  if (value === undefined || value === null || value === "") return "—";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (Array.isArray(value)) return value.length ? value.map((item) => _formatValue(item)).join(", ") : "—";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function _renderSummary(el, rows, emptyText) {
  if (!el) return;
  const safeRows = Array.isArray(rows) ? rows.filter(Boolean) : [];
  if (!safeRows.length) {
    el.innerHTML = `<div class="structuredSummaryMeta">${escapeHTML(String(emptyText || "No structured details available."))}</div>`;
    return;
  }
  el.innerHTML = safeRows.map((row) => `
    <div class="structuredSummaryRow">
      <div class="structuredSummaryLabel">${escapeHTML(String(row.label || "Field"))}</div>
      <div class="structuredSummaryValue">${escapeHTML(String(row.value || "—"))}</div>
      <div class="structuredSummaryMeta">${escapeHTML(String(row.meta || ""))}</div>
    </div>
  `).join("");
}

function _explainSummaryRows(explain, row) {
  const safe = explain && typeof explain === "object" ? explain : {};
  const blockers = [
    ...(Array.isArray(safe.blockers) ? safe.blockers : []),
    ...(Array.isArray(safe.issues) ? safe.issues : []),
  ].filter(Boolean);
  const rows = [
    {
      label: "Alert",
      value: row && row.event_title ? String(row.event_title) : "Alert explain",
      meta: row && row.ts_ms ? `Raised ${fmtTime(row.ts_ms)}` : "Timestamp unavailable",
    },
  ];
  _flattenScalarEntries(safe, ["action", "recommendation", "confidence", "severity", "summary", "reason"]).forEach(([key, value]) => {
    rows.push({
      label: String(key).replace(/_/g, " "),
      value: _formatValue(value),
      meta: "",
    });
  });
  if (blockers.length) {
    rows.push({
      label: "Blockers",
      value: `${blockers.length} active`,
      meta: blockers.slice(0, 3).map((item) => _formatValue(item)).join(" • "),
    });
  }
  return rows.slice(0, 8);
}

function _promoSummaryRows(payload) {
  const safe = payload && typeof payload === "object" ? payload : {};
  const blockers = [
    ...(Array.isArray(safe.blockers) ? safe.blockers : []),
    ...(Array.isArray(safe.reasons) ? safe.reasons : []),
    ...(Array.isArray(safe.issues) ? safe.issues : []),
  ].filter(Boolean);
  const rows = _flattenScalarEntries(safe, ["status", "mode", "reason", "model", "symbol", "ts_ms"]).map(([key, value]) => ({
    label: String(key).replace(/_/g, " "),
    value: key === "ts_ms" ? fmtTime(value) : _formatValue(value),
    meta: "",
  }));
  if (blockers.length) {
    rows.push({
      label: "Blocking reasons",
      value: `${blockers.length} reported`,
      meta: blockers.slice(0, 4).map((item) => _formatValue(item)).join(" • "),
    });
  }
  return rows.slice(0, 8);
}

export function _safeParseJSON(s) {
  try {
    return JSON.parse(String(s));
  } catch {
    return null;
  }
}

export function _ALERT_SNAPSHOT_KEY(id) {
  return `alert_snapshot_${String(id)}`;
}

export function _diffObjects(prev, cur) {
  const out = [];
  const keys = new Set([
    ...Object.keys(prev || {}),
    ...Object.keys(cur || {})
  ]);

  for (const k of keys) {
    const a = prev ? prev[k] : undefined;
    const b = cur ? cur[k] : undefined;
    if (JSON.stringify(a) !== JSON.stringify(b)) {
      out.push({ key: k, before: a, after: b });
    }
  }

  return out;
}

export function _diffRelevance(prevStats, curStats) {
  const rows = [];
  const keys = new Set([
    ...Object.keys(prevStats || {}),
    ...Object.keys(curStats || {})
  ]);

  for (const k of keys) {
    const a = Number(prevStats?.[k]);
    const b = Number(curStats?.[k]);

    if (!Number.isFinite(a) && !Number.isFinite(b)) continue;

    const delta = (Number.isFinite(b) ? b : 0) - (Number.isFinite(a) ? a : 0);

    rows.push({
      key: k,
      before: a,
      after: b,
      delta
    });
  }

  rows.sort((x, y) => Math.abs(y.delta) - Math.abs(x.delta));
  return rows.slice(0, 20);
}

export function _parseRelevanceStats(obj) {
  if (!obj) return {};

  if (obj.relevance_stats && typeof obj.relevance_stats === "object") {
    return obj.relevance_stats;
  }

  if (obj.stats && typeof obj.stats === "object") {
    return obj.stats;
  }

  return {};
}

export async function openWhyModal(row, deps) {

  const {
    fetchJSON
  } = deps;

  const modal = document.getElementById("whyModal");
  if (!modal) return;

  const title = document.getElementById("whyTitle");
  const body = document.getElementById("whyBody");
  const raw = document.getElementById("whyRaw");

  if (title) {
    title.textContent =
      `${row.symbol} • ${row.event_title} • ${fmtTime(row.ts_ms)}`;
  }

  let ex = null;

  try {
    const res = await fetchJSON(
      `/api/alerts/by_id?id=${encodeURIComponent(row.id)}`
    );

    if (res && res.ok && res.alert) {
      ex = row.explain_json
        ? _safeParseJSON(row.explain_json)
        : null;
    }

  } catch {}

  if (!ex) {
    _renderSummary(body, [], "No explain data available.");
    if (raw) raw.textContent = "";
    modal.style.display = "block";
    return;
  }

  _renderSummary(body, _explainSummaryRows(ex, row), "No structured explain data available.");
  if (raw) raw.textContent = JSON.stringify(ex, null, 2);

  modal.style.display = "block";
}

export function closeWhyModal() {
  const modal = document.getElementById("whyModal");
  if (modal) modal.style.display = "none";
}

export function openPromoWhyModal(payload) {

  const modal = document.getElementById("promoWhyModal");
  const body = document.getElementById("promoWhyBody");
  const raw = document.getElementById("promoWhyRaw");

  if (!modal || !body) return;

  _renderSummary(body, _promoSummaryRows(payload), "No promotion detail payload available.");
  if (raw) raw.textContent = JSON.stringify(payload, null, 2);

  modal.style.display = "block";
}

export function closePromoWhyModal() {

  const modal = document.getElementById("promoWhyModal");
  if (modal) modal.style.display = "none";
}
