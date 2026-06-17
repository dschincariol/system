"use strict";

/*
  ui/alerts.js — Alert rendering + filtering engine
  Extracted from ui/dashboard.js (Phase 4)
*/

import {
  esc,
  escapeHTML,
  fmtTime,
  statusAriaLabel,
  statusGlyph,
  statusLabel,
  statusPillClasses,
  statusToken,
} from "./utils.js";

function _pickAlertValue(row, keys) {
  const source = row && typeof row === "object" ? row : {};
  for (const key of keys || []) {
    const value = source[key];
    if (value != null && String(value).trim() !== "") return value;
  }
  return null;
}

function _pickAlertTs(row) {
  const candidates = [
    _pickAlertValue(row, ["ts", "ts_ms", "created_ts_ms", "updated_ts_ms", "time"]),
  ];
  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function _normalizeSeverity(value) {
  const raw = String(value || "").trim().toUpperCase();
  if (raw === "CRIT") return "CRIT";
  if (raw === "HIGH") return "HIGH";
  if (raw === "WARN") return "WARN";
  if (raw === "INFO") return "INFO";
  return "INFO";
}

function _normalizeStatus(row) {
  const raw = String(_pickAlertValue(row, ["status"]) || "").trim().toLowerCase();
  if (raw === "resolved" || raw === "closed") return "resolved";
  if (row && (row.resolved === true || row.resolved_ts_ms != null || row.resolved_at != null)) {
    return "resolved";
  }
  return "active";
}

function _normalizeId(row, severity, symbol, ts, message) {
  const explicit = _pickAlertValue(row, ["id", "alert_id"]);
  const explicitText = String(explicit || "").trim();
  if (explicitText) return explicitText;
  return `ui:${severity}:${symbol || "SYSTEM"}:${ts || 0}:${message || "Alert"}`;
}

export function normalizeAlert(row) {
  if (!row || typeof row !== "object" || Array.isArray(row)) return null;

  const symbol = String(_pickAlertValue(row, ["symbol", "ticker"]) || "").trim().toUpperCase();
  const severity = _normalizeSeverity(_pickAlertValue(row, ["severity", "level"]));
  const ts = _pickAlertTs(row);
  const message = String(
    _pickAlertValue(row, ["message", "event_title", "title", "description", "reason"]) || "Alert"
  ).trim() || "Alert";
  const status = _normalizeStatus(row);
  const id = _normalizeId(row, severity, symbol, ts, message);
  const resolved = status === "resolved";

  return {
    ...row,
    id,
    symbol,
    severity,
    message,
    ts,
    ts_ms: ts,
    status,
    resolved,
    event_title: String(_pickAlertValue(row, ["event_title", "message", "title"]) || message),
    horizon_s: _pickAlertValue(row, ["horizon_s", "horizon", "window_s"]),
    reason: _pickAlertValue(row, ["reason", "message", "detail"]) || "",
    confidence: _pickAlertValue(row, ["confidence", "confidence_score"]),
    confidence_raw: _pickAlertValue(row, ["confidence_raw"]),
    prediction_strength: _pickAlertValue(row, ["prediction_strength"]),
    expected_z: _pickAlertValue(row, ["expected_z", "z_score", "z"]),
    acked: !!(row && row.acked),
    acked_by: _pickAlertValue(row, ["acked_by"]),
    resolved_reason: _pickAlertValue(row, ["resolved_reason"]),
  };
}

export function normalizeAlertsPayload(payload) {
  const list = Array.isArray(payload)
    ? payload
    : (payload && typeof payload === "object"
      ? (Array.isArray(payload.items)
        ? payload.items
        : (Array.isArray(payload.rows)
          ? payload.rows
          : []))
      : []);
  return list
    .map((row) => normalizeAlert(row))
    .filter(Boolean);
}

export function normalizeAlertDetailPayload(payload, fallback = null) {
  const detail = payload && typeof payload === "object"
    ? (payload.alert || payload.item || payload.row || null)
    : null;
  return normalizeAlert(detail || fallback);
}

export function applyAlertLocalState(rows, localState = {}) {
  const isAcked = typeof localState.isAcked === "function" ? localState.isAcked : (() => false);
  const isResolved = typeof localState.isResolved === "function" ? localState.isResolved : (() => false);
  return (rows || [])
    .map((raw) => {
      const alert = normalizeAlert(raw);
      if (!alert) return null;
      if (!alert.acked && isAcked(alert.id)) {
        alert.acked = true;
        if (!alert.acked_by) alert.acked_by = "local";
      }
      if (alert.status !== "resolved" && isResolved(alert.id)) {
        alert.status = "resolved";
        alert.resolved = true;
        if (!alert.resolved_reason) alert.resolved_reason = "local fallback";
      }
      return alert;
    })
    .filter(Boolean);
}

// -----------------------------
// Severity helpers
// -----------------------------
export function severityRank(s) {
  const sev = _normalizeSeverity(s);
  if (sev === "CRIT") return 4;
  if (sev === "HIGH") return 3;
  if (sev === "WARN") return 2;
  if (sev === "INFO") return 1;
  return 0;
}

export function cellColor(r) {
  const alert = normalizeAlert(r);
  const tokenKey =
    !alert ? "neutral"
      : alert.status === "resolved" ? "ok"
        : alert.severity === "CRIT" ? "crit"
          : alert.severity === "HIGH" ? "high"
            : alert.severity === "WARN" ? "warn"
              : alert.severity === "INFO" ? "info"
                : "neutral";
  const token = statusToken(tokenKey);
  return {
    cls: token.className,
    sw: token.color,
    glyph: token.glyph,
    label: token.label,
    key: token.key,
  };
}

// -----------------------------
// Filtering
// -----------------------------
export function filterAlerts(rows, filters, localState) {
  const rangeMs = Number(filters && filters.rangeMs);
  const fallbackRangeMs = (() => {
    const range = String(filters && filters.range || "6h").trim();
    const map = {
      "15m": 15 * 60 * 1000,
      "1h": 60 * 60 * 1000,
      "6h": 6 * 60 * 60 * 1000,
      "24h": 24 * 60 * 60 * 1000,
      "7d": 7 * 24 * 60 * 60 * 1000,
    };
    return map[range] || map["6h"];
  })();
  const minRank =
    filters.sev === "ALL" ? 0 : severityRank(filters.sev);

  const sinceMs = Date.now() - (Number.isFinite(rangeMs) ? rangeMs : fallbackRangeMs);
  const out = [];

  for (const raw of rows || []) {
    const r = normalizeAlert(raw);
    if (!r) continue;
    const ts = Number(r.ts);
    if (Number.isFinite(ts) && ts < sinceMs) continue;

    const sym = String(r.symbol || "").toUpperCase();
    if (filters.sym && sym !== filters.sym) continue;

    if (severityRank(r.severity) < minRank) continue;

    if (filters.changedOnly) {
      if (r.status === "resolved") continue;
      if (r.acked || localState.isAcked(r.id)) continue;
      if (localState.isSnoozed(r.id)) continue;
    } else {
      if (localState.isSnoozed(r.id)) continue;
    }

    out.push(r);
  }

  return out;
}

// -----------------------------
// Heatmap
// -----------------------------
function scoreCell(rows) {
  let best = null;
  for (const raw of rows) {
    const r = normalizeAlert(raw);
    if (!r) continue;
    const sev = severityRank(r.severity);
    const conf = Number(r.confidence);
    const z = Math.abs(Number(r.expected_z));
    if (!Number.isFinite(conf) || !Number.isFinite(z)) continue;

    const score = sev * conf * (0.6 + Math.min(3.0, z));
    if (!best || score > best.score) best = { score, r };
  }
  return best ? best.r : null;
}

export function renderHeatmap(host, rows, onSelectSymbol) {
  if (!host) return;

  const horizons = ["60", "300", "3600", "14400"];
  const labels = { "60": "1m", "300": "5m", "3600": "1h", "14400": "4h" };

  const by = {};
  for (const raw of rows || []) {
    const r = normalizeAlert(raw);
    if (!r) continue;
    const sym = String(r.symbol || "").toUpperCase();
    if (!sym) continue;
    const hs = Number(r.horizon_s);
    if (!Number.isFinite(hs)) continue;

    let b = "3600";
    if (hs <= 120) b = "60";
    else if (hs <= 900) b = "300";
    else if (hs <= 7200) b = "3600";
    else b = "14400";

    (by[sym] ||= {})[b] ||= [];
    by[sym][b].push(r);
  }

  const syms = Object.keys(by).sort().slice(0, 22);

  host.innerHTML = "";
  host.setAttribute("role", "grid");
  host.setAttribute("aria-label", "Alert heatmap by symbol and horizon");

  const mk = (cls, html) => {
    const d = document.createElement("div");
    d.className = `hmCell ${cls || ""}`;
    d.innerHTML = html;
    return d;
  };

  host.appendChild(mk("hmHead", "<span class='mono'>symbol</span>"));
  horizons.forEach(h =>
    host.appendChild(
      mk("hmHead", `<span class='mono'>${labels[h]}</span>`)
    )
  );

  for (const sym of syms) {
    host.appendChild(mk("hmSym", `<span class='mono'>${sym}</span>`));

    for (const h of horizons) {
      const best = scoreCell(by[sym][h] || []);
      const c = cellColor(best);
      const severityLabel = best
        ? (best.status === "resolved" ? "RESOLVED" : best.severity)
        : "NONE";
      const aria = best
        ? `${sym}, ${labels[h]} horizon, ${statusAriaLabel(c.key, `${severityLabel} alert: ${best.message || best.event_title || "alert"}`)}`
        : `${sym}, ${labels[h]} horizon, no alert`;

      const cell = mk(
        `hmStatus-${c.key}`,
        `<span class="hmSwatch" style="--hm-color:${c.sw};" aria-hidden="true">${esc(c.glyph || statusGlyph(c.key))}</span>
         <span class="mono">${sym}</span>
         <span class="hmStatusLabel">${esc(best ? (c.label || statusLabel(c.key)) : "No alert")}</span>
         <span class="hmMeta">${esc(severityLabel)}</span>`
      );
      cell.tabIndex = 0;
      cell.setAttribute("role", "button");
      cell.setAttribute("aria-label", aria);
      cell.dataset.status = c.key;
      cell.dataset.severity = severityLabel;

      cell.addEventListener("click", () => {
        if (onSelectSymbol) onSelectSymbol(sym);
      });
      cell.addEventListener("keydown", (event) => {
        if (!event) return;
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          if (onSelectSymbol) onSelectSymbol(sym);
        }
      });

      host.appendChild(cell);
    }
  }
}

// -----------------------------
// Incident queue
// -----------------------------
export function renderIncidentQueue(host, rows, opts) {
  if (!host) return;
  host.innerHTML = "";

  const list = (Array.isArray(rows) ? rows : [])
    .map((row) => normalizeAlert(row))
    .filter(Boolean);

  if (!list.length) {
    host.innerHTML =
      `<div class="small" style="color:var(--muted);">
        No alerts in the selected window.
       </div>`;
    return;
  }

  list
    .slice()
    .sort((a, b) => {
      const sa = severityRank(a.severity);
      const sb = severityRank(b.severity);
      if (sb !== sa) return sb - sa;
      return Number(b.ts_ms) - Number(a.ts_ms);
    })
    .slice(0, 18)
    .forEach(r => {
      const ts = Number(r.ts);
      const ageMin = Number.isFinite(ts)
        ? Math.max(0, Math.floor((Date.now() - ts) / 60000))
        : null;

      const c = cellColor(r);
      const title = r.message || r.event_title || "Alert";
      const symbolMeta = r.symbol ? `<span class="pill dim mono">${esc(r.symbol)}</span>` : "";
      const timeMeta = Number.isFinite(ts) ? `<span class="pill dim mono">${esc(fmtTime(ts))}</span>` : "";
      const ageMeta = ageMin == null ? "" : `<span class="pill dim">${ageMin}m ago</span>`;
      const horizonMeta = r.horizon_s != null && String(r.horizon_s).trim() !== ""
        ? `<span class="pill dim">h=${esc(String(r.horizon_s))}s</span>`
        : "";

      const item = document.createElement("div");
      item.className = "incidentItem";
      item.dataset.sev = r.status === "resolved" ? "OK" : String(r.severity || "INFO").toUpperCase();
      item.dataset.status = c.key;
      item.tabIndex = 0;
      item.innerHTML = `
        <div class="incidentTop">
          <span class="${statusPillClasses(c.key, "incidentSeverity")}" data-status="${esc(c.key)}" aria-label="${esc(statusAriaLabel(c.key, r.status === "resolved" ? "RESOLVED" : r.severity))}">
            ${r.status === "resolved" ? "RESOLVED" : r.severity}
          </span>
          <div class="incidentTitle">
            ${escapeHTML(title)}
          </div>
        </div>
        <div class="incidentSub">
          ${symbolMeta}
          ${timeMeta}
          ${horizonMeta}
          ${ageMeta}
        </div>
      `;

      item.addEventListener("click", () => {
        if (opts?.onOpen) opts.onOpen(r);
      });
      item.addEventListener("keydown", (event) => {
        if (!event) return;
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          if (opts?.onOpen) opts.onOpen(r);
        }
      });

      host.appendChild(item);
    });
}
