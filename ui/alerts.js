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

export const ALERT_SEVERITY_ORDER = Object.freeze(["INFO", "WARN", "HIGH", "CRIT"]);

const ALERT_SEVERITY_ALIASES = Object.freeze({
  INFO: "INFO",
  WARN: "WARN",
  WARNING: "WARN",
  HIGH: "HIGH",
  CRIT: "CRIT",
  CRITICAL: "CRIT",
});

export function normalizeSeverity(value) {
  const raw = String(value || "").trim().toUpperCase();
  return ALERT_SEVERITY_ALIASES[raw] || "INFO";
}

function _normalizeStatus(row) {
  const raw = String(_pickAlertValue(row, ["status"]) || "").trim().toLowerCase();
  if (raw === "resolved" || raw === "closed") return "resolved";
  if (row && (row.resolved === true || row.resolved_ts_ms != null || row.resolved_at != null)) {
    return "resolved";
  }
  return "active";
}

function _normalizeTsValue(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function _normalizeLifecycleList(value) {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item) => item && typeof item === "object" && !Array.isArray(item))
    .map((item) => ({
      ...item,
      ts_ms: _normalizeTsValue(item.ts_ms ?? item.ts ?? item.time),
      state: String(item.state || item.lifecycle_state || "triggered").trim().toLowerCase(),
      actor: _pickAlertValue(item, ["actor", "owner", "who"]) || "",
      reason: _pickAlertValue(item, ["reason", "message", "detail"]) || "",
      source: _pickAlertValue(item, ["source", "surface"]) || "",
    }))
    .sort((a, b) => Number(a.ts_ms || 0) - Number(b.ts_ms || 0));
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
  const severity = normalizeSeverity(_pickAlertValue(row, ["severity", "level"]));
  const ts = _pickAlertTs(row);
  const message = String(
    _pickAlertValue(row, ["message", "event_title", "title", "description", "reason"]) || "Alert"
  ).trim() || "Alert";
  const status = _normalizeStatus(row);
  const id = _normalizeId(row, severity, symbol, ts, message);
  const resolved = status === "resolved";
  const ackExpiresTsMs = _normalizeTsValue(_pickAlertValue(row, ["ack_expires_ts_ms", "ack_expiry_ts_ms"]));
  const shelveExpiresTsMs = _normalizeTsValue(_pickAlertValue(row, ["shelve_expires_ts_ms", "shelf_expires_ts_ms", "snooze_until_ts_ms"]));
  const lifecycle = _normalizeLifecycleList(row.lifecycle);
  const lifecycleState = String(
    _pickAlertValue(row, ["lifecycle_state", "escalation_state"])
    || (resolved ? "resolved" : (lifecycle.length ? lifecycle[lifecycle.length - 1].state : "triggered"))
  ).trim().toLowerCase() || "triggered";

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
    ack_expired: !!(row && row.ack_expired),
    retriggered: !!(row && row.retriggered),
    acked_ts_ms: _normalizeTsValue(_pickAlertValue(row, ["acked_ts_ms", "ack_ts_ms", "acked_at"])),
    ack_expires_ts_ms: ackExpiresTsMs,
    ack_reason: _pickAlertValue(row, ["ack_reason"]),
    acked_by: _pickAlertValue(row, ["acked_by"]),
    shelved: !!(row && row.shelved),
    shelve_expired: !!(row && row.shelve_expired),
    shelved_ts_ms: _normalizeTsValue(_pickAlertValue(row, ["shelved_ts_ms", "shelve_ts_ms", "shelved_at"])),
    shelve_expires_ts_ms: shelveExpiresTsMs,
    shelved_by: _pickAlertValue(row, ["shelved_by"]),
    shelve_reason: _pickAlertValue(row, ["shelve_reason", "snooze_reason"]),
    lifecycle,
    lifecycle_state: lifecycleState,
    escalation_state: _pickAlertValue(row, ["escalation_state"]) || lifecycleState,
    next_escalation_ts_ms: _normalizeTsValue(_pickAlertValue(row, ["next_escalation_ts_ms"])),
    notification_policy: row.notification_policy && typeof row.notification_policy === "object"
      ? { ...row.notification_policy }
      : null,
    resolved_ts_ms: _normalizeTsValue(_pickAlertValue(row, ["resolved_ts_ms", "resolved_at"])),
    resolved_by: _pickAlertValue(row, ["resolved_by"]),
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
  const sev = normalizeSeverity(s);
  const index = ALERT_SEVERITY_ORDER.indexOf(sev);
  return index >= 0 ? index + 1 : 0;
}

export function severityAtLeast(value, minimum) {
  const minRank = severityRank(minimum);
  return minRank > 0 && severityRank(value) >= minRank;
}

const ALERT_STALE_THRESHOLDS_MS = Object.freeze({
  CRIT: 15 * 60 * 1000,
  HIGH: 15 * 60 * 1000,
  WARN: 60 * 60 * 1000,
  INFO: 4 * 60 * 60 * 1000,
});

const ALERT_LIFECYCLE_SORT_RANK = Object.freeze({
  retriggered: 6,
  active: 5,
  stale: 4,
  acknowledged: 3,
  shelved: 2,
  resolved: 1,
});

function _finitePositiveMs(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function _alertAgeMs(alert, nowMs) {
  const ts = _finitePositiveMs(alert && (alert.ts_ms ?? alert.ts));
  return ts == null ? null : Math.max(0, Number(nowMs) - ts);
}

function _alertExplicitlyStale(alert) {
  if (!alert || typeof alert !== "object") return false;
  if (alert.stale === true || alert.is_stale === true || alert.freshness_stale === true) return true;
  const freshness = String(
    alert.freshness ||
    alert.freshness_state ||
    alert.data_freshness ||
    alert.health_state ||
    ""
  ).trim().toLowerCase();
  return freshness === "stale" || freshness === "expired";
}

function _alertStaleThresholdMs(alert) {
  const explicit = _finitePositiveMs(
    alert && (
      alert.stale_after_ms ??
      alert.max_age_ms ??
      alert.freshness_max_age_ms ??
      (alert.max_age_s != null ? Number(alert.max_age_s) * 1000 : null)
    )
  );
  if (explicit != null) return explicit;
  return ALERT_STALE_THRESHOLDS_MS[normalizeSeverity(alert && alert.severity)] || ALERT_STALE_THRESHOLDS_MS.INFO;
}

export function formatAlertDurationMs(ms) {
  const n = Math.max(0, Number(ms) || 0);
  if (n < 60_000) return `${Math.max(0, Math.round(n / 1000))}s`;
  if (n < 3_600_000) return `${Math.round(n / 60_000)}m`;
  if (n < 86_400_000) return `${Math.round(n / 3_600_000)}h`;
  return `${Math.round(n / 86_400_000)}d`;
}

export function alertAffectedEntity(row) {
  const alert = normalizeAlert(row) || {};
  const value =
    alert.symbol ||
    _pickAlertValue(alert, ["entity", "affected_entity", "component", "service", "source", "scope"]) ||
    "SYSTEM";
  return String(value || "SYSTEM").trim().toUpperCase() || "SYSTEM";
}

export function alertLifecycleState(row, nowMs = Date.now()) {
  const alert = normalizeAlert(row) || {};
  const resolved = alert.status === "resolved" || alert.resolved === true;
  const ageMs = _alertAgeMs(alert, nowMs);
  const stale = !resolved && (
    _alertExplicitlyStale(alert) ||
    (ageMs != null && ageMs >= _alertStaleThresholdMs(alert))
  );
  const policy = alert.notification_policy && typeof alert.notification_policy === "object"
    ? alert.notification_policy
    : {};
  const shelved = !resolved && !!alert.shelved && !alert.shelve_expired;
  const suppressed = !resolved && !!(policy.suppressed || shelved || alert.suppressed);
  const actionability = normalizeSeverity(alert.severity) === "INFO" ? "notification" : "alarm";

  if (resolved) {
    return {
      key: "resolved",
      label: "Resolved",
      tone: "ok",
      actionability: "closed",
      ageMs,
      stale: false,
      suppressed: true,
      description: alert.resolved_reason ? `Resolved: ${alert.resolved_reason}` : "Resolved",
    };
  }
  if (alert.shelve_expired) {
    return {
      key: "retriggered",
      label: "Shelving expired",
      tone: normalizeSeverity(alert.severity) === "CRIT" ? "crit" : "high",
      actionability,
      ageMs,
      stale,
      suppressed: false,
      description: "Shelving expired while the alert was still unresolved.",
    };
  }
  if (shelved) {
    const remaining = Number(alert.shelve_expires_ts_ms || 0) - Number(nowMs || Date.now());
    return {
      key: "shelved",
      label: "Shelved, unresolved",
      tone: "warn",
      actionability,
      ageMs,
      stale,
      suppressed,
      expiresTsMs: _finitePositiveMs(alert.shelve_expires_ts_ms),
      remainingMs: Number.isFinite(remaining) ? Math.max(0, remaining) : null,
      description: `Shelved until ${alert.shelve_expires_ts_ms ? fmtTime(alert.shelve_expires_ts_ms) : "expiry unavailable"}.`,
    };
  }
  if (alert.ack_expired || alert.retriggered || alert.lifecycle_state === "retriggered") {
    return {
      key: "retriggered",
      label: "Re-triggered",
      tone: normalizeSeverity(alert.severity) === "CRIT" ? "crit" : "high",
      actionability,
      ageMs,
      stale,
      suppressed,
      description: "Acknowledgement expired while the alert remained unresolved.",
    };
  }
  if (alert.acked) {
    const remaining = Number(alert.ack_expires_ts_ms || 0) - Number(nowMs || Date.now());
    return {
      key: "acknowledged",
      label: "Acknowledged, unresolved",
      tone: "warn",
      actionability,
      ageMs,
      stale,
      suppressed,
      expiresTsMs: _finitePositiveMs(alert.ack_expires_ts_ms),
      remainingMs: Number.isFinite(remaining) ? Math.max(0, remaining) : null,
      description: "Acknowledged only; this is not resolved.",
    };
  }
  if (stale) {
    return {
      key: "stale",
      label: "Stale active",
      tone: severityAtLeast(alert.severity, "HIGH") ? "high" : "warn",
      actionability,
      ageMs,
      stale: true,
      suppressed,
      description: "Alert evidence is stale while unresolved.",
    };
  }
  return {
    key: "active",
    label: "Active",
    tone: cellColor(alert).key,
    actionability,
    ageMs,
    stale: false,
    suppressed,
    description: "Active unresolved alert.",
  };
}

export function defaultAlertRecommendedAction(row, nowMs = Date.now()) {
  const alert = normalizeAlert(row) || {};
  const state = alertLifecycleState(alert, nowMs);
  if (state.key === "resolved") return "No action: resolution recorded";
  if (state.key === "shelved") {
    const remaining = state.remainingMs == null ? "expiry unavailable" : `${formatAlertDurationMs(state.remainingMs)} remaining`;
    return `Shelved only; re-check before expiry (${remaining})`;
  }
  if (state.key === "acknowledged") return "Acknowledged only; verify it clears or resolve after root cause fix";
  if (state.key === "retriggered") return "Act now: escalation resumed before resolution";
  if (state.key === "stale") return "Refresh evidence, then resolve or escalate";
  if (normalizeSeverity(alert.severity) === "CRIT") return "Act now; pause affected path if needed";
  if (normalizeSeverity(alert.severity) === "HIGH") return "Investigate before adding risk";
  if (normalizeSeverity(alert.severity) === "WARN") return "Review and watch for repeats";
  return "Informational; observe unless it repeats";
}

function _alertMessageSignature(alert) {
  const raw = String(alert.message || alert.event_title || alert.reason || "alert")
    .toLowerCase()
    .replace(/[0-9a-f]{8,}/g, "#")
    .replace(/\d+(\.\d+)?/g, "#")
    .replace(/[^a-z0-9_#]+/g, " ")
    .trim()
    .slice(0, 80);
  return raw || "alert";
}

function _alertHorizonBucket(alert) {
  const s = Number(alert.horizon_s);
  if (!Number.isFinite(s) || s <= 0) return "h:none";
  if (s <= 120) return "h:1m";
  if (s <= 900) return "h:5m";
  if (s <= 7200) return "h:1h";
  return "h:4h";
}

export function alertIncidentGroupKey(row, nowMs = Date.now()) {
  const alert = normalizeAlert(row) || {};
  const state = alertLifecycleState(alert, nowMs);
  const lifecycleFamily = state.key === "resolved" ? "resolved" : "open";
  const rule = String(
    _pickAlertValue(alert, ["incident_id", "rule_id", "alert_type", "event_type", "type"]) ||
    _alertMessageSignature(alert)
  ).trim().toLowerCase();
  return [
    lifecycleFamily,
    alertAffectedEntity(alert),
    _alertHorizonBucket(alert),
    rule,
  ].join("|");
}

function _alertSortScore(alert, nowMs) {
  const state = alertLifecycleState(alert, nowMs);
  return (
    (ALERT_LIFECYCLE_SORT_RANK[state.key] || 0) * 1000 +
    severityRank(alert.severity) * 100 +
    (state.actionability === "alarm" ? 10 : 0)
  );
}

function _sortAlertsForIncidents(a, b, nowMs) {
  const scoreDelta = _alertSortScore(b, nowMs) - _alertSortScore(a, nowMs);
  if (scoreDelta !== 0) return scoreDelta;
  return Number(b.ts_ms || b.ts || 0) - Number(a.ts_ms || a.ts || 0);
}

export function buildAlertIncidentGroups(rows, nowMs = Date.now()) {
  const groups = new Map();
  for (const raw of rows || []) {
    const alert = normalizeAlert(raw);
    if (!alert) continue;
    const key = alertIncidentGroupKey(alert, nowMs);
    if (!groups.has(key)) {
      groups.set(key, {
        key,
        alerts: [],
        parent: null,
        severity: "INFO",
        entity: alertAffectedEntity(alert),
        title: alert.message || alert.event_title || "Alert",
        latestTsMs: 0,
      });
    }
    const group = groups.get(key);
    group.alerts.push(alert);
    if (severityRank(alert.severity) > severityRank(group.severity)) group.severity = alert.severity;
    group.latestTsMs = Math.max(group.latestTsMs, Number(alert.ts_ms || alert.ts || 0));
  }

  return Array.from(groups.values())
    .map((group) => {
      group.alerts.sort((a, b) => _sortAlertsForIncidents(a, b, nowMs));
      group.parent = group.alerts[0] || null;
      group.count = group.alerts.length;
      group.hiddenCount = Math.max(0, group.count - 1);
      group.lifecycle = alertLifecycleState(group.parent, nowMs);
      group.title = group.parent
        ? (group.parent.message || group.parent.event_title || group.title || "Alert")
        : (group.title || "Alert");
      return group;
    })
    .sort((a, b) => {
      const parentDelta = _sortAlertsForIncidents(a.parent || {}, b.parent || {}, nowMs);
      if (parentDelta !== 0) return parentDelta;
      if (b.count !== a.count) return b.count - a.count;
      return Number(b.latestTsMs || 0) - Number(a.latestTsMs || 0);
    });
}

export function summarizeAlertLifecycleCounts(rows, nowMs = Date.now()) {
  const summary = {
    total: 0,
    open: 0,
    alarms: 0,
    notifications: 0,
    crit: 0,
    high: 0,
    warn: 0,
    info: 0,
    acknowledged: 0,
    shelved: 0,
    suppressed: 0,
    stale: 0,
    resolved: 0,
    grouped_incidents: 0,
    flood_groups: 0,
  };
  const normalized = [];
  for (const raw of rows || []) {
    const alert = normalizeAlert(raw);
    if (!alert) continue;
    normalized.push(alert);
    summary.total += 1;
    const sev = normalizeSeverity(alert.severity);
    if (sev === "CRIT") summary.crit += 1;
    else if (sev === "HIGH") summary.high += 1;
    else if (sev === "WARN") summary.warn += 1;
    else summary.info += 1;

    const state = alertLifecycleState(alert, nowMs);
    if (state.key === "resolved") {
      summary.resolved += 1;
      continue;
    }
    summary.open += 1;
    if (state.actionability === "alarm") summary.alarms += 1;
    else summary.notifications += 1;
    if (state.key === "acknowledged") summary.acknowledged += 1;
    if (state.key === "shelved") summary.shelved += 1;
    if (state.suppressed) summary.suppressed += 1;
    if (state.stale) summary.stale += 1;
  }
  const groups = buildAlertIncidentGroups(normalized, nowMs);
  summary.grouped_incidents = groups.length;
  summary.flood_groups = groups.filter((group) => Number(group.count || 0) > 1).length;
  return summary;
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
      if (r.shelved) continue;
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

  const nowMs = opts && Number.isFinite(Number(opts.nowMs)) ? Number(opts.nowMs) : Date.now();
  const groups = buildAlertIncidentGroups(rows, nowMs);

  if (!groups.length) {
    host.innerHTML =
      `<div class="small" style="color:var(--muted);">
        No alerts in the selected window.
       </div>`;
    return;
  }

  groups
    .slice(0, 18)
    .forEach(group => {
      const r = group.parent;
      if (!r) return;
      const ts = Number(r.ts);
      const ageMin = Number.isFinite(ts)
        ? Math.max(0, Math.floor((nowMs - ts) / 60000))
        : null;

      const c = cellColor(r);
      const title = r.message || r.event_title || "Alert";
      const state = alertLifecycleState(r, nowMs);
      const entity = alertAffectedEntity(r);
      const action = defaultAlertRecommendedAction(r, nowMs);
      const symbolMeta = entity ? `<span class="pill dim mono">Entity ${esc(entity)}</span>` : "";
      const timeMeta = Number.isFinite(ts) ? `<span class="pill dim mono">${esc(fmtTime(ts))}</span>` : "";
      const ageMeta = ageMin == null
        ? "<span class=\"pill dim\">Freshness unknown</span>"
        : `<span class="pill dim">${ageMin}m ago${state.stale ? " • stale" : ""}</span>`;
      const horizonMeta = r.horizon_s != null && String(r.horizon_s).trim() !== ""
        ? `<span class="pill dim">h=${esc(String(r.horizon_s))}s</span>`
        : "";
      const groupMeta = group.count > 1
        ? `<span class="pill dim" title="Similar alerts collapsed into this parent incident">${esc(String(group.count))} related</span>`
        : "";
      const actionabilityMeta = `<span class="pill dim">${state.actionability === "notification" ? "Notification" : "Alarm"}</span>`;
      const stateMeta = `<span class="${statusPillClasses(state.tone, "incidentState")}" data-state="${esc(state.key)}">${esc(state.label)}</span>`;
      const shelfMeta = state.key === "shelved" && state.remainingMs != null
        ? `<span class="pill dim">shelf ${esc(formatAlertDurationMs(state.remainingMs))} left</span>`
        : "";

      const item = document.createElement("div");
      item.className = "incidentItem";
      item.dataset.alertId = String(r.id || "");
      item.dataset.sev = r.status === "resolved" ? "OK" : String(r.severity || "INFO").toUpperCase();
      item.dataset.status = c.key;
      item.dataset.lifecycle = state.key;
      item.dataset.groupCount = String(group.count || 1);
      item.dataset.actionability = state.actionability;
      item.tabIndex = 0;
      item.setAttribute(
        "aria-label",
        `${state.actionability === "notification" ? "Notification" : "Alarm"} ${r.status === "resolved" ? "resolved" : r.severity}, ${entity}, ${state.label}, ${title}. ${action}`
      );
      item.innerHTML = `
        <div class="incidentTop">
          <span class="${statusPillClasses(c.key, "incidentSeverity")}" data-status="${esc(c.key)}" aria-label="${esc(statusAriaLabel(c.key, r.status === "resolved" ? "RESOLVED" : r.severity))}">
            ${r.status === "resolved" ? "RESOLVED" : r.severity}
          </span>
          ${actionabilityMeta}
          ${stateMeta}
          ${groupMeta}
          <div class="incidentTitle">
            ${escapeHTML(title)}
          </div>
        </div>
        <div class="incidentSub">
          ${symbolMeta}
          ${timeMeta}
          ${horizonMeta}
          ${ageMeta}
          ${shelfMeta}
        </div>
        <div class="incidentSub incidentAction">
          <span class="pill dim">Recommended action</span>
          <span>${escapeHTML(action)}</span>
        </div>
      `;

      item.addEventListener("click", () => {
        if (opts?.onOpen) opts.onOpen(r, group);
      });
      item.addEventListener("keydown", (event) => {
        if (!event) return;
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          if (opts?.onOpen) opts.onOpen(r, group);
        }
      });

      host.appendChild(item);
    });
}
