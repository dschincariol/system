/*
  FILE: ui/decision_drilldown.mjs

  Small pure helpers for dashboard decision drill-down links and stage rows.
  The modal renderer in dashboard.js owns DOM updates; this module keeps the
  lookup and stage formatting testable without booting the dashboard.
*/

const LOOKUP_KEYS = Object.freeze([
  "decisionId",
  "sourceAlertId",
  "portfolioOrderId",
  "ledgerId",
  "clientOrderId",
]);

const STAGE_TONES = Object.freeze({
  available: "ok",
  executed: "ok",
  partial: "warn",
  suppressed: "warn",
  blocked: "crit",
  unavailable: "dim",
  loading: "dim",
});

function present(value) {
  return value !== undefined && value !== null && String(value).trim() !== "";
}

function firstPresent(...values) {
  for (const value of values) {
    if (present(value)) return value;
  }
  return "";
}

function toNumberId(value) {
  if (!present(value)) return "";
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? String(Math.trunc(n)) : "";
}

function cleanString(value) {
  return present(value) ? String(value).trim() : "";
}

function prettyStatus(status) {
  const raw = cleanString(status).toLowerCase() || "unavailable";
  return raw.replace(/_/g, " ");
}

function formatStageTimestamp(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "";
  const date = new Date(n);
  if (!Number.isFinite(date.getTime())) return "";
  return `ts ${date.toISOString()}`;
}

export function normalizeDecisionLookup(input = {}) {
  const source = input && typeof input === "object" ? input : {};
  const lookup = {
    decisionId: toNumberId(firstPresent(source.decisionId, source.decision_id, source.decisionID)),
    sourceAlertId: toNumberId(firstPresent(source.sourceAlertId, source.source_alert_id, source.alertId, source.alert_id)),
    portfolioOrderId: toNumberId(firstPresent(source.portfolioOrderId, source.portfolio_order_id, source.portfolio_orders_id, source.source_order_id)),
    ledgerId: toNumberId(firstPresent(source.ledgerId, source.ledger_id)),
    clientOrderId: cleanString(firstPresent(source.clientOrderId, source.client_order_id)),
    surface: cleanString(source.surface),
  };
  return lookup;
}

export function hasDecisionLookup(input = {}) {
  const lookup = normalizeDecisionLookup(input);
  return LOOKUP_KEYS.some((key) => present(lookup[key]));
}

export function buildDecisionDetailUrl(input = {}) {
  const lookup = normalizeDecisionLookup(input);
  const params = new URLSearchParams();
  if (lookup.decisionId) params.set("id", lookup.decisionId);
  if (lookup.sourceAlertId) params.set("source_alert_id", lookup.sourceAlertId);
  if (lookup.portfolioOrderId) params.set("portfolio_order_id", lookup.portfolioOrderId);
  if (lookup.ledgerId) params.set("ledger_id", lookup.ledgerId);
  if (lookup.clientOrderId) params.set("client_order_id", lookup.clientOrderId);
  const query = params.toString();
  return query ? `/api/ui/decision?${query}` : "/api/ui/decision";
}

export function buildDecisionStageRows(payload = {}) {
  const stages = Array.isArray(payload && payload.stages) ? payload.stages : [];
  if (!stages.length) {
    return [
      {
        key: "decision_path",
        label: "Decision path",
        value: "unavailable",
        status: "unavailable",
        reason: "No stage data was returned for this drill-down.",
        meta: "No stage data was returned for this drill-down.",
        tone: "dim",
        timestamp: "",
        timestampMs: null,
      },
    ];
  }

  return stages.map((stage) => {
    const rawStatus = cleanString(stage && stage.status).toLowerCase() || "unavailable";
    const status = prettyStatus(rawStatus);
    const unavailable = cleanString(stage && stage.unavailable_reason);
    const summary = cleanString(stage && stage.summary) || unavailable;
    const timestamp = formatStageTimestamp(stage && stage.ts_ms);
    const meta = [summary, timestamp].filter(Boolean).join(" | ") || "Stage detail unavailable.";
    const timestampMs = Number(stage && stage.ts_ms);
    return {
      key: cleanString(stage && stage.key),
      label: cleanString(stage && stage.label) || cleanString(stage && stage.key) || "Stage",
      value: status,
      status: rawStatus,
      reason: summary || "Stage detail unavailable.",
      meta,
      tone: STAGE_TONES[rawStatus] || "dim",
      timestamp: timestamp.replace(/^ts\s+/, ""),
      timestampMs: Number.isFinite(timestampMs) && timestampMs > 0 ? timestampMs : null,
    };
  });
}

export function buildDecisionRelatedSummary(payload = {}) {
  const related = payload && payload.related && typeof payload.related === "object"
    ? payload.related
    : {};
  const count = (value) => Array.isArray(value) ? value.length : (value && typeof value === "object" ? 1 : 0);
  return {
    alert: count(related.alert),
    portfolioOrders: count(related.portfolio_orders),
    policyRows: count(related.execution_policy_audit),
    executionOrders: count(related.execution_orders),
    fills: count(related.fills),
    attributionRows: count(related.trade_attribution_ledger),
  };
}
