/*
  FILE: ui/ui_metrics.js

  Browser-side normalizers for the canonical /api/ui/metrics payload. Keep this
  module DOM-free so render paths can be tested without loading dashboard.js.
*/

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function numOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function intOrZero(value) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function pickNumber(...values) {
  for (const value of values) {
    const n = numOrNull(value);
    if (n !== null) return n;
  }
  return null;
}

function sourceFor(metrics, key) {
  return asObject(asObject(metrics).sources)[key] || {};
}

function sourceLabel(source) {
  const item = asObject(source);
  const endpoint = String(item.endpoint || "").trim();
  if (!endpoint) return "source unavailable";
  if (item.missing) return `${endpoint} missing`;
  if (item.stale) return `${endpoint} stale`;
  return endpoint;
}

function anySourceProblem(metrics, keys) {
  return asArray(keys).some((key) => {
    const source = sourceFor(metrics, key);
    return !!(source.missing || source.stale);
  });
}

export function normalizeUiMetricsPayload(payload) {
  const root = asObject(payload);
  const pnl = asObject(root.pnl);
  const exposure = asObject(root.exposure);
  const positions = asObject(root.positions);
  const account = asObject(root.account);
  const risk = asObject(root.risk);
  const summary = asObject(root.summary);
  const sources = asObject(root.sources);
  return {
    ok: root.ok !== false,
    schemaVersion: intOrZero(root.schema_version) || intOrZero(root.schemaVersion) || 1,
    tsMs: intOrZero(root.ts_ms) || intOrZero(root.tsMs),
    staleAfterMs: intOrZero(root.stale_after_ms) || intOrZero(root.staleAfterMs),
    pnl,
    exposure,
    positions,
    account,
    risk,
    sources,
    summary,
    degraded: !!summary.degraded,
    missingSources: asArray(summary.missing_sources).map(String),
    staleSources: asArray(summary.stale_sources).map(String),
  };
}

export function canonicalPnlValues(metrics) {
  const normalized = normalizeUiMetricsPayload(metrics);
  const pnl = normalized.pnl;
  const source = sourceFor(normalized, "pnl");
  const today = pickNumber(pnl.today_pnl, pnl.daily_pnl, pnl.day_pnl, pnl.total_pnl);
  const total = pickNumber(pnl.total_pnl, today);
  const realized = pickNumber(pnl.realized_pnl, pnl.realized);
  const unrealized = pickNumber(pnl.unrealized_pnl, pnl.unrealized);
  const missing = !!source.missing || (today === null && total === null && realized === null && unrealized === null);
  return {
    today,
    total,
    realized,
    unrealized,
    tsMs: intOrZero(source.ts_ms) || normalized.tsMs,
    missing,
    stale: !!source.stale,
    degraded: missing || !!source.stale || normalized.degraded,
    source,
    sourceLabel: sourceLabel(source),
  };
}

export function canonicalExposureValues(metrics) {
  const normalized = normalizeUiMetricsPayload(metrics);
  const exposure = normalized.exposure;
  const risk = normalized.risk;
  const account = normalized.account;
  const positions = normalized.positions;
  const riskSource = sourceFor(normalized, "risk_summary");
  const accountSource = sourceFor(normalized, "broker");
  const positionSource = sourceFor(normalized, "portfolio");
  const gross = pickNumber(exposure.gross, exposure.gross_exposure);
  const net = pickNumber(exposure.net, exposure.net_exposure);
  const drawdown = pickNumber(risk.max_drawdown_pct, risk.drawdown);
  const missing = !!riskSource.missing || (gross === null && net === null && drawdown === null);
  return {
    gross,
    net,
    drawdown,
    riskStatus: String(risk.status || "unknown"),
    riskBlocked: typeof risk.blocked === "boolean" ? risk.blocked : null,
    barrier: asObject(risk.execution_barrier),
    cash: pickNumber(account.cash),
    equity: pickNumber(account.equity),
    targetCount: pickNumber(positions.target_count),
    liveCount: pickNumber(positions.live_count),
    orderCount: pickNumber(positions.order_count),
    missing,
    stale: anySourceProblem(normalized, ["risk_summary", "broker", "portfolio"]),
    degraded: missing || normalized.degraded,
    sourceTsMs: Math.max(
      intOrZero(riskSource.ts_ms),
      intOrZero(accountSource.ts_ms),
      intOrZero(positionSource.ts_ms)
    ),
    riskSource,
    accountSource,
    positionSource,
    sourceLabel: [
      sourceLabel(riskSource),
      sourceLabel(accountSource),
      sourceLabel(positionSource),
    ].join(" · "),
  };
}

export function canonicalSourceNotes(metrics, keys) {
  const normalized = normalizeUiMetricsPayload(metrics);
  return asArray(keys).flatMap((key) => {
    const source = sourceFor(normalized, key);
    if (!source || !source.endpoint) return [`${key} source missing`];
    if (source.missing) return [`${source.endpoint} missing`];
    if (source.stale) return [`${source.endpoint} stale`];
    return [];
  });
}
