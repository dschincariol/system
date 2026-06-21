"use strict";

/*
  FILE: ui/operator_overview.js

  At-a-glance non-technical Overview composition for the main dashboard.
  This module is intentionally a read-only renderer: it pulls production
  runtime endpoints, reuses existing status/decision/risk helpers, and never
  introduces mutation controls.
*/

import {
  ageMsFromTimestamp,
  escapeHTML,
  formatAgeMs,
  formatSigned,
  numOrNull,
  pickTimestamp,
  statusToken
} from "./utils.js";
import { computeHealthScore } from "./health_score.js";
import { summarizeRuntimeStatus, unwrapHealthResponse } from "./runtime_status_summary.js";
import {
  buildDecisionDetailUrl,
  hasDecisionLookup,
  normalizeDecisionLookup
} from "./decision_drilldown.mjs";
import {
  buildDecisionStepperModel,
  renderDecisionStepperHtml
} from "./decision_stepper.js";
import {
  buildRiskHeadroomViewModel,
  renderBulletBars
} from "./bullet_bars.js";
import {
  canonicalPnlValues,
  normalizeUiMetricsPayload
} from "./ui_metrics.js";
import { buildMarketStressTopDriver } from "./market_stress.js";

const OVERVIEW_STALE_AFTER_MS = 300_000;
const OVERVIEW_WARN_AFTER_MS = 60_000;
const MAX_TIMELINE_DECISIONS = 12;

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function asBoolean(value) {
  if (typeof value === "boolean") return value;
  if (value === 1 || value === "1" || value === "true" || value === "TRUE") return true;
  if (value === 0 || value === "0" || value === "false" || value === "FALSE") return false;
  return null;
}

function pickNumber(...values) {
  for (const value of values) {
    const n = numOrNull(value);
    if (n != null) return n;
  }
  return null;
}

function normalizeFailures(items) {
  return asArray(items)
    .map((item) => {
      if (!item) return null;
      if (typeof item === "string") return { label: "route", message: item };
      return {
        label: String(item.label || item.key || "route").trim() || "route",
        message: String(item.message || item.error || item.reason || "").trim() || "request_failed",
      };
    })
    .filter(Boolean);
}

function sourceTimestamp({
  systemState,
  healthPayload,
  readinessPayload,
  executionBarrier,
  stressPayload,
  uiMetrics,
  decisionsPayload,
  riskPortfolio,
  pnlPayload,
} = {}) {
  const readiness = asObject(readinessPayload);
  const readinessBody = asObject(readiness.readiness);
  const health = unwrapHealthResponse(healthPayload);
  const stress = asObject(asObject(stressPayload).stress);
  const metrics = asObject(uiMetrics);
  const decisions = asArray(asObject(decisionsPayload).decisions);
  const latestDecisionTs = decisions.reduce((max, row) => {
    const ts = pickTimestamp(
      row && row.ts_ms,
      row && row.created_ts_ms,
      row && row.updated_ts_ms,
      row && row.decision_ts_ms
    );
    return Math.max(max, ts || 0);
  }, 0);
  return pickTimestamp(
    systemState && systemState.ts_ms,
    health && health.ts_ms,
    readiness.ts_ms,
    readinessBody.ts_ms,
    executionBarrier && executionBarrier.ts_ms,
    stressPayload && stressPayload.ts_ms,
    stress.ts_ms,
    metrics.ts_ms,
    decisionsPayload && decisionsPayload.ts_ms,
    latestDecisionTs,
    riskPortfolio && riskPortfolio.ts_ms,
    pnlPayload && pnlPayload.ts_ms
  );
}

function healthExecutionBarrier(healthPayload) {
  const health = unwrapHealthResponse(healthPayload);
  return asObject(health.execution_barrier);
}

function executionAllowed(executionBarrier, healthPayload) {
  const explicit = asBoolean(asObject(executionBarrier).allowed);
  if (explicit !== null) return explicit;
  return asBoolean(healthExecutionBarrier(healthPayload).allowed);
}

function normalizeSystemStateToken(systemState) {
  return String(asObject(systemState).state || asObject(systemState).status || "")
    .trim()
    .toUpperCase();
}

function isReadinessReady(readinessPayload) {
  const readiness = asObject(readinessPayload);
  const nested = asObject(readiness.readiness);
  const ready = asBoolean(readiness.ready);
  if (ready !== null) return ready;
  const nestedReady = asBoolean(nested.ready);
  if (nestedReady !== null) return nestedReady;
  return asBoolean(readiness.ok);
}

function isHealthOk(healthPayload) {
  return asBoolean(unwrapHealthResponse(healthPayload).ok);
}

function statusFreshness(tsMs, nowMs = Date.now()) {
  const ts = numOrNull(tsMs);
  if (ts == null || ts <= 0) {
    return {
      tsMs: null,
      ageMs: null,
      tone: "dim",
      stale: true,
      text: "Freshness unavailable",
    };
  }
  const ageMs = Math.max(0, nowMs - ts);
  let tone = "ok";
  if (ageMs >= OVERVIEW_STALE_AFTER_MS) tone = "warn";
  else if (ageMs >= OVERVIEW_WARN_AFTER_MS) tone = "warn";
  return {
    tsMs: ts,
    ageMs,
    tone,
    stale: ageMs >= OVERVIEW_STALE_AFTER_MS,
    text: `Backend ${formatAgeMs(ageMs)} ago`,
  };
}

function scoreText(scorecard) {
  const score = scorecard && scorecard.score;
  if (!Number.isFinite(Number(score))) return "Health score unavailable";
  const base = `Health ${Math.round(Number(score))}/100`;
  if (scorecard && scorecard.partialCoverage && scorecard.coverageText) {
    return `${base} (${scorecard.coverageText})`;
  }
  return base;
}

function firstNextStep(runtimeSummary) {
  return asArray(runtimeSummary && runtimeSummary.next)[0] || "Monitor the detailed panels before intervening.";
}

function statusWordAndTone({
  scorecard,
  runtimeSummary,
  systemState,
  healthPayload,
  readinessPayload,
  executionBarrier,
  failures,
  freshness,
} = {}) {
  const systemToken = normalizeSystemStateToken(systemState);
  const allowed = executionAllowed(executionBarrier, healthPayload);
  const healthOk = isHealthOk(healthPayload);
  const readinessReady = isReadinessReady(readinessPayload);
  const score = scorecard && Number.isFinite(Number(scorecard.score)) ? Number(scorecard.score) : null;
  const badge = String(scorecard && scorecard.badgeClassName || "").toLowerCase();
  const partialCoverage = !!(scorecard && scorecard.partialCoverage);
  const tradingPill = asObject(asObject(runtimeSummary).pills).trading || {};

  const stoppedSystem = ["KILL_SWITCH", "HALTED", "STOP", "STOPPED", "FAILED"].includes(systemToken);
  if (allowed === false || stoppedSystem || String(tradingPill.cls || "") === "bad") {
    return {
      word: "STOP",
      tone: "crit",
      icon: "X",
      reason: allowed === false ? "Execution barrier is blocking trading." : "Runtime state is stopped.",
    };
  }

  const cautious = !!failures.length
    || !!(freshness && freshness.stale)
    || readinessReady === false
    || healthOk === false
    || badge === "warn"
    || badge === "high"
    || badge === "crit"
    || badge === "unavailable"
    || partialCoverage
    || (score != null && score < 80);

  if (cautious) {
    return {
      word: "CAUTION",
      tone: "warn",
      icon: "!",
      reason: failures.length
        ? "One or more Overview source routes are unavailable."
        : "Runtime visibility or health is degraded.",
    };
  }

  return {
    word: "SAFE",
    tone: "ok",
    icon: "OK",
    reason: "Runtime health, freshness, and execution availability are clean.",
  };
}

export function buildOverviewStatusModel({
  systemState = null,
  healthPayload = null,
  readinessPayload = null,
  executionBarrier = null,
  stressPayload = null,
  alerts = null,
  systemStatus = null,
  executionDegraded = false,
  failures = [],
  nowMs = Date.now(),
} = {}) {
  const normalizedFailures = normalizeFailures(failures);
  const scorecard = computeHealthScore({
    alerts,
    health: healthPayload,
    readiness: readinessPayload,
    systemState,
    systemStatus,
    executionBarrier,
    executionDegraded,
  });
  const runtimeSummary = summarizeRuntimeStatus({
    systemState,
    stressPayload,
    barrierPayload: executionBarrier,
    healthPayload,
    readinessPayload,
  });
  const latestTs = sourceTimestamp({
    systemState,
    healthPayload,
    readinessPayload,
    executionBarrier,
    stressPayload,
  });
  const freshness = statusFreshness(latestTs, nowMs);
  const mapped = statusWordAndTone({
    scorecard,
    runtimeSummary,
    systemState,
    healthPayload,
    readinessPayload,
    executionBarrier,
    failures: normalizedFailures,
    freshness,
  });
  const headline = normalizedFailures.length
    ? "Overview source routes are degraded"
    : (runtimeSummary.headline || "System status unavailable");
  const meaning = normalizedFailures.length
    ? normalizedFailures.slice(0, 2).map((item) => `${item.label}: ${item.message}`).join(" | ")
    : (runtimeSummary.meaning || mapped.reason);
  const nextStep = normalizedFailures.length
    ? "Treat the Overview as degraded until the failed route recovers."
    : firstNextStep(runtimeSummary);
  const fallbackText = `${mapped.word}. ${scoreText(scorecard)}. ${freshness.text}. ${headline}. ${meaning} Next step: ${nextStep}`;

  return {
    ...mapped,
    score: scorecard.score,
    scoreText: scoreText(scorecard),
    coverageText: scorecard.coverageText || "",
    freshness,
    headline,
    meaning,
    nextStep,
    failures: normalizedFailures,
    fallbackText,
  };
}

function decisionTs(row) {
  return pickTimestamp(
    row && row.ts_ms,
    row && row.decision_ts_ms,
    row && row.created_ts_ms,
    row && row.updated_ts_ms
  );
}

export function normalizeOverviewDecisionRows(payload = {}) {
  return asArray(asObject(payload).decisions)
    .filter((row) => row && typeof row === "object")
    .slice()
    .sort((a, b) => (decisionTs(b) || 0) - (decisionTs(a) || 0));
}

function decisionStatus(row) {
  const raw = String(
    row && (
      row.status ||
      row.decision_status ||
      row.suppression_tier ||
      row.execution_status ||
      row.action
    ) || ""
  ).trim().toLowerCase();
  if (row && row.blocked === true) return "blocked";
  if (raw.includes("hard_block") || raw.includes("blocked")) return "blocked";
  if (raw.includes("soft") || raw.includes("suppress") || raw.includes("throttle")) return "suppressed";
  if (raw.includes("fill") || raw.includes("executed")) return "executed";
  if (raw.includes("buy") || raw.includes("sell") || raw.includes("hold")) return raw;
  return raw || "unavailable";
}

function decisionTone(row) {
  const status = decisionStatus(row);
  if (status === "blocked") return "crit";
  if (status === "suppressed" || status.includes("throttle")) return "warn";
  if (status === "executed" || status === "buy" || status === "sell" || status === "hold") return "ok";
  return "unavailable";
}

function decisionTitle(row) {
  const symbol = String(row && (row.symbol || row.asset || row.instrument) || "").trim().toUpperCase();
  const action = String(row && (row.action || row.side || row.decision || row.status) || "decision").trim();
  const confidence = pickNumber(row && row.confidence, row && row.score, row && row.probability);
  return [
    symbol || "Latest decision",
    action ? action.toUpperCase() : "",
    confidence == null ? "" : `confidence ${(confidence * 100).toFixed(0)}%`,
  ].filter(Boolean).join(" | ");
}

function decisionReason(row) {
  return String(
    row && (
      row.why ||
      row.reason ||
      row.summary ||
      row.explanation ||
      row.risk_reason ||
      row.suppression_reason
    ) || ""
  ).trim();
}

function latestDecisionDetailPayload({ latest, detailPayload, detailError }) {
  if (detailPayload && typeof detailPayload === "object" && detailPayload.ok !== false) {
    return detailPayload;
  }
  if (latest && Array.isArray(latest.stages)) {
    return { ok: true, stages: latest.stages };
  }
  if (detailError) {
    return {
      ok: false,
      stages: [],
    };
  }
  return {
    loading: true,
    stages: [
      {
        label: "Decision path",
        status: "loading",
        summary: "Loading the latest decision flow from the production drill-down endpoint.",
      },
    ],
  };
}

function timelineItem(row) {
  const lookup = normalizeDecisionLookup(row || {});
  const ts = decisionTs(row);
  const tone = decisionTone(row);
  const token = statusToken(tone);
  const status = decisionStatus(row);
  const label = decisionTitle(row);
  const reason = decisionReason(row);
  return {
    id: lookup.decisionId || lookup.portfolioOrderId || lookup.sourceAlertId || lookup.clientOrderId || "",
    tsMs: ts,
    tone: token.key,
    glyph: token.glyph,
    status,
    label,
    reason,
    fallbackText: [
      label,
      status ? `status ${status}` : "",
      ts ? `at ${new Date(ts).toISOString()}` : "timestamp unavailable",
      reason,
    ].filter(Boolean).join("; "),
  };
}

export function buildOverviewDecisionModel({
  decisionsPayload = null,
  detailPayload = null,
  detailError = null,
} = {}) {
  const rows = normalizeOverviewDecisionRows(decisionsPayload || {});
  if (!rows.length) {
    return {
      state: "empty",
      latest: null,
      title: "No recent decisions",
      meta: "Decision endpoint returned no rows.",
      summary: "The system has not published a recent automated decision to display.",
      stepperHtml: "",
      stepperModel: null,
      timeline: [],
      timelineLabel: "No recent decisions are available.",
      fallbackText: "No recent decisions are available. The Overview remains read-only.",
    };
  }

  const latest = rows[0];
  const detail = latestDecisionDetailPayload({ latest, detailPayload, detailError });
  const stepperModel = buildDecisionStepperModel(detail || {});
  const markers = rows.slice(0, MAX_TIMELINE_DECISIONS).reverse().map(timelineItem);
  const latestTs = decisionTs(latest);
  const reason = decisionReason(latest) || stepperModel.summary.text;
  const title = decisionTitle(latest);
  const meta = latestTs ? `latest ${formatAgeMs(ageMsFromTimestamp(latestTs))} ago` : "latest timestamp unavailable";
  const timelineLabel = markers.length
    ? `Recent decision timeline with ${markers.length} markers. Latest: ${title}.`
    : "No recent decision markers are available.";
  const fallbackText = `${title}. ${meta}. ${reason}`;

  return {
    state: detailError ? "partial" : "ready",
    latest,
    title,
    meta,
    summary: reason,
    stepperHtml: renderDecisionStepperHtml(detail || {}, { summaryId: "overviewDecisionStepperStatus" }),
    stepperModel,
    timeline: markers,
    timelineLabel,
    fallbackText,
  };
}

function extractPnlData(payload) {
  const root = asObject(payload);
  const data = asObject(root.data);
  return Object.keys(data).length ? data : root;
}

function extractPnlSeries(...sources) {
  for (const source of sources) {
    const root = asObject(source);
    const pnl = asObject(root.pnl);
    const data = extractPnlData(source);
    const candidates = [
      root.pnl_series,
      root.pnl_history,
      pnl.series,
      pnl.history,
      data.series,
      data.history,
      data.pnl_series,
    ];
    for (const candidate of candidates) {
      const rows = asArray(candidate)
        .map((row, index) => {
          if (typeof row === "number") return { tsMs: index + 1, value: row };
          const item = asObject(row);
          const value = pickNumber(
            item.total,
            item.today,
            item.today_pnl,
            item.daily_pnl,
            item.pnl,
            item.value
          );
          return {
            tsMs: pickTimestamp(item.ts_ms, item.t, item.time, index + 1) || index + 1,
            value,
          };
        })
        .filter((row) => row.value != null);
      if (rows.length) return rows;
    }
  }
  return [];
}

function buildSparklinePoints(values, width = 120, height = 34) {
  const rows = asArray(values).filter((row) => row && row.value != null);
  if (rows.length < 2) return "";
  const ys = rows.map((row) => Number(row.value));
  const min = Math.min(...ys);
  const max = Math.max(...ys);
  const span = max - min || 1;
  return rows.map((row, index) => {
    const x = rows.length === 1 ? width : (index / (rows.length - 1)) * width;
    const y = height - ((Number(row.value) - min) / span) * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
}

function buildPnlTrendModel({ uiMetrics = null, pnlPayload = null } = {}) {
  const metrics = uiMetrics ? normalizeUiMetricsPayload(uiMetrics) : null;
  const canonical = metrics ? canonicalPnlValues(metrics) : null;
  const pnlData = extractPnlData(pnlPayload || {});
  const today = canonical && canonical.today != null
    ? canonical.today
    : pickNumber(pnlData.total, pnlData.today, pnlData.today_pnl, pnlData.daily_pnl, pnlData.pnl);
  const series = extractPnlSeries(metrics, pnlPayload);
  const first = series.length ? series[0].value : null;
  const last = series.length ? series[series.length - 1].value : today;
  const trendDelta = first != null && last != null ? last - first : null;
  const direction = trendDelta == null
    ? (today == null ? "unavailable" : (today > 0 ? "up" : today < 0 ? "down" : "flat"))
    : (trendDelta > 0 ? "up" : trendDelta < 0 ? "down" : "flat");
  const tone = today == null ? "unavailable" : (today < 0 ? "warn" : "ok");
  const todayText = today == null ? "PnL unavailable" : `Today PnL ${formatSigned(today, 2)}`;
  const trendText = series.length >= 2
    ? `${todayText}; trend ${direction} over ${series.length} points.`
    : `${todayText}; trend history unavailable.`;
  const sparklinePoints = buildSparklinePoints(series);

  return {
    today,
    direction,
    tone,
    text: trendText,
    series,
    sparklinePoints,
    fallbackText: trendText,
  };
}

export function buildOverviewTrustModel({
  uiMetrics = null,
  portfolioRisk = null,
  riskSummary = null,
  pnlPayload = null,
  stressPayload = null,
  executionBarrier = null,
} = {}) {
  const normalizedMetrics = uiMetrics ? normalizeUiMetricsPayload(uiMetrics) : null;
  const barrierAllowed = asBoolean(asObject(executionBarrier).allowed);
  const riskModel = buildRiskHeadroomViewModel({
    uiMetrics: normalizedMetrics && normalizedMetrics.ok ? normalizedMetrics : null,
    portfolioRisk,
    riskSummary,
    blocked: barrierAllowed === false ? true : null,
    source: normalizedMetrics && normalizedMetrics.ok ? "/api/ui/metrics" : "/api/risk/portfolio",
  });
  const pnl = buildPnlTrendModel({ uiMetrics: normalizedMetrics, pnlPayload });
  const stress = buildMarketStressTopDriver(stressPayload);
  const riskUnavailable = !riskModel.ok;
  const fallbackText = [
    riskUnavailable ? "Risk headroom unavailable." : riskModel.fallbackText,
    pnl.fallbackText,
    stress.fallbackText,
  ].filter(Boolean).join(" ");

  return {
    riskModel,
    riskUnavailable,
    pnl,
    stress,
    fallbackText,
  };
}

export function buildOperatorOverviewModel({
  systemState = null,
  healthPayload = null,
  readinessPayload = null,
  executionBarrier = null,
  stressPayload = null,
  alerts = null,
  systemStatus = null,
  executionDegraded = false,
  failures = [],
  decisionsPayload = null,
  decisionDetailPayload = null,
  decisionDetailError = null,
  uiMetrics = null,
  portfolioRisk = null,
  riskSummary = null,
  pnlPayload = null,
  nowMs = Date.now(),
} = {}) {
  return {
    status: buildOverviewStatusModel({
      systemState,
      healthPayload,
      readinessPayload,
      executionBarrier,
      stressPayload,
      alerts,
      systemStatus,
      executionDegraded,
      failures,
      nowMs,
    }),
    decision: buildOverviewDecisionModel({
      decisionsPayload,
      detailPayload: decisionDetailPayload,
      detailError: decisionDetailError,
    }),
    trust: buildOverviewTrustModel({
      uiMetrics,
      portfolioRisk,
      riskSummary,
      pnlPayload,
      stressPayload,
      executionBarrier,
    }),
  };
}

function setText(doc, id, text) {
  const el = doc.getElementById(id);
  if (el) el.textContent = text == null ? "" : String(text);
  return el;
}

function setAttr(el, name, value) {
  if (el && typeof el.setAttribute === "function") {
    el.setAttribute(name, String(value == null ? "" : value));
  }
}

function setTileLabel(doc, id, fallbackText) {
  const el = doc.getElementById(id);
  if (!el) return;
  setAttr(el, "aria-label", fallbackText);
  setAttr(el, "role", "group");
}

function renderStatusTile(doc, model) {
  const status = asObject(model.status);
  const tile = doc.getElementById("overviewStatusTile");
  if (tile) {
    tile.className = `overviewTile overviewStatusTile is-${escapeHTML(status.tone || "unavailable")}`;
    setTileLabel(doc, "overviewStatusTile", status.fallbackText);
  }
  setText(doc, "overviewStatusIcon", status.icon || "?");
  setText(doc, "overviewStatusWord", status.word || "CAUTION");
  setText(doc, "overviewStatusScore", status.scoreText || "Health score unavailable");
  setText(doc, "overviewStatusFreshness", status.freshness && status.freshness.text || "Freshness unavailable");
  setText(doc, "overviewStatusHeadline", status.headline || "System status unavailable");
  setText(doc, "overviewStatusMeaning", status.meaning || "Runtime summary unavailable.");
  setText(doc, "overviewStatusNext", status.nextStep || "Review detailed runtime panels before intervening.");
  setText(doc, "overviewStatusFallback", status.fallbackText || "");
}

function renderDecisionTimeline(model) {
  const decision = asObject(model.decision);
  if (!decision.timeline || !decision.timeline.length) {
    return `<div class="overviewEmpty">No recent decision markers.</div>`;
  }
  return `
    <div class="overviewDecisionTimelineTrack" aria-hidden="true">
      ${decision.timeline.map((item) => `
        <span class="overviewDecisionMarker is-${escapeHTML(item.tone)}" title="${escapeHTML(item.fallbackText)}">
          <span class="overviewDecisionMarkerGlyph">${escapeHTML(item.glyph || "?")}</span>
        </span>
      `).join("")}
    </div>
    <ol class="sr-only">
      ${decision.timeline.map((item) => `<li>${escapeHTML(item.fallbackText)}</li>`).join("")}
    </ol>
  `;
}

function renderDecisionTile(doc, model) {
  const decision = asObject(model.decision);
  setTileLabel(doc, "overviewDecisionTile", decision.fallbackText || "Decision overview unavailable.");
  setText(doc, "overviewDecisionTitle", decision.title || "No recent decisions");
  setText(doc, "overviewDecisionMeta", decision.meta || "Decision freshness unavailable");
  setText(doc, "overviewDecisionSummary", decision.summary || "Decision summary unavailable.");
  setText(doc, "overviewDecisionFallback", decision.fallbackText || "");

  const stepper = doc.getElementById("overviewDecisionStepper");
  if (stepper) {
    stepper.innerHTML = decision.stepperHtml || `<div class="overviewEmpty">No decision flow to display.</div>`;
  }
  const timeline = doc.getElementById("overviewDecisionTimeline");
  if (timeline) {
    timeline.innerHTML = renderDecisionTimeline(model);
    setAttr(timeline, "role", "img");
    setAttr(timeline, "aria-label", decision.timelineLabel || "Recent decision timeline unavailable.");
  }
}

function renderPnlSparkline(pnl) {
  const tone = statusToken(pnl.tone || "unavailable").key;
  if (!pnl.sparklinePoints) {
    return `<div class="overviewSparklineFallback is-${escapeHTML(tone)}">PnL sparkline unavailable</div>`;
  }
  return `
    <svg class="overviewSparklineSvg is-${escapeHTML(tone)}" viewBox="0 0 120 34" role="img" aria-label="${escapeHTML(pnl.fallbackText || "PnL trend")}">
      <polyline points="${escapeHTML(pnl.sparklinePoints)}" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
    </svg>
  `;
}

function renderTrustTile(doc, model) {
  const trust = asObject(model.trust);
  setTileLabel(doc, "overviewTrustTile", trust.fallbackText || "Trust and risk headroom unavailable.");
  setText(doc, "overviewTrustFallback", trust.fallbackText || "");
  renderBulletBars(doc.getElementById("overviewRiskBars"), trust.riskModel || {});
  setText(
    doc,
    "overviewRiskUnavailable",
    trust.riskUnavailable ? "Risk headroom unavailable from production risk endpoints." : ""
  );
  setText(doc, "overviewPnlTrendText", trust.pnl && trust.pnl.text || "PnL trend unavailable.");
  const pnlSpark = doc.getElementById("overviewPnlSparkline");
  if (pnlSpark) {
    pnlSpark.innerHTML = renderPnlSparkline(trust.pnl || {});
    setAttr(pnlSpark, "role", "img");
    setAttr(pnlSpark, "aria-label", trust.pnl && trust.pnl.fallbackText || "PnL trend unavailable.");
  }
  const stress = trust.stress || {};
  const stressEl = doc.getElementById("overviewStressDriver");
  if (stressEl) {
    stressEl.className = `overviewStressDriver is-${escapeHTML(stress.tone || "unavailable")}`;
    stressEl.innerHTML = `
      <span class="overviewStressScore">${escapeHTML(stress.scoreText || "Stress unavailable")}</span>
      <span class="overviewStressText">${escapeHTML(stress.text || "Top market-stress driver unavailable.")}</span>
    `;
    setAttr(stressEl, "role", "img");
    setAttr(stressEl, "aria-label", stress.fallbackText || "Market stress top driver unavailable.");
  }
}

export function renderOperatorOverview(model, { document: doc = document } = {}) {
  if (!doc || !doc.getElementById || !doc.getElementById("operatorOverviewCard")) return;
  renderStatusTile(doc, model || {});
  renderDecisionTile(doc, model || {});
  renderTrustTile(doc, model || {});
}

async function fetchOptional(fetchJSON, path, options = {}, failures = [], label = path) {
  try {
    return await fetchJSON(path, options);
  } catch (error) {
    failures.push({
      label,
      message: String(error && error.message ? error.message : error),
    });
    return null;
  }
}

export async function loadOperatorOverview(fetchJSON, preloaded = {}) {
  const card = typeof document !== "undefined" ? document.getElementById("operatorOverviewCard") : null;
  if (!card || typeof fetchJSON !== "function") return null;

  const hasOwn = (key) => Object.prototype.hasOwnProperty.call(preloaded || {}, key);
  const failures = normalizeFailures(hasOwn("sharedFailures") ? preloaded.sharedFailures : (window.__LAST_REFRESH_FAILURES__ || []));

  const [
    systemState,
    healthPayload,
    readinessPayload,
    executionBarrier,
    stressPayload,
    uiMetrics,
    portfolioRisk,
    riskSummary,
    pnlPayload,
    decisionsPayload,
  ] = await Promise.all([
    hasOwn("systemState") ? preloaded.systemState : fetchOptional(fetchJSON, "/api/system/state", { allowBusinessFalse: true }, failures, "/api/system/state"),
    hasOwn("health") ? preloaded.health : fetchOptional(fetchJSON, "/api/health", { allowBusinessFalse: true }, failures, "/api/health"),
    hasOwn("readiness") ? preloaded.readiness : fetchOptional(fetchJSON, "/api/readiness", { allowBusinessFalse: true }, failures, "/api/readiness"),
    hasOwn("executionBarrier") ? preloaded.executionBarrier : fetchOptional(fetchJSON, "/api/execution/barrier", { allowBusinessFalse: true }, failures, "/api/execution/barrier"),
    hasOwn("stressPayload") ? preloaded.stressPayload : fetchOptional(fetchJSON, "/api/market_stress", {}, failures, "/api/market_stress"),
    fetchOptional(fetchJSON, "/api/ui/metrics", { allowBusinessFalse: true }, failures, "/api/ui/metrics"),
    fetchOptional(fetchJSON, "/api/risk/portfolio", {}, failures, "/api/risk/portfolio"),
    fetchOptional(fetchJSON, "/api/risk/summary", {}, failures, "/api/risk/summary"),
    fetchOptional(fetchJSON, "/api/pnl", {}, [], "/api/pnl"),
    fetchOptional(fetchJSON, "/api/ui/decisions?limit=12", {}, failures, "/api/ui/decisions"),
  ]);

  if (systemState) window.__LAST_SYSTEM_STATE__ = systemState;
  if (healthPayload) window.__LAST_HEALTH__ = healthPayload;
  if (readinessPayload) window.__LAST_READINESS__ = readinessPayload;
  if (executionBarrier) window.__LAST_EXECUTION_BARRIER__ = executionBarrier;
  if (stressPayload) window.__LAST_MARKET_STRESS__ = stressPayload;

  let decisionDetailPayload = null;
  let decisionDetailError = null;
  const latestDecision = normalizeOverviewDecisionRows(decisionsPayload || {})[0] || null;
  if (latestDecision && hasDecisionLookup(latestDecision)) {
    try {
      decisionDetailPayload = await fetchJSON(buildDecisionDetailUrl(latestDecision), { allowBusinessFalse: true });
    } catch (error) {
      decisionDetailError = error;
    }
  }

  const model = buildOperatorOverviewModel({
    systemState,
    healthPayload,
    readinessPayload,
    executionBarrier,
    stressPayload,
    failures,
    decisionsPayload,
    decisionDetailPayload,
    decisionDetailError,
    uiMetrics,
    portfolioRisk,
    riskSummary,
    pnlPayload,
  });
  renderOperatorOverview(model);
  try {
    const setPanelState = typeof window !== "undefined" ? window.__setDashboardPanelState__ : null;
    if (typeof setPanelState === "function") {
      setPanelState("operatorOverviewCard", {
        state: model.status.word === "STOP"
          ? "error"
          : (model.status.word === "CAUTION" ? "stale" : "fresh"),
        reason: model.status.fallbackText,
      });
    }
  } catch {}
  return model;
}
