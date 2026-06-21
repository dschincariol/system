/*
  FILE: ui/market_stress_thresholds.js

  Shared market-stress threshold semantics for badges, summaries, charts, and
  metric explanations.
*/

export const MARKET_STRESS_WARNING_THRESHOLD = 0.55;
export const MARKET_STRESS_CRITICAL_THRESHOLD = 0.75;

export const MARKET_STRESS_THRESHOLDS = Object.freeze({
  warning: MARKET_STRESS_WARNING_THRESHOLD,
  critical: MARKET_STRESS_CRITICAL_THRESHOLD,
});

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function finiteThreshold(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

export function normalizeMarketStressThresholds(source = {}) {
  const root = asObject(source);
  const candidate = asObject(root.thresholds || root.market_stress_thresholds || root);
  let warning = finiteThreshold(
    candidate.warning ?? candidate.warn ?? candidate.warning_threshold,
    MARKET_STRESS_WARNING_THRESHOLD,
  );
  let critical = finiteThreshold(
    candidate.critical ?? candidate.crit ?? candidate.critical_threshold,
    MARKET_STRESS_CRITICAL_THRESHOLD,
  );

  if (critical <= warning) {
    warning = MARKET_STRESS_WARNING_THRESHOLD;
    critical = MARKET_STRESS_CRITICAL_THRESHOLD;
  }

  return Object.freeze({ warning, critical });
}

export function classifyMarketStressScore(value, thresholds = MARKET_STRESS_THRESHOLDS) {
  const score = Number(value);
  const t = normalizeMarketStressThresholds(thresholds);
  if (!Number.isFinite(score)) {
    return Object.freeze({
      state: "unknown",
      tone: "unavailable",
      pillClass: "pill dim",
      label: "unknown",
      score: null,
      thresholds: t,
    });
  }
  if (score >= t.critical) {
    return Object.freeze({
      state: "critical",
      tone: "crit",
      pillClass: "pill bad",
      label: "high stress",
      score,
      thresholds: t,
    });
  }
  if (score >= t.warning) {
    return Object.freeze({
      state: "warning",
      tone: "warn",
      pillClass: "pill warn",
      label: "elevated stress",
      score,
      thresholds: t,
    });
  }
  return Object.freeze({
    state: "normal",
    tone: "ok",
    pillClass: "pill ok",
    label: "normal",
    score,
    thresholds: t,
  });
}

export function marketStressThresholdRangeText(thresholds = MARKET_STRESS_THRESHOLDS) {
  const t = normalizeMarketStressThresholds(thresholds);
  return Object.freeze({
    normal: `< ${t.warning.toFixed(2)}`,
    warning: `${t.warning.toFixed(2)} to < ${t.critical.toFixed(2)}`,
    critical: `>= ${t.critical.toFixed(2)}`,
  });
}
