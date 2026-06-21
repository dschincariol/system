"use strict";

/*
  Shared risk-headroom thresholds for bullet-bar visual bands and status labels.
  The cap boundary is intentionally inclusive for Watch: a ratio exactly at cap
  is at capacity, while only ratios above cap are Over.
*/

export const RISK_HEADROOM_TRACK_MAX_RATIO = 1.25;
export const RISK_HEADROOM_WATCH_START_RATIO = 0.85;
export const RISK_HEADROOM_CAP_RATIO = 1.0;

export const RISK_HEADROOM_THRESHOLDS = Object.freeze({
  okEnd: RISK_HEADROOM_WATCH_START_RATIO,
  watchStart: RISK_HEADROOM_WATCH_START_RATIO,
  cap: RISK_HEADROOM_CAP_RATIO,
  overStart: RISK_HEADROOM_CAP_RATIO,
  trackMax: RISK_HEADROOM_TRACK_MAX_RATIO,
});

export const RISK_HEADROOM_BANDS = Object.freeze([
  Object.freeze({ key: "ok", label: "OK", start: 0.0, end: RISK_HEADROOM_WATCH_START_RATIO }),
  Object.freeze({ key: "watch", label: "Watch", start: RISK_HEADROOM_WATCH_START_RATIO, end: RISK_HEADROOM_CAP_RATIO }),
  Object.freeze({ key: "over", label: "Over", start: RISK_HEADROOM_CAP_RATIO, end: RISK_HEADROOM_TRACK_MAX_RATIO }),
]);

export const DEFAULT_RISK_CAPS = Object.freeze({
  gross: 1.0,
  net: 0.6,
  drawdown: 0.06,
  vol: 0.02,
});

export function classifyRiskHeadroomRatio(ratio, blocked = false) {
  if (blocked) {
    return Object.freeze({ tone: "blocked", statusWord: "Blocked" });
  }
  const n = Number(ratio);
  if (!Number.isFinite(n)) {
    return Object.freeze({ tone: "unavailable", statusWord: "Unavailable" });
  }
  if (n > RISK_HEADROOM_CAP_RATIO) {
    return Object.freeze({ tone: "over", statusWord: "Over cap" });
  }
  if (n >= RISK_HEADROOM_WATCH_START_RATIO) {
    return Object.freeze({ tone: "watch", statusWord: "Watch" });
  }
  return Object.freeze({ tone: "ok", statusWord: "OK" });
}
