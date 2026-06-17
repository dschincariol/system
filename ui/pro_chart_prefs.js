/*
  FILE: ui/pro_chart_prefs.js

  Shared persisted preferences for dashboard and terminal pro charts.
  Keep these localStorage keys stable for existing operator browsers.
*/

export const PRO_CHART_PREF_KEYS = {
  enabled: "proCharts.enabled",
  tf: "proCharts.tf",
  type: "proCharts.type",
};

export const DASHBOARD_PRO_CHART_OVERLAY_KEYS = {
  vwap: "dashboard.proCharts.ov.vwap",
  ema: "dashboard.proCharts.ov.ema",
  trades: "dashboard.proCharts.ov.trades",
  pnl: "dashboard.proCharts.ov.pnl",
};

export const PRO_CHART_PREF_DEFAULTS = {
  enabled: true,
  tf: "1m",
  type: "candle",
};

export const DASHBOARD_PRO_CHART_OVERLAY_DEFAULTS = {
  vwap: true,
  ema: true,
  trades: true,
  pnl: true,
};

function _storage() {
  try {
    return typeof localStorage !== "undefined" ? localStorage : null;
  } catch {
    return null;
  }
}

function _lsGet(key, fallback) {
  try {
    const storage = _storage();
    if (!storage) return fallback;
    const value = storage.getItem(key);
    return value === null || value === undefined ? fallback : value;
  } catch {
    return fallback;
  }
}

function _lsSet(key, value) {
  try {
    const storage = _storage();
    if (storage) storage.setItem(key, String(value));
  } catch {}
}

function _lsBoolGet(key, fallback) {
  const value = _lsGet(key, fallback ? "1" : "0");
  return value === "1";
}

function _lsBoolSet(key, value) {
  _lsSet(key, value ? "1" : "0");
}

export function getProChartsState() {
  const enabled = _lsBoolGet(PRO_CHART_PREF_KEYS.enabled, PRO_CHART_PREF_DEFAULTS.enabled);
  const tf = String(_lsGet(PRO_CHART_PREF_KEYS.tf, PRO_CHART_PREF_DEFAULTS.tf) || PRO_CHART_PREF_DEFAULTS.tf).trim();
  const type = String(_lsGet(PRO_CHART_PREF_KEYS.type, PRO_CHART_PREF_DEFAULTS.type) || PRO_CHART_PREF_DEFAULTS.type).trim();
  return { enabled, tf, type };
}

export function setProChartsState(patch) {
  const current = getProChartsState();
  const next = { ...current, ...(patch || {}) };
  _lsBoolSet(PRO_CHART_PREF_KEYS.enabled, !!next.enabled);
  _lsSet(PRO_CHART_PREF_KEYS.tf, next.tf || PRO_CHART_PREF_DEFAULTS.tf);
  _lsSet(PRO_CHART_PREF_KEYS.type, next.type || PRO_CHART_PREF_DEFAULTS.type);
  return getProChartsState();
}

export function applyProChartsVisibility(cardId = "proChartsCard", documentRef = null) {
  const state = getProChartsState();
  const doc = documentRef || (typeof document !== "undefined" ? document : null);
  const el = doc && typeof doc.getElementById === "function" ? doc.getElementById(cardId) : null;
  if (el) {
    el.style.display = state.enabled ? "" : "none";
  }
  return state;
}

export function getDashboardProChartOverlayState() {
  return {
    vwap: _lsBoolGet(DASHBOARD_PRO_CHART_OVERLAY_KEYS.vwap, DASHBOARD_PRO_CHART_OVERLAY_DEFAULTS.vwap),
    ema: _lsBoolGet(DASHBOARD_PRO_CHART_OVERLAY_KEYS.ema, DASHBOARD_PRO_CHART_OVERLAY_DEFAULTS.ema),
    trades: _lsBoolGet(DASHBOARD_PRO_CHART_OVERLAY_KEYS.trades, DASHBOARD_PRO_CHART_OVERLAY_DEFAULTS.trades),
    pnl: _lsBoolGet(DASHBOARD_PRO_CHART_OVERLAY_KEYS.pnl, DASHBOARD_PRO_CHART_OVERLAY_DEFAULTS.pnl),
  };
}

export function setDashboardProChartOverlayState(patch) {
  const next = { ...getDashboardProChartOverlayState(), ...(patch || {}) };
  _lsBoolSet(DASHBOARD_PRO_CHART_OVERLAY_KEYS.vwap, !!next.vwap);
  _lsBoolSet(DASHBOARD_PRO_CHART_OVERLAY_KEYS.ema, !!next.ema);
  _lsBoolSet(DASHBOARD_PRO_CHART_OVERLAY_KEYS.trades, !!next.trades);
  _lsBoolSet(DASHBOARD_PRO_CHART_OVERLAY_KEYS.pnl, !!next.pnl);
  return getDashboardProChartOverlayState();
}

export function applyDashboardProChartOverlayInputs(documentRef = null) {
  const doc = documentRef || (typeof document !== "undefined" ? document : null);
  if (!doc || typeof doc.getElementById !== "function") return getDashboardProChartOverlayState();

  const overlays = getDashboardProChartOverlayState();
  const map = {
    proChartsOvVWAP: !!overlays.vwap,
    proChartsOvEMA: !!overlays.ema,
    proChartsOvTrades: !!overlays.trades,
    proChartsOvPnL: !!overlays.pnl,
  };

  for (const [id, checked] of Object.entries(map)) {
    const el = doc.getElementById(id);
    if (el) el.checked = checked;
  }

  return overlays;
}
