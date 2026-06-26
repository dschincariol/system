"use strict";

import {
  recordConnectionFailure,
  recordConnectionSuccess,
} from "./panel_state.js";

export const FETCH_TIMEOUT_MS = 15000;
export const DASHBOARD_API_TOKEN_STORAGE_KEY = "dashboard_api_token";
export const DASHBOARD_API_READ_BURST_LIMIT = 48;
export const DASHBOARD_API_READ_WINDOW_MS = 60000;

let dashboardApiTokenResolved = false;
let dashboardApiToken = "";
let dashboardApiReadStarts = [];
let dashboardApiReadQueue = Promise.resolve();
let dashboardApiReadThrottleConfig = {
  burstLimit: DASHBOARD_API_READ_BURST_LIMIT,
  windowMs: DASHBOARD_API_READ_WINDOW_MS,
};

function browserLocation() {
  if (typeof window !== "undefined" && window.location) return window.location;
  if (typeof globalThis !== "undefined" && globalThis.location) return globalThis.location;
  return null;
}

function locationHref() {
  const loc = browserLocation();
  return String((loc && loc.href) || "http://127.0.0.1/");
}

function locationSearch() {
  const loc = browserLocation();
  if (loc && typeof loc.search === "string") return loc.search;
  try {
    return new URL(locationHref()).search;
  } catch {
    return "";
  }
}

function browserLocalStorage() {
  try {
    if (typeof window !== "undefined" && window.localStorage) return window.localStorage;
    if (typeof globalThis !== "undefined" && globalThis.localStorage) return globalThis.localStorage;
  } catch {
    return null;
  }
  return null;
}

function truthyParam(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

export function clearDashboardApiToken() {
  dashboardApiTokenResolved = true;
  dashboardApiToken = "";
  const storage = browserLocalStorage();
  try {
    storage?.removeItem(DASHBOARD_API_TOKEN_STORAGE_KEY);
  } catch {
    // localStorage may be disabled; the in-memory token cache is still cleared.
  }
}

export function resolveDashboardApiToken({ force = false } = {}) {
  if (dashboardApiTokenResolved && !force) return dashboardApiToken;

  const params = new URLSearchParams(locationSearch());
  if (truthyParam(params.get("clear_token") || params.get("clearToken"))) {
    clearDashboardApiToken();
    return "";
  }

  const storage = browserLocalStorage();
  const urlToken = params.get("token");
  let token = "";
  if (urlToken !== null && String(urlToken).trim()) {
    token = String(urlToken).trim();
    try {
      storage?.setItem(DASHBOARD_API_TOKEN_STORAGE_KEY, token);
    } catch {
      // Browser storage is optional; URL-supplied token still works for this page load.
    }
  } else {
    try {
      token = String(storage?.getItem(DASHBOARD_API_TOKEN_STORAGE_KEY) || "").trim();
    } catch {
      token = "";
    }
  }

  dashboardApiTokenResolved = true;
  dashboardApiToken = token;
  return dashboardApiToken;
}

export function isSameOriginApiRequest(path) {
  try {
    const base = new URL(locationHref());
    const requestUrl = new URL(String(path || ""), base);
    return requestUrl.origin === base.origin && /^\/api(?:\/|$)/.test(requestUrl.pathname);
  } catch {
    return false;
  }
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

function requestMethod(requestOptions) {
  return String((requestOptions && requestOptions.method) || "GET").trim().toUpperCase() || "GET";
}

function appendTokenQueryParam(path, token) {
  const value = String(token || "").trim();
  if (!value) return path;
  try {
    const base = new URL(locationHref());
    const requestUrl = new URL(String(path || ""), base);
    if (requestUrl.origin !== base.origin || !/^\/api(?:\/|$)/.test(requestUrl.pathname)) {
      return path;
    }
    if (!requestUrl.searchParams.has("token")) {
      requestUrl.searchParams.set("token", value);
    }
    if (String(path || "").startsWith("http://") || String(path || "").startsWith("https://")) {
      return requestUrl.href;
    }
    return `${requestUrl.pathname}${requestUrl.search}${requestUrl.hash}`;
  } catch {
    return path;
  }
}

function abortReason(signal, fallback) {
  try {
    return signal && "reason" in signal && signal.reason ? signal.reason : fallback;
  } catch {
    return fallback;
  }
}

function withTimeoutSignal(url, signal, timeoutMs) {
  const controller = new AbortController();
  const timeout = Math.max(1, Number(timeoutMs) || FETCH_TIMEOUT_MS);
  const timeoutId = setTimeout(() => controller.abort(new Error(`fetch_timeout:${url}`)), timeout);
  const onAbort = () => controller.abort(abortReason(signal, new Error(`fetch_aborted:${url}`)));
  if (signal) {
    if (signal.aborted) {
      onAbort();
    } else {
      try {
        signal.addEventListener("abort", onAbort, { once: true });
      } catch {}
    }
  }
  return {
    signal: controller.signal,
    cleanup() {
      clearTimeout(timeoutId);
      if (signal) {
        try {
          signal.removeEventListener("abort", onAbort);
        } catch {}
      }
    },
  };
}

function prepareDashboardApiRequest(path, options = {}) {
  const requestOptions = { ...(options || {}) };
  const headers = new Headers(requestOptions.headers || {});
  const apiToken = isSameOriginApiRequest(path) ? resolveDashboardApiToken() : "";
  if (apiToken && !headers.has("X-API-Token")) {
    headers.set("X-API-Token", apiToken);
  }
  if (requestOptions.body !== undefined && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  requestOptions.headers = headers;
  return requestOptions;
}

export function businessDegradedReason(data) {
  if (!data || typeof data !== "object") return "";
  const direct = data.message || data.reason || data.reason_code;
  if (direct) return String(direct).trim();
  const lists = [data.reason_codes, data.reasons];
  for (const values of lists) {
    if (Array.isArray(values)) {
      const first = values.map((item) => String(item || "").trim()).find(Boolean);
      if (first) return first;
    } else if (values) {
      const value = String(values).trim();
      if (value) return value;
    }
  }
  return "";
}

function apiPayloadMessage(data) {
  if (!data || typeof data !== "object") return "";
  return String(data.message || data.reason || data.reason_code || data.error || "").trim();
}

export function isBusinessDegradedPayload(data) {
  return !!(data && typeof data === "object" && data.ok === false && businessDegradedReason(data));
}

function shouldThrottleDashboardApiRead(path, requestOptions) {
  return requestMethod(requestOptions) === "GET" && isSameOriginApiRequest(path);
}

function pruneDashboardApiReadStarts(now, windowMs) {
  dashboardApiReadStarts = dashboardApiReadStarts.filter((startedAt) => now - startedAt < windowMs);
}

async function waitForDashboardApiReadBudgetSlot() {
  const previous = dashboardApiReadQueue.catch(() => undefined);
  const next = previous.then(async () => {
    const burstLimit = Math.max(1, Number(dashboardApiReadThrottleConfig.burstLimit) || DASHBOARD_API_READ_BURST_LIMIT);
    const windowMs = Math.max(1, Number(dashboardApiReadThrottleConfig.windowMs) || DASHBOARD_API_READ_WINDOW_MS);
    for (;;) {
      const now = Date.now();
      pruneDashboardApiReadStarts(now, windowMs);
      if (dashboardApiReadStarts.length < burstLimit) {
        dashboardApiReadStarts.push(now);
        return;
      }
      const oldest = dashboardApiReadStarts[0] || now;
      await delay(Math.max(1, windowMs - (now - oldest) + 1));
    }
  });
  dashboardApiReadQueue = next.catch(() => undefined);
  return next;
}

export function resetDashboardApiReadThrottleForTests() {
  dashboardApiReadStarts = [];
  dashboardApiReadQueue = Promise.resolve();
  dashboardApiReadThrottleConfig = {
    burstLimit: DASHBOARD_API_READ_BURST_LIMIT,
    windowMs: DASHBOARD_API_READ_WINDOW_MS,
  };
}

export function configureDashboardApiReadThrottleForTests({ burstLimit, windowMs } = {}) {
  dashboardApiReadThrottleConfig = {
    burstLimit: Math.max(1, Number(burstLimit) || DASHBOARD_API_READ_BURST_LIMIT),
    windowMs: Math.max(1, Number(windowMs) || DASHBOARD_API_READ_WINDOW_MS),
  };
  dashboardApiReadStarts = [];
  dashboardApiReadQueue = Promise.resolve();
}

export async function apiFetch(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const requestOptions = prepareDashboardApiRequest(url, options);
  if (shouldThrottleDashboardApiRead(url, requestOptions)) {
    await waitForDashboardApiReadBudgetSlot();
  }
  const callerSignal = requestOptions.signal;
  const timed = withTimeoutSignal(url, callerSignal, timeoutMs);
  requestOptions.signal = timed.signal;
  try {
    return await fetch(url, {
      cache: "no-store",
      ...options,
      ...requestOptions,
    });
  } finally {
    timed.cleanup();
  }
}

export async function fetchWithTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  return apiFetch(url, options, timeoutMs);
}

export function dashboardApiEventSourceUrl(url) {
  if (!isSameOriginApiRequest(url)) return url;
  return appendTokenQueryParam(url, resolveDashboardApiToken());
}

export function apiEventSource(url, options = {}) {
  return new EventSource(dashboardApiEventSourceUrl(url), options);
}

export async function fetchJSON(path, options = {}) {
  const startedAt = Date.now();
  const requestOptions = { ...(options || {}) };
  const allowBusinessFalse = !!requestOptions.allowBusinessFalse;
  delete requestOptions.allowBusinessFalse;
  try {
    const res = await apiFetch(path, requestOptions);
    const txt = await res.text();

    let data = null;
    try {
      data = txt ? JSON.parse(txt) : null;
    } catch {
      console.warn("JSON parse error", path, typeof txt === "string" ? txt.slice(0, 200) : "");
      data = null;
    }

    const allowedBusinessRefusal = allowBusinessFalse
      && res.status >= 400
      && res.status < 500
      && data
      && typeof data === "object"
      && data.ok === false;
    if (!res.ok && !allowedBusinessRefusal) {
      const msg = apiPayloadMessage(data) || txt;
      throw new Error(`${res.status} ${res.statusText}: ${msg}`);
    }

    if (!data || typeof data !== "object") {
      throw new Error(`invalid_json_response: ${path}`);
    }

    const businessDegraded = res.ok && isBusinessDegradedPayload(data);
    if (data.ok === false && !allowBusinessFalse && !businessDegraded) {
      throw new Error(String(apiPayloadMessage(data) || `api_error: ${path}`));
    }

    recordConnectionSuccess(path, {
      startedAt,
      endedAt: Date.now(),
      status: res.status,
    });
    return data;
  } catch (error) {
    recordConnectionFailure(path, {
      startedAt,
      endedAt: Date.now(),
      error,
    });
    throw error;
  }
}
