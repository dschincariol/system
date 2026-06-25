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

export async function fetchWithTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(new Error(`fetch_timeout:${url}`)), timeoutMs);
  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeoutId);
  }
}

export async function fetchJSON(path, options = {}) {
  const startedAt = Date.now();
  const requestOptions = { ...(options || {}) };
  const allowBusinessFalse = !!requestOptions.allowBusinessFalse;
  delete requestOptions.allowBusinessFalse;
  const headers = new Headers(requestOptions.headers || {});
  const apiToken = isSameOriginApiRequest(path) ? resolveDashboardApiToken() : "";
  if (apiToken && !headers.has("X-API-Token")) {
    headers.set("X-API-Token", apiToken);
  }
  if (requestOptions.body !== undefined && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  requestOptions.headers = headers;
  try {
    if (shouldThrottleDashboardApiRead(path, requestOptions)) {
      await waitForDashboardApiReadBudgetSlot();
    }
    const res = await fetchWithTimeout(path, { cache: "no-store", ...requestOptions });
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
      const msg = (data && (data.message || data.reason || data.reason_code || data.error)) ? (data.message || data.reason || data.reason_code || data.error) : txt;
      throw new Error(`${res.status} ${res.statusText}: ${msg}`);
    }

    if (!data || typeof data !== "object") {
      throw new Error(`invalid_json_response: ${path}`);
    }

    if (data.ok === false && !allowBusinessFalse) {
      throw new Error(String(data.error || `api_error: ${path}`));
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
