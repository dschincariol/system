"use strict";

export const FETCH_TIMEOUT_MS = 15000;

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
  const requestOptions = { ...(options || {}) };
  const allowBusinessFalse = !!requestOptions.allowBusinessFalse;
  delete requestOptions.allowBusinessFalse;
  const headers = new Headers(requestOptions.headers || {});
  if (requestOptions.body !== undefined && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  requestOptions.headers = headers;
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

  return data;
}
