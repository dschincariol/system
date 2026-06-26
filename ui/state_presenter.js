"use strict";

const EMPTY_COPY = Object.freeze({
  configured: Object.freeze({
    title: "Nothing configured",
    message: "No configuration records were returned for this view.",
    why: "There is nothing for the operator to inspect or recover yet.",
    action: "Add or enable the required configuration, then refresh.",
  }),
  active: Object.freeze({
    title: "Nothing active",
    message: "No active runtime records were returned for this view.",
    why: "The surface is reachable, but there is no live activity to show.",
    action: "Start the relevant job or wait for the next runtime update.",
  }),
  filtered: Object.freeze({
    title: "Filtered to none",
    message: "The current filter hides all available records.",
    why: "Data may exist, but it is not visible with the current filter.",
    action: "Clear or broaden the filter.",
  }),
  data: Object.freeze({
    title: "No data returned",
    message: "The backend returned an empty result for this view.",
    why: "The request completed, but there is no payload to render.",
    action: "Refresh after the next runtime update.",
  }),
});

const STATE_LABELS = Object.freeze({
  loading: "Loading",
  empty: "Empty",
  degraded: "Degraded",
  error: "Error",
  ok: "Ready",
});

const STATE_ICONS = Object.freeze({
  loading: "...",
  empty: "-",
  degraded: "!",
  error: "X",
  ok: "OK",
});

const SECRET_WORD_RE = /\b[A-Z0-9_]*(?:API[_-]?KEY|TOKEN|SECRET|PASSWORD|PASSWD|PWD|CREDENTIAL|MASTER[_-]?KEY)[A-Z0-9_]*\b/gi;
const ROUTE_RE = /(?:https?:\/\/[^\s)]+|\/api\/[^\s)]+|\/operator\/api\/[^\s)]+|\/ws\/[^\s)]+)/gi;
const STACK_LINE_RE = /\b(?:Traceback|at\s+\S+\s+\(|File\s+"[^"]+"|line\s+\d+|stack|Error:\s*)/i;

export function escapeStateHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeState(state) {
  const token = String(state || "").trim().toLowerCase();
  if (token === "warn" || token === "stale" || token === "partial") return "degraded";
  if (token === "bad" || token === "crit" || token === "failed" || token === "fail") return "error";
  if (token === "ready" || token === "fresh" || token === "success") return "ok";
  if (Object.prototype.hasOwnProperty.call(STATE_LABELS, token)) return token;
  return "empty";
}

function pickFirstText(...values) {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function objectMessage(value) {
  if (!value || typeof value !== "object") return "";
  return pickFirstText(
    value.operator_message,
    value.message,
    value.reason,
    value.summary,
    value.detail,
    value.error,
    value.reason_code,
    value.status,
    value.state
  );
}

export function rawErrorText(value, fallback = "request failed") {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "string") return value || fallback;
  if (value instanceof Error) return value.message || fallback;
  if (typeof value === "object") return objectMessage(value) || fallback;
  return String(value || fallback);
}

export function sanitizePrimaryText(value, {
  fallback = "No operator-readable detail was returned.",
  maxLength = 180,
} = {}) {
  let text = rawErrorText(value, fallback);
  text = String(text || "").replace(/\r/g, "\n");

  const firstUsefulLine = text
    .split("\n")
    .map((line) => line.trim())
    .find((line) => line && !STACK_LINE_RE.test(line));

  text = firstUsefulLine || fallback;
  if (/^\s*[\[{]/.test(text)) text = fallback;
  text = text
    .replace(ROUTE_RE, "backend route")
    .replace(SECRET_WORD_RE, "credential")
    .replace(/\s+/g, " ")
    .trim();
  if (!text) text = fallback;
  if (text.length > maxLength) text = `${text.slice(0, Math.max(1, maxLength - 3)).trim()}...`;
  return text;
}

export function summarizeOperatorError(value, fallback = "request failed") {
  return sanitizePrimaryText(value, { fallback });
}

export function emptyStateCopy(kind = "active", overrides = {}) {
  const key = String(kind || "active").trim().toLowerCase();
  return {
    ...(EMPTY_COPY[key] || EMPTY_COPY.active),
    ...Object.fromEntries(
      Object.entries(overrides || {}).filter(([, value]) => value !== undefined && value !== null && String(value).trim())
    ),
  };
}

export function formatTechnicalDetails(value) {
  if (value === undefined || value === null || value === "") return "(no technical details)";
  if (value instanceof Error) {
    return JSON.stringify({
      name: value.name || "Error",
      message: value.message || "",
      stack: value.stack || "",
    }, null, 2);
  }
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function technicalDetailsHtml(details, {
  summary = "Technical details",
  open = false,
  className = "",
} = {}) {
  return `
    <details class="uiStateDetails ${escapeStateHtml(className)}" ${open ? "open" : ""}>
      <summary>${escapeStateHtml(summary || "Technical details")}</summary>
      <pre class="uiStateRaw">${escapeStateHtml(formatTechnicalDetails(details))}</pre>
    </details>
  `;
}

export function stateBlockHtml(options = {}) {
  const state = normalizeState(options.state);
  const emptyCopy = state === "empty" ? emptyStateCopy(options.emptyKind, options) : {};
  const title = sanitizePrimaryText(
    pickFirstText(options.title, emptyCopy.title, STATE_LABELS[state]),
    { fallback: STATE_LABELS[state], maxLength: 90 }
  );
  const message = sanitizePrimaryText(
    pickFirstText(options.message, emptyCopy.message),
    { fallback: state === "loading" ? "Loading the latest runtime snapshot." : "No operator-readable detail was returned." }
  );
  const why = sanitizePrimaryText(
    pickFirstText(options.why, emptyCopy.why),
    { fallback: "", maxLength: 220 }
  );
  const action = sanitizePrimaryText(
    pickFirstText(options.action, options.nextStep, emptyCopy.action),
    { fallback: "", maxLength: 220 }
  );
  const role = state === "error" ? "alert" : "status";
  const detailHtml = Object.prototype.hasOwnProperty.call(options, "details")
    ? technicalDetailsHtml(options.details, {
        summary: options.detailsLabel || "Technical details",
        open: !!options.detailsOpen,
      })
    : "";
  const bodyHtml = typeof options.bodyHtml === "string" ? options.bodyHtml : "";
  const classes = [
    "uiState",
    `uiState--${state}`,
    options.compact ? "uiState--compact" : "",
    options.className || "",
  ].filter(Boolean).join(" ");

  return `
    <div class="${escapeStateHtml(classes)}" role="${role}" aria-live="polite">
      <div class="uiStateIcon" aria-hidden="true">${escapeStateHtml(STATE_ICONS[state] || "-")}</div>
      <div class="uiStateBody">
        <div class="uiStateTitle">${escapeStateHtml(title)}</div>
        <div class="uiStateMessage">${escapeStateHtml(message)}</div>
        ${why ? `<div class="uiStateMeta"><strong>Why it matters:</strong> ${escapeStateHtml(why)}</div>` : ""}
        ${action ? `<div class="uiStateMeta"><strong>Next step:</strong> ${escapeStateHtml(action)}</div>` : ""}
        ${bodyHtml}
        ${detailHtml}
      </div>
    </div>
  `;
}

export function renderState(target, options = {}) {
  const el = typeof target === "string" ? document.getElementById(target) : target;
  if (!el) return null;
  el.innerHTML = stateBlockHtml(options);
  return el;
}

export function renderTechnicalDetails(target, details, options = {}) {
  const el = typeof target === "string" ? document.getElementById(target) : target;
  if (!el) return null;
  const label = options.summary || options.detailsLabel || "Technical details";
  if (String(el.tagName || "").toLowerCase() === "pre") {
    const wrapper = typeof el.closest === "function" ? el.closest("details") : null;
    if (wrapper) {
      const summary = wrapper.querySelector("summary");
      if (summary) summary.textContent = label;
      wrapper.classList.add("uiStateDetails");
    }
    el.classList.add("uiStateRaw");
    el.textContent = formatTechnicalDetails(details);
    return el;
  }
  el.innerHTML = technicalDetailsHtml(details, options);
  return el;
}

export function loadingStateHtml(message = "Loading the latest runtime snapshot.", options = {}) {
  return stateBlockHtml({ state: "loading", title: "Loading", message, ...options });
}

export function emptyStateHtml(kind = "active", options = {}) {
  return stateBlockHtml({ state: "empty", emptyKind: kind, ...options });
}
