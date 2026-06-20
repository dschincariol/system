// services/operator_ai/agent.js
//
// Bounded operator-AI analysis layer.
//
// Responsibilities:
// - gather repair-oriented runtime evidence from the local operator API
// - ask the configured LLM for strict JSON only
// - normalize the response into a narrow diagnosis contract
// - remain diagnostics-only with no autonomous action execution
//
// This module does not own trading logic and does not have direct broker authority.

"use strict";

const fetchImpl = global.fetch;
if (typeof fetchImpl !== "function") {
  throw new Error("global_fetch_unavailable_requires_node_18_plus");
}

const BASE = "http://127.0.0.1:4001";

// -------- INTERNAL HELPERS --------

async function get(path) {
  const r = await fetchImpl(BASE + path);
  const text = await r.text();

  try {
    return JSON.parse(text);
  } catch (e) {
    return {
      ok: false,
      error: "invalid_json_response",
      path,
      status: r.status,
      body_preview: text.slice(0, 500)
    };
  }
}

// -------- CONTEXT BUILDER --------

async function buildContext() {
  const [
    status,
    health,
    logs,
    supportSnapshot,
    snapshot,
    telemetry,
    watchdogs,
    barrier
  ] = await Promise.all([
    get("/api/operator/service_status"),
    get("/api/operator/health"),
    get("/api/operator/runtime_logs?lines=80"),
    get("/api/operator/support_snapshot?mode=quick"),
    get("/api/operator/snapshot?mode=quick"),
    get("/api/operator/provider_telemetry"),
    get("/api/operator/runtime_watchdogs"),
    get("/api/execution/barrier")
  ]);

  return {
    status,
    health,
    logs,
    support_snapshot: supportSnapshot,
    snapshot,
    telemetry,
    watchdogs,
    barrier,
    allowed_actions: ALLOWED_ACTIONS
  };
}

// Diagnostics-only mode: the LLM can describe issues but cannot trigger any
// runtime actions or operator endpoints.
const ALLOWED_ACTIONS = Object.freeze([]);

function sanitizeSummary(value) {
  return String(value == null ? "" : value).trim().slice(0, 2000);
}

function sanitizeShort(value, max = 1000) {
  return String(value == null ? "" : value).trim().slice(0, max);
}

function normalizeDecision(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return {
      ok: false,
      error: "invalid_llm_response_shape",
      decision: {
        summary: "",
        root_cause: "",
        failing_component: "",
        file: "",
        patch: "",
        action: null
      }
    };
  }

  const summary = sanitizeSummary(raw.summary);
  const root_cause = sanitizeShort(raw.root_cause, 1500);
  const failing_component = sanitizeShort(raw.failing_component, 500);
  const file = sanitizeShort(raw.file, 500);
  const patch = sanitizeShort(raw.patch, 2000);

  return {
    ok: true,
    error: null,
    decision: {
      summary,
      root_cause,
      failing_component,
      file,
      patch,
      action: null
    }
  };
}

// -------- MAIN RUNNER --------

let _lastCtxHash = null;
let _lastRunTs = 0;

function hashCtx(obj) {
  try {
    return require("crypto")
      .createHash("sha1")
      .update(JSON.stringify(obj).slice(0, 10000))
      .digest("hex");
  } catch {
    return null;
  }
}

async function runAgent(llm) {
  const ctx = await buildContext();

const shouldRun =
  ctx?.health?.ok === false ||
  ctx?.status?.engine?.status !== "RUNNING" ||
  (ctx?.status?.engine?.restartAttempts || 0) > 0;

if (!shouldRun) {
  return { skipped: true, reason: "no_failure_detected" };
}

  const now = Date.now();
  const hash = hashCtx(ctx);

  if (hash && hash === _lastCtxHash && now - _lastRunTs < 15000) {
    return {
      skipped: true,
      reason: "duplicate_context"
    };
  }

  _lastCtxHash = hash;
  _lastRunTs = now;

const prompt = `
SYSTEM SNAPSHOT:
${JSON.stringify(ctx).slice(0, 15000)}

INSTRUCTIONS:
- Return ONLY valid JSON
- No text, no explanation outside JSON
- Identify the most likely exact failing file, module, endpoint, or subsystem when possible
- Prefer concrete root cause over generic advice
- If a fix is obvious, provide a short exact patch recommendation
- If uncertain, say so explicitly

FORMAT:
{
  "summary": "string",
  "root_cause": "string",
  "failing_component": "string",
  "file": "string",
  "patch": "string",
  "action": null
}

RULES:
- action must always be null
- NEVER invent actions
- NEVER suggest trading actions
- file must be a single string path or empty string
- patch must be a short exact recommendation or empty string
`;

let raw;

try {
  raw = await llm(prompt);

  if (typeof raw === "string") {
    try {
      raw = JSON.parse(raw);
    } catch {
      raw = await llm(prompt + "\nRETURN STRICT JSON ONLY.");
      if (typeof raw === "string") {
        raw = JSON.parse(raw);
      }
    }
  }

} catch (e) {
  return {
    analysis: {
      error: "llm_failure",
      message: String(e)
    },
    action: null,
    executed: null
  };
}

// -------- USE SAFE NORMALIZER --------

const normalized = normalizeDecision(raw);

if (!normalized.ok) {
  return {
    analysis: {
      ...normalized.decision,
      error: normalized.error
    },
    action: null,
    executed: null
  };
}

const fs = require("fs");
const path = require("path");

const decision = normalized.decision;

function operatorAiLogPath() {
  const configured = String(process.env.AI_OPERATOR_LOG_PATH || process.env.OPERATOR_AI_LOG_PATH || "").trim();
  if (configured) return path.resolve(configured);
  const configuredLogDir = String(process.env.TRADING_LOGS || process.env.LOG_DIR || "").trim();
  const logDir = configuredLogDir ? path.resolve(configuredLogDir) : path.join(process.cwd(), "var", "log");
  return path.join(logDir, "ai_operator_log.jsonl");
}

try {
  const logPath = operatorAiLogPath();
  fs.mkdirSync(path.dirname(logPath), { recursive: true });
  fs.appendFileSync(
    logPath,
    JSON.stringify({
      ts: new Date().toISOString(),
      decision
    }) + "\n"
  );
} catch {}

return {
  analysis: decision,
  action: null,
  executed: null
};
}

module.exports = { runAgent };
