// FILE: boot/operator_server.js

// boot/operator_server.js
// Production Operator Control Center (Non-technical UI)
// - Guided start (Safe / Shadow / Live)
// - Preflight checks (python, port, db writable, entry exists)
// - Readiness + health polling
// - Start/Stop/Restart + Emergency Stop
// - Log tail + snapshot export
// - AutoFix/Repair (pip install, DB touch, quick self-heal)
// - Institutional Check (health + telemetry changes)
// This service is the local operator control plane. It proxies dashboard and
// runtime information, owns local launcher/process management, and exposes
// guided recovery controls without duplicating core trading logic.

const express = require("express");
const path = require("path");
const fs = require("fs");
const os = require("os");

function logAgentAction(entry) {
  try {
    const p = path.join(__dirname, "../var/log/agent_actions.jsonl");
    fs.mkdirSync(path.dirname(p), { recursive: true });
    fs.appendFileSync(p, JSON.stringify(entry) + "\n");
  } catch {}
}

const crypto = require("crypto");
const http = require("http");
const https = require("https");
const net = require("net");
const { spawn, spawnSync } = require("child_process");
const { URL: NodeURL } = require("url");

const app = express();
const WebSocket = require("ws");
let _wsServer = null;
let _wsHeartbeatTimer = null;
let _watchdogTimer = null;
let _httpServer = null;
let _operatorShutdownPromise = null;
let _restartInFlight = false;

let _lastAgentActionTs = 0;
let _lastAgentActionType = null;
let _lastAppliedPatchMeta = null;

function agentCooldownOk(action = null) {
  const now = Date.now();

  // Self-heal/agent actions are deliberately rate-limited here so the operator
  // cannot thrash the repo or runtime with repeated automated interventions.
  if (now - _lastAgentActionTs < 30000) return false;

  if (action && action === _lastAgentActionType) return false;

  _lastAgentActionTs = now;
  if (action) _lastAgentActionType = action;

  return true;
}

// --------------------------------------------------
// SYSTEM STATE
// --------------------------------------------------

let ENGINE_PROCESS = null;

const SYSTEM_STATE = {
  state: "STOPPED",
  started_at: null,
  engine_pid: null
};

function clampNumber(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function jsonOk(res, payload = {}, statusCode = 200) {
  return jsonState(res, {
    ok: true,
    error: null,
    ...(payload && typeof payload === "object" ? payload : { data: payload })
  }, statusCode);
}

function jsonFail(res, error, statusCode = 500, extra = {}) {
  if (statusCode === 429) {
    const retryAfter = Number(extra && extra.retry_after_s);
    if (Number.isFinite(retryAfter) && retryAfter > 0) {
      res.setHeader("Retry-After", String(Math.max(1, Math.ceil(retryAfter))));
    }
  }
  return jsonState(res, {
    ok: false,
    error: String(error || "request_failed"),
    ...(extra && typeof extra === "object" ? extra : {})
  }, statusCode);
}

function jsonState(res, payload = {}, statusCode = 200) {
  const body = (payload && typeof payload === "object")
    ? { ...payload }
    : { data: payload };
  const meta = (body.meta && typeof body.meta === "object") ? { ...body.meta } : {};
  meta.status = Number(statusCode || 200);
  body.meta = meta;
  if (!Object.prototype.hasOwnProperty.call(body, "ok")) {
    body.ok = Number(statusCode || 200) < 400;
  }
  if (!Object.prototype.hasOwnProperty.call(body, "error")) {
    body.error = body.ok ? null : "request_failed";
  }
  return res.status(Number(statusCode || 200)).json(body);
}

function operatorErrorStatus(error, fallback = 500) {
  const code = String(error || "").trim().toLowerCase();
  if (!code) return fallback;
  if (code.includes("timeout")) return 504;
  if (code === "unauthorized" || code.endsWith("_unauthorized")) return 401;
  if (code.includes("forbidden")) return 403;
  if (code.includes("not_found") || code.includes("not_registered")) return 404;
  if (code.startsWith("deprecated") || code.startsWith("gone")) return 410;
  if (code.includes("cooldown") || code.includes("rate_limit") || code.includes("too_many_requests")) return 429;
  if (
    code.startsWith("missing_") ||
    code.startsWith("invalid_") ||
    code.startsWith("unsupported_") ||
    code.endsWith("_required") ||
    code.includes("validation")
  ) {
    return 400;
  }
  if (code.includes("unreachable") || code.includes("unavailable") || code.includes("bind_timeout")) return 503;
  return fallback;
}

function looksLikeOperatorStatePayload(payload) {
  if (!payload || typeof payload !== "object") return false;
  const hintKeys = [
    "status",
    "state",
    "mode",
    "execution_mode",
    "execution_allowed",
    "health",
    "body",
    "services",
    "readiness",
    "issues",
    "checks",
    "timestamps",
    "restartAttempts",
    "restartBlocked",
    "fatal",
    "healthy"
  ];
  return hintKeys.filter((key) => Object.prototype.hasOwnProperty.call(payload, key)).length >= 2;
}

function payloadStatusCode(payload, defaultStatus = 200) {
  if (!payload || typeof payload !== "object") return Number(defaultStatus || 200);
  const explicit = Number(
    payload?.meta?.status ??
    payload?.statusCode ??
    payload?.httpStatus ??
    NaN
  );
  if (Number.isFinite(explicit) && explicit >= 100 && explicit <= 599) {
    return explicit;
  }
  if (payload.ok === false && !looksLikeOperatorStatePayload(payload)) {
    return operatorErrorStatus(
      payload.error || payload.code || payload.reason || payload.root_cause_code,
      Number(defaultStatus || 500)
    );
  }
  return Number(defaultStatus || 200);
}

function sendOperatorPayload(res, payload, defaultStatus = 200) {
  return jsonState(res, payload, payloadStatusCode(payload, defaultStatus));
}

const _operatorActionGuards = new Map();

function beginOperatorAction(actionKey, cooldownMs = 0) {
  const now = Date.now();
  const key = String(actionKey || "").trim() || "operator_action";
  const state = _operatorActionGuards.get(key) || {
    inFlight: false,
    lastCompletedAtMs: 0
  };

  if (state.inFlight) {
    return {
      ok: false,
      statusCode: 409,
      error: `${key}_in_flight`
    };
  }

  const waitMs = Math.max(0, Number(cooldownMs || 0));
  if (waitMs > 0 && state.lastCompletedAtMs > 0 && (now - state.lastCompletedAtMs) < waitMs) {
    const remainingMs = waitMs - (now - state.lastCompletedAtMs);
    return {
      ok: false,
      statusCode: 429,
      error: `${key}_cooldown_active`,
      retry_after_s: Math.max(1, Math.ceil(remainingMs / 1000))
    };
  }

  state.inFlight = true;
  _operatorActionGuards.set(key, state);
  return { ok: true, key };
}

function finishOperatorAction(actionKey, completedOk) {
  const key = String(actionKey || "").trim() || "operator_action";
  const state = _operatorActionGuards.get(key);
  if (!state) return;
  state.inFlight = false;
  if (completedOk) {
    state.lastCompletedAtMs = Date.now();
  }
  _operatorActionGuards.set(key, state);
}

function wrapOperatorRoute(handler) {
  return async (req, res, next) => {
    try {
      await handler(req, res, next);
    } catch (e) {
      logOperatorCatch("route", e, { method: req.method, path: req.path });
      if (res.headersSent) return;
      return jsonFail(res, "internal_server_error", 500, {
        detail: String(e && e.message ? e.message : e)
      });
    }
  };
}

function setState(s) {
  SYSTEM_STATE.state = s;
  if (s === "RUNNING") {
    SYSTEM_STATE.started_at = Date.now();
  }
}

function logOperatorCatch(scope, error, extra = undefined) {
  const message = `[OPERATOR][${String(scope || "unknown")}] ${error && error.stack ? error.stack : String(error || "unknown_error")}`;
  try {
    console.error(message, extra || "");
  } catch {}
  try {
    ensureLogDir();
    fs.appendFileSync(RUNTIME_LOG, `[${nowIso()}] ${message}${extra ? ` extra=${JSON.stringify(extra)}` : ""}\n`);
  } catch {}
}
// Operator server
const OPERATOR_PORT = clampNumber(process.env.OPERATOR_PORT || 4001, 4001, 1, 65535);
const OPERATOR_BIND_HOST = String(process.env.OPERATOR_BIND_HOST || "127.0.0.1");
const OPERATOR_AUTO_START_DELAY_MS = clampNumber(
  process.env.OPERATOR_AUTO_START_DELAY_MS || 8000,
  8000,
  0,
  300000
);
const OPERATOR_DISABLE_INTERNAL_ENGINE_START = ["1", "true", "yes", "on"].includes(
  String(process.env.OPERATOR_DISABLE_INTERNAL_ENGINE_START || "0").trim().toLowerCase()
);
const PRODUCTION_MODE = process.env.NODE_ENV === "production";

// `operator_server.js` is the human-facing control plane. It should orchestrate
// process lifecycle and status aggregation, not duplicate the engine's internal
// runtime logic or become another source of truth for trading state.

/* -------------------------------------------------
   CORS (UI runs on :8000, Operator on :4001)
------------------------------------------------- */
app.use((req, res, next) => {
  const env = readEnv();
  const allowed = String(env.OPERATOR_ALLOWED_ORIGIN || "http://127.0.0.1:8000").trim();

  // Security headers (safe defaults for local/prod)
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader("Permissions-Policy", "geolocation=(), microphone=(), camera=()");
  // HSTS only in production + https
  if (PRODUCTION_MODE && req.secure) {
    res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
  }

  // CSP (allow local UI + inline assets used by dashboard)
  res.setHeader(
    "Content-Security-Policy",
    "default-src 'self'; " +
    "script-src 'self' 'unsafe-inline'; " +
    "style-src 'self' 'unsafe-inline'; " +
    "img-src 'self' data:; " +
    "connect-src 'self' http://127.0.0.1:4001 http://127.0.0.1:8000 ws://127.0.0.1:*; " +
    "frame-ancestors 'none'; " +
    "base-uri 'none'"
  );

  // CORS (strict single origin)
  res.setHeader("Access-Control-Allow-Origin", allowed);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, X-Requested-With, X-Operator-Token, X-API-Token, X-Request-ID, X-Correlation-ID"
  );
  res.setHeader("Access-Control-Allow-Credentials", "true");

  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: "1mb" }));
app.use("/ui", express.static(path.join(__dirname, "../ui")));
app.get("/favicon.ico", (_req, res) => res.status(204).end());
app.use((err, _req, res, next) => {
  if (!err) return next();
  if (err instanceof SyntaxError && err.status === 400 && "body" in err) {
    return jsonFail(res, "invalid_json", 400);
  }
  logOperatorCatch("express_json", err);
  return jsonFail(res, "invalid_request_body", 400);
});

app.get("/api/operator/ping", (_req, res) => {
  return jsonOk(res, { ts: Date.now() });
});

app.use(["/api/operator", "/api/operator_summary", "/api/execution/barrier"], (req, res, next) => {
  if (!operatorRouteRequiresAuth(req)) {
    return next();
  }
  if (operatorMutationAuthorized(req)) {
    return next();
  }
  return res.status(403).json({
    ok: false,
    error: "operator_forbidden",
    auth_required: true,
    meta: { status: 403 }
  });
});

// system summary
app.get("/api/operator_summary", wrapOperatorRoute(async (req, res) => {
  const engineStatus = status();
  const readiness = await getReadiness();
  const health = readiness && readiness.health ? readiness.health : null;
  const healthBody = health && health.body && typeof health.body === "object" ? health.body : null;

  const runtimeState = String(
    (healthBody && healthBody.lifecycle && healthBody.lifecycle.state)
    || (healthBody && healthBody.status)
    || engineStatus
    || "UNKNOWN"
  ).trim().toUpperCase();

  let stateValue = "STOPPED";
  let severity = "info";
  let meaning = "Operator control panel active.";
  let recommendedAction = "Review readiness and start system.";

  // Summary state is a synthesized operator view built from process state plus
  // dashboard health. It is intentionally higher-level than raw lifecycle enums.
  if (engineStatus !== "RUNNING") {
    stateValue = "STOPPED";
    severity = "warn";
    meaning = "Engine process is not running.";
    recommendedAction = "Start system setup or start engine.";
  } else if (health && health.ok && healthBody && healthBody.ok) {
    stateValue = runtimeState || "RUNNING";
    severity = "ok";
    meaning = "Backend reachable and health snapshot is healthy.";
    recommendedAction = "Open dashboard and verify live data.";
  } else if (health && health.ok) {
    stateValue = runtimeState || "WARMING_UP";
    severity = "warn";
    meaning = "Backend reachable but startup and data warm-up are not complete.";
    recommendedAction = "Wait for first price tick and provider health to populate.";
  } else {
    stateValue = "STARTING";
    severity = "warn";
    meaning = "Backend process is running but the dashboard health endpoint is not reachable yet.";
    recommendedAction = "Wait for dashboard bind and ingestion stabilization.";
  }

  return sendOperatorPayload(res, {
    ok: true,
    state: stateValue,
    severity,
    headline: `Engine state: ${stateValue}`,
    meaning,
    recommended_action: recommendedAction,
    readiness,
    health,
    currentBlocker: readiness.currentBlocker || null,
    degradedComponents: Array.isArray(readiness.degradedComponents) ? readiness.degradedComponents : []
  });
}));

app.get("/api/operator/market_data", wrapOperatorRoute(async (req, res) => {
  const envObj = readEnv();
  const base = dashBaseUrlFromEnv(envObj);
  const symbol = String(req.query.symbol || "SPY").trim().toUpperCase() || "SPY";
  const tf = String(req.query.tf || "1m").trim() || "1m";
  const limit = Math.max(5, Math.min(200, Number(req.query.limit || 30) || 30));

  // The operator proxies dashboard/runtime reads instead of reading the DB
  // directly, which keeps the operator in the control-plane role only.
  const telemetry = await httpGetJson(`${base}/api/telemetry`);
  const candles = await httpGetJson(
    `${base}/api/market/candles?symbol=${encodeURIComponent(symbol)}&tf=${encodeURIComponent(tf)}&limit=${limit}`
  );

  const ok = !!(telemetry.ok || candles.ok);
  return sendOperatorPayload(res, {
    ok,
    symbol,
    tf,
    dashboardBase: base,
    telemetry: telemetry.ok ? telemetry.json : null,
    candles: candles.ok ? candles.json : null,
    degradedComponents: [
      ...(telemetry.ok ? [] : ["telemetry"]),
      ...(candles.ok ? [] : ["candles"])
    ],
    upstream: {
      telemetry: { status: Number(telemetry.status || 0), error: telemetry.error || null },
      candles: { status: Number(candles.status || 0), error: candles.error || null }
    }
  }, ok ? 200 : operatorErrorStatus(telemetry.error || candles.error || "dashboard_unreachable", 503));
}));

// stderr tail
app.get("/api/operator/stderr_tail", wrapOperatorRoute(async (req, res) => {
  const limit = Math.max(512, Math.min(512000, Number(req.query.limit || 2000) || 2000));
    const tail = currentAttemptStderrTail(limit);
    if (!tail) {
      if (!fs.existsSync(ENGINE_STDERR_LOG)) {
        return jsonFail(res, "no_stderr_log", 404, {
          currentAttemptOnly: true,
          attemptId: state.currentAttemptId || null,
          attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null
        });
      }
    }

    const redactedTail = redactOperatorSensitiveText(String(tail || ""));
    const lines = redactedTail.split(/\r?\n/).filter(Boolean);

    return jsonOk(res, {
      ok:true,
      tail: redactedTail,
      lines,
      currentAttemptOnly: true,
      attemptId: state.currentAttemptId || null,
      attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null
    });
}));

// proxy endpoints used by snapshot bundle
app.get("/api/operator/proxy/jobs",
  operatorProxyGet("/api/jobs", "invalid_jobs_response")
);

app.get("/api/operator/proxy/telemetry",
  operatorProxyGet("/api/telemetry", "invalid_telemetry_response")
);

app.get("/api/operator/proxy/system_state",
  operatorCanonicalProxyGet("/api/system/state", "invalid_system_state_response")
);

app.get("/api/operator/proxy/validation",
  operatorProxyGet("/api/validation", "invalid_validation_response")
);

app.get("/api/operator/proxy/health",
  operatorCanonicalProxyGet("/api/health", "invalid_system_health_response")
);

const ROOT = path.join(__dirname, "..");
const VAR_DIR = path.join(ROOT, "var");
const DEFAULT_LOCAL_DB_PATH = "./var/db/trading.db";

// Python dashboard entrypoint (this repo ships start_system.py)
const ENTRY = path.join(ROOT, "start_system.py");

// Files
const ENV_PATH = String(process.env.OPERATOR_ENV_PATH || "").trim()
  ? path.resolve(String(process.env.OPERATOR_ENV_PATH || "").trim())
  : path.join(ROOT, ".env");
const LOG_DIR = String(process.env.TRADING_LOGS || process.env.LOG_DIR || "").trim()
  ? path.resolve(String(process.env.TRADING_LOGS || process.env.LOG_DIR || "").trim())
  : path.join(VAR_DIR, "log");
const RUNTIME_LOG = path.join(LOG_DIR, "runtime.log");
const ENGINE_STDERR_LOG = path.join(LOG_DIR, "engine_stderr.log");
const OPERATOR_DATA_DIR = String(process.env.OPERATOR_DATA_DIR || "").trim()
  ? path.resolve(String(process.env.OPERATOR_DATA_DIR || "").trim())
  : path.join(VAR_DIR, "tmp", "operator");
const SECRETS_PATH = path.join(OPERATOR_DATA_DIR, "operator.secrets.json");
const STATE_PATH = path.join(OPERATOR_DATA_DIR, "operator.state.json");
const OPERATOR_CONFIRMATION_AUDIT_PATH = path.join(OPERATOR_DATA_DIR, "operator_confirmation_audit.jsonl");

// Linux appliance deploy helpers
const DEPLOY_DIR = path.join(ROOT, "deploy");
const DEPLOY_BIN_DIR = path.join(DEPLOY_DIR, "bin");
const SERVICE_CTL = path.join(DEPLOY_BIN_DIR, "service_ctl.sh");
const BACKUP_SCRIPT = path.join(DEPLOY_BIN_DIR, "backup_trading_db.sh");
const UPGRADE_SCRIPT = path.join(DEPLOY_BIN_DIR, "upgrade_trading_system.sh");

function isLinuxManagedMode() {
  if (process.platform !== "linux") return false;
  try {
    fs.accessSync(SERVICE_CTL, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

let child = null;
let installing = false;

function _safeRepoPath(target) {
  const resolvedRoot = path.resolve(ROOT);
  const resolvedTarget = path.resolve(target);
  if (
    resolvedTarget !== resolvedRoot &&
    !resolvedTarget.startsWith(resolvedRoot + path.sep)
  ) {
    throw new Error(`unsafe_repo_path:${resolvedTarget}`);
  }
  return resolvedTarget;
}

function safeAtomicWrite(file, data) {
  const safeFile = _safeRepoPath(file);
  atomicWrite(safeFile, data);
}

function safeUnlinkIfExists(file) {
  const safeFile = _safeRepoPath(file);
  try {
    if (fs.existsSync(safeFile)) fs.unlinkSync(safeFile);
    return true;
  } catch {
    return false;
  }
}

function ensureOperatorPatchDir() {
  const dir = path.join(OPERATOR_DATA_DIR, "patches");
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function normalizeAiPatchFile(file) {
  const raw = String(file || "").trim();
  if (!raw) throw new Error("patch_file_missing");
  const cleaned = raw.replace(/^[/\\]+/, "");
  const target = _safeRepoPath(path.join(ROOT, cleaned));
  const rel = path.relative(ROOT, target).replace(/\\/g, "/");

  if (
    rel.startsWith("data/operator/patches/") ||
    rel.startsWith("var/tmp/operator/patches/") ||
    rel === ".env" ||
    rel.startsWith("logs/") ||
    rel.startsWith("var/log/") ||
    rel.startsWith("var/db/") ||
    rel.startsWith("var/artifacts/") ||
    rel.startsWith("var/audit/") ||
    rel.startsWith("var/tmp/")
  ) {
    throw new Error(`patch_target_forbidden:${rel}`);
  }

  if (!/\.(js|py|json|txt|md|yaml|yml|ini|cfg|toml|html|css)$/i.test(rel)) {
    throw new Error(`patch_target_extension_forbidden:${rel}`);
  }

  return target;
}

function buildPatchDiff(beforeText, afterText, findText, replaceText) {
  return {
    before_length: String(beforeText || "").length,
    after_length: String(afterText || "").length,
    find_preview: String(findText || "").slice(0, 500),
    replace_preview: String(replaceText || "").slice(0, 500)
  };
}

function applyAiPatchWithBackup(file, patch, meta = {}) {
  if (!patch || typeof patch !== "object") {
    throw new Error("patch_missing");
  }

  const findText = String(patch.find || "");
  const replaceText = String(patch.replace || "");

  if (!findText) {
    throw new Error("patch_find_missing");
  }

  const targetPath = normalizeAiPatchFile(file);

  if (!fs.existsSync(targetPath)) {
    throw new Error(`patch_target_missing:${targetPath}`);
  }

  const before = fs.readFileSync(targetPath, "utf-8");
  const hitCount = before.split(findText).length - 1;

  if (hitCount !== 1) {
    throw new Error(`patch_find_match_count:${hitCount}`);
  }

  const after = before.replace(findText, replaceText);

  if (after === before) {
    throw new Error("patch_no_effect");
  }

  const patchDir = ensureOperatorPatchDir();
  const patchId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const backupPath = path.join(patchDir, `${patchId}.bak`);
  const metaPath = path.join(patchDir, `${patchId}.json`);

  fs.writeFileSync(backupPath, before, "utf-8");
  atomicWrite(targetPath, after);

  const info = {
    patchId,
    at: nowIso(),
    file: path.relative(ROOT, targetPath).replace(/\\/g, "/"),
    backupPath,
    metaPath,
    diff: buildPatchDiff(before, after, findText, replaceText),
    meta: meta || {}
  };

  safeAtomicWrite(metaPath, JSON.stringify(info, null, 2));
  _lastAppliedPatchMeta = info;

  return info;
}

function rollbackAiPatch(patchId) {
  const dir = ensureOperatorPatchDir();
  const safePatchId = String(patchId || "").trim();
  if (!safePatchId) {
    throw new Error("patch_id_missing");
  }

  const metaPath = path.join(dir, `${safePatchId}.json`);
  if (!fs.existsSync(metaPath)) {
    throw new Error("patch_meta_missing");
  }

  const info = JSON.parse(fs.readFileSync(metaPath, "utf-8"));
  const targetPath = normalizeAiPatchFile(info.file);
  const backupPath = _safeRepoPath(info.backupPath);

  if (!fs.existsSync(backupPath)) {
    throw new Error("patch_backup_missing");
  }

  const backupText = fs.readFileSync(backupPath, "utf-8");
  atomicWrite(targetPath, backupText);

  const rollbackInfo = {
    ok: true,
    patchId: safePatchId,
    file: info.file,
    restoredAt: nowIso()
  };

  _lastAppliedPatchMeta = null;
  return rollbackInfo;
}

function isLoopbackRequest(req) {
  const raw =
    req.ip ||
    req.socket?.remoteAddress ||
    req.connection?.remoteAddress ||
    "";
  const ip = String(raw).trim();
  return (
    ip === "127.0.0.1" ||
    ip === "::1" ||
    ip === "::ffff:127.0.0.1"
  );
}

function timingSafeTokenEquals(supplied, expected) {
  const a = Buffer.from(String(supplied || ""), "utf8");
  const b = Buffer.from(String(expected || ""), "utf8");
  if (a.length === 0 || b.length === 0 || a.length !== b.length) return false;
  try {
    return crypto.timingSafeEqual(a, b);
  } catch {
    return false;
  }
}

function readSecretFileText(filePath) {
  const raw = String(filePath || "").trim();
  if (!raw) return "";
  try {
    return String(fs.readFileSync(raw, "utf-8") || "").trim();
  } catch {
    return "";
  }
}

function readCredentialSecretText(secretName) {
  const name = String(secretName || "").trim();
  if (!name || !/^[A-Za-z0-9_.@:-]+$/.test(name)) return "";
  for (const dirRaw of [process.env.CREDENTIALS_DIRECTORY, process.env.TS_DEV_SECRETS_DIR]) {
    const dir = String(dirRaw || "").trim();
    if (!dir) continue;
    try {
      const target = path.resolve(dir, name);
      const resolvedDir = path.resolve(dir);
      if (target !== resolvedDir && target.startsWith(resolvedDir + path.sep)) {
        const value = readSecretFileText(target);
        if (value) return value;
      }
    } catch {}
  }
  return "";
}

function operatorApiTokenFromConfig() {
  let env = {};
  try {
    env = readEnv();
  } catch {}
  const fileToken = readSecretFileText(process.env.OPERATOR_API_TOKEN_FILE || env.OPERATOR_API_TOKEN_FILE);
  if (fileToken) return fileToken;
  const providerToken = readCredentialSecretText(
    process.env.OPERATOR_API_TOKEN_SECRET || env.OPERATOR_API_TOKEN_SECRET || "operator_api_token"
  );
  if (providerToken) return providerToken;
  return String(
    process.env.OPERATOR_API_TOKEN ||
    env.OPERATOR_API_TOKEN ||
    ""
  ).trim();
}

function dashboardApiTokenFromConfig() {
  let env = {};
  try {
    env = readEnv();
  } catch {}
  const fileToken = readSecretFileText(process.env.DASHBOARD_API_TOKEN_FILE || env.DASHBOARD_API_TOKEN_FILE);
  if (fileToken) return fileToken;
  const providerToken = readCredentialSecretText(
    process.env.DASHBOARD_API_TOKEN_SECRET || env.DASHBOARD_API_TOKEN_SECRET || "dashboard_api_token"
  );
  if (providerToken) return providerToken;
  return String(
    process.env.DASHBOARD_API_TOKEN ||
    env.DASHBOARD_API_TOKEN ||
    ""
  ).trim();
}

function normalizeUrlHostForCompare(host) {
  return String(host || "").trim().replace(/^\[/, "").replace(/\]$/, "").toLowerCase();
}

function isLoopbackHost(host) {
  const h = normalizeUrlHostForCompare(host);
  return h === "127.0.0.1" || h === "localhost" || h === "::1" || h === "::ffff:127.0.0.1";
}

function isOperatorSidecarUrl(urlText) {
  try {
    const parsed = new URL(String(urlText || ""));
    const parsedPort = Number(parsed.port || (parsed.protocol === "https:" ? 443 : 80));
    if (parsedPort !== Number(OPERATOR_PORT)) return false;
    const host = normalizeUrlHostForCompare(parsed.hostname);
    const bindHost = normalizeUrlHostForCompare(OPERATOR_BIND_HOST);
    if (host && host === bindHost) return true;
    if ((bindHost === "0.0.0.0" || bindHost === "::") && (isLoopbackHost(host) || host === "0.0.0.0")) return true;
    return isLoopbackHost(host) && isLoopbackHost(bindHost);
  } catch {
    return false;
  }
}

function trustedControlPlaneAuthHeaders(method, urlText) {
  const upper = String(method || "GET").toUpperCase();
  const headers = {};
  if (isOperatorSidecarUrl(urlText)) {
    const operatorToken = operatorApiTokenFromConfig();
    if (operatorToken) headers["X-Operator-Token"] = operatorToken;
    return headers;
  }
  if (upper === "GET" || upper === "HEAD" || upper === "OPTIONS") return {};
  try {
    const parsed = new URL(String(urlText || ""));
    if (String(parsed.pathname || "").startsWith("/api/")) {
      const dashboardToken = dashboardApiTokenFromConfig();
      if (dashboardToken) headers["X-API-Token"] = dashboardToken;
    }
  } catch {}
  return headers;
}

const OPERATOR_CONFIRMATION_REGISTRY = Object.freeze({
  "operator.start": {
    requiredToken: "START_OPERATOR",
    severity: "high",
    consequence: "Starts operator-controlled runtime processes and may start data or pipeline jobs.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.live_start": {
    requiredToken: "TRADE",
    severity: "emergency",
    consequence: "Starts the runtime in TRADING mode, enabling live execution only if backend gates also allow it.",
    requireReason: true,
    minReasonLength: 10,
    holdMs: 3000,
  },
  "operator.guided_bootstrap": {
    requiredToken: "GUIDED_BOOTSTRAP",
    severity: "high",
    consequence: "Runs the guided startup workflow, including engine start, health wait, and bootstrap pipeline work.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.guided_bootstrap_live": {
    requiredToken: "TRADE",
    severity: "emergency",
    consequence: "Runs guided startup in TRADING mode. Backend live execution gates remain authoritative.",
    requireReason: true,
    minReasonLength: 10,
    holdMs: 3000,
  },
  "operator.stop": {
    requiredToken: "STOP_OPERATOR",
    severity: "high",
    consequence: "Stops operator-controlled runtime processes.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.restart": {
    requiredToken: "RESTART_OPERATOR",
    severity: "high",
    consequence: "Restarts operator-controlled runtime processes.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.emergency_stop": {
    requiredToken: "KILL",
    severity: "emergency",
    consequence: "Forces SAFE mode, disarms execution, trips the global kill switch, and stops the engine.",
    requireReason: true,
    minReasonLength: 10,
    holdMs: 3000,
  },
  "operator.broker_risk": {
    requiredToken: "BROKER_RISK",
    severity: "emergency",
    consequence: "Cancels live broker orders and may submit flattening orders under configured shutdown-risk limits.",
    requireReason: true,
    minReasonLength: 10,
    requireTarget: true,
    holdMs: 3000,
  },
  "operator.config_write": {
    requiredToken: "SAVE_CONFIG",
    severity: "high",
    consequence: "Writes normalized environment configuration used by future runtime starts.",
    requireReason: true,
    minReasonLength: 8,
  },
  "operator.set_mode": {
    requiredToken: "SET_MODE",
    severity: "high",
    consequence: "Changes operator and execution mode values in environment configuration.",
    requireReason: true,
    minReasonLength: 8,
  },
  "operator.training_control": {
    requiredToken: "TRAINING_CONTROL",
    severity: "high",
    consequence: "Changes automatic training pipeline configuration.",
    requireReason: true,
    minReasonLength: 8,
  },
  "operator.secrets_write": {
    requiredToken: "SAVE_SECRET",
    severity: "high",
    consequence: "Writes encrypted operator-side secret values. Secret values are not stored in audit records.",
    requireReason: true,
    minReasonLength: 8,
  },
  "operator.factory_reset": {
    requiredToken: "FACTORY_RESET",
    severity: "emergency",
    consequence: "Deletes .env and operator secrets, clears operator state, and emergency-stops the runtime first.",
    requireReason: true,
    minReasonLength: 12,
    holdMs: 5000,
  },
  "operator.self_repair": {
    requiredToken: "SYSTEM_FIX",
    severity: "high",
    consequence: "Runs automatic system repair actions through the dashboard/runtime repair surface.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.repair_schema": {
    requiredToken: "REPAIR_SCHEMA",
    severity: "high",
    consequence: "Runs schema repair through the dashboard/runtime repair surface.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.restart_feeds": {
    requiredToken: "RESTART_FEEDS",
    severity: "high",
    consequence: "Stops and starts market-data jobs and then requests a pipeline refresh.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.backup": {
    requiredToken: "RUN_BACKUP",
    severity: "high",
    consequence: "Starts the appliance backup workflow.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.system_update": {
    requiredToken: "SYSTEM_UPDATE",
    severity: "high",
    consequence: "Starts the appliance system update workflow.",
    requireReason: true,
    minReasonLength: 8,
  },
  "operator.restart_operator": {
    requiredToken: "RESTART_OPERATOR_UI",
    severity: "high",
    consequence: "Restarts the operator UI service and may interrupt active operator sessions.",
    requireReason: true,
    minReasonLength: 6,
  },
  "operator.job_start": {
    requiredToken: "JOB_ACTION",
    severity: "high",
    consequence: "Starts a runtime job through the dashboard job catalog.",
    requireReason: true,
    minReasonLength: 6,
    requireTarget: true,
  },
  "operator.job_stop": {
    requiredToken: "JOB_ACTION",
    severity: "high",
    consequence: "Stops a runtime job through the dashboard job catalog.",
    requireReason: true,
    minReasonLength: 6,
    requireTarget: true,
  },
  "operator.promote_model": {
    requiredToken: "PROMOTION",
    severity: "high",
    consequence: "Requests model promotion through the dashboard governance endpoint.",
    requireReason: true,
    minReasonLength: 10,
  },
  "operator.ai_apply_patch": {
    requiredToken: "APPLY_PATCH",
    severity: "high",
    consequence: "Applies a guarded operator-AI patch to a repository file. Live mode remains blocked.",
    requireReason: true,
    minReasonLength: 10,
    requireTarget: true,
  },
  "operator.ai_rollback_patch": {
    requiredToken: "ROLLBACK_PATCH",
    severity: "high",
    consequence: "Restores a previously backed-up operator-AI patch target.",
    requireReason: true,
    minReasonLength: 10,
    requireTarget: true,
  },
});

function truthyConfirmationValue(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  return ["1", "true", "yes", "on", "ack", "confirmed"].includes(
    String(value || "").trim().toLowerCase()
  );
}

function operatorConfirmationHash(spec) {
  const text = String((spec && spec.consequence) || "");
  if (!text) return "";
  return crypto.createHash("sha256").update(text, "utf8").digest("hex");
}

function operatorRequestId(req, body = {}) {
  const fromBody = String(body.request_id || body.requestId || "").trim();
  if (fromBody) return fromBody;
  return String(
    req.headers?.["x-request-id"] ||
    req.headers?.["x-correlation-id"] ||
    `operator-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
  ).trim();
}

function operatorConfirmationTarget(req, spec, body = {}, fallbackTarget = "") {
  const explicit = String(body.target || body.target_id || body.targetId || "").trim();
  if (explicit) return explicit;
  const fallback = String(fallbackTarget || spec?.target || "").trim();
  if (fallback) return fallback;
  const name = String(body.name || body.job || req.query?.name || "").trim();
  if (name) return name;
  const file = String(body.file || body.patchId || "").trim();
  if (file) return file;
  return "";
}

function appendOperatorConfirmationAudit(payload) {
  try {
    fs.mkdirSync(path.dirname(OPERATOR_CONFIRMATION_AUDIT_PATH), { recursive: true });
    const sanitized = redactOperatorSecrets(payload);
    fs.appendFileSync(OPERATOR_CONFIRMATION_AUDIT_PATH, JSON.stringify(sanitized) + "\n");
  } catch {}
}

function operatorConfirmationAuditPayload(req, confirmation, outcome, statusCode = 0, extra = {}) {
  const body = req.body && typeof req.body === "object" && !Array.isArray(req.body) ? req.body : {};
  const spec = confirmation && confirmation.spec ? confirmation.spec : {};
  return {
    ts_ms: Date.now(),
    request_id: String((confirmation && confirmation.request_id) || operatorRequestId(req, body)),
    method: String(req.method || ""),
    path: String(req.path || req.originalUrl || ""),
    outcome: String(outcome || ""),
    status: Number(statusCode || 0),
    action_id: String((confirmation && confirmation.action_id) || spec.action_id || ""),
    actor: String((confirmation && confirmation.actor) || ""),
    source_surface: String((confirmation && confirmation.source_surface) || ""),
    reason: String((confirmation && confirmation.reason) || ""),
    target: String((confirmation && confirmation.target) || ""),
    confirmation_method: String((confirmation && confirmation.confirmation_method) || ""),
    confirmation_hold_ms: Number((confirmation && confirmation.confirmation_hold_ms) || 0),
    consequence_hash: operatorConfirmationHash(spec),
    client_ip: String(req.ip || req.socket?.remoteAddress || ""),
    confirmed: outcome === "confirmation_accepted",
    ...extra,
  };
}

function requireOperatorConfirmation(req, res, actionId, overrides = {}) {
  const baseSpec = OPERATOR_CONFIRMATION_REGISTRY[String(actionId || "")];
  if (!baseSpec) {
    throw new Error(`operator_confirmation_spec_missing:${actionId}`);
  }
  const spec = {
    action_id: String(actionId || ""),
    requireAck: true,
    requireActor: true,
    requireSource: true,
    requireActionId: true,
    holdMs: 0,
    minReasonLength: 0,
    requireTarget: false,
    ...baseSpec,
    ...overrides,
  };
  const body = req.body && typeof req.body === "object" && !Array.isArray(req.body) ? req.body : {};
  const expected = String(spec.requiredToken || "").trim();
  const actual = String(body.confirmation || body.confirm || body.confirmation_token || "").trim();
  const suppliedActionId = String(body.action_id || body.actionId || "").trim();
  const actor = String(body.actor || body.who || "").trim();
  const sourceSurface = String(body.source_surface || body.source || "").trim();
  const reason = String(body.reason || body.justification || body.note || "").trim();
  const request_id = operatorRequestId(req, body);
  const target = operatorConfirmationTarget(req, spec, body, overrides.target);
  let holdMs = 0;
  try {
    holdMs = Math.max(0, Number(body.confirmation_hold_ms ?? body.hold_ms ?? 0));
  } catch {
    holdMs = 0;
  }

  const missing = [];
  if (!expected || !timingSafeTokenEquals(actual, expected)) missing.push("confirmation");
  if (spec.requireActionId && suppliedActionId !== spec.action_id) missing.push("action_id");
  if (spec.requireAck && !truthyConfirmationValue(body.consequence_ack)) missing.push("consequence_ack");
  if (spec.requireActor && !actor) missing.push("actor");
  if (spec.requireSource && !sourceSurface) missing.push("source_surface");
  if (spec.requireTarget && !target) missing.push("target");
  if (spec.requireReason && reason.length < Number(spec.minReasonLength || 0)) missing.push("reason");
  if (Number(spec.holdMs || 0) > 0 && holdMs < Number(spec.holdMs || 0)) missing.push("confirmation_hold_ms");

  const confirmation = {
    spec,
    action_id: spec.action_id,
    severity: String(spec.severity || ""),
    actor,
    source_surface: sourceSurface,
    reason,
    request_id,
    target,
    confirmation_method: String(body.confirmation_method || (holdMs > 0 ? "typed_phrase_hold" : "typed_phrase")),
    confirmation_hold_ms: holdMs,
  };

  if (missing.length) {
    appendOperatorConfirmationAudit(operatorConfirmationAuditPayload(req, confirmation, "confirmation_denied", 422, {
      required_fields: missing,
      required_token: expected,
    }));
    jsonFail(res, "confirmation_required", 422, {
      required_confirm: expected,
      required_token: expected,
      required_fields: missing,
      action_id: spec.action_id,
      severity: String(spec.severity || ""),
      consequence: String(spec.consequence || ""),
      min_hold_ms: Number(spec.holdMs || 0),
    });
    return null;
  }

  req.operatorConfirmation = confirmation;
  appendOperatorConfirmationAudit(operatorConfirmationAuditPayload(req, confirmation, "confirmation_accepted", 202));
  return confirmation;
}

function operatorConfirmationBody(req, requiredToken, overrides = {}) {
  const confirmation = req.operatorConfirmation || {};
  const request_id = String(confirmation.request_id || `operator-${Date.now()}`);
  return {
    confirm: String(requiredToken || ""),
    confirmation: String(requiredToken || ""),
    confirmation_token: String(requiredToken || ""),
    confirmation_method: String(confirmation.confirmation_method || "typed_phrase"),
    confirmation_hold_ms: Number(overrides.holdMs ?? confirmation.confirmation_hold_ms ?? 0),
    consequence_ack: true,
    action_id: String(overrides.actionId || ""),
    actor: String(confirmation.actor || "operator_server"),
    source: String(overrides.source || confirmation.source_surface || "operator_sidecar"),
    source_surface: String(overrides.source || confirmation.source_surface || "operator_sidecar"),
    reason: String(overrides.reason || confirmation.reason || "operator sidecar confirmed downstream action"),
    request_id,
    target: String(overrides.target || confirmation.target || ""),
  };
}

function stripConfirmationFields(body = {}) {
  const out = {};
  const confirmationKeys = new Set([
    "confirm",
    "confirmation",
    "confirmation_token",
    "confirmation_method",
    "confirmation_hold_ms",
    "hold_ms",
    "consequence_ack",
    "action_id",
    "actionId",
    "actor",
    "who",
    "source",
    "source_surface",
    "reason",
    "justification",
    "note",
    "request_id",
    "requestId",
    "target",
    "target_id",
    "targetId",
  ]);
  for (const [key, value] of Object.entries(body || {})) {
    if (!confirmationKeys.has(String(key))) out[key] = value;
  }
  return out;
}

function operatorRequestToken(req) {
  const headerToken = String(req.headers?.["x-operator-token"] || "").trim();
  if (headerToken) return headerToken;

  const auth = String(req.headers?.authorization || "").trim();
  if (auth.startsWith("Bearer ")) {
    const bearer = auth.slice("Bearer ".length).trim();
    if (bearer) return bearer;
  }

  try {
    const rawUrl = String(req.url || "");
    const parsed = new NodeURL(rawUrl, "http://127.0.0.1");
    for (const name of ["operator_token", "operator_api_token", "token"]) {
      const value = String(parsed.searchParams.get(name) || "").trim();
      if (value) return value;
    }
  } catch {}

  return "";
}

function operatorMutationAuthorized(req) {
  const token = operatorApiTokenFromConfig();

  if (!token) return false;

  return timingSafeTokenEquals(operatorRequestToken(req), token);
}

function operatorRouteRequiresAuth(req) {
  const pathName = String(req.path || req.url || "");
  const method = String(req.method || "GET").toUpperCase();
  if (method === "OPTIONS") return false;
  if (pathName === "/ping" || pathName === "/api/operator/ping") return false;
  return true;
}

// --------------------------------------------------
// Persistent State
// --------------------------------------------------

function defaultState() {
  return {
    createdAt: new Date().toISOString(),
    lastExitCode: null,
    lastError: null, // { at, kind, message, details?, attemptId?, attemptStartedAt? }
    lastCrash: null,
    restartAttempts: 0,
    lastStartAt: null,
    lastStopAt: null,
    lastHealthyAt: null,
    lastMode: "safe", // safe | shadow | live
    nextRestartAt: null,
    currentAttemptId: null,
    currentAttemptStartedAt: null,
    currentRuntimeLogOffset: 0,
    currentStderrLogOffset: 0,
    _restartWindowStart: null,
    _restartCountWindow: 0,
    consecutiveStartupFailures: 0,
    lastExitAt: null,
    lastExitSignal: null,
    restartBlocked: false,
    fatal: false,
    fatalSince: null
  };
}

const OPERATOR_EXTERNAL_RUNTIME_CACHE_TTL_MS = clampNumber(
  process.env.OPERATOR_EXTERNAL_RUNTIME_CACHE_TTL_MS || 2500,
  2500,
  0,
  30000
);
const OPERATOR_PROCESS_SCAN_CACHE_TTL_MS = clampNumber(
  process.env.OPERATOR_PROCESS_SCAN_CACHE_TTL_MS || 5000,
  5000,
  0,
  60000
);
const OPERATOR_PREFLIGHT_CACHE_TTL_MS = clampNumber(
  process.env.OPERATOR_PREFLIGHT_CACHE_TTL_MS || 30000,
  30000,
  0,
  300000
);
const OPERATOR_STALE_ATTEMPT_GRACE_MS = clampNumber(
  process.env.OPERATOR_STALE_ATTEMPT_GRACE_MS || 90000,
  90000,
  30000,
  86400000
);

let _externalRuntimeCache = { tsMs: 0, payload: null };
let _repoStartSystemCandidatesCache = { tsMs: 0, payload: null };
let _validationGateCache = { key: "", tsMs: 0, payload: null };

function cloneJsonValue(value) {
  if (value === null || value === undefined) return value;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }
}

function readTimedCache(cacheState, ttlMs) {
  const ttl = Number(ttlMs || 0);
  if (ttl <= 0 || !cacheState || !cacheState.payload) return null;
  if ((Date.now() - Number(cacheState.tsMs || 0)) > ttl) return null;
  return cloneJsonValue(cacheState.payload);
}

function invalidateOperatorRuntimeCaches() {
  _externalRuntimeCache = { tsMs: 0, payload: null };
  _repoStartSystemCandidatesCache = { tsMs: 0, payload: null };
}

let state = loadState();
let _pendingRestartTimer = null;

function readPidFileRecord(filePath, label = "runtime") {
  try {
    if (!fs.existsSync(filePath)) {
      return { pid: 0, label: String(label || ""), raw: "" };
    }
    const raw = String(fs.readFileSync(filePath, "utf-8") || "").trim();
    if (!raw) {
      return { pid: 0, label: String(label || ""), raw };
    }
    if (raw.startsWith("{")) {
      const data = JSON.parse(raw);
      const pid = Number(data && data.pid);
      return {
        pid: Number.isInteger(pid) && pid > 0 ? pid : 0,
        label: String((data && data.label) || label || ""),
        entry: String((data && data.entry) || ""),
        base_dir: String((data && data.base_dir) || ""),
        owner_pid: Number((data && data.owner_pid) || 0) || 0,
        created_ts_ms: Number((data && data.created_ts_ms) || 0) || 0,
        raw,
      };
    }
    const pid = Number(raw);
    return {
      pid: Number.isInteger(pid) && pid > 0 ? pid : 0,
      label: String(label || ""),
      entry: "",
      base_dir: "",
      owner_pid: 0,
      created_ts_ms: 0,
      raw,
    };
  } catch {
    return { pid: 0, label: String(label || ""), raw: "" };
  }
}

function readPidFileInt(filePath) {
  try {
    return Number(readPidFileRecord(filePath).pid || 0);
  } catch {
    return 0;
  }
}

function operatorPidIsRunning(pid) {
  const n = Number(pid);
  if (!Number.isInteger(n) || n <= 0) return false;

  try {
    process.kill(n, 0);
    return true;
  } catch {}

  return false;
}

function pidRecordBelongsHere(record, label) {
  try {
    const recordLabel = String((record && record.label) || "").trim().toLowerCase();
    const recordBaseDir = path.resolve(String((record && record.base_dir) || "") || ".");
    const localBaseDir = path.resolve(path.join(__dirname, ".."));
    if (recordLabel && recordLabel !== String(label || "").trim().toLowerCase()) return false;
    if (String((record && record.base_dir) || "").trim() && recordBaseDir !== localBaseDir) return false;
    return true;
  } catch {
    return false;
  }
}

function validateOrClearPidFile(filePath, label) {
  const record = readPidFileRecord(filePath, label);
  const pid = Number(record.pid || 0);

  if (!pid) {
    try {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    } catch (e) {
      logOperatorCatch("validateOrClearPidFile.unlink_invalid", e, { filePath, label });
    }
    return { pid: 0, active: false, cleared: true, record };
  }

  if (pid === process.pid) {
    return { pid, active: true, cleared: false, record };
  }

  if (operatorPidIsRunning(pid) && pidRecordBelongsHere(record, label)) {
    return { pid, active: true, cleared: false, record };
  }

  try {
    fs.unlinkSync(filePath);
  } catch (e) {
    logOperatorCatch("validateOrClearPidFile.unlink_stale", e, { filePath, label, pid });
  }
  return { pid, active: false, cleared: true, record };
}

function clampCurrentAttemptOffsets(nextState) {
  const runtimeSize = fs.existsSync(RUNTIME_LOG) ? fs.statSync(RUNTIME_LOG).size : 0;
  const stderrSize = fs.existsSync(ENGINE_STDERR_LOG) ? fs.statSync(ENGINE_STDERR_LOG).size : 0;

  nextState.currentRuntimeLogOffset = Math.max(
    0,
    Math.min(runtimeSize, Number(nextState.currentRuntimeLogOffset || 0))
  );
  nextState.currentStderrLogOffset = Math.max(
    0,
    Math.min(stderrSize, Number(nextState.currentStderrLogOffset || 0))
  );
}

function loadState() {
  try {
    const base = defaultState();
    const obj = fs.existsSync(STATE_PATH)
      ? JSON.parse(fs.readFileSync(STATE_PATH, "utf-8"))
      : {};
    const nextState = { ...base, ...obj };

    const runtimePidState = validateOrClearPidFile(path.join(LOG_DIR, "runtime.pid"), "runtime");
    validateOrClearPidFile(path.join(LOG_DIR, "ingestion.pid"), "ingestion");

    if (!runtimePidState.active) {
      nextState.nextRestartAt = null;

      const hadAttemptContext =
        !!String(nextState.currentAttemptId || "").trim() ||
        !!String(nextState.currentAttemptStartedAt || "").trim() ||
        Number(nextState.currentRuntimeLogOffset || 0) > 0 ||
        Number(nextState.currentStderrLogOffset || 0) > 0;

      if (hadAttemptContext) {
        clampCurrentAttemptOffsets(nextState);
      } else {
        nextState.currentAttemptId = null;
        nextState.currentAttemptStartedAt = null;
        nextState.currentRuntimeLogOffset = fs.existsSync(RUNTIME_LOG) ? fs.statSync(RUNTIME_LOG).size : 0;
        nextState.currentStderrLogOffset = fs.existsSync(ENGINE_STDERR_LOG) ? fs.statSync(ENGINE_STDERR_LOG).size : 0;
      }
    } else {
      clampCurrentAttemptOffsets(nextState);
    }

    return nextState;
  } catch {
    return defaultState();
  }
}

function saveState() {
  try {
    clampCurrentAttemptOffsets(state);
    safeAtomicWrite(STATE_PATH, JSON.stringify(state, null, 2));
  } catch {
    // ignore
  }
}

function isCrashLikeError(kind, details) {
  const k = String(kind || "").trim().toUpperCase();
  if (!k) return false;
  if (k.includes("CRASH")) return true;
  if (k.includes("FATAL")) return true;
  if (k.includes("BOOTSTRAP_FAIL")) return true;
  return !!(details && details.fatal === true);
}

function setLastError(kind, message, details) {
  const mergedDetails = details && typeof details === "object"
    ? normalizeCrashDetails({ ...details })
    : (details == null ? null : { value: details });
  const attemptId = (mergedDetails && mergedDetails.attemptId) || state.currentAttemptId || null;
  const attemptStartedAt = (mergedDetails && mergedDetails.attemptStartedAt) || state.currentAttemptStartedAt || state.lastStartAt || null;
  if (mergedDetails && !mergedDetails.attemptId && attemptId) mergedDetails.attemptId = attemptId;
  if (mergedDetails && !mergedDetails.attemptStartedAt && attemptStartedAt) mergedDetails.attemptStartedAt = attemptStartedAt;
  state.lastError = {
    at: new Date().toISOString(),
    kind,
    message,
    details: mergedDetails,
    attemptId,
    attemptStartedAt
  };
  if (isCrashLikeError(kind, mergedDetails)) {
    state.lastCrash = { ...state.lastError };
  }
  saveState();
}

function clearLastError() {
  state.lastError = null;
  saveState();
}

function normalizeCrashDetails(details) {
  if (!details || typeof details !== "object") return details || null;
  const out = { ...details };
  for (const key of ["stderrTail", "stdoutTail", "runtimeTail"]) {
    if (typeof out[key] === "string" && out[key].length > 20000) {
      out[key] = out[key].slice(-20000);
    }
  }
  return out;
}

function setFatalRestartBlock(kind, message, details) {
  clearPendingRestartTimer();
  state.restartBlocked = true;
  state.fatal = true;
  state.fatalSince = state.fatalSince || new Date().toISOString();
  state.nextRestartAt = null;
  setLastError(kind, message, {
    ...normalizeCrashDetails(details || {}),
    restartBlocked: true,
    fatal: true,
    fatalSince: state.fatalSince
  });
  saveState();
}

function clearFatalRestartBlock() {
  state.restartBlocked = false;
  state.fatal = false;
  state.fatalSince = null;
  state.nextRestartAt = null;
  saveState();
}

function errorMatchesCurrentAttempt(err) {
  if (!err || typeof err !== "object") return false;
  const errAttemptId = String(err.attemptId || ((err.details || {}).attemptId) || "").trim();
  const currentAttemptId = String(state.currentAttemptId || "").trim();
  if (errAttemptId && currentAttemptId) {
    return errAttemptId === currentAttemptId;
  }
  const errAttemptStartedAt = String(err.attemptStartedAt || ((err.details || {}).attemptStartedAt) || "").trim();
  const currentAttemptStartedAt = String(state.currentAttemptStartedAt || state.lastStartAt || "").trim();
  return !!(errAttemptStartedAt && currentAttemptStartedAt && errAttemptStartedAt === currentAttemptStartedAt);
}

function currentAttemptLastError() {
  return errorMatchesCurrentAttempt(state.lastError) ? state.lastError : null;
}

function staleAttemptWithoutRuntime(ext = null) {
  if (child && child.exitCode === null && !child.killed) return false;
  if (ext && ext.active) return false;
  if (state.lastExitAt) return false;

  const startedMs = Date.parse(String(state.currentAttemptStartedAt || state.lastStartAt || ""));
  if (!startedMs) return false;
  return (Date.now() - startedMs) > OPERATOR_STALE_ATTEMPT_GRACE_MS;
}

function currentAttemptLastErrorForRuntime(ext = null) {
  const err = currentAttemptLastError();
  if (!err) return null;

  if (staleAttemptWithoutRuntime(ext)) {
    return null;
  }

  const externalStartedMs = Number(
    (ext && (ext.started_ts_ms || (ext.record && ext.record.created_ts_ms))) || 0
  );
  const errorAttemptStartedMs = Date.parse(
    String(err.attemptStartedAt || (err.details && err.details.attemptStartedAt) || "")
  ) || 0;

  if (
    ext &&
    ext.active &&
    ext.managedByChild === false &&
    externalStartedMs > 0 &&
    errorAttemptStartedMs > 0 &&
    externalStartedMs > (errorAttemptStartedMs + 1000)
  ) {
    return null;
  }

  return err;
}

function lastRecordedCrash() {
  const candidate = state.lastCrash || (isCrashLikeError(state.lastError?.kind, state.lastError?.details) ? state.lastError : null);
  return errorMatchesCurrentAttempt(candidate) ? candidate : null;
}

const OPERATOR_RESTART_BASE_MS = clampNumber(process.env.OPERATOR_RESTART_BASE_MS || 3000, 3000, 1000, 600000);
const OPERATOR_RESTART_MAX_MS = clampNumber(process.env.OPERATOR_RESTART_MAX_MS || 60000, 60000, 1000, 3600000);
const OPERATOR_RESTART_WINDOW_MS = clampNumber(process.env.OPERATOR_RESTART_WINDOW_MS || 300000, 300000, 1000, 86400000);
const OPERATOR_RESTART_MAX_IN_WINDOW = clampNumber(process.env.OPERATOR_RESTART_MAX_IN_WINDOW || 6, 6, 1, 100);
const OPERATOR_RESTART_COOLDOWN_MS = clampNumber(process.env.OPERATOR_RESTART_COOLDOWN_MS || 300000, 300000, 1000, 86400000);
const OPERATOR_FATAL_STARTUP_MAX = clampNumber(process.env.OPERATOR_FATAL_STARTUP_MAX || 3, 3, 1, 20);

function clearPendingRestartTimer() {
  if (_pendingRestartTimer) {
    try { clearTimeout(_pendingRestartTimer); } catch (e) { logOperatorCatch("clearPendingRestartTimer", e); }
    _pendingRestartTimer = null;
  }
  state.nextRestartAt = null;
}

function isPreHealthyCrash() {
  const startedAtMs = state.lastStartAt ? (Date.parse(state.lastStartAt) || 0) : 0;
  const healthyAtMs = state.lastHealthyAt ? (Date.parse(state.lastHealthyAt) || 0) : 0;
  return !!startedAtMs && (!healthyAtMs || healthyAtMs < startedAtMs);
}

function tailHasDeterministicFatalSignal(text) {
  return /SyntaxError|IndentationError|ModuleNotFoundError|ImportError|NameError|runtime_architecture_invalid|bootstrap_runtime failed|INGESTION_FAILED_TO_START|missing_ingestion_entry|ingestion_entry_import_failed|Address already in use|run_http_server returned None|HTTP server failed to start/i.test(String(text || ""));
}

function classifyEngineExit(details) {
  const preHealthyCrash = isPreHealthyCrash();
  const stderrTail = String(details?.stderrTail || "");
  const stdoutTail = String(details?.stdoutTail || "");
  const mergedTail = `${stderrTail}\n${stdoutTail}`;
  const deterministic = tailHasDeterministicFatalSignal(mergedTail);
  const nextStartupFailureCount = preHealthyCrash
    ? (Number(state.consecutiveStartupFailures || 0) + 1)
    : 0;

  if (preHealthyCrash && deterministic) {
    return {
      kind: "ENGINE_STARTUP_FATAL",
      message: "Deterministic startup failure before dashboard health bind. Auto-restart suppressed.",
      fatal: true
    };
  }

  if (preHealthyCrash && nextStartupFailureCount >= OPERATOR_FATAL_STARTUP_MAX) {
    return {
      kind: "ENGINE_STARTUP_CRASH_LOOP",
      message: `Startup failed before dashboard health bind ${nextStartupFailureCount} consecutive times. Auto-restart suppressed.`,
      fatal: true
    };
  }

  return {
    kind: "ENGINE_CRASH",
    message: "Engine exited unexpectedly",
    fatal: false
  };
}

function scheduleEngineRestart(reason, message, details) {
  const now = Date.now();

  if (!state._restartWindowStart || (now - Number(state._restartWindowStart || 0)) > OPERATOR_RESTART_WINDOW_MS) {
    state._restartWindowStart = now;
    state._restartCountWindow = 0;
  }

  state._restartCountWindow = Number(state._restartCountWindow || 0) + 1;
  state.restartAttempts = Number(state.restartAttempts || 0) + 1;

  if (details && details.fatal === true) {
    clearPendingRestartTimer();
    saveState();
    return;
  }

  let delayMs;
  if (state._restartCountWindow > OPERATOR_RESTART_MAX_IN_WINDOW) {
    delayMs = OPERATOR_RESTART_COOLDOWN_MS;
  } else {
    const expN = Math.max(0, state._restartCountWindow - 1);
    delayMs = Math.min(OPERATOR_RESTART_MAX_MS, OPERATOR_RESTART_BASE_MS * (2 ** expN));
  }

  clearPendingRestartTimer();
  state.nextRestartAt = new Date(now + delayMs).toISOString();
  appendAttemptMarker(RUNTIME_LOG, state.currentAttemptId, "restart_scheduled", {
    attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
    reason,
    message,
    delayMs,
    nextRestartAt: state.nextRestartAt,
    restartAttempts: state.restartAttempts
  });
  saveState();

  _pendingRestartTimer = setTimeout(() => {
    _pendingRestartTimer = null;
    if (!child) {
      startEngine(state.lastMode || "safe");
    }
  }, delayMs);
}
// --------------------------------------------------
// Utilities
// --------------------------------------------------

function ensureLogDir() {
  if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });
}

const OPERATOR_LOG_ROTATE_MAX_BYTES = Number(process.env.OPERATOR_LOG_ROTATE_MAX_BYTES || (25 * 1024 * 1024));
const OPERATOR_LOG_ROTATE_KEEP = Math.max(1, Number(process.env.OPERATOR_LOG_ROTATE_KEEP || 2));

function appendAttemptMarker(filePath, attemptId, marker, fields = {}) {
  try {
    ensureLogDir();
    const payload = {
      ts: nowIso(),
      scope: "startup_attempt",
      attemptId: attemptId || null,
      marker: String(marker || "event"),
      ...fields
    };
    fs.appendFileSync(filePath, `${JSON.stringify(payload)}
`);
  } catch (e) {
    logOperatorCatch("appendAttemptMarker", e, { filePath, attemptId, marker, fields });
    throw e;
  }
}

function rotateSingleLogIfNeeded(filePath) {
  try {
    if (!fs.existsSync(filePath)) return;
    const st = fs.statSync(filePath);
    if (!st || Number(st.size || 0) < OPERATOR_LOG_ROTATE_MAX_BYTES) return;
    for (let i = OPERATOR_LOG_ROTATE_KEEP; i >= 1; i--) {
      const src = `${filePath}.${i}`;
      const dst = `${filePath}.${i + 1}`;
      if (fs.existsSync(src)) {
        if (i >= OPERATOR_LOG_ROTATE_KEEP) {
          try { fs.unlinkSync(src); } catch (e) { logOperatorCatch("rotateSingleLogIfNeeded.unlink", e, { src, filePath }); }
        } else {
          try { fs.renameSync(src, dst); } catch (e) { logOperatorCatch("rotateSingleLogIfNeeded.rename", e, { src, dst, filePath }); throw e; }
        }
      }
    }
    fs.renameSync(filePath, `${filePath}.1`);
  } catch (e) {
    logOperatorCatch("rotateSingleLogIfNeeded", e, { filePath });
    throw e;
  }
}

function rotateLogsIfNeeded() {
  rotateSingleLogIfNeeded(RUNTIME_LOG);
  rotateSingleLogIfNeeded(ENGINE_STDERR_LOG);
}

function atomicWrite(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });

  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, data);

  try {
    fs.renameSync(tmp, file);
    return;
  } catch (err) {
    if (!err || (err.code !== "EPERM" && err.code !== "EACCES" && err.code !== "EXDEV")) {
      try { fs.unlinkSync(tmp); } catch {}
      throw err;
    }
  }

  try {
    fs.copyFileSync(tmp, file);
  } finally {
    try { fs.unlinkSync(tmp); } catch {}
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function parseEnvText(text) {
  const out = {};
  for (const line of String(text || "").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const idx = trimmed.indexOf("=");
    if (idx === -1) continue;
    const k = trimmed.slice(0, idx).trim();
    const v = trimmed.slice(idx + 1).trim();
    if (k) out[k] = v;
  }
  return out;
}

function serializeEnv(obj) {
  return Object.entries(obj)
    .map(([k, v]) => `${k}=${v}`)
    .join("\n");
}

function readEnvFileRaw() {
  if (!fs.existsSync(ENV_PATH)) return "";
  return fs.readFileSync(ENV_PATH, "utf-8");
}

function readEnv() {
  return parseEnvText(readEnvFileRaw());
}

function normalizeBool(v) {
  const s = String(v ?? "").trim().toLowerCase();
  if (s === "1" || s === "true" || s === "yes" || s === "y" || s === "on") return true;
  if (s === "0" || s === "false" || s === "no" || s === "n" || s === "off") return false;
  return null;
}

function liveExecutionEnvCleared(v) {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "0" || s === "false" || s === "no" || s === "off";
}

function liveExecutionEnvBlocker(v) {
  if (liveExecutionEnvCleared(v)) return null;
  const raw = String(v ?? "").trim();
  return raw ? `DISABLE_LIVE_EXECUTION=${raw}` : "DISABLE_LIVE_EXECUTION unset";
}

function nowIso() {
  return new Date().toISOString();
}

function _normalizeDashHostForLoopback(host) {
  const h = String(host || "").trim();
  // If dashboard binds to 0.0.0.0, callers should use loopback to reach it.
  if (h === "0.0.0.0") return "127.0.0.1";
  if (h.toLowerCase() === "localhost") return "127.0.0.1";
  return h || "127.0.0.1";
}

function dashBaseUrlFromEnv(envObj) {
  const raw =
    (envObj && envObj.DASHBOARD_BASE) ||
    process.env.DASHBOARD_BASE ||
    "http://127.0.0.1:8000";

  let base = String(raw).trim();
  if (!base.startsWith("http://") && !base.startsWith("https://")) {
    base = "http://" + base;
  }
  return base.replace(/\/+$/, "");
}

// --------------------------------------------------
// Typed Config Validation + Sanitization
// --------------------------------------------------

// Keep config aligned with python dashboard defaults.
// NOTE: start_system.py / dashboard_server.py uses 8000 by default.
const ENV_SPEC = [
  { key: "DASHBOARD_HOST", type: "string", required: false, default: "127.0.0.1" },
  { key: "DASHBOARD_PORT", type: "int", required: false, min: 1, max: 65535, default: 8000 },
  { key: "DASHBOARD_API_TOKEN", type: "string", required: false, default: "" },

  { key: "DB_PATH", type: "string", required: false, default: DEFAULT_LOCAL_DB_PATH },

  // Boot behavior (deterministic)
  { key: "AUTO_BOOT_DAEMONS", type: "bool", required: false, default: true },
  { key: "AUTO_BOOT_TARGETS", type: "string", required: false, default: "" },

  { key: "OPERATOR_AUTORESTART", type: "bool", required: false, default: true },
  { key: "OPERATOR_HEALTH_URL", type: "string", required: false, default: "" },
  { key: "OPERATOR_ALLOWED_ORIGIN", type: "string", required: false, default: "http://127.0.0.1:8000" }
];

function validateAndSanitizeEnv(envObj) {
  const sanitized = { ...envObj };
  const issues = [];

  for (const spec of ENV_SPEC) {
    const raw = sanitized[spec.key];

    if (raw === undefined || raw === null || String(raw).trim() === "") {
      if (spec.required) {
        issues.push({ key: spec.key, level: "error", message: "Missing required value" });
      }
      if (spec.default !== undefined) {
        sanitized[spec.key] = String(spec.default);
      }
      continue;
    }

    if (spec.type === "string") {
      sanitized[spec.key] = String(raw).trim();
      continue;
    }

    if (spec.type === "int") {
      const n = Number(String(raw).trim());
      if (!Number.isFinite(n) || !Number.isInteger(n)) {
        issues.push({ key: spec.key, level: "error", message: "Must be an integer" });
        continue;
      }
      if (spec.min !== undefined && n < spec.min) {
        issues.push({ key: spec.key, level: "error", message: `Must be >= ${spec.min}` });
        continue;
      }
      if (spec.max !== undefined && n > spec.max) {
        issues.push({ key: spec.key, level: "error", message: `Must be <= ${spec.max}` });
        continue;
      }
      sanitized[spec.key] = String(n);
      continue;
    }

    if (spec.type === "bool") {
      const b = normalizeBool(raw);
      if (b === null) {
        issues.push({ key: spec.key, level: "error", message: "Must be a boolean (true/false)" });
        continue;
      }
      sanitized[spec.key] = b ? "true" : "false";
      continue;
    }
  }

  return { sanitized, issues };
}

function ensureEnvFile() {
  if (!fs.existsSync(ENV_PATH)) {
    const base = {};
    for (const spec of ENV_SPEC) {
      if (spec.default !== undefined) base[spec.key] = String(spec.default);
    }
    atomicWrite(ENV_PATH, serializeEnv(base));
  }

  // Ensure dashboard token exists (non-technical hardening)
  try {
    const envNow = readEnv();
    const tok = String(envNow.DASHBOARD_API_TOKEN || "").trim();
    const externalToken = (
      readSecretFileText(process.env.DASHBOARD_API_TOKEN_FILE || envNow.DASHBOARD_API_TOKEN_FILE) ||
      readCredentialSecretText(process.env.DASHBOARD_API_TOKEN_SECRET || envNow.DASHBOARD_API_TOKEN_SECRET || "dashboard_api_token")
    );
    if (!tok && !externalToken) {
      const secrets = loadSecrets();
      let token = "";

      // reuse existing token if present
      try {
        if (secrets && secrets.dashboard_api_token_enc) {
          token = String(decrypt(secrets.dashboard_api_token_enc) || "").trim();
        }
      } catch {}

      // generate new token if needed
      if (!token) {
        token = crypto.randomBytes(24).toString("hex");
        try {
          secrets.dashboard_api_token_enc = encrypt(token);
          saveSecrets(secrets);
        } catch {}
      }

      envNow.DASHBOARD_API_TOKEN = token;
      const { sanitized, issues } = validateAndSanitizeEnv(envNow);
      if (!issues.some((i) => i.level === "error")) {
        atomicWrite(ENV_PATH, serializeEnv(sanitized));
      }
    }
  } catch {}
}
function writeEnv(obj) {
  const { sanitized, issues } = validateAndSanitizeEnv(obj);
  if (issues.some((i) => i.level === "error")) {
    setLastError("CONFIG_VALIDATION", "Config validation failed", issues);
    return { ok: false, issues, sanitized };
  }
  safeAtomicWrite(ENV_PATH, serializeEnv(sanitized));
  return { ok: true, issues: [], sanitized };
}

// --------------------------------------------------
// Secrets (AES-256-GCM) + Atomic writes
// --------------------------------------------------

const MASTER_KEY = crypto.createHash("sha256").update(os.hostname()).digest();

function encrypt(text) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", MASTER_KEY, iv);
  const enc = Buffer.concat([cipher.update(String(text), "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, enc]).toString("base64");
}

function decrypt(enc) {
  const buf = Buffer.from(enc, "base64");
  const iv = buf.slice(0, 12);
  const tag = buf.slice(12, 28);
  const data = buf.slice(28);
  const decipher = crypto.createDecipheriv("aes-256-gcm", MASTER_KEY, iv);
  decipher.setAuthTag(tag);
  return decipher.update(data, null, "utf8") + decipher.final("utf8");
}

function loadSecrets() {
  if (!fs.existsSync(SECRETS_PATH)) return {};
  try {
    return JSON.parse(fs.readFileSync(SECRETS_PATH, "utf-8"));
  } catch {
    return {};
  }
}

function saveSecrets(obj) {
  safeAtomicWrite(SECRETS_PATH, JSON.stringify(obj, null, 2));
}

// --------------------------------------------------
// Engine lifecycle
// --------------------------------------------------

const OPERATOR_SPAWN_SYNC_TIMEOUT_MS = clampNumber(process.env.OPERATOR_SPAWN_SYNC_TIMEOUT_MS || 30000, 30000, 1000, 600000);
const OPERATOR_PIP_TIMEOUT_MS = clampNumber(process.env.OPERATOR_PIP_TIMEOUT_MS || 900000, 900000, 1000, 3600000);
const OPERATOR_VALIDATION_TIMEOUT_MS = clampNumber(process.env.OPERATOR_VALIDATION_TIMEOUT_MS || 180000, 180000, 1000, 1800000);

function spawnSyncSafe(cmd, args, options = {}) {
  const timeout = Number.isFinite(Number(options.timeout)) ? Number(options.timeout) : OPERATOR_SPAWN_SYNC_TIMEOUT_MS;
  const maxBuffer = Number.isFinite(Number(options.maxBuffer)) ? Number(options.maxBuffer) : 16 * 1024 * 1024;
  return spawnSync(cmd, args, {
    encoding: "utf-8",
    ...options,
    timeout,
    maxBuffer
  });
}

function runServiceCtl(args, { parseJson = true, timeout = 30000 } = {}) {
  try {
    const r = spawnSync(
      SERVICE_CTL,
      args,
      {
        cwd: ROOT,
        env: { ...process.env, TRADING_REPO: ROOT },
        encoding: "utf-8",
        timeout
      }
    );

    const stdout = String(r.stdout || "").trim();
    const stderr = String(r.stderr || "").trim();
    const combined = [stdout, stderr].filter(Boolean).join("\n").trim();

    if (r.error) {
      return { ok: false, error: String(r.error), stdout, stderr, status: r.status };
    }

    if ((r.status || 0) !== 0) {
      if (parseJson) {
        try {
          const obj = JSON.parse(stdout || "{}");
          return { ok: false, ...obj, stdout, stderr, status: r.status };
        } catch {}
      }
      return { ok: false, error: combined || `service_ctl_failed:${r.status}`, stdout, stderr, status: r.status };
    }

    if (!parseJson) {
      return { ok: true, text: stdout, stdout, stderr, status: r.status };
    }

    try {
      return { ok: true, ...(JSON.parse(stdout || "{}")), stdout, stderr, status: r.status };
    } catch {
      return { ok: false, error: "invalid_service_ctl_json", stdout, stderr, status: r.status };
    }
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

function managedEngineState() {
  if (!isLinuxManagedMode()) {
    return {
      ok: false,
      status: "LOCAL",
      mode: "unmanaged"
    };
  }

  const r = runServiceCtl(["status", "engine"]);
  if (!r || !r.ok) return { ok: false, active: "unknown", detail: r };

  const active = String(r.active || "").toLowerCase();
  if (active === "active") return { ok: true, status: "RUNNING", active, detail: r };
  if (active === "activating") return { ok: true, status: "STARTING", active, detail: r };
  if (active === "deactivating") return { ok: true, status: "STOPPING", active, detail: r };
  return { ok: true, status: "STOPPED", active, detail: r };
}

function readManagedAttemptJournal(lines = 200) {
  if (!isLinuxManagedMode()) return "";
  const since = String(state.currentAttemptStartedAt || state.lastStartAt || "").trim();
  if (!since) return "";
  const r = runServiceCtl(["logs_since", "engine", since, String(Math.max(50, Number(lines || 200) * 3))], {
    parseJson: false,
    timeout: 30000
  });
  if (!r || !r.ok) return "";
  return String(r.text || r.stdout || "").trim();
}

function status() {
  if (installing) return "INSTALLING";

  if (state.restartBlocked || state.fatal) {
    return "FATAL";
  }

  if (isLinuxManagedMode()) {
    const managed = managedEngineState();
    if (managed && managed.ok) {
      return managed.status;
    }
    return "STOPPED";
  }

  if (child && child.exitCode === null && !child.killed) return "RUNNING";

  const ext = externalRuntimeState();
  if (ext.active) return "RUNNING";

  const runtimePidState = validateOrClearPidFile(path.join(LOG_DIR, "runtime.pid"), "runtime");
  if (runtimePidState.active) return "RUNNING";

  if (
    state.currentAttemptId &&
    state.currentAttemptStartedAt &&
    !state.lastExitAt
  ) {
    const startedMs = Date.parse(String(state.currentAttemptStartedAt || ""));
    if (startedMs && (Date.now() - startedMs) <= OPERATOR_STALE_ATTEMPT_GRACE_MS) {
      return "STARTING";
    }
  }

  return "STOPPED";
}

function pickPythonCmd() {
  const override = String(process.env.OPERATOR_PYTHON || "").trim();
  const candidates = [];

  if (override) {
    candidates.push(override);
  }

  candidates.push("python3", "python");

  for (const c of candidates) {
    try {
      const args = ["--version"];
      const r = spawnSyncSafe(c, args, { stdio: "pipe" });

      if (r && !r.error && r.status === 0) {
        try {
          const exeArgs = ["-c", "import sys; print(sys.executable)"];
          const exe = spawnSyncSafe(c, exeArgs, { stdio: "pipe" });
          const resolved = String(
            (exe && (exe.stdout || exe.text || "")) || ""
          ).trim();
          if (resolved && fs.existsSync(resolved)) {
            return resolved;
          }
        } catch {}
        return c;
      }
    } catch {}
  }

  return "python3";
}

function spawnEngineProcess(python, sanitized, finalMode) {
  const args = [ENTRY, finalMode];

  const spawnOptions = {
    cwd: ROOT,
    env: {
      ...process.env,
      ...sanitized,
      PYTHONPATH: ROOT
    },
    stdio: ["ignore", "pipe", "pipe"]
  };

  spawnOptions.detached = true;

  return spawn(python, args, spawnOptions);
}

function killPidTree(pid, signalName = "SIGTERM") {
  if (!pid) return;

  try {
    process.kill(-pid, signalName);
    return;
  } catch {}

  try {
    process.kill(pid, signalName);
  } catch {}
}

function _pidIsDescendantOf(pid, ancestorPid) {
  const targetPid = Number(pid || 0);
  const rootPid = Number(ancestorPid || 0);
  if (!(targetPid > 0) || !(rootPid > 0) || targetPid === rootPid) return false;

  try {
    let cur = targetPid;
    for (let depth = 0; depth < 32; depth += 1) {
      const r = spawnSyncSafe("ps", ["-o", "ppid=", "-p", String(cur)], {
        stdio: "pipe",
        timeout: 2000
      });
      if (!r || r.error || r.status !== 0) return false;
      cur = Number(String(r.stdout || r.text || "").trim() || 0);
      if (!(cur > 0)) return false;
      if (cur === rootPid) return true;
    }
  } catch {}
  return false;
}

function _processCommandLine(pid) {
  const targetPid = Number(pid || 0);
  if (!(targetPid > 0)) return "";

  try {
    const r = spawnSyncSafe("ps", ["-o", "args=", "-p", String(targetPid)], {
      stdio: "pipe",
      timeout: 2000
    });
    if (r && !r.error && r.status === 0) return String(r.stdout || r.text || "").trim();
  } catch {}
  return "";
}

function commandLineLooksLikeStartSystem(commandLine) {
  const lowered = String(commandLine || "").toLowerCase();
  const entryNameLower = path.basename(ENTRY).toLowerCase();
  return !!lowered && lowered.includes(entryNameLower) && !lowered.includes("start_ingestion.py");
}

function _repoStartSystemCandidates() {
  const cached = readTimedCache(_repoStartSystemCandidatesCache, OPERATOR_PROCESS_SCAN_CACHE_TTL_MS);
  if (cached) return cached;

  const childPid = Number((child && child.pid) || 0);
  const rootLower = path.resolve(ROOT).toLowerCase();
  const entryLower = path.resolve(ENTRY).toLowerCase();
  const entryNameLower = path.basename(ENTRY).toLowerCase();
  const ingestionRecord = readPidFileRecord(path.join(LOG_DIR, "ingestion.pid"), "ingestion");
  const ingestionOwnerPid = pidRecordBelongsHere(ingestionRecord, "ingestion")
    ? Number(ingestionRecord.owner_pid || 0)
    : 0;
  const candidates = [];

  const considerCandidate = (pid, ppid, commandLine, creationDate = null) => {
    const normalizedPid = Number(pid || 0);
    const normalizedPpid = Number(ppid || 0);
    const text = String(commandLine || "");
    const lowered = text.toLowerCase();

    if (!(normalizedPid > 0)) return;
    if (!lowered) return;
    if (!lowered.includes(entryNameLower)) return;
    if (!(lowered.includes(entryLower) || lowered.includes(`\\${entryNameLower}`) || lowered.includes(`/${entryNameLower}`))) {
      return;
    }
    if (!lowered.includes(rootLower) && normalizedPid !== ingestionOwnerPid) {
      return;
    }
    if (childPid > 0 && (normalizedPid === childPid || _pidIsDescendantOf(normalizedPid, childPid))) {
      return;
    }

    const startedMs = Date.parse(String(creationDate || "")) || 0;
    candidates.push({
      pid: normalizedPid,
      ppid: normalizedPpid > 0 ? normalizedPpid : 0,
      commandLine: text,
      started_ts_ms: startedMs || null
    });
  };

  try {
    const r = spawnSyncSafe("ps", ["-eo", "pid=,ppid=,args="], {
      stdio: "pipe",
      timeout: 5000
    });
    if (r && !r.error && r.status === 0) {
      const raw = String(r.stdout || r.text || "");
      for (const line of raw.split(/\r?\n/)) {
        const match = String(line || "").trim().match(/^(\d+)\s+(\d+)\s+(.*)$/);
        if (!match) continue;
        considerCandidate(match[1], match[2], match[3]);
      }
    }
  } catch {}

  candidates.sort((a, b) => a.pid - b.pid);
  _repoStartSystemCandidatesCache = { tsMs: Date.now(), payload: cloneJsonValue(candidates) };
  return candidates;
}

function stopEngineProcessTree(graceMs = 2500) {
  if (!child) return false;

  const pid = child.pid;

  killPidTree(pid, "SIGTERM");

  setTimeout(() => {
    try {
      if (operatorPidIsRunning(pid)) {
        killPidTree(pid, "SIGKILL");
      }
    } catch {}
  }, graceMs);

  return true;
}

function _computeExternalRuntimeState() {
  if (isLinuxManagedMode()) {
    return { ok: false, active: false, managed: true };
  }

  const childPidEarly = Number((child && child.pid) || 0);
  if (childPidEarly > 0 && child && child.exitCode === null && !child.killed) {
    return {
      ok: true,
      active: false,
      pid: 0,
      pids: [],
      childPid: childPidEarly,
      record: null,
      managedByChild: true,
      source: "operator_child"
    };
  }

  const runtimePidState = validateOrClearPidFile(path.join(LOG_DIR, "runtime.pid"), "runtime");
  const pid = Number(runtimePidState.pid || 0);
  const childPid = Number((child && child.pid) || 0);
  const managedByChild = !!childPid && (childPid === pid || _pidIsDescendantOf(pid, childPid));
  if (!!runtimePidState.active && pid > 0 && !managedByChild) {
    return {
      ok: true,
      active: true,
      pid,
      pids: [pid],
      childPid: childPid || null,
      record: runtimePidState.record || null,
      managedByChild: false,
      source: "runtime_pid"
    };
  }

  const ingestionPidState = validateOrClearPidFile(path.join(LOG_DIR, "ingestion.pid"), "ingestion");
  const ingestionRecord = ingestionPidState.record || {};
  const ingestionPid = Number(ingestionPidState.pid || 0);
  const ingestionOwnerPid = Number(ingestionRecord.owner_pid || 0);
  const ingestionOwnerCommandLine = _processCommandLine(ingestionOwnerPid);
  const ingestionManagedByChild = !!childPid && (
    childPid === ingestionOwnerPid ||
    childPid === ingestionPid ||
    _pidIsDescendantOf(ingestionOwnerPid, childPid) ||
    _pidIsDescendantOf(ingestionPid, childPid)
  );
  if (
    ingestionPidState.active &&
    ingestionOwnerPid > 0 &&
    operatorPidIsRunning(ingestionOwnerPid) &&
    commandLineLooksLikeStartSystem(ingestionOwnerCommandLine) &&
    !ingestionManagedByChild
  ) {
    return {
      ok: true,
      active: true,
      pid: ingestionOwnerPid,
      pids: [ingestionOwnerPid, ingestionPid].filter((value, idx, arr) => value > 0 && arr.indexOf(value) === idx),
      childPid: childPid || null,
      record: ingestionRecord || null,
      ownerCommandLine: ingestionOwnerCommandLine,
      managedByChild: false,
      source: "ingestion_pid_owner",
      started_ts_ms: Number(ingestionRecord.created_ts_ms || 0) || null
    };
  }

  const orphanCandidates = _repoStartSystemCandidates();
  if (orphanCandidates.length > 0) {
    return {
      ok: true,
      active: true,
      pid: Number(orphanCandidates[0].pid || 0),
      pids: orphanCandidates.map((row) => Number(row.pid || 0)).filter((value) => value > 0),
      childPid: childPid || null,
      record: null,
      managedByChild: false,
      source: "process_scan",
      started_ts_ms: Number(orphanCandidates[0].started_ts_ms || 0) || null,
      processes: orphanCandidates
    };
  }

  return {
    ok: true,
    active: false,
    pid,
    pids: [],
    childPid: childPid || null,
    record: runtimePidState.record || null,
    managedByChild,
    source: "none"
  };
}

function externalRuntimeState(options = {}) {
  if (!(options && options.force)) {
    const cached = readTimedCache(_externalRuntimeCache, OPERATOR_EXTERNAL_RUNTIME_CACHE_TTL_MS);
    if (cached) return cached;
  }

  const result = _computeExternalRuntimeState();
  _externalRuntimeCache = { tsMs: Date.now(), payload: cloneJsonValue(result) };
  return result;
}

function waitForPidExit(pid, timeoutMs = 8000) {
  return new Promise((resolve) => {
    const started = Date.now();

    const tick = () => {
      if (!operatorPidIsRunning(pid)) {
        resolve(true);
        return;
      }
      if (Date.now() - started >= Math.max(1000, Number(timeoutMs || 0))) {
        resolve(false);
        return;
      }
      setTimeout(tick, 250);
    };

    tick();
  });
}

async function stopExternalRuntime(graceMs = 8000) {
  invalidateOperatorRuntimeCaches();
  const ext = externalRuntimeState();
  const targetPids = Array.isArray(ext.pids) ? ext.pids.filter((value) => Number(value || 0) > 0) : [];
  if (!ext.active || targetPids.length === 0) {
    return { ok: true, active: false, stopped: true };
  }

    try {
      for (const targetPid of targetPids) {
        try {
          killPidTree(targetPid, "SIGTERM");
        } catch {}
        const exited = await waitForPidExit(targetPid, graceMs);
        if (!exited) {
          try { killPidTree(targetPid, "SIGKILL"); } catch {}
        }
    }

    let stopped = true;
    for (const targetPid of targetPids) {
      const exited = await waitForPidExit(targetPid, graceMs);
      if (!exited) stopped = false;
    }
    if (stopped) {
      validateOrClearPidFile(path.join(LOG_DIR, "runtime.pid"), "runtime");
      validateOrClearPidFile(path.join(LOG_DIR, "ingestion.pid"), "ingestion");
      invalidateOperatorRuntimeCaches();
    }

    return {
      ok: !!stopped,
      active: !stopped,
      stopped: !!stopped,
      pid: Number(targetPids[0] || 0),
      pids: targetPids,
      record: ext.record || null
    };
  } catch (e) {
    return {
      ok: false,
      active: true,
      stopped: false,
      pid: Number(targetPids[0] || 0),
      pids: targetPids,
      record: ext.record || null,
      error: String(e)
    };
  }
}

function waitForEngineExit(timeoutMs = 10000) {
  return new Promise((resolve) => {
    if (!child) {
      resolve({ ok: true, alreadyStopped: true });
      return;
    }

    const proc = child;
    let settled = false;

    const finish = (payload) => {
      if (settled) return;
      settled = true;
      try { clearTimeout(timer); } catch {}
      try { proc.removeListener("exit", onExit); } catch {}
      resolve(payload);
    };

    const onExit = (code, signal) => finish({ ok: true, code, signal });
    const timer = setTimeout(
      () => finish({ ok: false, timeout: true }),
      Math.max(250, Number(timeoutMs) || 10000)
    );

    proc.once("exit", onExit);

    if (proc.exitCode !== null || proc.killed) {
      finish({ ok: true, code: proc.exitCode, signal: null });
    }
  });
}

async function restartEngineGracefully(mode = "safe", options = {}) {
  if (_restartInFlight) {
    return { ok: false, error: "restart_in_progress", status: status() };
  }

  _restartInFlight = true;
  try {
    const normalizedMode = String(mode || "safe").trim().toLowerCase();
    const finalMode = (normalizedMode === "live" || normalizedMode === "shadow") ? normalizedMode : "safe";
    const stopped = stopEngine();
    const wait = await waitForEngineExit(options.stopTimeoutMs || 15000);

    if (!wait.ok && child) {
      return { ok: false, error: "engine_stop_timeout", stopped, wait, status: status() };
    }

    const started = startEngine(finalMode);
    return {
      ok: !!(stopped && stopped.ok && started && started.ok),
      stopped,
      wait,
      started,
      mode: finalMode,
      status: status()
    };
  } finally {
    _restartInFlight = false;
  }
}

function startEngine(mode = "safe") {
  invalidateOperatorHealthCache();
  invalidateOperatorRuntimeCaches();
  if (OPERATOR_DISABLE_INTERNAL_ENGINE_START) {
    return {
      ok: false,
      status: status(),
      disabled: true,
      reason: "OPERATOR_DISABLE_INTERNAL_ENGINE_START"
    };
  }

  if (!isLinuxManagedMode() && child) return { ok: true, status: "RUNNING" };

  clearPendingRestartTimer();
  ensureEnvFile();
  ensureLogDir();
  rotateLogsIfNeeded();

  const runtimePidState = validateOrClearPidFile(path.join(LOG_DIR, "runtime.pid"), "runtime");
  validateOrClearPidFile(path.join(LOG_DIR, "ingestion.pid"), "ingestion");
  if (!isLinuxManagedMode()) {
    const ext = externalRuntimeState();
    if (ext.active) {
      const orphanDetail = {
        pid: Number(ext.pid || runtimePidState.pid || 0),
        pids: Array.isArray(ext.pids) ? ext.pids : [Number(ext.pid || 0)].filter((value) => value > 0),
        record: ext.record || null,
        source: String(ext.source || "runtime_pid"),
        processes: Array.isArray(ext.processes) ? ext.processes.slice(0, 8) : []
      };
      setLastError(
        "ENGINE_UNMANAGED_RUNNING",
        "Cannot start: existing unmanaged trading runtime detected",
        orphanDetail
      );
      return {
        ok: false,
        status: "RUNNING",
        pid: Number(ext.pid || runtimePidState.pid || 0),
        pids: orphanDetail.pids,
        unmanaged: true,
        record: ext.record || null,
        source: orphanDetail.source,
        processes: orphanDetail.processes
      };
    }
  }

  const current = readEnv();
  const { sanitized, issues } = validateAndSanitizeEnv(current);
  if (issues.some((i) => i.level === "error")) {
    setLastError("CONFIG_INVALID", "Cannot start: invalid .env", issues);
    return { ok: false, status: "STOPPED", issues };
  }

  const validationGate = runProductionValidationGate(sanitized);
  if (!validationGate.ok) {
    setLastError("VALIDATION_GATE_FAILED", "Cannot start: production validation gate failed", validationGate.checks);
    try {
      if (agentCooldownOk("validation_failed")) {
        runAgent(_llm).catch((err) => {
          logOperatorCatch("runAgent.validation_failed", err, { checks: validationGate.checks });
        });
      }
    } catch (err) {
      logOperatorCatch("runAgent.validation_failed.dispatch", err, { checks: validationGate.checks });
    }
    return { ok: false, status: "STOPPED", issues: validationGate.checks, validationGate };
  }

  const m = String(mode || "safe").toLowerCase().trim();
  const finalMode = (m === "live" || m === "shadow") ? m : "safe";

  sanitized.AUTO_BOOT_DAEMONS = "true";
  sanitized.EXECUTION_MODE = finalMode;
  sanitized.ENGINE_MODE = finalMode;
  sanitized.OPERATOR_MODE = finalMode;

  atomicWrite(ENV_PATH, serializeEnv(sanitized));

  const python = pickPythonCmd();
  const attemptStartedAt = nowIso();
  const attemptId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const runtimeOffset = fs.existsSync(RUNTIME_LOG) ? fs.statSync(RUNTIME_LOG).size : 0;
  const stderrOffset = fs.existsSync(ENGINE_STDERR_LOG) ? fs.statSync(ENGINE_STDERR_LOG).size : 0;

  state.lastStartAt = attemptStartedAt;
  state.lastExitCode = null;
  state.lastExitAt = null;
  state.lastExitSignal = null;
  state.lastMode = finalMode;
  state.lastStopAt = null;
  state.stopRequestedAt = null;
  state.currentAttemptId = attemptId;
  state.currentAttemptStartedAt = attemptStartedAt;
  state.currentRuntimeLogOffset = runtimeOffset;
  state.currentStderrLogOffset = stderrOffset;
  state.lastError = null;
  state.lastCrash = null;
  state.restartBlocked = false;
  state.fatal = false;
  state.fatalSince = null;
  state.nextRestartAt = null;
  saveState();

  const logStream = fs.createWriteStream(RUNTIME_LOG, { flags: "a" });
  const stderrStream = fs.createWriteStream(ENGINE_STDERR_LOG, { flags: "a" });
  appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_begin", {
    attemptStartedAt,
    mode: finalMode,
    python,
    managed: isLinuxManagedMode()
  });
  appendAttemptMarker(ENGINE_STDERR_LOG, attemptId, "startup_begin", {
    attemptStartedAt,
    mode: finalMode,
    python,
    managed: isLinuxManagedMode(),
    stream: "stderr"
  });

  const boot = runPythonBootstrap(python);
  if (!boot.ok) {
    const bootTail = JSON.stringify(boot.steps || [], null, 2);
    const bootDetails = {
      attemptId,
      attemptStartedAt,
      steps: boot.steps || [],
      stderrTail: bootTail,
      stdoutTail: "",
      preHealthyCrash: true
    };
    const crash = classifyEngineExit(bootDetails);

    state.lastExitCode = 1;
    state.lastExitAt = nowIso();
    state.lastExitSignal = null;
    state.consecutiveStartupFailures = Number(state.consecutiveStartupFailures || 0) + 1;

    bootDetails.errorKind = crash.kind;
    bootDetails.errorMessage = crash.message;
    bootDetails.fatal = !!crash.fatal;

    logStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] bootstrap FAILED: ${bootTail}\n`);
    stderrStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] bootstrap FAILED: ${bootTail}\n`);
    appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_crash", {
      attemptStartedAt,
      phase: "bootstrap",
      reason: crash.kind,
      fatal: !!crash.fatal
    });
    appendAttemptMarker(ENGINE_STDERR_LOG, attemptId, "startup_crash", {
      attemptStartedAt,
      phase: "bootstrap",
      reason: crash.kind,
      fatal: !!crash.fatal,
      stream: "stderr"
    });

    if (crash.fatal) {
      setFatalRestartBlock(crash.kind, crash.message, bootDetails);
    } else {
      setLastError(crash.kind, crash.message, bootDetails);
    }

    try { stderrStream.end(); } catch {}
    try { logStream.end(); } catch {}
    saveState();
    return { ok: false, status: "STOPPED", bootstrap: boot, error: bootDetails };
  } else {
    logStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] bootstrap ok\n`);
  }

  try {
    const targetExecMode =
      finalMode === "live" ? "live" :
      finalMode === "shadow" ? "shadow" :
      "paper";

    const pyArgs = python === "py" ? ["-3", "-u", "-c"] : ["-u", "-c"];

    const sync = spawnSyncSafe(
      python,
      [
        ...pyArgs,
        "from engine.execution.execution_mode import set_execution_mode, set_execution_armed; "
          + `set_execution_mode(${JSON.stringify(targetExecMode)}, actor='operator_server', reason='operator_start', keep_armed=False); `
          + "set_execution_armed(0, actor='operator_server', reason='operator_start_disarmed'); "
          + "print('ok')"
      ],
      {
        cwd: ROOT,
        env: {
          ...process.env,
          ...sanitized,
          PYTHONPATH: ROOT
        },
        stdio: "pipe"
      }
    );

    const syncOut =
      (sync.stdout ? String(sync.stdout) : "") +
      (sync.stderr ? String(sync.stderr) : "");

    if (sync.status !== 0) {
      logStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] execution safety sync FAILED: ${syncOut}\n`);
      stderrStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] execution safety sync FAILED: ${syncOut}\n`);
      appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_crash", {
        attemptStartedAt,
        phase: "execution_safety_sync",
        reason: "EXECUTION_SAFETY_SYNC_FAIL"
      });
      appendAttemptMarker(ENGINE_STDERR_LOG, attemptId, "startup_crash", {
        attemptStartedAt,
        phase: "execution_safety_sync",
        reason: "EXECUTION_SAFETY_SYNC_FAIL",
        stream: "stderr"
      });
      setLastError("EXECUTION_SAFETY_SYNC_FAIL", "Failed to synchronize DB execution mode", {
        attemptId,
        attemptStartedAt,
        syncOut: syncOut.trim()
      });
      try { stderrStream.end(); } catch {}
      try { logStream.end(); } catch {}
      return { ok: false, status: "STOPPED", sync: syncOut.trim() };
    }

    logStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] execution safety sync ok mode=${targetExecMode} armed=0\n`);
  } catch (e) {
    logStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] execution safety sync exception: ${String(e)}\n`);
    stderrStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] execution safety sync exception: ${String(e)}\n`);
    appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_crash", {
      attemptStartedAt,
      phase: "execution_safety_sync",
      reason: "EXECUTION_SAFETY_SYNC_EXCEPTION"
    });
    appendAttemptMarker(ENGINE_STDERR_LOG, attemptId, "startup_crash", {
      attemptStartedAt,
      phase: "execution_safety_sync",
      reason: "EXECUTION_SAFETY_SYNC_EXCEPTION",
      stream: "stderr"
    });
    setLastError("EXECUTION_SAFETY_SYNC_EXCEPTION", "Execution safety sync exception", {
      attemptId,
      attemptStartedAt,
      error: String(e)
    });
    try { stderrStream.end(); } catch {}
    try { logStream.end(); } catch {}
    return { ok: false, status: "STOPPED", sync: String(e) };
  }

  if (isLinuxManagedMode()) {
    const svc = runServiceCtl(["start", "engine"]);
    if (!svc.ok) {
      appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_crash", {
        attemptStartedAt,
        phase: "service_start",
        reason: "ENGINE_SERVICE_START_FAIL"
      });
      appendAttemptMarker(ENGINE_STDERR_LOG, attemptId, "startup_crash", {
        attemptStartedAt,
        phase: "service_start",
        reason: "ENGINE_SERVICE_START_FAIL",
        stream: "stderr"
      });
      setLastError("ENGINE_SERVICE_START_FAIL", "Failed to start trading-engine.service", {
        attemptId,
        attemptStartedAt,
        service: svc
      });
      try { stderrStream.end(); } catch {}
      try { logStream.end(); } catch {}
      return { ok: false, status: "STOPPED", service: svc };
    }

    appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_begin_managed", {
      attemptStartedAt,
      phase: "systemd_start_requested",
      service: "trading-engine.service"
    });
    saveState();
    clearLastError();
    try { stderrStream.end(); } catch {}
    try { logStream.end(); } catch {}
    return { ok: true, status: "STARTING", mode: finalMode, managed: true, service: svc };
  }

  child = spawnEngineProcess(python, sanitized, finalMode);

  const stderrChunks = [];
  const stdoutChunks = [];

  function pushChunk(chunks, value) {
    chunks.push(value);
    if (chunks.length > 50) chunks.shift();
  }

  if (child.stdout) {
    child.stdout.on("data", (d) => {
      const s = String(d);
      pushChunk(stdoutChunks, s);
      try { logStream.write(s); } catch (e) { logOperatorCatch("engine_stdout_write", e); }
    });
  }

  if (child.stderr) {
    child.stderr.on("data", (d) => {
      const s = String(d);
      pushChunk(stderrChunks, s);
      try { stderrStream.write(s); } catch (e) { logOperatorCatch("engine_stderr_write", e); }
      try { logStream.write(s); } catch (e) { logOperatorCatch("engine_stderr_runtime_write", e); }
    });
  }

  saveState();

  child.on("error", (err) => {
    stderrStream.write(`[${nowIso()}] [attempt:${attemptId}] [startup] ENGINE_SPAWN_ERROR: ${String(err)}\n`);
    appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_crash", {
      attemptStartedAt,
      phase: "spawn",
      reason: "ENGINE_SPAWN_ERROR"
    });
    appendAttemptMarker(ENGINE_STDERR_LOG, attemptId, "startup_crash", {
      attemptStartedAt,
      phase: "spawn",
      reason: "ENGINE_SPAWN_ERROR",
      stream: "stderr"
    });
    setLastError("ENGINE_SPAWN_ERROR", "Engine process spawn failed", {
      attemptId,
      attemptStartedAt,
      message: String(err)
    });
  });

  child.on("exit", (code, signal) => {
    const preHealthyCrash = isPreHealthyCrash();
    const stopRequestedAt = state.stopRequestedAt ? (Date.parse(state.stopRequestedAt) || 0) : 0;
    const exitAtIso = nowIso();
    const exitAtMs = Date.parse(exitAtIso) || Date.now();
    const intentionalStop = !!stopRequestedAt && stopRequestedAt <= exitAtMs;

    state.lastExitCode = code;
    state.lastExitAt = exitAtIso;
    state.lastExitSignal = signal || null;
    child = null;

    try { stderrStream.end(); } catch {}
    try { logStream.end(); } catch {}

    if (code === 0 || intentionalStop) {
      appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_end", {
        attemptStartedAt,
        endedAt: exitAtIso,
        exitCode: code,
        signal,
        clean: true,
        managed: false,
        reason: intentionalStop ? "operator_stop" : "process_exit_clean"
      });
      appendAttemptMarker(ENGINE_STDERR_LOG, attemptId, "startup_end", {
        attemptStartedAt,
        endedAt: exitAtIso,
        exitCode: code,
        signal,
        clean: true,
        managed: false,
        reason: intentionalStop ? "operator_stop" : "process_exit_clean",
        stream: "stderr"
      });
      clearPendingRestartTimer();
      state.restartAttempts = 0;
      state._restartWindowStart = null;
      state._restartCountWindow = 0;
      state.consecutiveStartupFailures = 0;
      state.restartBlocked = false;
      state.fatal = false;
      state.fatalSince = null;
      state.lastStopAt = intentionalStop ? exitAtIso : state.lastStopAt;
      state.stopRequestedAt = null;
      saveState();
      return;
    }

    if (preHealthyCrash) {
      state.consecutiveStartupFailures = Number(state.consecutiveStartupFailures || 0) + 1;
    } else {
      state.consecutiveStartupFailures = 0;
    }

    const stderrTail = stderrChunks.slice(-50).join("");
    const stdoutTail = stdoutChunks.slice(-50).join("");
    const exitDetails = {
      code,
      signal,
      stderrTail,
      stdoutTail,
      runtimeTail: currentAttemptRuntimeTail(200),
      preHealthyCrash
    };
    const crash = classifyEngineExit(exitDetails);

    exitDetails.errorKind = crash.kind;
    exitDetails.errorMessage = crash.message;
    exitDetails.fatal = !!crash.fatal;
    exitDetails.consecutiveStartupFailures = Number(state.consecutiveStartupFailures || 0);

    appendAttemptMarker(RUNTIME_LOG, attemptId, "startup_crash", {
      attemptStartedAt,
      endedAt: nowIso(),
      exitCode: code,
      signal,
      reason: crash.kind,
      fatal: !!crash.fatal
    });
    appendAttemptMarker(ENGINE_STDERR_LOG, attemptId, "startup_crash", {
      attemptStartedAt,
      endedAt: nowIso(),
      exitCode: code,
      signal,
      reason: crash.kind,
      fatal: !!crash.fatal,
      stream: "stderr"
    });

    const envNow = readEnv();
    const ar = normalizeBool(envNow.OPERATOR_AUTORESTART);
    const autoRestartEnabled = ar === null ? true : ar;

    if (!autoRestartEnabled || crash.fatal) {
      if (crash.fatal) {
        setFatalRestartBlock(crash.kind, crash.message, exitDetails);
      } else {
        clearPendingRestartTimer();
        try {
  if (agentCooldownOk(crash.kind)) {
    console.log("[AI] crash-triggered analysis");

    runAgent(_llm).catch((e) => {
      console.warn("[AI] crash agent error", e);
    });
  }
} catch {}
      }
      saveState();
      return;
    }

    state.restartBlocked = false;
    state.fatal = false;
    state.fatalSince = null;
    try {
  if (agentCooldownOk(crash.kind)) {
    console.log("[AI] crash-triggered analysis");

    runAgent(_llm).catch((e) => {
      console.warn("[AI] crash agent error", e);
    });
  }
} catch {}
    scheduleEngineRestart(crash.kind, crash.message, exitDetails);
  });

  clearLastError();
  return { ok: true, status: "STARTING", mode: finalMode, managed: false };
}

function stopEngine() {
  invalidateOperatorHealthCache();
  invalidateOperatorRuntimeCaches();
  clearPendingRestartTimer();

  if (isLinuxManagedMode()) {
    const svc = runServiceCtl(["stop", "engine"]);
    if (!svc.ok) {
      setLastError("STOP_FAILED", "Failed to stop trading-engine.service", svc);
      return { ok: false, service: svc };
    }

    state.lastStopAt = nowIso();
    appendAttemptMarker(RUNTIME_LOG, state.currentAttemptId, "startup_end", {
      attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
      endedAt: state.lastStopAt,
      clean: true,
      managed: true,
      reason: "operator_stop"
    });
    saveState();
    return { ok: true, status: "STOPPING", managed: true, service: svc };
  }

  if (!child) {
    const ext = externalRuntimeState();
    if (ext.active) {
      const stopAt = nowIso();
      state.lastStopAt = stopAt;
      state.stopRequestedAt = stopAt;
      let stopped = false;

      try {
        const targetPids = Array.isArray(ext.pids) ? ext.pids.filter((value) => Number(value || 0) > 0) : [];
        for (const targetPid of targetPids) {
          killPidTree(targetPid, "SIGTERM");
        }
        stopped = targetPids.length > 0;
      } catch {}

      validateOrClearPidFile(path.join(LOG_DIR, "runtime.pid"), "runtime");
      validateOrClearPidFile(path.join(LOG_DIR, "ingestion.pid"), "ingestion");
      invalidateOperatorRuntimeCaches();
      appendAttemptMarker(RUNTIME_LOG, state.currentAttemptId, "startup_end", {
        attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
        endedAt: state.lastStopAt,
        clean: true,
        managed: false,
        reason: "operator_stop_external_runtime",
        externalPid: ext.pid,
        externalPids: Array.isArray(ext.pids) ? ext.pids : [ext.pid]
      });
      saveState();
      return {
        ok: !!stopped,
        status: (stopped ? "STOPPING" : "STOPPED"),
        external: true,
        pid: ext.pid,
        pids: Array.isArray(ext.pids) ? ext.pids : [ext.pid]
      };
    }

    const stopAt = nowIso();
    const shouldAppendStopMarker =
      !state.stopRequestedAt &&
      !!(state.currentAttemptStartedAt || state.lastStartAt);

    state.lastStopAt = stopAt;
    state.stopRequestedAt = stopAt;

    if (shouldAppendStopMarker) {
      appendAttemptMarker(RUNTIME_LOG, state.currentAttemptId, "startup_end", {
        attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
        endedAt: state.lastStopAt,
        clean: true,
        managed: false,
        reason: "operator_stop_no_child"
      });
    }

    saveState();
    return { ok: true, status: "STOPPED" };
  }

  try {
    state.stopRequestedAt = nowIso();
    stopEngineProcessTree(2500);
  } catch (e) {
    state.stopRequestedAt = null;
    setLastError("STOP_FAILED", "Failed to stop engine", { message: String(e) });
    return { ok: false };
  }

  state.lastStopAt = nowIso();
  appendAttemptMarker(RUNTIME_LOG, state.currentAttemptId, "startup_end", {
    attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
    endedAt: state.lastStopAt,
    clean: true,
    managed: false,
    reason: "operator_stop"
  });
  saveState();
  return { ok: true, status: "STOPPING", managed: false };
}

function emergencyStop() {
  invalidateOperatorHealthCache();
  try {
    ensureEnvFile();
    const envObj = readEnv();
    const { sanitized } = validateAndSanitizeEnv(envObj);

    sanitized.ENGINE_MODE = "safe";
    sanitized.EXECUTION_MODE = "safe";
    sanitized.OPERATOR_MODE = "safe";
    sanitized.DISABLE_LIVE_EXECUTION = "1";
    sanitized.KILL_SWITCH_GLOBAL = "1";
    sanitized.AUTO_BOOT_DAEMONS = "true";

    atomicWrite(ENV_PATH, serializeEnv(sanitized));

    const python = pickPythonCmd();
    const pyArgs = python === "py" ? ["-3", "-u", "-c"] : ["-u", "-c"];

    spawnSyncSafe(
      python,
      [
        ...pyArgs,
        "from engine.execution.execution_mode import set_execution_mode, set_execution_armed; "
          + "from engine.execution.kill_switch import activate; "
          + "set_execution_mode('paper', actor='operator_server', reason='emergency_stop', keep_armed=False); "
          + "set_execution_armed(0, actor='operator_server', reason='emergency_stop'); "
          + "activate('global', 'global', reason='operator_emergency_stop', actor='operator_server')"
      ],
      {
        cwd: ROOT,
        env: { ...process.env, ...sanitized, PYTHONPATH: ROOT },
        stdio: "pipe"
      }
    );
  } catch {}

  if (isLinuxManagedMode()) {
    const svc = runServiceCtl(["stop", "engine"]);
    state.lastMode = "safe";
    state.lastStopAt = nowIso();
    saveState();
    return { ok: !!svc.ok, status: svc.ok ? "STOPPING" : "STOPPED", managed: true, service: svc };
  }

  if (!child) {
    state.lastMode = "safe";
    state.lastStopAt = nowIso();
    saveState();
    return { ok: true, status: "STOPPED" };
  }

  try {
    stopEngineProcessTree(2500);
  } catch {}

  state.lastMode = "safe";
  state.lastStopAt = nowIso();
  saveState();
  return { ok: true, status: "STOPPING", managed: false };
}

function runBrokerRiskCommand(body = {}, confirmation = {}) {
  const payload = (body && typeof body === "object" && !Array.isArray(body)) ? body : {};
  const policy = String(payload.policy || payload.action || "").trim();
  if (!policy) {
    return { ok: false, error: "broker_risk_policy_required" };
  }

  let sanitized = {};
  try {
    ensureEnvFile();
    const envObj = readEnv();
    sanitized = validateAndSanitizeEnv(envObj).sanitized || {};
  } catch (e) {
    return { ok: false, error: "broker_risk_env_load_failed", detail: String(e && e.message ? e.message : e) };
  }

  const actor = String(payload.actor || payload.who || confirmation.actor || "operator_server").trim() || "operator_server";
  const reason = String(payload.reason || payload.justification || confirmation.reason || "operator broker risk action").trim();
  const broker = String(payload.broker || "").trim();
  const engineMode = String(payload.engine_mode || payload.mode || sanitized.ENGINE_MODE || process.env.ENGINE_MODE || "").trim();
  const commandId = String(payload.command_id || payload.request_id || confirmation.request_id || `broker-risk-${Date.now()}`).trim();
  const timeoutS = clampNumber(payload.timeout_s || payload.timeout || 15, 15, 1, 120);
  const python = pickPythonCmd();
  const args = python === "py" ? ["-3", "-u", "-m"] : ["-u", "-m"];
  const cliArgs = [
    ...args,
    "engine.execution.broker_shutdown_risk",
    "--policy", policy,
    "--timeout-s", String(timeoutS),
    "--command-id", commandId,
    "--actor", actor,
    "--reason", reason,
    "--source", "operator_sidecar",
  ];
  if (broker) cliArgs.push("--broker", broker);
  if (engineMode) cliArgs.push("--engine-mode", engineMode);

  const r = spawnSyncSafe(python, cliArgs, {
    cwd: ROOT,
    env: { ...process.env, ...sanitized, PYTHONPATH: ROOT },
    stdio: "pipe",
    timeout: Math.max(5000, Math.ceil(timeoutS * 1000) + 5000),
  });
  const stdout = String(r.stdout || "").trim();
  const stderr = String(r.stderr || "").trim();
  let parsed = null;
  try {
    const lines = stdout.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    parsed = lines.length ? JSON.parse(lines[lines.length - 1]) : null;
  } catch (e) {
    parsed = null;
  }
  const ok = Number(r.status || 0) === 0 && !!(parsed && parsed.ok);
  return {
    ok,
    status: ok ? "broker_risk_complete" : "broker_risk_failed",
    command: parsed,
    exit_status: Number(r.status || 0),
    signal: r.signal || null,
    stdout_tail: stdout.slice(-4000),
    stderr_tail: stderr.slice(-4000),
  };
}

// --------------------------------------------------
// Health + Readiness (backend integration)
// --------------------------------------------------

function _httpJsonRequest(method, url, body = null, timeoutMs = 20000) {
  return new Promise((resolve) => {
    const startedAtMs = Date.now();
    let settled = false;

    function finish(result) {
      if (settled) return;
      settled = true;
      resolve({
        url,
        method: String(method || "GET").toUpperCase(),
        duration_ms: Math.max(0, Date.now() - startedAtMs),
        ...result
      });
    }

    try {
      const payload = body === null ? "" : JSON.stringify(body || {});
      const lib = url.startsWith("https://") ? https : http;
      const methodUpper = String(method || "GET").toUpperCase();

      const req = lib.request(
        url,
        {
          method: methodUpper,
          headers: {
            "Accept": "application/json",
            "Connection": "close",
            ...trustedControlPlaneAuthHeaders(methodUpper, url),
            ...(body === null ? {} : {
              "Content-Type": "application/json",
              "Content-Length": Buffer.byteLength(payload)
            })
          }
        },
        (res) => {
          let data = "";
          res.setEncoding("utf8");
          res.on("data", (chunk) => {
            data += chunk;
          });
          res.on("end", () => {
            const status = Number(res.statusCode || 0);
            const statusText = String(res.statusMessage || "");
            let parsed = null;

            if (data) {
              try {
                parsed = JSON.parse(data);
              } catch {
                finish({
                  ok: false,
                  status,
                  statusText,
                  json: null,
                  text: data,
                  error: "invalid_json_response"
                });
                return;
              }
            }

            finish({
              ok: status >= 200 && status < 300,
              status,
              statusText,
              json: parsed,
              text: data,
              error: status >= 200 && status < 300
                ? null
                : String((parsed && parsed.error) || `http_${status || 0}`)
            });
          });
        }
      );

      req.on("error", (err) => {
        finish({
          ok: false,
          status: 0,
          statusText: "",
          json: null,
          text: "",
          error: err && err.message === "request_timeout"
            ? "request_timeout"
            : String(err && (err.code || err.message) || "request_failed"),
          detail: String(err && err.message || err || "request_failed")
        });
      });

      req.setTimeout(timeoutMs, () => {
        try {
          req.destroy(new Error("request_timeout"));
        } catch {}
        finish({
          ok: false,
          status: 0,
          statusText: "",
          json: null,
          text: "",
          error: "request_timeout",
          timed_out: true
        });
      });

      if (body !== null) {
        req.write(payload);
      }
      req.end();
    } catch (e) {
      finish({
        ok: false,
        status: 0,
        statusText: "",
        json: null,
        text: "",
        error: "request_failed",
        detail: String(e && e.message || e || "request_failed")
      });
    }
  });
}

function httpGetJson(url, timeoutMs = 20000) {
  return _httpJsonRequest("GET", url, null, timeoutMs);
}

function httpPostJson(url, body = {}, timeoutMs = 20000) {
  return _httpJsonRequest("POST", url, body, timeoutMs);
}

const OPERATOR_HEALTH_CACHE_TTL_MS = clampNumber(
  process.env.OPERATOR_HEALTH_CACHE_TTL_MS || 1500,
  1500,
  0,
  30000
);
let _verifyHealthCache = { tsMs: 0, payload: null };
const OPERATOR_DB_CACHE_TTL_MS = clampNumber(
  process.env.OPERATOR_DB_CACHE_TTL_MS || 2000,
  2000,
  0,
  30000
);
let _bootstrapCountsCache = { tsMs: 0, payload: null };
let _dbSchemaCache = { tsMs: 0, payload: null };

function invalidateOperatorHealthCache() {
  _verifyHealthCache = { tsMs: 0, payload: null };
}

function invalidateOperatorDbCaches() {
  _bootstrapCountsCache = { tsMs: 0, payload: null };
  _dbSchemaCache = { tsMs: 0, payload: null };
}

function readOperatorDbCache(cacheState) {
  const now = Date.now();
  if (
    OPERATOR_DB_CACHE_TTL_MS <= 0 ||
    !cacheState ||
    !cacheState.payload ||
    (now - Number(cacheState.tsMs || 0)) > OPERATOR_DB_CACHE_TTL_MS
  ) {
    return null;
  }
  return {
    ...cacheState.payload,
    cache_age_ms: Math.max(0, now - Number(cacheState.tsMs || 0))
  };
}

function writeOperatorDbCache(key, payload) {
  const entry = {
    tsMs: Date.now(),
    payload: payload && typeof payload === "object" ? { ...payload } : payload
  };
  if (key === "bootstrap_counts") {
    _bootstrapCountsCache = entry;
  } else if (key === "db_schema") {
    _dbSchemaCache = entry;
  }
}

function rowsFromOperatorPayload(payload, keys = []) {
  if (Array.isArray(payload)) return payload;
  if (!payload || typeof payload !== "object") return [];
  for (const key of keys) {
    const rows = payload[key];
    if (Array.isArray(rows)) return rows;
  }
  if (Array.isArray(payload.rows)) return payload.rows;
  if (payload.data && typeof payload.data === "object") {
    for (const key of keys) {
      const rows = payload.data[key];
      if (Array.isArray(rows)) return rows;
    }
    if (Array.isArray(payload.data.rows)) return payload.data.rows;
  }
  return [];
}

async function verifyHealth(options = {}) {
  const force = !!(options && options.force);
  const now = Date.now();
  if (
    !force &&
    OPERATOR_HEALTH_CACHE_TTL_MS > 0 &&
    _verifyHealthCache.payload &&
    (now - Number(_verifyHealthCache.tsMs || 0)) <= OPERATOR_HEALTH_CACHE_TTL_MS
  ) {
    return {
      ..._verifyHealthCache.payload,
      cache_age_ms: Math.max(0, now - Number(_verifyHealthCache.tsMs || 0))
    };
  }

  const env = readEnv();
  const port = Number(process.env.DASHBOARD_PORT || env.DASHBOARD_PORT || 8000);
  const hostRaw = String(process.env.DASHBOARD_HOST || env.DASHBOARD_HOST || "127.0.0.1");
  const host = _normalizeDashHostForLoopback(hostRaw);
  const override = String(process.env.OPERATOR_HEALTH_URL || env.OPERATOR_HEALTH_URL || "").trim();

  const url = override || `http://${host}:${port}/api/health`;

  const r = await httpGetJson(url);
  const body = (r && r.json && typeof r.json === "object") ? r.json : null;
  const bodyOk = !!(body && body.ok === true);
  const warmup = String((body && body.status) || "").toUpperCase() === "WARMING_UP";
  const reachable = !!(r && r.ok && body);

  let result;

  if (reachable && bodyOk) {
    state.lastHealthyAt = nowIso();
    state.restartAttempts = 0;
    state.consecutiveStartupFailures = 0;
    state._restartWindowStart = null;
    state._restartCountWindow = 0;
    state.restartBlocked = false;
    state.fatal = false;
    state.fatalSince = null;
    state.nextRestartAt = null;
    _crashLoopDetected = false;
    saveState();
    result = {
      ok: true,
      healthy: true,
      url,
      status: r.status,
      body,
      warmup,
      error: null,
      transport_error: null
    };
  } else {
    result = {
      ok: reachable,
      healthy: bodyOk,
      url,
      status: Number(r.status || 0),
      body,
      warmup,
      error: reachable ? String((body && body.error) || "health_not_ok") : String(r.error || "dashboard_unreachable"),
      transport_error: reachable ? null : String(r.error || "dashboard_unreachable")
    };
  }

  if (OPERATOR_HEALTH_CACHE_TTL_MS > 0) {
    _verifyHealthCache = {
      tsMs: Date.now(),
      payload: result
    };
  }
  return { ...result };
}

async function verifyDashboardReadiness() {
  const env = readEnv();
  const port = Number(process.env.DASHBOARD_PORT || env.DASHBOARD_PORT || 8000);
  const hostRaw = String(process.env.DASHBOARD_HOST || env.DASHBOARD_HOST || "127.0.0.1");
  const host = _normalizeDashHostForLoopback(hostRaw);
  const override = String(process.env.OPERATOR_READINESS_URL || env.OPERATOR_READINESS_URL || "").trim();
  const url = override || `http://${host}:${port}/api/readiness`;
  const r = await httpGetJson(url);
  const body = (r && r.json && typeof r.json === "object") ? r.json : null;
  const ready = !!(r && r.ok && body && body.ok === true && body.ready === true);
  return {
    ok: !!(r && r.ok && body),
    ready,
    url,
    status: Number((r && r.status) || 0),
    body,
    error: ready ? null : String((body && (body.error || body.status || body.summary_reason)) || (r && r.error) || "readiness_not_ready")
  };
}

async function checkPortAvailable(port, host = "127.0.0.1") {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.once("error", () => resolve(false));
    srv.once("listening", () => srv.close(() => resolve(true)));
    srv.listen(port, host);
  });
}

function isPathWritable(p) {
  try {
    // If p is a file path that doesn't exist yet, check its parent directory.
    let target = p;

    try {
      const st = fs.statSync(p);
      if (!st.isDirectory()) target = path.dirname(p);
    } catch {
      target = path.dirname(p);
    }

    fs.accessSync(target, fs.constants.W_OK);
    return true;
  } catch {
    return false;
  }
}

function pythonAvailable() {
  const python = pickPythonCmd();
  try {
    const r = spawnSyncSafe(python, ["--version"], { stdio: "pipe", timeout: 10000 });
    const out = (r.stdout ? String(r.stdout) : "") + (r.stderr ? String(r.stderr) : "");
    const ok = r && !r.error && r.status === 0;
    return { ok, python, version: out.trim() };
  } catch (e) {
    return { ok: false, python, version: "", error: String(e) };
  }
}

function resolveDbPathFromSanitized(sanitized) {
  const dbPath = String(sanitized.DB_PATH || DEFAULT_LOCAL_DB_PATH);
  const resolvedDb = path.isAbsolute(dbPath) ? dbPath : path.join(ROOT, dbPath);
  return { dbPath, resolvedDb };
}

function buildOperatorPythonEnv(envObj = {}) {
  const currentPyPath = String(process.env.PYTHONPATH || "").trim();
  const nextPyPath = currentPyPath ? `${ROOT}${path.delimiter}${currentPyPath}` : ROOT;
  return {
    ...process.env,
    ...envObj,
    PYTHONPATH: nextPyPath
  };
}

function readDataSourceRows(envObj = {}) {
  const python = pickPythonCmd();
  const pyArgs = python === "py" ? ["-3", "-u", "-c"] : ["-u", "-c"];
  const script = [
    "import json",
    "from services.data_source_manager import get_manager",
    "mgr = get_manager()",
    "mgr.initialize()",
    "rows = mgr.list_sources()",
    "selected = {}",
    "for key in ('polygon', 'polygon_ws'):",
    "    selected[key] = next((row for row in rows if str(row.get('source_key') or '') == key), None)",
    "print(json.dumps({'ok': True, 'sources': selected}, separators=(',', ':')))",
  ].join("\n");

  try {
    const r = spawnSyncSafe(
      python,
      [...pyArgs, script],
      {
        cwd: ROOT,
        env: buildOperatorPythonEnv(envObj),
        stdio: "pipe",
        timeout: 20000
      }
    );
    const stdout = String(r.stdout || "").trim();
    const stderr = String(r.stderr || "").trim();
    if (r.error || r.status !== 0) {
      return {
        ok: false,
        error: "data_source_query_failed",
        details: stderr || stdout || String(r.error || "python_query_failed"),
        sources: {}
      };
    }
    const line = stdout.split(/\r?\n/).filter(Boolean).slice(-1)[0] || "{}";
    const payload = JSON.parse(line);
    return {
      ok: true,
      details: "",
      sources: payload && typeof payload === "object" ? (payload.sources || {}) : {}
    };
  } catch (e) {
    return {
      ok: false,
      error: "data_source_query_failed",
      details: String(e),
      sources: {}
    };
  }
}

function describeSourceSetupState(source, label) {
  if (!source || typeof source !== "object") {
    return { ok: false, details: `${label} is not registered in the Data Sources Control Center.` };
  }
  if (!source.enabled) {
    return { ok: false, details: `${label} is disabled in the Data Sources Control Center.` };
  }
  if (String(source.credential_error || "").trim()) {
    return {
      ok: false,
      details: `${label} has unreadable stored credentials. Reset or replace them in the Data Sources Control Center.`
    };
  }
  if (Array.isArray(source.credential_fields) && source.credential_fields.length > 0 && !source.credentials_configured) {
    return { ok: false, details: `${label} needs credentials in the Data Sources Control Center.` };
  }
  return { ok: true, details: `${label} is configured in the Data Sources Control Center.` };
}

function getPolygonWebsocketStatus(envObj = {}) {
  const snapshot = readDataSourceRows(envObj);
  if (!snapshot.ok) {
    return {
      ok: false,
      details: `Could not read data-source state: ${snapshot.details || "query failed"}`
    };
  }
  return describeSourceSetupState(snapshot.sources.polygon_ws, "Polygon WebSocket");
}

function getPrimaryPolygonSourceStatus(envObj = {}) {
  const snapshot = readDataSourceRows(envObj);
  if (!snapshot.ok) {
    return {
      ok: false,
      details: `Could not read data-source state: ${snapshot.details || "query failed"}`
    };
  }

  const candidates = [
    ["polygon_ws", "Polygon WebSocket"],
    ["polygon", "Polygon REST"]
  ];
  const states = candidates.map(([key, label]) => describeSourceSetupState(snapshot.sources[key], label));
  const ready = states.find((item) => item.ok);
  if (ready) return ready;
  return {
    ok: false,
    details: states.map((item) => item.details).filter(Boolean).join(" ")
  };
}

function runProductionValidationGate(sanitized) {
  const python = pickPythonCmd();
  const item = { id: "runtime_graph", file: path.join(ROOT, "tools", "runtime_graph_check.py") };
  const checks = [];

  if (!fs.existsSync(item.file)) {
    checks.push({
      id: item.id,
      ok: false,
      details: `missing validation script: ${item.file}`,
      exitCode: null,
      stdout: "",
      stderr: ""
    });
  } else {
    const args = python === "py"
      ? ["-3", item.file]
      : [item.file];

    const result = spawnSyncSafe(python, args, {
      cwd: ROOT,
      env: {
        ...process.env,
        ...(sanitized || {}),
        PYTHONPATH: ROOT,
        TRADING_VALIDATION_MODE: "startup"
      },
      stdio: "pipe",
      timeout: OPERATOR_VALIDATION_TIMEOUT_MS
    });

    const stdout = String(result && result.stdout ? result.stdout : "");
    const stderr = String(result && result.stderr ? result.stderr : "");
    const ok = !!(result && !result.error && result.status === 0);

    checks.push({
      id: item.id,
      ok,
      details: ok ? "ok" : `validation failed (${item.id})`,
      exitCode: result ? result.status : null,
      stdout: stdout.slice(-12000),
      stderr: stderr.slice(-12000),
      error: result && result.error ? String(result.error) : ""
    });
  }

  return {
    ok: checks.every((c) => !!c.ok),
    checks
  };
}

function validationGateCacheKey(sanitized) {
  try {
    const entries = Object.entries(sanitized || {})
      .map(([key, value]) => [String(key), String(value)])
      .sort((a, b) => a[0].localeCompare(b[0]));
    return JSON.stringify(entries);
  } catch {
    return "";
  }
}

function runProductionValidationGateCached(sanitized, options = {}) {
  if (options && options.skip === true) {
    return {
      ok: true,
      skipped: true,
      checks: [{
        id: "runtime_graph",
        ok: true,
        details: "skipped_for_fast_operator_refresh",
        exitCode: null,
        stdout: "",
        stderr: ""
      }]
    };
  }

  const key = validationGateCacheKey(sanitized);
  const ttl = Number(OPERATOR_PREFLIGHT_CACHE_TTL_MS || 0);
  if (
    !(options && options.force === true) &&
    ttl > 0 &&
    _validationGateCache.payload &&
    _validationGateCache.key === key &&
    (Date.now() - Number(_validationGateCache.tsMs || 0)) <= ttl
  ) {
    return cloneJsonValue(_validationGateCache.payload);
  }

  const result = runProductionValidationGate(sanitized);
  _validationGateCache = { key, tsMs: Date.now(), payload: cloneJsonValue(result) };
  return result;
}

async function getPreflight(mode = "safe", options = {}) {
  ensureEnvFile();

  const envObj = readEnv();
  const { sanitized, issues: cfgIssues } = validateAndSanitizeEnv(envObj);
  const engineStatus = status();

  // Preflight must be read-only; do not mutate production config during checks.
  const checks = [];

  // Entry exists
  checks.push({
    id: "entry",
    label: "Backend entrypoint exists",
    ok: fs.existsSync(ENTRY),
    details: ENTRY
  });

  // Python exists
  const py = pythonAvailable();
  checks.push({
    id: "python",
    label: "Python available",
    ok: !!py.ok,
    details: py.ok ? `${py.python} (${py.version || "ok"})` : (py.error || "not found")
  });

  // Dashboard port available (if not running)
  const dashPort = Number(sanitized.DASHBOARD_PORT || 8000);
  const dashHost = String(sanitized.DASHBOARD_HOST || "127.0.0.1");
  if (Number.isFinite(dashPort) && dashPort > 0 && dashPort <= 65535) {
    const portOk = await checkPortAvailable(dashPort, dashHost);
    checks.push({
      id: "port",
      label: `Dashboard port available (${dashHost}:${dashPort})`,
      ok: portOk || engineStatus === "RUNNING",
      details: portOk ? "free" : (engineStatus === "RUNNING" ? "engine running" : "in use")
    });
  } else {
    checks.push({
      id: "port",
      label: "Dashboard port configured",
      ok: false,
      details: "invalid DASHBOARD_PORT"
    });
  }

  // DB path writable
  const { dbPath, resolvedDb } = resolveDbPathFromSanitized(sanitized);
  checks.push({
    id: "db",
    label: "DB path writable",
    ok: isPathWritable(resolvedDb),
    details: resolvedDb
  });

  // Config validity
  const cfgOk = !cfgIssues.some((i) => i.level === "error");
  checks.push({
    id: "config",
    label: ".env config valid",
    ok: cfgOk,
    details: cfgOk ? "ok" : cfgIssues.map((x) => `${x.key}: ${x.message}`).join("; ")
  });

  // Production validation gate
  const validationGate = runProductionValidationGateCached(sanitized, {
    force: options && options.forceValidation === true,
    skip: options && options.skipValidationGate === true
  });
  checks.push({
    id: "validation_gate",
    label: "Production validation gate",
    ok: !!validationGate.ok,
    details: validationGate.skipped
      ? "skipped for fast operator refresh"
      : validationGate.ok
      ? "runtime_graph_check passed"
      : validationGate.checks
          .filter((c) => !c.ok)
          .map((c) => `${c.id}: exit=${c.exitCode} ${c.error || c.details || ""}`.trim())
          .join("; ")
  });

  // Live/shadow key requirements (soft check)
  const m = String(mode || "safe").toLowerCase().trim();
  const wantData = (m === "shadow" || m === "live");
  if (wantData) {
    const polygonStatus = getPrimaryPolygonSourceStatus(sanitized);
    checks.push({
      id: "polygon_source",
      label: "Polygon market data source configured",
      ok: !!polygonStatus.ok,
      details: polygonStatus.details
    });
  }

  if (m === "live") {
    const liveBlockers = [];
    const disableLiveBlocker = liveExecutionEnvBlocker(sanitized.DISABLE_LIVE_EXECUTION);
    if (disableLiveBlocker) liveBlockers.push(disableLiveBlocker);
    if (String(sanitized.KILL_SWITCH_GLOBAL || "").trim() === "1") {
      liveBlockers.push("KILL_SWITCH_GLOBAL=1");
    }

    checks.push({
      id: "live_controls",
      label: "Live execution controls clear",
      ok: liveBlockers.length === 0,
      details: liveBlockers.length ? liveBlockers.join("; ") : "clear"
    });
  }

  const ok = checks.every((c) => !!c.ok);

  return {
    ok,
    mode: m,
    status: engineStatus,
    productionMode: PRODUCTION_MODE,
    checks,
    configIssues: cfgIssues
  };
}

async function getReadiness() {
  const issues = [];
  const engineStatus = status();
  const ext = externalRuntimeState();

  // Files
  if (!fs.existsSync(ENTRY)) issues.push({ level: "error", code: "ENTRY_MISSING", message: `Backend entrypoint missing (${path.basename(ENTRY)})` });
  if (!fs.existsSync(ENV_PATH)) issues.push({ level: "error", code: "ENV_MISSING", message: ".env missing" });
  if (!fs.existsSync(LOG_DIR)) issues.push({ level: "warn", code: "LOGDIR_MISSING", message: "var/log folder missing (will be created on start)" });

  // Config validity
  const envObj = readEnv();
  const { issues: cfgIssues } = validateAndSanitizeEnv(envObj);
  for (const ci of cfgIssues) {
    issues.push({
      level: ci.level === "error" ? "error" : "warn",
      code: "CONFIG_" + ci.key,
      message: `${ci.key}: ${ci.message}`
    });
  }

  if (ext.active) {
    issues.push({
      level: "warn",
      code: "UNMANAGED_RUNTIME",
      message: `Backend is running outside operator supervision (pid ${ext.pid})`
    });
  }

  // Health (if running)
  let health = null;
  let dashboardReadiness = null;
  let backendHealthy = false;
  let backendReady = false;

  if (engineStatus === "RUNNING") {
    health = await verifyHealth();
    backendHealthy = !!(health && health.ok && health.body && health.body.ok);
    dashboardReadiness = await verifyDashboardReadiness();
    backendReady = !!(backendHealthy && dashboardReadiness && dashboardReadiness.ready);

    if (!health || !health.ok) {
      issues.push({
        level: "error",
        code: "DASHBOARD_UNREACHABLE",
        message: "Dashboard health endpoint is not reachable"
      });
    } else if (!backendHealthy) {
      const reasons = Array.isArray(health.body && health.body.reasons)
        ? health.body.reasons.filter(Boolean)
        : [];

      issues.push({
        level: "warn",
        code: "BACKEND_WARMING_UP",
        message: reasons.length
          ? `Backend warming up: ${reasons.join(", ")}`
          : "Backend reachable but not healthy yet"
      });
    }
    if (!dashboardReadiness || !dashboardReadiness.ok) {
      issues.push({
        level: "error",
        code: "READINESS_UNREACHABLE",
        message: "Dashboard readiness endpoint is not reachable"
      });
    } else if (!dashboardReadiness.ready) {
      const body = dashboardReadiness.body || {};
      const reasons = Array.isArray(body.reasons) ? body.reasons.filter(Boolean) : [];
      issues.push({
        level: "warn",
        code: "READINESS_NOT_READY",
        message: reasons.length
          ? `Readiness blocked: ${reasons.join(", ")}`
          : `Readiness blocked: ${String(body.status || dashboardReadiness.error || "not ready")}`
      });
    }
  }

  // Current-attempt error persistence only
  const activeAttemptError = currentAttemptLastErrorForRuntime(ext);
  if (activeAttemptError) {
    issues.push({
      level: "warn",
      code: "LAST_ERROR_PRESENT",
      message: `Current attempt error: ${activeAttemptError.kind} — ${activeAttemptError.message}`
    });
  }

  const hasError = issues.some((i) => i.level === "error");
  const ready = !hasError && engineStatus === "RUNNING" && backendReady;
  const readinessStatus = ready
    ? "RUNNING"
    : (engineStatus === "RUNNING" ? "DEGRADED" : engineStatus);
  const degradedComponents = [];

  if (engineStatus !== "RUNNING") degradedComponents.push("engine_process");
  if (ext.active) degradedComponents.push("operator_supervision");
  if (engineStatus === "RUNNING" && (!health || !health.ok)) degradedComponents.push("dashboard_transport");
  if (engineStatus === "RUNNING" && health && health.ok && !backendHealthy) degradedComponents.push("runtime_health");
  if (engineStatus === "RUNNING" && dashboardReadiness && dashboardReadiness.ok && !dashboardReadiness.ready) degradedComponents.push("runtime_readiness");
  if (engineStatus === "RUNNING" && (!dashboardReadiness || !dashboardReadiness.ok)) degradedComponents.push("readiness_transport");
  if (activeAttemptError) degradedComponents.push("startup_attempt");
  if (state.restartBlocked === true || state.fatal === true) degradedComponents.push("restart_supervision");

  const currentBlocker = (() => {
    const primary = issues.find((item) => item && item.level === "error") || issues[0] || null;
    return primary
      ? {
          code: String(primary.code || "READINESS_BLOCKED"),
          level: String(primary.level || "warn"),
          message: String(primary.message || "")
        }
      : null;
  })();

  return {
    ok: ready,
    ready,
    degraded: !ready,
    status: readinessStatus,
    engineStatus,
    productionMode: PRODUCTION_MODE,
    mode: state.lastMode || "safe",
    issues,
    health,
    dashboardReadiness,
    lastHealthyAt: state.lastHealthyAt || null,
    restartAttempts: Number(state.restartAttempts || 0),
    restartBlocked: state.restartBlocked === true,
    fatal: state.fatal === true,
    externalRuntime: ext.active ? {
      pid: ext.pid,
      record: ext.record || null,
      managedByOperator: false
    } : null,
    currentBlocker,
    degradedComponents
  };
}

// --------------------------------------------------
// Logs + Snapshot
// --------------------------------------------------

function readLogSegment(filePath, startOffset = 0, maxChars = 12000) {
  try {
    if (!fs.existsSync(filePath)) return "";
    const stat = fs.statSync(filePath);
    const size = Number(stat.size || 0);
    const safeStart = Math.max(0, Math.min(Number(startOffset || 0), size));
    const readLen = Math.max(0, Math.min(Number(maxChars || 12000), size - safeStart));
    if (readLen <= 0) return "";
    const fd = fs.openSync(filePath, "r");
    try {
      const buffer = Buffer.alloc(readLen);
      fs.readSync(fd, buffer, 0, readLen, safeStart);
      return buffer.toString("utf8");
    } finally {
      try { fs.closeSync(fd); } catch {}
    }
  } catch {
    return "";
  }
}

function currentAttemptRuntimeTail(lines = 200) {
  const safeLines = Math.max(1, Math.min(5000, Number(lines || 200)));
  if (isLinuxManagedMode()) {
    const journalText = readManagedAttemptJournal(safeLines);
    if (!journalText) return "";
    return journalText.split("\n").slice(-safeLines).join("\n");
  }
  const maxChars = Math.max(4096, Math.min(512000, safeLines * 400));
  const currentOnly = readLogSegment(RUNTIME_LOG, state.currentRuntimeLogOffset || 0, maxChars);
  if (currentOnly) {
    return currentOnly.split("\n").slice(-safeLines).join("\n");
  }
  if (state.currentAttemptStartedAt || state.currentAttemptId || state.lastExitCode !== null) {
    return historicalRuntimeTail(safeLines);
  }
  return "";
}

function currentAttemptStderrTail(limit = 12000) {
  const maxChars = Math.max(512, Math.min(512000, Number(limit || 12000)));
  if (isLinuxManagedMode()) {
    const approxLines = Math.max(50, Math.ceil(maxChars / 160));
    const journalText = readManagedAttemptJournal(approxLines);
    if (!journalText) return "";
    return String(journalText).slice(-maxChars);
  }
  const currentOnly = readLogSegment(ENGINE_STDERR_LOG, state.currentStderrLogOffset || 0, maxChars);
  if (currentOnly) {
    return currentOnly;
  }
  if (state.currentAttemptStartedAt || state.currentAttemptId || state.lastExitCode !== null) {
    return historicalStderrTail(maxChars);
  }
  return "";
}

function historicalRuntimeTail(lines = 200) {
  try {
    if (!fs.existsSync(RUNTIME_LOG)) return "";
    return fs.readFileSync(RUNTIME_LOG, "utf-8").split("\n").slice(-Math.max(1, Number(lines || 200))).join("\n");
  } catch {
    return "";
  }
}

function historicalStderrTail(limit = 12000) {
  return readLogSegment(ENGINE_STDERR_LOG, 0, Math.max(512, Math.min(512000, Number(limit || 12000))));
}

function tailLog(lines = 200) {
  return currentAttemptRuntimeTail(lines);
}

const REDACTED_SECRET = "***REDACTED***";
const OPERATOR_SENSITIVE_KEY_RE = /(?:^|[_\-.])(?:token|secret|password|passwd|passphrase|api[_\-.]?key|key[_\-.]?id|access[_\-.]?key|private[_\-.]?key|master[_\-.]?key|hmac[_\-.]?key|credential|credentials|dsn|database[_\-.]?url|redis[_\-.]?url|broker[_\-.]?key|provider[_\-.]?key)(?:$|[_\-.])/i;
const OPERATOR_SENSITIVE_TEXT_KEY_RE = /\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|PASSPHRASE|API_KEY|KEY_ID|ACCESS_KEY|PRIVATE_KEY|MASTER_KEY|HMAC_KEY|CREDENTIAL|CREDENTIALS|DSN|DATABASE_URL|REDIS_URL|BROKER_KEY|PROVIDER_KEY)[A-Z0-9_]*)\s*=\s*([^\s"'`]+)/gi;
const OPERATOR_SENSITIVE_JSON_KEY_RE = /("([^"]*(?:token|secret|password|passwd|passphrase|api[_-]?key|key[_-]?id|access[_-]?key|private[_-]?key|master[_-]?key|hmac[_-]?key|credential|credentials|dsn|database[_-]?url|redis[_-]?url|broker[_-]?key|provider[_-]?key)[^"]*)"\s*:\s*)"([^"]*)"/gi;
const URL_PASSWORD_RE = /([a-z][a-z0-9+.-]*:\/\/[^:\s/@]+:)([^@\s/]+)(@)/gi;

function isOperatorSensitiveKey(keyName) {
  return OPERATOR_SENSITIVE_KEY_RE.test(String(keyName || ""));
}

function redactOperatorSensitiveText(text) {
  if (text === null || text === undefined) return text;
  return String(text)
    .replace(OPERATOR_SENSITIVE_TEXT_KEY_RE, (_match, key) => `${key}=${REDACTED_SECRET}`)
    .replace(OPERATOR_SENSITIVE_JSON_KEY_RE, (_match, prefix) => `${prefix}"${REDACTED_SECRET}"`)
    .replace(URL_PASSWORD_RE, `$1${REDACTED_SECRET}$3`);
}

function redactOperatorSecrets(value, keyName = "") {
  if (value === null || value === undefined) return value;

  if (isOperatorSensitiveKey(keyName)) {
    return REDACTED_SECRET;
  }

  if (Array.isArray(value)) {
    return value.map((item) => redactOperatorSecrets(item, keyName));
  }

  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = redactOperatorSecrets(v, k);
    }
    return out;
  }

  if (typeof value === "string") {
    return redactOperatorSensitiveText(value);
  }

  return value;
}

function safeEnvForSnapshot(envObj) {
  return redactOperatorSecrets({ ...(envObj || {}) });
}

function readTailChars(filePath, maxChars = 12000) {
  try {
    if (!fs.existsSync(filePath)) {
      return { ok: false, error: "file_missing", path: filePath };
    }

    const isRuntime = filePath === RUNTIME_LOG;
    const isStderr = filePath === ENGINE_STDERR_LOG;

    if (isRuntime) {
      const tail = currentAttemptRuntimeTail(Math.max(50, Math.ceil(Number(maxChars || 12000) / 200)));
      return {
        ok: true,
        path: filePath,
        currentAttemptOnly: true,
        attemptId: state.currentAttemptId || null,
        attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
        logScope: "file_offset_current_attempt",
        tail: String(tail || "").slice(-Math.max(0, Number(maxChars || 0)))
      };
    }

    if (isStderr) {
      const tail = currentAttemptStderrTail(maxChars);
      return {
        ok: true,
        path: filePath,
        currentAttemptOnly: true,
        attemptId: state.currentAttemptId || null,
        attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
        logScope: "file_offset_current_attempt",
        tail
      };
    }

    // fallback ONLY for non-engine logs
    const text = fs.readFileSync(filePath, "utf8");
    return {
      ok: true,
      path: filePath,
      chars: text.length,
      tail: String(text || "").slice(-Math.max(0, Number(maxChars || 0))),
      currentAttemptOnly: false,
      logScope: "full_file_fallback_non_engine"
    };
  } catch (e) {
    return { ok: false, error: String(e), path: filePath };
  }
}

function snapshotModeConfig(mode) {
  const normalized = String(mode || "repair").trim().toLowerCase();
  if (normalized === "quick") {
    return {
      mode: "quick",
      runtimeLogLines: 120,
      stderrChars: 8000,
      maxString: 8000,
      maxArray: 20,
      maxObjectKeys: 60,
      maxDepth: 6,
      endpointTimeoutMs: 2500
    };
  }
  if (normalized === "deep") {
    return {
      mode: "deep",
      runtimeLogLines: 600,
      stderrChars: 30000,
      maxString: 30000,
      maxArray: 200,
      maxObjectKeys: 250,
      maxDepth: 10,
      endpointTimeoutMs: 15000
    };
  }
  return {
    mode: "repair",
    runtimeLogLines: 250,
    stderrChars: 16000,
    maxString: 16000,
    maxArray: 80,
    maxObjectKeys: 120,
    maxDepth: 8,
    endpointTimeoutMs: 5000
  };
}

function redactSnapshotSecrets(value, keyName = "") {
  if (value === null || value === undefined) return value;

  if (isOperatorSensitiveKey(keyName)) {
    return REDACTED_SECRET;
  }

  if (Array.isArray(value)) {
    return value.map((item) => redactSnapshotSecrets(item, keyName));
  }

  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = redactSnapshotSecrets(v, k);
    }
    return out;
  }

  if (typeof value === "string") {
    return redactOperatorSensitiveText(value);
  }

  return value;
}

function truncateSnapshotValue(value, cfg, depth = 0) {
  const maxDepth = Number(cfg.maxDepth || 8);
  const maxString = Number(cfg.maxString || 16000);
  const maxArray = Number(cfg.maxArray || 80);
  const maxObjectKeys = Number(cfg.maxObjectKeys || 120);

  if (value === null || value === undefined) return value;
  if (depth >= maxDepth) return "[TRUNCATED_DEPTH]";

  if (typeof value === "string") {
    if (value.length <= maxString) return value;
    return value.slice(0, maxString) + `\n[TRUNCATED ${value.length - maxString} chars]`;
  }

  if (Array.isArray(value)) {
    const out = value.slice(0, maxArray).map((item) => truncateSnapshotValue(item, cfg, depth + 1));
    if (value.length > maxArray) {
      out.push(`[TRUNCATED ${value.length - maxArray} items]`);
    }
    return out;
  }

  if (typeof value === "object") {
    const keys = Object.keys(value);
    const out = {};
    for (const k of keys.slice(0, maxObjectKeys)) {
      out[k] = truncateSnapshotValue(value[k], cfg, depth + 1);
    }
    if (keys.length > maxObjectKeys) {
      out.__truncated_keys__ = keys.length - maxObjectKeys;
    }
    return out;
  }

  return value;
}

async function tcpProbe(host, port, timeoutMs = 1500) {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const socket = new net.Socket();

    let settled = false;
    function done(payload) {
      if (settled) return;
      settled = true;
      try { socket.destroy(); } catch {}
      resolve({
        host,
        port,
        latency_ms: Date.now() - startedAt,
        ...payload
      });
    }

    socket.setTimeout(timeoutMs);
    socket.once("connect", () => done({ ok: true }));
    socket.once("timeout", () => done({ ok: false, error: "timeout" }));
    socket.once("error", (err) => done({ ok: false, error: String(err && err.message ? err.message : err) }));

    try {
      socket.connect(Number(port), String(host));
    } catch (e) {
      done({ ok: false, error: String(e) });
    }
  });
}

function normalizeHttpProbeResult(name, result) {
  if (!result || typeof result !== "object") {
    return { ok: false, name, status: 0, error: "invalid_http_probe_result" };
  }
  return {
    ok: !!result.ok,
    name,
    status: Number(result.status || 0),
    error: result.ok ? null : "dashboard_unreachable",
    body: (result.json && typeof result.json === "object") ? result.json : null
  };
}

function buildOperatorSnapshotDiagnostics(snapshot) {
  const out = {
    top_failures: [],
    blocking_issues: [],
    suspected_root_causes: [],
    inspect_files_first: []
  };

  function addTop(value) {
    const v = String(value || "").trim();
    if (v && !out.top_failures.includes(v)) out.top_failures.push(v);
  }

  function addBlock(value) {
    const v = String(value || "").trim();
    if (v && !out.blocking_issues.includes(v)) out.blocking_issues.push(v);
  }

  function addRoot(value) {
    const v = String(value || "").trim();
    if (v && !out.suspected_root_causes.includes(v)) out.suspected_root_causes.push(v);
  }

  function addFile(value) {
    const v = String(value || "").trim();
    if (v && !out.inspect_files_first.includes(v)) out.inspect_files_first.push(v);
  }

  const support = snapshot.support_snapshot || {};
  const supportDiag = support.diagnostics || {};
  const readiness = snapshot.readiness || {};
  const health = snapshot.health || {};
  const endpointChecks = (((snapshot.dashboard || {}).reachability || {}).endpoint_checks) || {};
  const dbSchema = snapshot.db_schema || {};

  for (const item of (supportDiag.top_failures || [])) addTop(item);
  for (const item of (supportDiag.blocking_issues || [])) addBlock(item);
  for (const item of (supportDiag.suspected_root_causes || [])) addRoot(item);
  for (const item of (supportDiag.inspect_files_first || [])) addFile(item);

  for (const item of (readiness.issues || [])) {
    if (item && item.code) addTop(item.code);
    if (item && item.code) addRoot(item.code);
  }

  if (health && health.ok === false) {
    addBlock("dashboard_health_not_ok");
  }

  if (endpointChecks.health && endpointChecks.health.ok === false) {
    addTop("dashboard_unreachable");
    addRoot("dashboard_unreachable");
    addFile("dashboard_server.py");
    addFile("start_system.py");
  }

  if (dbSchema && dbSchema.ok === false) {
    addTop("db_schema_probe_failed");
    addRoot("db_schema_probe_failed");
    addFile("engine/runtime/storage.py");
    addFile("engine/runtime/jobs/repair_schema.py");
  }

  addFile("boot/operator_server.js");
  addFile("boot/operator_ui.html");

  out.top_failures = out.top_failures.slice(0, 20);
  out.blocking_issues = out.blocking_issues.slice(0, 20);
  out.suspected_root_causes = out.suspected_root_causes.slice(0, 20);
  out.inspect_files_first = out.inspect_files_first.slice(0, 12);
  return out;
}

async function buildOperatorSnapshot(mode = "repair") {
  const cfg = snapshotModeConfig(mode);
  const envObj = readEnv();
  const base = dashBaseUrlFromEnv(envObj);
  const readiness = await getReadiness();
  const preflight = await getPreflight(state.lastMode || "safe", { skipValidationGate: true });
  const health = (status() === "RUNNING") ? await verifyHealth() : null;

  const dashPort = Number(envObj.DASHBOARD_PORT || 8000);
  const dashHost = _normalizeDashHostForLoopback(String(envObj.DASHBOARD_HOST || "127.0.0.1"));

  const endpointChecksRaw = await Promise.all([
    httpGetJson(`${base}/api/health`, cfg.endpointTimeoutMs),
    httpGetJson(`${base}/api/system/state`, cfg.endpointTimeoutMs),
    httpGetJson(`${base}/api/operator/support_snapshot?mode=${encodeURIComponent(cfg.mode)}`, cfg.endpointTimeoutMs),
    httpGetJson(`${base}/api/validation`, cfg.endpointTimeoutMs),
    httpGetJson(`${base}/api/telemetry`, cfg.endpointTimeoutMs),
    httpGetJson(`${base}/api/operator/db_schema`, cfg.endpointTimeoutMs)
  ]);

  const endpointChecks = {
    health: normalizeHttpProbeResult("health", endpointChecksRaw[0]),
    system_state: normalizeHttpProbeResult("system_state", endpointChecksRaw[1]),
    support_snapshot: normalizeHttpProbeResult("support_snapshot", endpointChecksRaw[2]),
    validation: normalizeHttpProbeResult("validation", endpointChecksRaw[3]),
    telemetry: normalizeHttpProbeResult("telemetry", endpointChecksRaw[4]),
    db_schema: normalizeHttpProbeResult("db_schema", endpointChecksRaw[5])
  };

  const supportSnapshot = endpointChecks.support_snapshot.body || {
    ok: false,
    error: endpointChecks.support_snapshot.error || "dashboard_unreachable"
  };

  const dbSchema = endpointChecks.db_schema.body || {
    ok: false,
    error: endpointChecks.db_schema.error || "dashboard_unreachable"
  };

  const operatorTcp = await tcpProbe(OPERATOR_BIND_HOST, OPERATOR_PORT, 1500);
  const dashboardTcp = await tcpProbe(dashHost, dashPort, 1500);

  const currentAttemptRuntime = currentAttemptRuntimeTail(cfg.runtimeLogLines);
  const currentAttemptStderr = currentAttemptStderrTail(cfg.stderrChars);
  const stderrTail = {
    ok: true,
    chars: cfg.stderrChars,
    currentAttemptOnly: true,
    attemptId: state.currentAttemptId || null,
    attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
    currentStderrLogOffset: Number(state.currentStderrLogOffset || 0),
    logScope: isLinuxManagedMode() ? "managed_journald_current_attempt_window" : "file_offset_current_attempt",
    tail: currentAttemptStderr,
    limitation: isLinuxManagedMode() && !currentAttemptStderr
      ? "managed_mode_current_attempt_log_isolation_unavailable"
      : null
  };
  const runtimeTail = {
    ok: true,
    lines: cfg.runtimeLogLines,
    currentAttemptOnly: true,
    attemptId: state.currentAttemptId || null,
    attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
    currentRuntimeLogOffset: Number(state.currentRuntimeLogOffset || 0),
    logScope: isLinuxManagedMode() ? "managed_journald_current_attempt_window" : "file_offset_current_attempt",
    tail: currentAttemptRuntime,
    limitation: isLinuxManagedMode() && !currentAttemptRuntime
      ? "managed_mode_current_attempt_log_isolation_unavailable"
      : null
  };

  const externalRuntime = externalRuntimeState();
  const runtimeError = currentAttemptLastErrorForRuntime(externalRuntime);
  const processTree = {
    operator: {
      pid: process.pid,
      ppid: process.ppid,
      node: process.version,
      platform: os.platform()
    },
    engine_child: child ? {
      pid: child.pid || null,
      connected: !!child.connected,
      killed: !!child.killed,
      spawnfile: child.spawnfile || null
    } : null,
    external_runtime: externalRuntime,
    managed_engine: managedEngineState()
  };

  const snap = {
    snapshot_schema: {
      name: "operator_repair_snapshot",
      version: 3,
      producer: "boot/operator_server.js",
      mode: cfg.mode,
      stable_sections: [
        "snapshot_schema",
        "snapshot_mode",
        "at",
        "operator",
        "engine",
        "dashboard",
        "readiness",
        "preflight",
        "health",
        "support_snapshot",
        "db_schema",
        "runtime_log_tail",
        "python_stderr_tail",
        "diagnostics",
        "snapshot_meta"
      ]
    },
    snapshot_mode: cfg.mode,
    at: nowIso(),
    operator: {
      host: OPERATOR_BIND_HOST,
      port: OPERATOR_PORT,
      pid: process.pid,
      node: process.version,
      platform: os.platform(),
      productionMode: PRODUCTION_MODE,
      tcp_probe: operatorTcp
    },
    engine: {
      status: status(),
      entry: ENTRY,
      lastMode: state.lastMode || "safe",
      lastStartAt: state.lastStartAt,
      lastStopAt: state.lastStopAt,
      lastHealthyAt: state.lastHealthyAt,
      lastExitCode: state.lastExitCode,
      currentAttemptId: state.currentAttemptId || null,
      currentAttemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
      currentRuntimeLogOffset: Number(state.currentRuntimeLogOffset || 0),
      currentStderrLogOffset: Number(state.currentStderrLogOffset || 0),
      currentAttemptStatus: status(),
      currentAttemptLastError: runtimeError,
      lastRecordedCrash: lastRecordedCrash(),
      lastError: runtimeError,
      restartAttempts: state.restartAttempts,
      process_tree: processTree
    },
    dashboard: {
      baseUrl: base,
      host: dashHost,
      port: dashPort,
      tcp_probe: dashboardTcp,
      reachability: {
        endpoint_checks: endpointChecks
      }
    },
    readiness,
    preflight,
    health,
    support_snapshot: supportSnapshot,
    db_schema: dbSchema,
    runtime_log_tail: runtimeTail,
    python_stderr_tail: stderrTail,
    env: safeEnvForSnapshot(envObj),
    snapshot_meta: {
      captured_at: nowIso(),
      mode: cfg.mode,
      logScope: {
        runtime: runtimeTail.logScope,
        stderr: stderrTail.logScope,
        currentAttemptId: state.currentAttemptId || null,
        currentAttemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
        currentRuntimeLogOffset: Number(state.currentRuntimeLogOffset || 0),
        currentStderrLogOffset: Number(state.currentStderrLogOffset || 0),
        managedMode: isLinuxManagedMode()
      },
      truncation: {
        maxString: cfg.maxString,
        maxArray: cfg.maxArray,
        maxObjectKeys: cfg.maxObjectKeys,
        maxDepth: cfg.maxDepth
      }
    }
  };

  snap.diagnostics = buildOperatorSnapshotDiagnostics(snap);
  return truncateSnapshotValue(redactSnapshotSecrets(snap), cfg);
}

// --------------------------------------------------
// AutoFix / Repair
// --------------------------------------------------

function runPipInstallRequirements() {
  const python = pickPythonCmd();
  const reqPath = path.join(ROOT, "requirements.txt");
  if (!fs.existsSync(reqPath)) {
    return { ok: false, kind: "REQ_MISSING", message: "requirements.txt missing", details: reqPath };
  }

  const r = spawnSyncSafe(python, ["-m", "pip", "install", "-r", reqPath], {
    cwd: ROOT,
    stdio: "pipe",
    env: process.env,
    timeout: OPERATOR_PIP_TIMEOUT_MS,
    maxBuffer: 32 * 1024 * 1024
  });

  const out = (r.stdout ? String(r.stdout) : "") + (r.stderr ? String(r.stderr) : "");
  const ok = r.status === 0;

  return { ok, kind: ok ? "PIP_OK" : "PIP_FAIL", message: ok ? "pip install -r requirements.txt ok" : "pip install failed", details: out.trim() };
}

function touchDbFile(resolvedDb) {
  try {
    const dir = path.dirname(resolvedDb);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    if (!fs.existsSync(resolvedDb)) fs.writeFileSync(resolvedDb, "");
    return { ok: true, kind: "DB_OK", message: "DB file present", details: resolvedDb };
  } catch (e) {
    return { ok: false, kind: "DB_FAIL", message: "Could not create DB file", details: String(e) };
  }
}

// --------------------------------------------------
// Python bootstrap (schema/module DB init) — consolidated from ui_console.pyw
// --------------------------------------------------
function runPythonBootstrap(pythonCmd) {
  try {
    const steps = [];
    const pyPrefix = pythonCmd === "py" ? ["-3"] : [];

    // 1) Module-owned schemas expected by engine preflight
    {
      const pyArgs = pythonCmd === "py" ? ["-3", "-u", "-c"] : ["-u", "-c"];

      const r = spawnSyncSafe(
        pythonCmd,
        [...pyArgs,
          "from engine.strategy.portfolio import init_portfolio_db; "
          + "from engine.execution.broker_sim import init_broker_db; "
          + "from engine.runtime.alerts import init_alerts_db; "
          + "from engine.strategy.validation import init_validation_db; "
          + "from engine.strategy.model_v2 import init_model_db; "
          + "init_portfolio_db(); init_broker_db(); init_alerts_db(); init_validation_db(); init_model_db(); "
          + "print('[startup] module db init ok')"
        ],
        {
  cwd: ROOT,
  env: { ...process.env, PYTHONPATH: ROOT },
  stdio: "pipe"
}
      );

      const out = (r.stdout ? String(r.stdout) : "") + (r.stderr ? String(r.stderr) : "");
      const ok = r.status === 0;
      if (!ok) console.error("[MODULE_DB_INIT ERROR]", out);
steps.push({ id: "module_db_init", ok, details: out.trim() });
      if (!ok) return { ok: false, steps };
    }

    // 2) Ensure backtest output tables exist (engine.strategy.portfolio_backtest)
    {
      const pyArgs = pythonCmd === "py" ? ["-3", "-u", "-c"] : ["-u", "-c"];

      const r = spawnSyncSafe(
        pythonCmd,
        [...pyArgs,
          "from engine.strategy.portfolio_backtest import SCHEMA; "
          + "from engine.runtime.storage import connect; "
          + "con=connect(); con.executescript(SCHEMA); con.commit(); con.close(); "
          + "print('[startup] portfolio_backtest schema ok')"
        ],
        {
  cwd: ROOT,
  env: { ...process.env, PYTHONPATH: ROOT },
  stdio: "pipe"
}
      );

      const out = (r.stdout ? String(r.stdout) : "") + (r.stderr ? String(r.stderr) : "");
      const ok = r.status === 0;
      if (!ok) console.error("[BACKTEST_SCHEMA ERROR]", out);
steps.push({ id: "backtest_schema", ok, details: out.trim() });
      if (!ok) return { ok: false, steps };
    }

    return { ok: true, steps };
  } catch (e) {
    return { ok: false, steps: [{ id: "bootstrap_exception", ok: false, details: String(e) }] };
  }
}

// --------------------------------------------------
// Institutional Check (health + telemetry changes)
// --------------------------------------------------

async function checkTelemetryFlow() {
  const envObj = readEnv();
  const base = dashBaseUrlFromEnv(envObj);
  const url = `${base}/api/telemetry`;

  const a = await httpGetJson(url);
  if (!a.ok) return { ok: false, url, detail: "telemetry not responding" };

  await sleep(1200);

  const b = await httpGetJson(url);
  if (!b.ok) return { ok: false, url, detail: "telemetry not responding (second sample)" };

  const sa = JSON.stringify(a.json || {});
  const sb = JSON.stringify(b.json || {});
  const changed = sa !== sb;

  return { ok: changed, url, detail: changed ? "telemetry changed" : "telemetry unchanged" };
}

// --------------------------------------------
// Ensure Polygon stream job exists + running
// --------------------------------------------
app.post("/api/operator/ensure_polygon_stream", async (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.job_start", {
    target: "polygon_stream",
  });
  if (!confirmation) return;
  try {
    const envObj = readEnv();
    const mode = state.lastMode || "safe";

    // Do not start in SAFE mode
    if (mode === "safe") {
      return jsonOk(res, { action: "safe_mode_blocked" });
    }

    const polygonWsStatus = getPolygonWebsocketStatus(envObj);
    if (!polygonWsStatus.ok) {
      return jsonFail(res, "polygon_ws_not_ready", 503, {
        detail: polygonWsStatus.details,
        canonical_url: "/ui/data_sources.html"
      });
    }

    const base = dashBaseUrlFromEnv(envObj);

    const jobsRes = await httpGetJson(`${base}/api/jobs`);
    if (!jobsRes.ok || !jobsRes.json) {
      return sendOperatorPayload(
        res,
        buildOperatorProxyFailure(jobsRes, "jobs_api_unreachable", {}, 503),
        503
      );
    }

    const jobs = jobsRes.json.jobs || [];
    const ingestionJob = jobs.find(j => j.name === "ingestion_runtime");
    const streamJob = jobs.find(j => j.name === "stream_prices_polygon_ws");

    if (!ingestionJob && !streamJob) {
      return jsonFail(res, "job_not_registered", 404);
    }

    const targetJob = ingestionJob || streamJob;
    const targetName = ingestionJob ? "ingestion_runtime" : "stream_prices_polygon_ws";

    if (!targetJob.running) {
      const started = await httpPostJson(`${base}/api/jobs/start?name=${encodeURIComponent(targetName)}`, {
        name: targetName,
        ...operatorConfirmationBody(req, "JOB_ACTION", {
          actionId: "jobs.start",
          target: targetName,
        }),
      });
      const payload = {
        ok: !!(started && started.ok && (!started.json || started.json.ok !== false)),
        action: started && started.ok ? "started" : "start_failed",
        job: targetName,
        result: started ? (started.json || null) : null,
        upstream_status: Number((started && started.status) || 0),
        upstream_error: started ? (started.error || null) : null
      };
      return sendOperatorPayload(
        res,
        payload,
        payload.ok ? 202 : operatorErrorStatus((started && (started.error || started.json?.error)) || "jobs_api_unreachable", 503)
      );
    }

    return jsonOk(res, { action: "already_running", job: targetName });

  } catch (e) {
    return jsonFail(res, "ensure_polygon_stream_failed", 500, { detail: String(e) });
  }
});

// --------------------------------------------------
// Proxy: stop job via dashboard
// --------------------------------------------------
app.post("/api/operator/jobs/stop", async (req, res) => {
  try {
    const name = String((req.body && req.body.name) || req.query.name || "").trim();
    if (!name) return jsonFail(res, "missing_name", 400);
    const confirmation = requireOperatorConfirmation(req, res, "operator.job_stop", {
      target: name,
    });
    if (!confirmation) return;

    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);

    const r = await httpPostJson(`${base}/api/jobs/stop?name=${encodeURIComponent(name)}`, {
      name,
      ...operatorConfirmationBody(req, "JOB_ACTION", {
        actionId: "jobs.stop",
        target: name,
      }),
    });
    if (!r.ok) {
      return sendOperatorPayload(
        res,
        buildOperatorProxyFailure(r, r.error || "dashboard_unreachable", { job: name }, operatorErrorStatus(r.error || "dashboard_unreachable", 503)),
        operatorErrorStatus(r.error || "dashboard_unreachable", 503)
      );
    }
    if (!r.json || typeof r.json !== "object") {
      return sendOperatorPayload(
        res,
        buildOperatorProxyFailure(r, "dashboard_invalid_json", { job: name }, 502),
        502
      );
    }

    return jsonState(res, r.json, Number(r.status || 200));
  } catch (e) {
    return jsonFail(res, "jobs_stop_failed", 500, { detail: String(e) });
  }
});

// --------------------------------------------------
// API
// --------------------------------------------------

app.get("/api/operator/status", wrapOperatorRoute(async (req, res) => {
  const ext = externalRuntimeState();
  const engineStatus = status();
  const runtimeError = currentAttemptLastErrorForRuntime(ext);
  const degradedComponents = [];
  if (engineStatus !== "RUNNING") degradedComponents.push("engine_process");
  if (ext.active) degradedComponents.push("operator_supervision");
  if (runtimeError) degradedComponents.push("startup_attempt");
  if (state.restartBlocked === true || state.fatal === true) degradedComponents.push("restart_supervision");
  const currentBlocker = runtimeError
    ? {
        code: String(runtimeError.kind || "LAST_ERROR_PRESENT"),
        level: "warn",
        message: `Current attempt error: ${runtimeError.kind} - ${runtimeError.message}`
      }
    : null;

  return sendOperatorPayload(res, {
    status: engineStatus,
    installing,
    productionMode: PRODUCTION_MODE,
    lastExitCode: state.lastExitCode,
    lastExitAt: state.lastExitAt || null,
    lastExitSignal: state.lastExitSignal || null,
    restartAttempts: state.restartAttempts,
    consecutiveStartupFailures: Number(state.consecutiveStartupFailures || 0),
    restartBlocked: state.restartBlocked === true,
    fatal: state.fatal === true,
    fatalSince: state.fatalSince || null,
    nextRestartAt: state.nextRestartAt || null,
    lastStartAt: state.lastStartAt,
    lastStopAt: state.lastStopAt,
    lastHealthyAt: state.lastHealthyAt,
    lastMode: state.lastMode || "safe",
    currentAttemptId: state.currentAttemptId || null,
    currentAttemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
    currentAttemptLastError: runtimeError,
    lastRecordedCrash: lastRecordedCrash(),
    lastError: runtimeError,
    ready: engineStatus === "RUNNING" && !runtimeError,
    degraded: engineStatus !== "RUNNING" || !!runtimeError || ext.active,
    currentBlocker,
    degradedComponents,
    health: null,
    externalRuntime: ext.active ? {
      pid: ext.pid,
      record: ext.record || null,
      managedByOperator: false
    } : null
  });
}));

app.get("/api/operator/bootstrap", (req, res) => {
  return jsonOk(res, {
    nodeVersion: process.version,
    platform: os.platform(),
    productionMode: PRODUCTION_MODE,
    envExists: fs.existsSync(ENV_PATH),
    entryExists: fs.existsSync(ENTRY),
    logDirExists: fs.existsSync(LOG_DIR),
    operator: { host: OPERATOR_BIND_HOST, port: OPERATOR_PORT }
  });
});

// UI expects this endpoint
app.get("/api/operator/bootstrapStatus", wrapOperatorRoute(async (req, res) => {
  const envObj = readEnv();
  const readiness = await getReadiness();
  const preflight = await getPreflight(state.lastMode || "safe", { skipValidationGate: true });
  const health = (status() === "RUNNING") ? await verifyHealth() : null;
  const ext = externalRuntimeState();
  const runtimeError = currentAttemptLastErrorForRuntime(ext);

  return sendOperatorPayload(res, {
    at: nowIso(),
    operator: {
      host: OPERATOR_BIND_HOST,
      port: OPERATOR_PORT,
      node: process.version,
      platform: os.platform(),
      productionMode: PRODUCTION_MODE
    },
    engine: {
      status: status(),
      entry: ENTRY,
      lastMode: state.lastMode || "safe",
      lastStartAt: state.lastStartAt,
      lastStopAt: state.lastStopAt,
      lastHealthyAt: state.lastHealthyAt,
      lastExitCode: state.lastExitCode,
      currentAttemptId: state.currentAttemptId || null,
      currentAttemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
      currentAttemptLastError: runtimeError,
      lastRecordedCrash: lastRecordedCrash(),
      lastError: runtimeError,
      restartAttempts: state.restartAttempts,
      externalRuntime: ext.active ? {
        pid: ext.pid,
        record: ext.record || null,
        managedByOperator: false
      } : null
    },
    dashboard: { baseUrl: dashBaseUrlFromEnv(envObj) },
    readiness,
    preflight,
    health,
    currentBlocker: readiness.currentBlocker || null,
    degradedComponents: Array.isArray(readiness.degradedComponents) ? readiness.degradedComponents : []
  });
}));

app.get("/api/operator/config", wrapOperatorRoute(async (req, res) => {
  return jsonOk(res, safeEnvForSnapshot(readEnv()));
}));

app.get("/api/operator/feed_configs", (req, res) => {
  return jsonOk(res, {
    deprecated: true,
    canonical_url: "/ui/data_sources.html",
    message: "Data source setup is managed from the single DB-backed Data Sources page.",
    feeds: []
  });
});

app.post("/api/operator/feed_configs", (req, res) => {
  res.status(410).json({
    ok: false,
    error: "deprecated_use_data_sources_ui",
    canonical_url: "/ui/data_sources.html",
    message: "Data source setup is managed from the single DB-backed Data Sources page."
  });
});

app.post("/api/operator/feed_configs/delete", (req, res) => {
  res.status(410).json({
    ok: false,
    error: "deprecated_use_data_sources_ui",
    canonical_url: "/ui/data_sources.html",
    message: "Data source setup is managed from the single DB-backed Data Sources page."
  });
});

app.post("/api/operator/config", wrapOperatorRoute(async (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.config_write", {
    target: ".env",
  });
  if (!confirmation) return;
  if (!req.body || typeof req.body !== "object" || Array.isArray(req.body)) {
    return jsonFail(res, "invalid_config_payload", 400);
  }

  const result = writeEnv(stripConfirmationFields(req.body || {}));
  invalidateOperatorHealthCache();
  invalidateOperatorDbCaches();

  if (result && result.ok) {
    try {
      const health = await verifyHealth({ force: true });
      broadcastTelemetry("health_update", health);
    } catch {}
  }

  return sendOperatorPayload(res, result, result && result.ok ? 200 : 422);
}));

app.get("/api/operator/config/validate", wrapOperatorRoute(async (req, res) => {
  const envObj = readEnv();
  const { sanitized, issues } = validateAndSanitizeEnv(envObj);
  return jsonState(res, {
    ok: !issues.some((i) => i.level === "error"),
    issues,
    sanitized: safeEnvForSnapshot(sanitized)
  }, 200);
}));

app.post("/api/operator/secrets", wrapOperatorRoute(async (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.secrets_write", {
    target: "operator.secrets.json",
  });
  if (!confirmation) return;
  if (!req.body || typeof req.body !== "object" || Array.isArray(req.body)) {
    return jsonFail(res, "invalid_secret_payload", 400);
  }

  const secretPayload = stripConfirmationFields(req.body || {});
  const entries = Object.entries(secretPayload)
    .filter(([key]) => String(key || "").trim().length > 0);
  if (entries.length === 0) {
    return jsonFail(res, "missing_secret_entries", 400);
  }

  const invalidKey = entries.find(([key]) => !/^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/.test(String(key || "")));
  if (invalidKey) {
    return jsonFail(res, "invalid_secret_key", 422, {
      key: String(invalidKey[0] || "")
    });
  }

  const secrets = loadSecrets();
  for (const [key, value] of entries) {
    if (value === undefined) {
      return jsonFail(res, "missing_secret_value", 422, {
        key: String(key || "")
      });
    }
    secrets[key] = encrypt(value);
  }
  saveSecrets(secrets);
  return jsonOk(res, {
    saved: entries.map(([key]) => String(key || "")),
    count: entries.length
  });
}));

app.get("/api/operator/secrets", (req, res) => {
  const secrets = loadSecrets();
  return jsonOk(res, { keys: Object.keys(secrets) });
});

app.get("/api/operator/logs", wrapOperatorRoute(async (req, res) => {
  const n = Math.max(50, Math.min(2000, Number(req.query.lines || 400)));
  const service = String(req.query.service || "").trim().toLowerCase();

  if (service && isLinuxManagedMode()) {
    const r = runServiceCtl(["logs", service, String(n)], { parseJson: false, timeout: 30000 });
    if (!r.ok) {
      return jsonFail(res, "service_log_read_failed", 500, {
        service,
        logs: "",
        detail: String(r.error || "service_log_read_failed")
      });
    }
    const logs = redactOperatorSensitiveText(String(r.text || ""));
    return jsonOk(res, {
      service,
      logs,
      lines: logs.split(/\r?\n/).filter(Boolean)
    });
  }

  const logs = redactOperatorSensitiveText(tailLog(n));
  return jsonOk(res, {
    service: service || "runtime",
    logs,
    lines: String(logs || "").split(/\r?\n/).filter(Boolean)
  });
}));

// --------------------------------------------------
// COMPAT LOG ENDPOINTS (UI + DEBUG)
// --------------------------------------------------

app.get("/api/operator/runtime_logs", wrapOperatorRoute(async (req, res) => {
  const n = Math.max(50, Math.min(2000, Number(req.query.lines || 200)));
  const logs = redactOperatorSensitiveText(currentAttemptRuntimeTail(n));
  return jsonOk(res, {
    logs,
    lines: String(logs || "").split(/\r?\n/).filter(Boolean),
    attemptId: state.currentAttemptId || null
  });
}));

app.get("/api/operator/stderr_logs", wrapOperatorRoute(async (req, res) => {
  const n = Math.max(1024, Math.min(512000, Number(req.query.lines || 200) * 200));
  const logs = redactOperatorSensitiveText(currentAttemptStderrTail(n));
  return jsonOk(res, {
    logs,
    lines: String(logs || "").split(/\r?\n/).filter(Boolean),
    attemptId: state.currentAttemptId || null
  });
}));

app.get("/api/operator/verifyHealth", wrapOperatorRoute(async (req, res) => {
  const r = await verifyHealth();
  return sendOperatorPayload(res, r, 200);
}));

app.get("/api/operator/readiness", wrapOperatorRoute(async (req, res) => {
  const r = await getReadiness();
  return sendOperatorPayload(res, r, 200);
}));

function buildCanonicalApiFailure(reason) {
  const ts = Date.now();
  return {
    ok: false,
    status: "STOPPED",
    state: "UNKNOWN",
    mode: "unknown",
    execution_mode: "unknown",
    execution_allowed: false,
    reasons: [String(reason || "unknown_error")],
    health: {},
    ingestion: {},
    services: {},
    readiness: {},
    timestamps: { ts_ms: ts, snapshot_ts_ms: ts }
  };
}

function buildOperatorProxyFailure(result, fallbackError, extra = {}, statusCodeOverride = null) {
  const error = String(
    fallbackError ||
    (result && result.error) ||
    "dashboard_unreachable"
  );
  const upstreamStatus = Number((result && result.status) || 0);
  const statusCode = Number.isFinite(Number(statusCodeOverride)) && Number(statusCodeOverride) >= 100
    ? Number(statusCodeOverride)
    : (
      upstreamStatus >= 400
        ? upstreamStatus
        : operatorErrorStatus(error, 503)
    );
  return {
    ok: false,
    error,
    upstream_status: upstreamStatus,
    upstream_error: String((result && result.error) || ""),
    upstream_duration_ms: Number((result && result.duration_ms) || 0),
    timed_out: !!(result && result.timed_out),
    ...extra,
    meta: {
      status: statusCode
    }
  };
}

function isCanonicalApiShape(payload) {
  if (!payload || typeof payload !== "object") return false;
  const required = [
    "ok",
    "status",
    "state",
    "mode",
    "execution_mode",
    "execution_allowed",
    "reasons",
    "health",
    "ingestion",
    "services",
    "readiness",
    "timestamps"
  ];
  return required.every((key) => Object.prototype.hasOwnProperty.call(payload, key));
}

function operatorCanonicalProxyGet(path, invalidError) {
  return wrapOperatorRoute(async (req, res) => {
      const envObj = readEnv();
      const base = dashBaseUrlFromEnv(envObj);
      const r = await httpGetJson(`${base}${path}`);
      if (!r.ok) {
        return sendOperatorPayload(
          res,
          buildOperatorProxyFailure(r, r.error || "dashboard_unreachable", {
            ...buildCanonicalApiFailure(r.error || "dashboard_unreachable")
          }, 503),
          503
        );
      }
      if (!isCanonicalApiShape(r.json)) {
        return sendOperatorPayload(
          res,
          buildOperatorProxyFailure(r, invalidError, {
            ...buildCanonicalApiFailure(invalidError)
          }, 502),
          502
        );
      }
      return jsonState(res, r.json, Number(r.status || 200));
  });
}

function operatorHealthProxyGet(path, invalidError) {
  return wrapOperatorRoute(async (req, res) => {
      const envObj = readEnv();
      const base = dashBaseUrlFromEnv(envObj);
      const r = await httpGetJson(`${base}${path}`);
      if (!r.ok) {
        return sendOperatorPayload(res, {
          ok: false,
          error: String(r.error || "dashboard_unreachable"),
          status: Number(r.status || 0),
          body: null,
          healthy: false,
          transport_error: String(r.error || "dashboard_unreachable"),
          timed_out: !!r.timed_out
        }, operatorErrorStatus(r.error || "dashboard_unreachable", 503));
      }
      if (!r.json || typeof r.json !== "object") {
        return sendOperatorPayload(res, {
          ok: false,
          error: invalidError,
          status: Number(r.status || 0),
          body: null,
          healthy: false
        }, 502);
      }
      return jsonState(res, {
        ok: !!r.json.ok,
        error: null,
        healthy: !!r.json.ok,
        degraded: r.json.ok === false,
        status: Number(r.status || 200),
        body: r.json
      }, 200);
  });
}

const OPERATOR_BARRIER_PROXY_TIMEOUT_MS = clampNumber(
  process.env.OPERATOR_BARRIER_PROXY_TIMEOUT_MS || 60000,
  60000,
  5000,
  300000
);

function operatorProxyGet(path, invalidError, timeoutOrOptions) {
  return wrapOperatorRoute(async (req, res) => {
      const options = (timeoutOrOptions && typeof timeoutOrOptions === "object")
        ? timeoutOrOptions
        : {};
      const timeout = (timeoutOrOptions && typeof timeoutOrOptions === "object")
        ? options.timeout
        : timeoutOrOptions;
      const envObj = readEnv();
      const base = dashBaseUrlFromEnv(envObj);
      const r = await httpGetJson(`${base}${path}`, timeout);
      if (!r.ok) {
        return sendOperatorPayload(
          res,
          buildOperatorProxyFailure(r, r.error || "dashboard_unreachable", {}, operatorErrorStatus(r.error || "dashboard_unreachable", 503)),
          operatorErrorStatus(r.error || "dashboard_unreachable", 503)
        );
      }
      if (!r.json || typeof r.json !== "object") {
        return sendOperatorPayload(
          res,
          buildOperatorProxyFailure(r, invalidError, {}, 502),
          502
        );
      }
      const payload = options.redact ? redactOperatorSecrets(r.json) : r.json;
      return jsonState(res, payload, Number(r.status || 200));
  });
}

function operatorProxyPost(path, invalidError, timeout) {
  return wrapOperatorRoute(async (req, res) => {
      const envObj = readEnv();
      const base = dashBaseUrlFromEnv(envObj);
      const r = await httpPostJson(`${base}${path}`, req.body || {}, timeout);
      if (!r.ok) {
        return sendOperatorPayload(
          res,
          buildOperatorProxyFailure(r, r.error || "dashboard_unreachable", {}, operatorErrorStatus(r.error || "dashboard_unreachable", 503)),
          operatorErrorStatus(r.error || "dashboard_unreachable", 503)
        );
      }
      if (!r.json || typeof r.json !== "object") {
        return sendOperatorPayload(
          res,
          buildOperatorProxyFailure(r, invalidError, {}, 502),
          502
        );
      }
      return jsonState(res, r.json, Number(r.status || 200));
  });
}

function operatorConfirmedProxyPost(path, invalidError, actionId, timeout) {
  return wrapOperatorRoute(async (req, res) => {
      const confirmation = requireOperatorConfirmation(req, res, actionId);
      if (!confirmation) return;
      const envObj = readEnv();
      const base = dashBaseUrlFromEnv(envObj);
      const r = await httpPostJson(`${base}${path}`, req.body || {}, timeout);
      if (!r.ok) {
        return sendOperatorPayload(
          res,
          buildOperatorProxyFailure(r, r.error || "dashboard_unreachable", {}, operatorErrorStatus(r.error || "dashboard_unreachable", 503)),
          operatorErrorStatus(r.error || "dashboard_unreachable", 503)
        );
      }
      if (!r.json || typeof r.json !== "object") {
        return sendOperatorPayload(
          res,
          buildOperatorProxyFailure(r, invalidError, {}, 502),
          502
        );
      }
      return jsonState(res, r.json, Number(r.status || 200));
  });
}

app.get("/api/operator/preflight", wrapOperatorRoute(async (req, res) => {
  const mode = String(req.query.mode || state.lastMode || "safe");
  const forceValidation = ["1", "true", "yes", "on"].includes(
    String(req.query.force || "").trim().toLowerCase()
  );
  const r = await getPreflight(mode, { forceValidation });
  return sendOperatorPayload(res, r, 200);
}));

app.get("/api/operator/preflight_report",
  operatorProxyGet("/api/operator/preflight_report", "invalid_preflight_report_response")
);

app.get("/api/operator/system_health",
  operatorHealthProxyGet("/api/health", "invalid_system_health_response")
);

app.get("/api/operator/trading_readiness",
  operatorProxyGet("/api/operator/trading_readiness", "invalid_trading_readiness_response")
);

app.get("/api/execution/barrier",
  operatorProxyGet("/api/execution/barrier", "invalid_execution_barrier_response", OPERATOR_BARRIER_PROXY_TIMEOUT_MS)
);

app.get("/api/operator/runtime_watchdogs",
  operatorProxyGet("/api/operator/runtime_watchdogs", "invalid_runtime_watchdogs_response")
);

app.get("/api/operator/provider_telemetry",
  operatorProxyGet("/api/operator/provider_telemetry", "invalid_provider_telemetry_response")
);

app.get("/api/operator/supervisor_diagnostics",
  operatorProxyGet("/api/operator/supervisor_diagnostics", "invalid_supervisor_diagnostics_response")
);

app.get("/api/operator/support_snapshot",
  operatorProxyGet("/api/operator/support_snapshot", "invalid_support_snapshot_response", { redact: true })
);

app.get("/api/operator/runtime_log_tail", wrapOperatorRoute(async (req, res) => {
    const lines = Math.max(50, Math.min(2000, Number(req.query.lines || 250) || 250));
    const tail = redactOperatorSensitiveText(tailLog(lines));
    return jsonOk(res, {
      ok: true,
      lines,
      tail,
      currentAttemptOnly: true,
      attemptId: state.currentAttemptId || null,
      attemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
      logScope: isLinuxManagedMode() ? "managed_journald_current_attempt_window" : "file_offset_current_attempt"
    });
}));

app.post("/api/operator/self_repair",
  operatorConfirmedProxyPost("/api/operator/self_repair", "invalid_self_repair_response", "operator.self_repair")
);

app.post("/api/operator/bootstrap_pipeline",
  operatorConfirmedProxyPost("/api/operator/bootstrap_pipeline", "invalid_bootstrap_pipeline_response", "operator.guided_bootstrap", 180000)
);

// UI expects this endpoint
app.get("/api/operator/institutionalCheck", wrapOperatorRoute(async (req, res) => {
  ensureEnvFile();
  const envObj = readEnv();
  const { sanitized, issues } = validateAndSanitizeEnv(envObj);

  const entryExists = fs.existsSync(ENTRY);
  const configValid = !issues.some((i) => i.level === "error");

  const { resolvedDb } = resolveDbPathFromSanitized(sanitized);
  const dbPathWritable = isPathWritable(resolvedDb);

const health = (status() === "RUNNING") ? await verifyHealth() : { ok: false };
const healthOk = !!(health && health.ok);

let schemaInvalid = false;
if (healthOk && health.body && Array.isArray(health.body.notes)) {
  const notesText = health.body.notes.join(" ");
  if (notesText.includes("missing_tables") || notesText.includes("missing_cols")) {
    schemaInvalid = true;
  }
}
  const telemetry = (status() === "RUNNING") ? await checkTelemetryFlow() : { ok: false, detail: "engine not running" };
  const dataFlowing = !!(telemetry && telemetry.ok);

  const errors = [];
  if (!configValid) errors.push("config invalid");
  if (!entryExists) errors.push("entry missing");
  if (!dbPathWritable) errors.push("db not writable");
  if (!healthOk) errors.push("health not ok");
  if (!dataFlowing) errors.push("telemetry not changing");

  const degradedComponents = [
    ...(configValid ? [] : ["config"]),
    ...(entryExists ? [] : ["entrypoint"]),
    ...(dbPathWritable ? [] : ["database_path"]),
    ...(healthOk ? [] : ["health"]),
    ...(dataFlowing ? [] : ["telemetry"])
  ];
  const currentBlocker = errors[0] || null;

  return jsonState(res, {
    ok: configValid && entryExists && healthOk && dataFlowing,
    configValid,
    entryExists,
    dbPathWritable,
    healthOk,
    schemaInvalid,
    requiresRepair: schemaInvalid,
    details: {
      dashboardBase: dashBaseUrlFromEnv(sanitized),
      telemetry: telemetry || null,
      health: health || null,
      resolvedDb
    },
    errors,
    currentBlocker,
    degradedComponents
  }, 200);
}));

app.post("/api/operator/repairSchema", async (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.repair_schema");
  if (!confirmation) return;
  try {
    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);
    const repairPayload = operatorConfirmationBody(req, "REPAIR_SCHEMA", {
      actionId: "operator.repair_schema",
      target: "runtime_schema",
    });

    const r = await new Promise((resolve) => {
      const lib = base.startsWith("https") ? https : http;
      const body = JSON.stringify(repairPayload);
      const req2 = lib.request(
        `${base}/api/system/repair_schema`,
        {
          method: "POST",
          headers: {
            ...trustedControlPlaneAuthHeaders("POST", `${base}/api/system/repair_schema`),
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(body),
          },
        },
        (resp) => {
          let data = "";
          resp.on("data", (c) => (data += c));
          resp.on("end", () => {
            try {
              resolve({ ok: true, json: JSON.parse(data || "{}") });
            } catch {
              resolve({ ok: false, json: null });
            }
          });
        }
      );
      req2.on("error", () => resolve({ ok: false }));
      req2.write(body);
      req2.end();
    });

    if (!r.ok || !r.json || !r.json.ok) {
      setLastError("SCHEMA_REPAIR_FAIL", "Schema repair failed", r.json);
      return sendOperatorPayload(res, { ok: false, result: r.json || null, error: "schema_repair_failed" }, 503);
    }

    clearLastError();
    invalidateOperatorDbCaches();
    return jsonOk(res, { result: r.json }, 202);
  } catch (e) {
    setLastError("SCHEMA_REPAIR_EXCEPTION", "Schema repair exception", String(e));
    return jsonFail(res, "schema_repair_exception", 500, { detail: String(e) });
  }
});

// UI calls this (AutoFix/Repair)
app.post("/api/operator/autofix", async (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.self_repair", {
    target: "autofix",
  });
  if (!confirmation) return;
  const steps = [];
  try {
    ensureEnvFile();
    ensureLogDir();

    // Normalize env first
    const envObj = readEnv();
    const { sanitized, issues } = validateAndSanitizeEnv(envObj);
    if (!issues.some((i) => i.level === "error")) {
      atomicWrite(ENV_PATH, serializeEnv(sanitized));
      steps.push({ ok: true, kind: "ENV_OK", message: "Normalized .env", details: ENV_PATH });
    } else {
      steps.push({ ok: false, kind: "ENV_INVALID", message: "Config has validation errors", details: issues });
    }

    // pip install
    steps.push(runPipInstallRequirements());

    // touch DB
    const { resolvedDb } = resolveDbPathFromSanitized(sanitized || envObj || {});
    steps.push(touchDbFile(resolvedDb));

    // clear last error
    clearLastError();
    steps.push({ ok: true, kind: "CLEAR_ERROR", message: "Cleared last error", details: null });
    invalidateOperatorDbCaches();

    const ok = steps.every((s) => !!s.ok);
    if (!ok) setLastError("AUTOFIX_PARTIAL", "AutoFix could not complete all steps", steps);

    return sendOperatorPayload(res, { ok, steps }, ok ? 200 : 503);
  } catch (e) {
    setLastError("AUTOFIX_FAIL", "AutoFix failed", { message: String(e) });
    return jsonFail(res, "autofix_failed", 500, { steps, detail: String(e) });
  }
});

function isLiveModeBlocked() {
  try {
    // adapt if your system_state path differs
    const state = global.__SYSTEM_STATE__ || {};
    return state.execution_mode === "live";
  } catch {
    return false;
  }
}

app.post("/api/operator/start", async (req, res) => {
  const mode = String((req.body && req.body.mode) || "safe").trim().toLowerCase() || "safe";
  const confirmation = requireOperatorConfirmation(
    req,
    res,
    mode === "live" ? "operator.live_start" : "operator.start",
    { target: `mode:${mode}` }
  );
  if (!confirmation) return;

  const steps = [];

  if (OPERATOR_DISABLE_INTERNAL_ENGINE_START) {
    const detail = startEngine(mode);
    return sendOperatorPayload(res, {
      ok: true,
      status: status(),
      disabled: true,
      reason: "OPERATOR_DISABLE_INTERNAL_ENGINE_START",
      steps: [
        {
          id: "spawn",
          ok: true,
          label: "Runtime ownership",
          detail
        }
      ]
    });
  }

  steps.push({ id: "preflight", ok: true, label: "Preflight checks", detail: "running" });
  const pre = await getPreflight(mode, { forceValidation: true });
  if (!pre.ok) {
    steps[steps.length - 1] = { id: "preflight", ok: false, label: "Preflight checks", detail: pre.checks };
    setLastError("PREFLIGHT_FAIL", "Preflight checks failed; cannot start", pre.checks);
    return jsonFail(res, "preflight_failed", 422, { status: "STOPPED", steps, preflight: pre });
  }
  steps[steps.length - 1] = { id: "preflight", ok: true, label: "Preflight checks", detail: "ok" };

  steps.push({ id: "spawn", ok: true, label: "Launching backend", detail: "starting python" });

    if ((state.restartBlocked === true || state.fatal === true || state.lastError?.kind === "CRASH_LOOP_DETECTED") && mode === "live") {
    return jsonFail(res, "safe_locked", 409, {
      ok: false,
      status: "SAFE_LOCKED",
      error: "System locked in SAFE mode due to crash loop. Manual intervention required."
    });
  }

  const r = startEngine(mode);
  if (!r.ok) {
    steps[steps.length - 1] = { id: "spawn", ok: false, label: "Launching backend", detail: r };
    setLastError("START_FAIL", "Could not start backend", r);
    return jsonFail(res, "start_failed", 503, { status: "STOPPED", steps });
  }
  steps[steps.length - 1] = { id: "spawn", ok: true, label: "Launching backend", detail: `started (${mode})` };

// Wait for backend socket bind first
steps.push({ id: "bind_wait", ok: false, label: "Waiting for backend bind", detail: "checking port" });

const envObjBind = readEnv();
const bindPort = Number(envObjBind.DASHBOARD_PORT || 8000);
const bindHost = _normalizeDashHostForLoopback(envObjBind.DASHBOARD_HOST || "127.0.0.1");
  const bindWaitMs = Math.max(
    10000,
    Number(envObjBind.OPERATOR_BIND_WAIT_MS || process.env.OPERATOR_BIND_WAIT_MS || 600000)
  );
const bindPollMs = 500;
const bindAttempts = Math.max(1, Math.ceil(bindWaitMs / bindPollMs));

let bound = false;
let bindFailure = null;

for (let i = 0; i < bindAttempts; i++) {
  await sleep(bindPollMs);

  if (!child) {
    const stderrTail = currentAttemptStderrTail(16000);
    const stdoutTail = currentAttemptRuntimeTail(120);
    bindFailure = {
      kind: "engine_exited_before_bind",
      code: state.lastExitCode,
      stderrTail,
      stdoutTail
    };
    break;
  }

  const portFree = await checkPortAvailable(bindPort, bindHost);
  if (!portFree) {
    bound = true;
    break;
  }
}

if (!bound) {
  const stderrTail = currentAttemptStderrTail(16000);
  const stdoutTail = currentAttemptRuntimeTail(120);
  const detail = bindFailure || {
    kind: "port_never_bound",
    waitMs: bindWaitMs,
    host: bindHost,
    port: bindPort,
    lastExitCode: state.lastExitCode,
    stderrTail,
    stdoutTail
  };

  steps[steps.length - 1] = {
    id: "bind_wait",
    ok: false,
    label: "Waiting for backend bind",
    detail
  };
  setLastError("BIND_TIMEOUT", "Backend never bound to port", detail);
  return jsonFail(res, "bind_timeout", 504, { status: "BIND_TIMEOUT", steps, detail });
}

steps[steps.length - 1] = {
  id: "bind_wait",
  ok: true,
  label: "Waiting for backend bind",
  detail: `port bound (${bindHost}:${bindPort})`
};

// Now poll health
steps.push({ id: "health", ok: false, label: "Waiting for health", detail: "polling /api/health" });

let healthy = false;
let lastHealth = null;

for (let i = 0; i < 120; i++) {
  await sleep(i < 10 ? 1000 : 2000);

  lastHealth = await verifyHealth();

  if (lastHealth && lastHealth.ok === true) {
    healthy = true;
    break;
  }
}

if (!healthy) {
  if (lastHealth && lastHealth.warmup === true) {
    steps[steps.length - 1] = { id: "health", ok: true, label: "Warming up", detail: "Waiting for first price tick" };
    return jsonOk(res, { status: "WARMING_UP", steps }, 202);
  }

  steps[steps.length - 1] = { id: "health", ok: false, label: "Waiting for health", detail: lastHealth || "not healthy" };
  setLastError("BOOT_HEALTH_FAIL", "Backend did not become healthy");
  return jsonFail(res, "backend_unhealthy", 503, { status: "UNHEALTHY", steps });
}

steps[steps.length - 1] = { id: "health", ok: true, label: "Waiting for health", detail: "healthy" };

  // Data verification: telemetry responds with an ok payload
  steps.push({ id: "telemetry", ok: false, label: "Checking telemetry", detail: "polling /api/telemetry" });

  const envObjTelemetry = readEnv();
  const base = dashBaseUrlFromEnv(envObjTelemetry);

  let dbCheck = { ok: false, status: 0, json: null };
  for (let i = 0; i < 20; i++) {
    dbCheck = await httpGetJson(`${base}/api/telemetry`);
    const body = (dbCheck && dbCheck.json && typeof dbCheck.json === "object") ? dbCheck.json : null;
    if (dbCheck.ok && body && body.ok === true) break;
    await sleep(i < 5 ? 500 : 1000);
  }

  const telemetryBody = (dbCheck && dbCheck.json && typeof dbCheck.json === "object") ? dbCheck.json : null;

  if (!dbCheck.ok || !telemetryBody || telemetryBody.ok !== true) {
    const detail = !dbCheck.ok
      ? "telemetry not responding"
      : (telemetryBody.error || telemetryBody.status || telemetryBody.reasons || "telemetry returned not-ok payload");

    steps[steps.length - 1] = {
      id: "telemetry",
      ok: false,
      label: "Checking telemetry",
      detail
    };
    setLastError("DATA_API_FAIL", "Telemetry API not ready", {
      status: dbCheck.status,
      body: telemetryBody
    });
    return jsonFail(res, "telemetry_not_ready", 503, { status: "NO_DATA_API", steps, telemetry: telemetryBody });
  }

  steps[steps.length - 1] = {
    id: "telemetry",
    ok: true,
    label: "Checking telemetry",
    detail: "telemetry responding"
  };
// ------------------------------------------------------------
// FULL AUTO BOOTSTRAP (single primary feed + pipeline)
// ------------------------------------------------------------
try {
  const envObjBootstrap = readEnv();
  const base2 = dashBaseUrlFromEnv(envObjBootstrap);

  const jobsRes2 = await httpGetJson(`${base2}/api/jobs`);
  const jobs2 = (jobsRes2 && jobsRes2.ok && jobsRes2.json && Array.isArray(jobsRes2.json.jobs))
    ? jobsRes2.json.jobs
    : [];

  const preferredFeeds = [
    "stream_prices_polygon_ws",
    "poll_prices",
  ];

  const ingestionEnabled = String(envObjBootstrap.START_INGESTION_WITH_SERVER || "").trim().toLowerCase();
  const isolatedIngestionEnabled = ingestionEnabled === "1" || ingestionEnabled === "true" || ingestionEnabled === "yes" || ingestionEnabled === "on";

  const ingestionRuntimeJob = jobs2.find((x) => x && x.name === "ingestion_runtime");
  const alreadyRunningFeed = preferredFeeds.find((name) => {
    const j = jobs2.find((x) => x && x.name === name);
    return !!(j && j.running);
  });

  const feedToStart =
    alreadyRunningFeed ||
    preferredFeeds.find((name) => jobs2.find((x) => x && x.name === name)) ||
    "stream_prices_polygon_ws";

  if (healthy) {
    const shouldStartLegacyFeed =
      !isolatedIngestionEnabled &&
      !(ingestionRuntimeJob && ingestionRuntimeJob.running);

    if (shouldStartLegacyFeed) {
      await new Promise((resolve) => {
        const lib = base2.startsWith("https") ? https : http;
        const body = JSON.stringify(operatorConfirmationBody(req, "JOB_ACTION", {
          actionId: "jobs.start",
          target: feedToStart,
        }));
        const req2 = lib.request(
          `${base2}/api/jobs/start?name=${encodeURIComponent(feedToStart)}`,
          {
            method: "POST",
            headers: {
              ...trustedControlPlaneAuthHeaders("POST", `${base2}/api/jobs/start?name=${encodeURIComponent(feedToStart)}`),
              "Content-Type": "application/json",
              "Content-Length": Buffer.byteLength(body),
            },
          },
          () => resolve()
        );
        req2.on("error", () => resolve());
        req2.write(body);
        req2.end();
      });

      // Warm only the selected primary feed
      await sleep(2000);
    }

    // Trigger full pipeline (POST required)
    await new Promise((resolve) => {
      const lib = base2.startsWith("https") ? https : http;
      const body = JSON.stringify(operatorConfirmationBody(req, "RUN_PIPELINE", {
        actionId: "pipeline.run",
        target: `mode:${mode}`,
      }));
      const req2 = lib.request(
        `${base2}/api/pipeline/run`,
        {
          method: "POST",
          headers: {
            ...trustedControlPlaneAuthHeaders("POST", `${base2}/api/pipeline/run`),
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(body),
          },
        },
        () => resolve()
      );
      req2.on("error", () => resolve());
      req2.write(body);
      req2.end();
    });
  }
} catch {}

return jsonOk(res, { status: "RUNNING", mode, steps });
});

app.post("/api/operator/stop", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.stop", {
    target: "engine",
  });
  if (!confirmation) return;
  const r = stopEngine();
  return sendOperatorPayload(res, r, 200);
});

app.post("/api/operator/restart", async (req, res) => {
  const mode = String((req.body && req.body.mode) || state.lastMode || "safe");
  const confirmation = requireOperatorConfirmation(req, res, "operator.restart", {
    target: `mode:${mode}`,
  });
  if (!confirmation) return;
  stopEngine();
  await sleep(1000);
  const r = startEngine(mode);
  return sendOperatorPayload(res, {
    ok: !!(r && r.ok),
    status: "RESTARTING",
    start: r
  }, r && r.ok ? 202 : 503);
});

app.post("/api/operator/emergencyStop", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.emergency_stop", {
    target: "global",
  });
  if (!confirmation) return;
  const r = emergencyStop();
  return sendOperatorPayload(res, r, r && r.ok ? 200 : 500);
});

app.post("/api/operator/clearLastError", (req, res) => {
  clearLastError();
  return jsonOk(res, {});
});

app.post("/api/operator/set_mode", (req, res) => {
  const requestedForTarget = String((req.body && req.body.mode) || "safe").trim().toLowerCase() || "safe";
  const confirmation = requireOperatorConfirmation(req, res, "operator.set_mode", {
    target: `mode:${requestedForTarget}`,
  });
  if (!confirmation) return;
  try {
    ensureEnvFile();

    const requested = requestedForTarget;
    const mode = (requested === "live" || requested === "shadow") ? requested : "safe";

    const envObj = readEnv();
    const { sanitized } = validateAndSanitizeEnv(envObj);

    sanitized.ENGINE_MODE = mode;
    sanitized.OPERATOR_MODE = mode;
    sanitized.EXECUTION_MODE = mode === "safe" ? "paper" : mode;

    atomicWrite(ENV_PATH, serializeEnv(sanitized));

    state.lastMode = mode;
    saveState();

    return jsonOk(res, { mode });
  } catch (e) {
    return jsonFail(res, "set_mode_failed", 500, { detail: String(e) });
  }
});

app.post("/api/operator/pause_training", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.training_control", {
    target: "AUTO_PIPELINE:false",
  });
  if (!confirmation) return;
  try {
    ensureEnvFile();

    const envObj = readEnv();
    const { sanitized } = validateAndSanitizeEnv(envObj);

    sanitized.AUTO_PIPELINE = "false";

    atomicWrite(ENV_PATH, serializeEnv(sanitized));
    return jsonOk(res, { AUTO_PIPELINE: "false" });
  } catch (e) {
    return jsonFail(res, "pause_training_failed", 500, { detail: String(e) });
  }
});

app.post("/api/operator/resume_training", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.training_control", {
    target: "AUTO_PIPELINE:true",
  });
  if (!confirmation) return;
  try {
    ensureEnvFile();

    const envObj = readEnv();
    const { sanitized } = validateAndSanitizeEnv(envObj);

    sanitized.AUTO_PIPELINE = "true";

    atomicWrite(ENV_PATH, serializeEnv(sanitized));
    return jsonOk(res, { AUTO_PIPELINE: "true" });
  } catch (e) {
    return jsonFail(res, "resume_training_failed", 500, { detail: String(e) });
  }
});

app.post("/api/operator/run_job", async (req, res) => {
  try {
    const name = String((req.body && req.body.job) || (req.body && req.body.name) || "").trim();
    if (!name) {
      return jsonFail(res, "missing_job_name", 400);
    }
    const confirmation = requireOperatorConfirmation(req, res, "operator.job_start", {
      target: name,
    });
    if (!confirmation) return;

    const guard = beginOperatorAction(`run_job:${name}`, 15000);
    if (!guard.ok) {
      return jsonFail(res, guard.error, guard.statusCode, { retry_after_s: guard.retry_after_s || null, job: name });
    }

    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);

    const r = await httpPostJson(`${base}/api/jobs/start?name=${encodeURIComponent(name)}`, {
      name,
      ...operatorConfirmationBody(req, "JOB_ACTION", {
        actionId: "jobs.start",
        target: name,
      }),
    });
    const payload = {
      ok: !!(r && r.ok && (!r.json || r.json.ok !== false)),
      job: name,
      result: r ? (r.json || null) : null,
      upstream_status: Number((r && r.status) || 0),
      upstream_error: r ? (r.error || null) : null
    };
    finishOperatorAction(`run_job:${name}`, payload.ok === true);
    return sendOperatorPayload(res, payload, payload.ok ? 202 : operatorErrorStatus((r && (r.error || r.json?.error)) || "dashboard_unreachable", 503));
  } catch (e) {
    finishOperatorAction(`run_job:${String((req.body && req.body.job) || (req.body && req.body.name) || "").trim()}`, false);
    return jsonFail(res, "run_job_failed", 500, { detail: String(e) });
  }
});

app.post("/api/operator/promote_model", async (req, res) => {
  try {
    const confirmation = requireOperatorConfirmation(req, res, "operator.promote_model", {
      target: "model_promotion",
    });
    if (!confirmation) return;
    const guard = beginOperatorAction("promote_model", 60000);
    if (!guard.ok) {
      return jsonFail(res, guard.error, guard.statusCode, { retry_after_s: guard.retry_after_s || null });
    }
    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);

    const r = await httpPostJson(`${base}/api/models/promote`, {
      on: "1",
      ...operatorConfirmationBody(req, "PROMOTION", {
        actionId: "models.promote",
        target: "model_promotion",
      }),
    });
    const payload = {
      ok: !!(r && r.ok && (!r.json || r.json.ok !== false)),
      result: r ? (r.json || null) : null,
      upstream_status: Number((r && r.status) || 0),
      upstream_error: r ? (r.error || null) : null
    };
    finishOperatorAction("promote_model", payload.ok === true);
    return sendOperatorPayload(res, payload, payload.ok ? 202 : operatorErrorStatus((r && (r.error || r.json?.error)) || "dashboard_unreachable", 503));
  } catch (e) {
    finishOperatorAction("promote_model", false);
    return jsonFail(res, "promote_model_failed", 500, { detail: String(e) });
  }
});

app.get("/api/operator/ai/last_patch", (req, res) => {
  return jsonOk(res, {
    patch: _lastAppliedPatchMeta || null
  });
});

app.get("/api/operator/service_status", wrapOperatorRoute(async (req, res) => {
  const readiness = await getReadiness();
  if (!isLinuxManagedMode()) {
    const currentStatus = status();
    const engineRunning = currentStatus === "RUNNING";
    const runtimeError = currentAttemptLastErrorForRuntime(externalRuntimeState());
    return jsonState(res, {
      ok: true,
      managed: false,
      engine: {
        status: currentStatus,
        running: engineRunning,
        lastExitCode: state.lastExitCode,
        restartAttempts: state.restartAttempts || 0,
        lastHealthyAt: state.lastHealthyAt || null,
        currentAttemptId: state.currentAttemptId || null,
        currentAttemptStartedAt: state.currentAttemptStartedAt || state.lastStartAt || null,
        currentAttemptLastError: runtimeError,
        lastRecordedCrash: lastRecordedCrash(),
        lastError: runtimeError || null
      },
      operator: {
        status: "RUNNING",
        running: true,
        pid: process.pid
      },
      ready: readiness.ready === true,
      currentBlocker: readiness.currentBlocker || null,
      degradedComponents: Array.isArray(readiness.degradedComponents) ? readiness.degradedComponents : []
    }, 200);
  }

  const engine = runServiceCtl(["status", "engine"]);
  const operator = runServiceCtl(["status", "operator"]);
  const backup = runServiceCtl(["status", "backup"]);
  const upgrade = runServiceCtl(["status", "upgrade"]);

  return jsonState(res, {
    ok: !!(engine && engine.ok && String(engine.status || "").toUpperCase() === "RUNNING"),
    managed: true,
    engine,
    operator,
    backup,
    upgrade,
    ready: readiness.ready === true,
    currentBlocker: readiness.currentBlocker || null,
    degradedComponents: Array.isArray(readiness.degradedComponents) ? readiness.degradedComponents : []
  }, 200);
}));

// ================================
// OPERATOR AI (SAFE AGENT LAYER)
// ================================

const { runAgent } = require("../services/operator_ai/agent");

// ================================
// PRODUCTION LLM (STRICT JSON SAFE)
// ================================

async function _llm(prompt) {

  const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
  if (!OPENAI_API_KEY) {
    return {
      summary: "llm_not_configured",
      action: null
    };
  }

  try {

    const body = {
      model: "gpt-4o-mini",
      temperature: 0,
      response_format: { type: "json_object" }, // 🔒 HARD JSON MODE
      messages: [
        {
          role: "system",
          content: [
            "You are a STRICT JSON debugging + patch engine.",
            "Return ONLY valid JSON. No text outside JSON.",
            "Never return text outside JSON.",
            "Never invent actions. Only use allowed actions.",
            "Allowed actions: restart_feeds, autofix, restart_engine, start_system, stop, emergency_stop.",
            "ALWAYS include:",
            "- root_cause",
            "- failing_component",
            "- file (real path if known)",
            "- patch (find/replace OR null)",
            "- confidence (0-1)",
            "Patch must be real code anchored find/replace when possible.",
            "If unsure → patch=null.",
            "If unsure, return action=null."
          ].join(" ")
        },
        {
          role: "user",
          content: prompt
        }
      ]
    };

    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });

    const text = await r.text();

    let json;

    try {
      json = JSON.parse(text);
    } catch {
      return {
        summary: "llm_invalid_http_response",
        action: null
      };
    }

    const content = json?.choices?.[0]?.message?.content;

    if (!content) {
      return {
        summary: "llm_empty_response",
        action: null
      };
    }

    let parsed;

    try {
      parsed = JSON.parse(content);
    } catch {
      return {
        summary: "llm_non_json_output",
        action: null
      };
    }

    // 🔒 FINAL HARD VALIDATION
    if (!parsed || typeof parsed !== "object") {
      return {
        summary: "llm_invalid_shape",
        action: null
      };
    }

    const action = parsed.action
      ? String(parsed.action).toLowerCase().trim()
      : null;

    const allowed = [
      "restart_feeds",
      "autofix",
      "restart_engine",
      "start_system",
      "stop",
      "emergency_stop"
    ];

    if (action && !allowed.includes(action)) {
      return {
        summary: parsed.summary || "invalid_action_blocked",
        action: null
      };
    }

    let patch = parsed.patch;

    if (patch && typeof patch === "object") {

      const forbidden = [
        "process.exit",
        "fs.rm",
        "fs.unlink",
        "child_process",
        "exec(",
        "spawn("
      ];

      const findText = typeof patch.find === "string" ? patch.find : "";
      const replaceText = typeof patch.replace === "string" ? patch.replace : "";
      const combined = (findText + replaceText).toLowerCase();

      if (
        !findText ||
        typeof patch.find !== "string" ||
        typeof patch.replace !== "string"
      ) {
        patch = null;
      } else if (forbidden.some(f => combined.includes(f))) {
        patch = null;
      } else {
        patch = {
          find: findText,
          replace: replaceText
        };
      }
    } else {
      patch = null;
    }

    return {
      summary: String(parsed.summary || "").slice(0, 2000),
      root_cause: String(parsed.root_cause || "").slice(0, 2000),
      failing_component: String(parsed.failing_component || "").slice(0, 500),
      file: String(parsed.file || "").slice(0, 500),
      patch,
      confidence: Math.max(0, Math.min(1, Number(parsed.confidence || 0))),
      action
    };

  } catch (e) {

    return {
      summary: "llm_exception",
      action: null,
      error: String(e)
    };

  }
}

app.post("/api/operator/ai/patch_preview", async (req, res) => {
  try {
    const result = await runAgent(_llm);
    const analysis = result && result.analysis ? result.analysis : null;
    if (!analysis) {
      return jsonFail(res, "analysis_unavailable", 503);
    }

    return jsonOk(res, {
      summary: analysis.summary || null,
      root_cause: analysis.root_cause || null,
      file: analysis.file || null,
      patch: analysis.patch || null,
      confidence: analysis.confidence || null
    });

  } catch (e) {
    return jsonFail(res, "patch_preview_failed", 503, { detail: String(e) });
  }
});

app.post("/api/operator/ai/apply_patch", async (req, res) => {
  try {
    const suppliedFile = String((req.body && req.body.file) || "").trim();
    const confirmation = requireOperatorConfirmation(req, res, "operator.ai_apply_patch", {
      target: suppliedFile,
    });
    if (!confirmation) return;

    if ((state.lastMode || "safe") === "live") {
      return jsonFail(res, "apply_patch_blocked_in_live_mode", 409);
    }

    const suppliedPatch = req.body && req.body.patch ? req.body.patch : null;
    const suppliedConfidence = Number((req.body && req.body.confidence) || 0);

    let analysis = null;

    if (suppliedFile && suppliedPatch) {
      analysis = {
        summary: String((req.body && req.body.summary) || "manual_patch_apply"),
        root_cause: String((req.body && req.body.root_cause) || ""),
        failing_component: String((req.body && req.body.failing_component) || ""),
        file: suppliedFile,
        patch: suppliedPatch,
        confidence: suppliedConfidence
      };
    } else {
      const result = await runAgent(_llm);
      analysis = result && result.analysis ? result.analysis : null;
    }

    if (!analysis) {
      return jsonFail(res, "analysis_missing", 503);
    }

    if (!analysis.file) {
      return jsonFail(res, "patch_file_missing", 422, { analysis });
    }

    if (!analysis.patch || typeof analysis.patch !== "object") {
      return jsonFail(res, "patch_missing", 422, { analysis });
    }

    const confidence = Number(analysis.confidence || 0);
    if (!(confidence >= 0.85)) {
      return jsonFail(res, "patch_confidence_too_low", 422, {
        confidence,
        required: 0.85,
        analysis
      });
    }

    const applied = applyAiPatchWithBackup(analysis.file, analysis.patch, {
      summary: analysis.summary || "",
      root_cause: analysis.root_cause || "",
      failing_component: analysis.failing_component || "",
      confidence
    });

    logAgentAction({
      ts: new Date().toISOString(),
      actor: "operator_ai_apply_patch",
      file: applied.file,
      patchId: applied.patchId,
      confidence,
      summary: analysis.summary || null,
      root_cause: analysis.root_cause || null,
      failing_component: analysis.failing_component || null,
      diff: applied.diff,
      confirmation: {
        action_id: confirmation.action_id,
        actor: confirmation.actor,
        source_surface: confirmation.source_surface,
        reason: confirmation.reason,
        request_id: confirmation.request_id,
        target: confirmation.target,
        confirmation_method: confirmation.confirmation_method,
        confirmation_hold_ms: confirmation.confirmation_hold_ms,
      }
    });

    return jsonOk(res, {
      applied
    }, 202);
  } catch (e) {
    return jsonFail(res, "apply_patch_failed", 500, { detail: String(e) });
  }
});

app.post("/api/operator/ai/rollback_patch", async (req, res) => {
  try {
    const patchId = String((req.body && req.body.patchId) || "").trim();
    if (!patchId) {
      return jsonFail(res, "missing_patch_id", 400);
    }
    const confirmation = requireOperatorConfirmation(req, res, "operator.ai_rollback_patch", {
      target: patchId,
    });
    if (!confirmation) return;
    const rolledBack = rollbackAiPatch(patchId);

    logAgentAction({
      ts: new Date().toISOString(),
      actor: "operator_ai_rollback_patch",
      patchId: rolledBack.patchId,
      file: rolledBack.file,
      confirmation: {
        action_id: confirmation.action_id,
        actor: confirmation.actor,
        source_surface: confirmation.source_surface,
        reason: confirmation.reason,
        request_id: confirmation.request_id,
        target: confirmation.target,
        confirmation_method: confirmation.confirmation_method,
        confirmation_hold_ms: confirmation.confirmation_hold_ms,
      }
    });

    return jsonOk(res, {
      rolledBack
    });
  } catch (e) {
    return jsonFail(res, "rollback_patch_failed", 500, { detail: String(e) });
  }
});

// RUN AGENT
app.post("/api/operator/ai/run", async (req, res) => {
  try {

if (!agentCooldownOk()) {
      return jsonFail(res, "cooldown_active", 429, { retry_after_s: 30 });
    }

let result;

try {
  result = await runAgent(_llm);

logAgentAction({
  ts: new Date().toISOString(),
  actor: "operator_ai",
  summary: result?.analysis?.summary || null,
  root_cause: result?.analysis?.root_cause || null,
  failing_component: result?.analysis?.failing_component || null,
  file: result?.analysis?.file || null,
  patch: result?.analysis?.patch || null,
  confidence: result?.analysis?.confidence || null,
  action: result?.action || null
});
} catch (err) {
  logAgentAction({
    ts: new Date().toISOString(),
    actor: "operator_ai",
    error: String(err)
  });
  throw err;
}

    return jsonOk(res, {
      result,
      patch: result?.analysis?.patch || null,
      file: result?.analysis?.file || null
    });
  } catch (e) {
    return jsonFail(res, "operator_ai_failed", 503, { detail: String(e) });
  }
});

// EXPLAIN ONLY (NO ACTION)
app.post("/api/operator/ai/explain", async (req, res) => {
  try {
    const result = await runAgent(_llm);
    return jsonOk(res, {
      result,
      patch: result?.analysis?.patch || null,
      file: result?.analysis?.file || null
    });
  } catch (e) {
    return jsonFail(res, "operator_ai_explain_failed", 503, { detail: String(e) });
  }
});

app.post("/api/operator/backup", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.backup", {
    target: "backup",
  });
  if (!confirmation) return;
  const guard = beginOperatorAction("backup", 60000);
  if (!guard.ok) {
    return jsonFail(res, guard.error, guard.statusCode, {
      retry_after_s: guard.retry_after_s || null
    });
  }
  if (isLinuxManagedMode()) {
    const r = runServiceCtl(["start", "backup"]);
    finishOperatorAction("backup", !!(r && r.ok));
    return sendOperatorPayload(res, {
      ok: !!(r && r.ok),
      managed: true,
      result: r
    }, r && r.ok ? 202 : 503);
  }

  if (!fs.existsSync(BACKUP_SCRIPT)) {
    finishOperatorAction("backup", false);
    return jsonFail(res, "backup_script_missing", 404);
  }

  try {
    const childProc = spawn(BACKUP_SCRIPT, [], {
      cwd: ROOT,
      env: { ...process.env, TRADING_REPO: ROOT },
      detached: true,
      stdio: "ignore"
    });
    childProc.unref();
    finishOperatorAction("backup", true);
    return jsonOk(res, { ok: true, managed: false, started: true }, 202);
  } catch (e) {
    finishOperatorAction("backup", false);
    return jsonFail(res, "backup_start_failed", 500, { detail: String(e) });
  }
});

app.post("/api/operator/update", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.system_update", {
    target: "system_update",
  });
  if (!confirmation) return;
  const guard = beginOperatorAction("system_update", 300000);
  if (!guard.ok) {
    return jsonFail(res, guard.error, guard.statusCode, {
      retry_after_s: guard.retry_after_s || null
    });
  }
  if (isLinuxManagedMode()) {
    const r = runServiceCtl(["start", "upgrade"]);
    finishOperatorAction("system_update", !!(r && r.ok));
    return sendOperatorPayload(res, {
      ok: !!(r && r.ok),
      managed: true,
      result: r
    }, r && r.ok ? 202 : 503);
  }

  if (!fs.existsSync(UPGRADE_SCRIPT)) {
    finishOperatorAction("system_update", false);
    return jsonFail(res, "upgrade_script_missing", 404);
  }

  try {
    const childProc = spawn(UPGRADE_SCRIPT, [], {
      cwd: ROOT,
      env: { ...process.env, TRADING_REPO: ROOT },
      detached: true,
      stdio: "ignore"
    });
    childProc.unref();
    finishOperatorAction("system_update", true);
    return jsonOk(res, { ok: true, managed: false, started: true }, 202);
  } catch (e) {
    finishOperatorAction("system_update", false);
    return jsonFail(res, "system_update_start_failed", 500, { detail: String(e) });
  }
});

app.post("/api/operator/restart_operator", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.restart_operator", {
    target: "operator_service",
  });
  if (!confirmation) return;
  const guard = beginOperatorAction("restart_operator", 60000);
  if (!guard.ok) {
    return jsonFail(res, guard.error, guard.statusCode, {
      retry_after_s: guard.retry_after_s || null
    });
  }
  if (isLinuxManagedMode()) {
    const r = runServiceCtl(["restart", "operator"]);
    finishOperatorAction("restart_operator", !!(r && r.ok));
    return sendOperatorPayload(res, {
      ok: !!(r && r.ok),
      managed: true,
      result: r
    }, r && r.ok ? 202 : 503);
  }

  finishOperatorAction("restart_operator", false);
  return jsonFail(res, "restart_operator_not_supported_in_dev_mode", 501, {
    managed: false
  });
});

app.get("/api/operator/snapshot", wrapOperatorRoute(async (req, res) => {
  const requestedMode = String(req.query.mode || "repair").trim().toLowerCase() || "repair";
  if (!["quick", "repair", "deep"].includes(requestedMode)) {
    return jsonFail(res, "invalid_snapshot_mode", 400, {
      allowed: ["quick", "repair", "deep"]
    });
  }
  try {
    const snap = await buildOperatorSnapshot(requestedMode);

    res.setHeader("Content-Type", "application/json");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="operator_snapshot_${requestedMode}_${Date.now()}.json"`
    );
    res.send(JSON.stringify(snap, null, 2));
  } catch (e) {
    return jsonFail(res, "snapshot_build_failed", 500, {
      detail: String(e),
      snapshot_schema: {
        name: "operator_repair_snapshot",
        version: 2,
        producer: "boot/operator_server.js",
        mode: "repair"
      }
    });
  }
}));

app.post("/api/operator/factoryReset", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.factory_reset", {
    target: "operator_state",
  });
  if (!confirmation) return;
  emergencyStop();

  const cleared = {
    secrets: safeUnlinkIfExists(SECRETS_PATH),
    env: safeUnlinkIfExists(ENV_PATH),
    state: false,
  };

  try {
    state = defaultState();
    saveState();
    cleared.state = true;
  } catch {}

  invalidateOperatorHealthCache();
  invalidateOperatorDbCaches();
  return jsonOk(res, { cleared });
});

// --------------------------------------------
// Python STDERR tail
// --------------------------------------------

// --------------------------------------------------
// Dashboard Proxy Endpoints (Unified Operator Layer)
// --------------------------------------------------
app.get("/api/operator/proxy/:name", wrapOperatorRoute(async (req, res) => {
  const name = String(req.params.name || "").trim();
  const envObj = readEnv();
  const base = dashBaseUrlFromEnv(envObj);

  const map = {
    jobs: "/api/jobs",
    telemetry: "/api/telemetry",
    system_state: "/api/system/state",
    validation: "/api/validation",
    health: "/api/health"
  };

  if (!map[name]) {
    return jsonFail(res, "invalid_proxy_target", 404, { target: name });
  }

  const r = await httpGetJson(base + map[name]);
  if (!r.ok) {
    return sendOperatorPayload(
      res,
      buildOperatorProxyFailure(r, r.error || "dashboard_unreachable", { target: name }, operatorErrorStatus(r.error || "dashboard_unreachable", 503)),
      operatorErrorStatus(r.error || "dashboard_unreachable", 503)
    );
  }
  if (!r.json || typeof r.json !== "object") {
    return sendOperatorPayload(
      res,
      buildOperatorProxyFailure(r, "invalid_proxy_response", { target: name }, 502),
      502
    );
  }

  return jsonState(res, r.json, Number(r.status || 200));
}));

// --------------------------------------------
// Live bootstrap counts
// --------------------------------------------
app.get("/api/operator/bootstrap_counts", wrapOperatorRoute(async (req, res) => {
  const payload = await loadBootstrapCountsPayload();
  return jsonState(res, payload, 200);
}));

// --------------------------------------------
// DB schema inspection
// --------------------------------------------
app.get("/api/operator/db_schema", wrapOperatorRoute(async (req, res) => {
  const payload = await loadDbSchemaPayload();
  return sendOperatorPayload(res, payload, payload && payload.ok ? 200 : 503);
}));

// UI
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "operator_ui.html"));
});

// --------------------------------------------
// Background Watchdog (Production Hardened)
// --------------------------------------------
let _healthFailCount = 0;
let _crashLoopDetected = false;

_watchdogTimer = setInterval(async () => {
  try {

    try {
      const health = await verifyHealth();
      broadcastTelemetry("health_update", health);
    } catch {}

    try {
      const envObj = readEnv();
      const base = dashBaseUrlFromEnv(envObj);

      const pnl = await httpGetJson(`${base}/api/pnl/summary`);
      if (pnl.ok) {
        broadcastTelemetry("pnl_update", pnl.json || {});
      }

      const positions = await httpGetJson(`${base}/api/terminal/positions`);
      const orders = await httpGetJson(`${base}/api/terminal/orders`);
      const fills = await httpGetJson(`${base}/api/terminal/fills`);

      broadcastTelemetry("trading_update", {
        positions: positions.ok ? (positions.json.positions || positions.json || []) : [],
        open_orders: orders.ok ? (orders.json.orders || orders.json || []) : [],
        recent_fills: fills.ok ? (fills.json.fills || fills.json || []) : []
      });
    } catch {}
    if (status() !== "RUNNING") {
      _healthFailCount = 0;
      return;
    }
// ----------------------------------
// AI AUTO-DIAG (SAFE INSERT POINT)
// ----------------------------------

try {
  const health = await verifyHealth();

const isHardFailure =
  (!health || health.ok !== true) ||
  (state.lastError && state.lastError.kind) ||
  (status() !== "RUNNING") ||
  (state.restartAttempts > 0) ||
  (state.consecutiveStartupFailures > 0);

if (isHardFailure && (state.lastMode || "safe") !== "safe") {
if (agentCooldownOk(state.lastError?.kind || "unknown")) {
      console.log("[AI] running diagnostic agent");

      try {
await runAgent(_llm);
      } catch (e) {
        console.warn("[AI] agent error", e);
      }
    }
  }
} catch (e) {
  console.warn("[AI] health check error", e);
}
    // NEVER watchdog-restart in SAFE mode
    if ((state.lastMode || "safe") === "safe") {
      _healthFailCount = 0;
      return;
    }

const readiness = await getReadiness();
const healthState = readiness.health;

    const startedAt = state.lastStartAt ? new Date(state.lastStartAt).getTime() : 0;
    const now = Date.now();
    const inStartupGrace = startedAt && (now - startedAt < 15000);

const hardFailure =
  !healthState ||
  healthState.ok !== true;

    if (!inStartupGrace && hardFailure) {
      const lastErrorKind = String(state.lastError?.kind || "").trim();
      const fatalStartupLocked =
        state.restartBlocked === true ||
        state.fatal === true ||
        lastErrorKind === "ENGINE_STARTUP_FATAL" ||
        lastErrorKind === "ENGINE_STARTUP_CRASH_LOOP" ||
        lastErrorKind === "CRASH_LOOP_DETECTED";

      if (fatalStartupLocked) {
        _healthFailCount = 0;
        return;
      }

      _healthFailCount++;

      if (_healthFailCount >= 5) {
        const currentAttemptFailedPreHealthy = isPreHealthyCrash();

        if (currentAttemptFailedPreHealthy) {
          _crashLoopDetected = true;
          setFatalRestartBlock(
            "ENGINE_STARTUP_HEALTH_TIMEOUT",
            "Backend never became healthy for the current startup attempt. Auto-restart suppressed.",
            {
              attemptId: state.currentAttemptId || null,
              attemptStartedAt: state.currentAttemptStartedAt || null,
              preHealthyCrash: true,
              runtimeTail: currentAttemptRuntimeTail(200),
              stderrTail: currentAttemptStderrTail(16000)
            }
          );
          stopEngine();
          state.lastMode = "safe";
          saveState();
          _healthFailCount = 0;
          return;
        }

        if (_crashLoopDetected) {
          setFatalRestartBlock(
            "CRASH_LOOP_DETECTED",
            "Crash loop detected. Locked to SAFE mode.",
            {
              attemptId: state.currentAttemptId || null,
              attemptStartedAt: state.currentAttemptStartedAt || null,
              runtimeTail: currentAttemptRuntimeTail(200),
              stderrTail: currentAttemptStderrTail(16000)
            }
          );
          stopEngine();
          state.lastMode = "safe";
          saveState();
          _healthFailCount = 0;
          return;
        }

        setLastError(
          "HEALTH_DEBOUNCED_FAIL",
          "Backend unreachable 5 consecutive checks. Restarting.",
          {
            attemptId: state.currentAttemptId || null,
            attemptStartedAt: state.currentAttemptStartedAt || null,
            runtimeTail: currentAttemptRuntimeTail(200),
            stderrTail: currentAttemptStderrTail(16000)
          }
        );

        await restartEngineGracefully(state.lastMode || "shadow", { stopTimeoutMs: 15000 });

        _healthFailCount = 0;
      }

      return;
    }

if (healthState && healthState.ok === true) {
      _healthFailCount = 0;
      state.restartAttempts = 0;
      state.consecutiveStartupFailures = 0;
      state._restartWindowStart = null;
      state._restartCountWindow = 0;
      state.restartBlocked = false;
      state.fatal = false;
      state.fatalSince = null;
      state.nextRestartAt = null;
      saveState();
}

/* -----------------------------------------
   EXTENDED WATCHDOG AUTOMATION
----------------------------------------- */

try {

  // provider failover
  await providerFailover();

  // rebuild pipeline if stalled
  await pipelineWatchdog();

  // promote models automatically
  await modelPromotionWatchdog();

  // change engine mode based on market session
  await marketSessionControl();

} catch (e) {

  console.warn("[WATCHDOG] extended checks failed", e);

}

  } catch (e) {
    setLastError("WATCHDOG_EXCEPTION", "Watchdog error", {
      message: String(e?.message || e || "watchdog_error")
    });
  }
}, 8000);
// --------------------------------------------------
// START OPERATOR SERVER
// --------------------------------------------------
if (!fs.existsSync(ROOT)) {
  console.error("Operator startup failed: repo root missing");
  process.exit(1);
}

let _operatorShuttingDown = false;

async function shutdownOperator() {
  if (_operatorShutdownPromise) return _operatorShutdownPromise;
  _operatorShuttingDown = true;

  _operatorShutdownPromise = (async () => {
    clearPendingRestartTimer();

    if (_watchdogTimer) {
      try { clearInterval(_watchdogTimer); } catch (e) { logOperatorCatch("shutdownOperator.clearWatchdog", e); }
      _watchdogTimer = null;
    }

    if (_wsHeartbeatTimer) {
      try { clearInterval(_wsHeartbeatTimer); } catch (e) { logOperatorCatch("shutdownOperator.clearWsHeartbeat", e); }
      _wsHeartbeatTimer = null;
    }

    try {
      if (_wsServer) {
        _wsServer.clients.forEach((ws) => {
          try { ws.close(); } catch {}
        });
        _wsServer.close();
        _wsServer = null;
      }
    } catch (e) {
      logOperatorCatch("shutdownOperator.wsClose", e);
    }

    try {
      stopEngine();
    } catch (e) {
      logOperatorCatch("shutdownOperator.stopEngine", e);
    }

    try {
      await waitForEngineExit(15000);
    } catch (e) {
      logOperatorCatch("shutdownOperator.waitForEngineExit", e);
    }

    try {
      if (_httpServer) {
        await new Promise((resolve) => _httpServer.close(() => resolve()));
      }
    } catch (e) {
      logOperatorCatch("shutdownOperator.httpClose", e);
    }

    try {
      process.exit(0);
    } catch (e) {
      logOperatorCatch("shutdownOperator.processExit", e);
      throw e;
    }
  })();

  return _operatorShutdownPromise;
}

try {
  process.on("SIGINT", shutdownOperator);
} catch (e) {
  logOperatorCatch("process.on.SIGINT", e);
  throw e;
}

try {
  process.on("SIGTERM", shutdownOperator);
} catch (e) {
  logOperatorCatch("process.on.SIGTERM", e);
  throw e;
}

try {
  process.on("SIGBREAK", shutdownOperator);
} catch (e) {
  logOperatorCatch("process.on.SIGBREAK", e);
  throw e;
}

// --------------------------------------------------
// OPERATOR WORKFLOW ALIASES + AGGREGATED VIEWS
// --------------------------------------------------

app.post("/api/operator/start_system", async (req, res) => {
  const mode = String((req.body && req.body.mode) || state.lastMode || "safe")
    .trim()
    .toLowerCase();

  const finalMode = (mode === "live" || mode === "shadow") ? mode : "safe";
  const confirmation = requireOperatorConfirmation(
    req,
    res,
    finalMode === "live" ? "operator.live_start" : "operator.start",
    { target: `mode:${finalMode}` }
  );
  if (!confirmation) return;
  const result = startEngine(finalMode);

  SYSTEM_STATE.state = status();
  SYSTEM_STATE.engine_pid = child ? child.pid : null;
  if (child && !SYSTEM_STATE.started_at) SYSTEM_STATE.started_at = Date.now();

  return sendOperatorPayload(res, {
    ok: !!result.ok,
    status: status(),
    mode: finalMode,
    result
  }, result && result.ok ? 202 : 503);
});

app.post("/api/operator/restart_engine", async (req, res) => {
  const requestedMode = String((req.body && req.body.mode) || state.lastMode || "safe")
    .trim()
    .toLowerCase();
  const confirmation = requireOperatorConfirmation(req, res, "operator.restart", {
    target: `mode:${requestedMode || "safe"}`,
  });
  if (!confirmation) return;
  if (isLiveModeBlocked()) {
    return res.status(403).json({ ok: false, error: "blocked_in_live_mode" });
  }

  try {
  const mode = requestedMode;

  const finalMode = (mode === "live" || mode === "shadow") ? mode : "safe";

  const restart = await restartEngineGracefully(finalMode, { stopTimeoutMs: 15000 });

  SYSTEM_STATE.state = status();
  SYSTEM_STATE.engine_pid = child ? child.pid : null;
  if (child) SYSTEM_STATE.started_at = Date.now();

  if (!restart.ok) {
    return jsonFail(res, restart.error || "restart_failed", 409, {
      status: status(),
      mode: finalMode,
      stopped: restart.stopped || null,
      wait: restart.wait || null,
      started: restart.started || null
    });
  }

return jsonOk(res, {
  status: status(),
  mode: finalMode,
  stopped: restart.stopped || null,
  wait: restart.wait || null,
  started: restart.started || null
});

} catch (e) {
  return jsonFail(res, "restart_failed", 500, { detail: String(e) });
}

});

app.post("/api/operator/emergency_stop", (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.emergency_stop", {
    target: "global",
  });
  if (!confirmation) return;
  const result = emergencyStop();

  SYSTEM_STATE.state = status();
  SYSTEM_STATE.engine_pid = child ? child.pid : null;
  if (!child) SYSTEM_STATE.started_at = null;

  return sendOperatorPayload(res, {
    ok: !!result.ok,
    status: status(),
    result
  }, result && result.ok ? 200 : 500);
});

app.post("/api/operator/broker_risk", (req, res) => {
  const body = req.body && typeof req.body === "object" && !Array.isArray(req.body) ? req.body : {};
  const policy = String(body.policy || body.action || "").trim() || "unspecified";
  const broker = String(body.broker || body.target || "").trim() || "configured";
  const confirmation = requireOperatorConfirmation(req, res, "operator.broker_risk", {
    target: `${broker}:${policy}`,
  });
  if (!confirmation) return;
  const result = runBrokerRiskCommand(body, confirmation);
  return sendOperatorPayload(res, {
    ok: !!result.ok,
    status: result.ok ? "BROKER_RISK_COMPLETE" : "BROKER_RISK_FAILED",
    result,
  }, result && result.ok ? 200 : 500);
});

app.get("/api/operator/system_state",
  operatorCanonicalProxyGet("/api/system/state", "invalid_system_state_response")
);

app.get("/api/operator/health",
  operatorHealthProxyGet("/api/health", "invalid_system_health_response")
);

app.get("/api/operator/strategy_decisions", wrapOperatorRoute(async (req, res) => {
  const envObj = readEnv();
  const base = dashBaseUrlFromEnv(envObj);

  const validation = await httpGetJson(`${base}/api/validation`);
  const strategyStatus = await httpGetJson(`${base}/api/strategy/status`);
  const strategyMetrics = await httpGetJson(`${base}/api/strategy_metrics`);
  const positions = await httpGetJson(`${base}/api/terminal/positions`);
  const orders = await httpGetJson(`${base}/api/terminal/orders`);

  const ok = !!(validation.ok || strategyStatus.ok || strategyMetrics.ok || positions.ok || orders.ok);

  return sendOperatorPayload(res, {
    ok,
    dashboardBase: base,
    validation: validation.ok ? validation.json : null,
    strategyStatus: strategyStatus.ok ? strategyStatus.json : null,
    strategyMetrics: strategyMetrics.ok ? strategyMetrics.json : null,
    positions: positions.ok ? positions.json : null,
    orders: orders.ok ? orders.json : null,
    degradedComponents: [
      ...(validation.ok ? [] : ["validation"]),
      ...(strategyStatus.ok ? [] : ["strategy_status"]),
      ...(strategyMetrics.ok ? [] : ["strategy_metrics"]),
      ...(positions.ok ? [] : ["positions"]),
      ...(orders.ok ? [] : ["orders"])
    ]
  }, ok ? 200 : 503);
}));

app.get("/api/operator/trading_monitor", wrapOperatorRoute(async (req, res) => {
    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);

    const positions = await httpGetJson(`${base}/api/terminal/positions`);
    const openOrders = await httpGetJson(`${base}/api/terminal/orders`);
    const fills = await httpGetJson(`${base}/api/terminal/fills`);
    const pnl = await httpGetJson(`${base}/api/pnl/summary`);
    const risk = await httpGetJson(`${base}/api/risk/summary`);

    const ok = !!(positions.ok || openOrders.ok || fills.ok || pnl.ok || risk.ok);
    return sendOperatorPayload(res, {
      ok,
      positions: positions.ok ? rowsFromOperatorPayload(positions.json, ["positions"]) : [],
      open_orders: openOrders.ok ? rowsFromOperatorPayload(openOrders.json, ["orders", "broker", "portfolio"]) : [],
      recent_fills: fills.ok ? rowsFromOperatorPayload(fills.json, ["fills"]) : [],
      pnl: pnl.ok ? (pnl.json || {}) : {},
      risk: risk.ok ? (risk.json || {}) : {},
      degradedComponents: [
        ...(positions.ok ? [] : ["positions"]),
        ...(openOrders.ok ? [] : ["open_orders"]),
        ...(fills.ok ? [] : ["fills"]),
        ...(pnl.ok ? [] : ["pnl"]),
        ...(risk.ok ? [] : ["risk"])
      ]
    }, ok ? 200 : 503);
}));

app.get("/api/operator/strategy_heatmap", wrapOperatorRoute(async (req, res) => {
    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);

    const metrics = await httpGetJson(`${base}/api/strategy_metrics`);

    const strategies = [];
    const rows = metrics.ok ? (metrics.json.strategies || metrics.json.rows || []) : [];

    for (const row of rows) {
      strategies.push({
        name: String(row.name || row.strategy || "unknown"),
        pnl: Number(row.pnl || row.day_pnl || row.total_pnl || 0)
      });
    }

    return sendOperatorPayload(res, {
      ok: true,
      strategies
    }, 200);
}));

app.get("/api/operator/trade_blotter", wrapOperatorRoute(async (req, res) => {
    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);

    const fills = await httpGetJson(`${base}/api/terminal/fills`);
    const tradesRaw = fills.ok ? rowsFromOperatorPayload(fills.json, ["fills"]) : [];

    const trades = tradesRaw.map((t) => ({
      ts: t.ts || t.timestamp || "",
      symbol: t.symbol || "",
      side: t.side || "",
      qty: t.qty || t.quantity || "",
      price: t.price || ""
    }));

    return sendOperatorPayload(res, {
      ok: true,
      trades
    }, 200);
}));

app.post("/api/operator/restart_feeds", async (req, res) => {
  const confirmation = requireOperatorConfirmation(req, res, "operator.restart_feeds", {
    target: "market_data_jobs",
  });
  if (!confirmation) return;
  const guard = beginOperatorAction("restart_feeds", 30000);
  if (!guard.ok) {
    return jsonFail(res, guard.error, guard.statusCode, { retry_after_s: guard.retry_after_s || null });
  }
  try {
    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);

    const jobsRes = await httpGetJson(`${base}/api/jobs`);
    const jobs = (jobsRes && jobsRes.ok && jobsRes.json && Array.isArray(jobsRes.json.jobs))
      ? jobsRes.json.jobs
      : [];

    const byName = {};
    for (const job of jobs) {
      const name = String(job && job.name || "").trim();
      if (name) byName[name] = job;
    }

    const preferredFeeds = [
      "stream_prices_polygon_ws",
      "poll_prices"
    ];

    const supportDaemons = [
      "provider_monitor"
    ];

    const availablePreferred = preferredFeeds.filter((name) => !!byName[name]);
    const runningPreferred = availablePreferred.filter((name) => !!(byName[name] && byName[name].running));
    const targetPrimary =
      runningPreferred[0] ||
      availablePreferred[0] ||
      null;

    const stopOrder = [...supportDaemons, ...preferredFeeds].filter((name) => !!(byName[name] && byName[name].running));
    const stopped = [];

    for (const name of stopOrder) {
      const r = await httpPostJson(`${base}/api/jobs/stop?name=${encodeURIComponent(name)}`, {
        name,
        ...operatorConfirmationBody(req, "JOB_ACTION", {
          actionId: "jobs.stop",
          target: name,
        }),
      });
      stopped.push({ name, ok: !!r.ok, result: r.json || null });
    }

    await sleep(1500);

    const started = [];

    for (const name of supportDaemons) {
      if (!byName[name]) continue;
      const r = await httpPostJson(`${base}/api/jobs/start?name=${encodeURIComponent(name)}`, {
        name,
        ...operatorConfirmationBody(req, "JOB_ACTION", {
          actionId: "jobs.start",
          target: name,
        }),
      });
      started.push({ name, ok: !!r.ok, result: r.json || null });
    }

    if (targetPrimary) {
      const r = await httpPostJson(`${base}/api/jobs/start?name=${encodeURIComponent(targetPrimary)}`, {
        name: targetPrimary,
        ...operatorConfirmationBody(req, "JOB_ACTION", {
          actionId: "jobs.start",
          target: targetPrimary,
        }),
      });
      started.push({ name: targetPrimary, ok: !!r.ok, result: r.json || null });
    }

    const pipeline = await httpPostJson(`${base}/api/pipeline/run`, operatorConfirmationBody(req, "RUN_PIPELINE", {
      actionId: "pipeline.run",
      target: "market_data_pipeline",
    }));
    const ok = started.some((x) => x && x.ok);

    const payload = {
      ok,
      targetPrimary,
      stopped,
      started,
      pipeline: pipeline.json || null
    };
    finishOperatorAction("restart_feeds", ok === true);
    return sendOperatorPayload(res, payload, ok ? 202 : 503);
  } catch (e) {
    finishOperatorAction("restart_feeds", false);
    return jsonFail(res, "restart_feeds_failed", 500, {
      detail: String(e)
    });
  }
});

function _dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) return reject(err);
      resolve(row || null);
    });
  });
}

function getOperatorSqliteModule() {
  try {
    return { ok: true, sqlite3: require("sqlite3") };
  } catch (e) {
    return {
      ok: false,
      error: "sqlite3_not_installed",
      detail: String(e)
    };
  }
}

async function loadBootstrapCountsPayload() {
  const cached = readOperatorDbCache(_bootstrapCountsCache);
  if (cached) return cached;

  const sqliteState = getOperatorSqliteModule();
  if (!sqliteState.ok) {
    return {
      ok: false,
      error: sqliteState.error,
      detail: sqliteState.detail,
      counts: {}
    };
  }

  const envObj = readEnv();
  const { sanitized } = validateAndSanitizeEnv(envObj);
  const { resolvedDb } = resolveDbPathFromSanitized(sanitized);
  const tables = [
    "symbols",
    "prices",
    "price_quotes",
    "price_provider_health",
    "events",
    "labels",
    "model_metrics",
    "model_registry"
  ];

  const payload = await new Promise((resolve) => {
    const counts = {};
    let settled = false;
    let db = null;
    try {
      db = new sqliteState.sqlite3.Database(resolvedDb, (openErr) => {
        if (openErr) {
          settled = true;
          resolve({
            ok: false,
            error: "db_open_failed",
            detail: String(openErr),
            dbPath: resolvedDb,
            counts: {}
          });
          return;
        }

        let remaining = tables.length;
        let partialError = null;
        for (const table of tables) {
          db.get(`SELECT COUNT(*) AS n FROM ${table}`, [], (err, row) => {
            if (settled) return;
            if (err) {
              counts[table] = 0;
              partialError = partialError || err;
            } else {
              counts[table] = Number((row && row.n) || 0);
            }
            remaining -= 1;
            if (remaining === 0) {
              settled = true;
              try { db.close(); } catch {}
              resolve({
                ok: !partialError,
                degraded: !!partialError,
                error: partialError ? "bootstrap_counts_partial" : null,
                detail: partialError ? String(partialError) : null,
                dbPath: resolvedDb,
                counts
              });
            }
          });
        }
      });
    } catch (e) {
      if (db) {
        try { db.close(); } catch {}
      }
      resolve({
        ok: false,
        error: "bootstrap_counts_failed",
        detail: String(e),
        dbPath: resolvedDb,
        counts: {}
      });
    }
  });

  writeOperatorDbCache("bootstrap_counts", payload);
  return payload;
}

async function loadDbSchemaPayload() {
  const cached = readOperatorDbCache(_dbSchemaCache);
  if (cached) return cached;

  const sqliteState = getOperatorSqliteModule();
  if (!sqliteState.ok) {
    return {
      ok: false,
      error: sqliteState.error,
      detail: sqliteState.detail,
      tables: []
    };
  }

  const envObj = readEnv();
  const { sanitized } = validateAndSanitizeEnv(envObj);
  const { resolvedDb } = resolveDbPathFromSanitized(sanitized);

  const payload = await new Promise((resolve) => {
    let db = null;
    try {
      db = new sqliteState.sqlite3.Database(resolvedDb, (openErr) => {
        if (openErr) {
          resolve({
            ok: false,
            error: "db_open_failed",
            detail: String(openErr),
            dbPath: resolvedDb,
            tables: []
          });
          return;
        }
        db.all("SELECT name FROM sqlite_master WHERE type='table'", [], (err, rows) => {
          try { db.close(); } catch {}
          if (err) {
            resolve({
              ok: false,
              error: "db_schema_query_failed",
              detail: String(err),
              dbPath: resolvedDb,
              tables: []
            });
            return;
          }
          resolve({
            ok: true,
            dbPath: resolvedDb,
            tables: Array.isArray(rows) ? rows.map((row) => row.name) : []
          });
        });
      });
    } catch (e) {
      if (db) {
        try { db.close(); } catch {}
      }
      resolve({
        ok: false,
        error: "db_schema_failed",
        detail: String(e),
        dbPath: resolvedDb,
        tables: []
      });
    }
  });

  writeOperatorDbCache("db_schema", payload);
  return payload;
}

async function getOperatorDbCounts() {
  const sqlite3 = require("sqlite3");
  const envObj = readEnv();
  const { sanitized } = validateAndSanitizeEnv(envObj);
  const { resolvedDb } = resolveDbPathFromSanitized(sanitized || envObj || {});

  return await new Promise((resolve) => {
    const db = new sqlite3.Database(resolvedDb, sqlite3.OPEN_READONLY, async (err) => {
      if (err) {
        resolve({ ok: false, error: String(err), dbPath: resolvedDb, counts: {} });
        return;
      }

      try {
        const tables = [
          "symbols",
          "prices",
          "price_quotes",
          "price_provider_health",
          "events",
          "labels",
          "model_metrics",
          "model_registry"
        ];

        const counts = {};
        for (const table of tables) {
          const row = await _dbGet(db, `SELECT COUNT(*) AS n FROM ${table}`);
          counts[table] = Number((row && row.n) || 0);
        }

        resolve({ ok: true, dbPath: resolvedDb, counts });
      } catch (e) {
        resolve({ ok: false, error: String(e), dbPath: resolvedDb, counts: {} });
      } finally {
        try { db.close(); } catch {}
      }
    });
  });
}

async function pipelineWatchdog(){

  try{

    const db = await getOperatorDbCounts();

    if(!db.ok) return;

    const c = db.counts || {};

if((c.prices || 0) === 0){
      console.log("[WATCHDOG] restarting feeds");
      await httpPostJson(`http://${OPERATOR_BIND_HOST}:${OPERATOR_PORT}/api/operator/restart_feeds`);
      return;
    }

if((c.events || 0) === 0){
      console.log("[WATCHDOG] rebuilding events");
      await httpPostJson(`http://${OPERATOR_BIND_HOST}:${OPERATOR_PORT}/api/operator/run_job`, { job: "process_events" });
      return;
    }

if((c.labels || 0) === 0){
      console.log("[WATCHDOG] rebuilding labels");
      await httpPostJson(`http://${OPERATOR_BIND_HOST}:${OPERATOR_PORT}/api/operator/run_job`, { job: "label_due_events" });
      return;
    }

if((c.model_registry || 0) === 0){
      console.log("[WATCHDOG] training model");
      await httpPostJson(`http://${OPERATOR_BIND_HOST}:${OPERATOR_PORT}/api/operator/run_job`, { job: "train_model_v2" });
      return;
    }

  }catch(e){
    console.warn("[WATCHDOG] pipeline error", e);
  }
}

async function providerFailover(){

  try{

    const health = await verifyHealth();

    if(!health || !health.body) return;

    const priceAge = Number(health.body.prices?.age_s || 9999);

    if(priceAge > 60){

      console.log("[WATCHDOG] price feed stale -> restart");

await httpPostJson(
        `http://${OPERATOR_BIND_HOST}:${OPERATOR_PORT}/api/operator/restart_feeds`
      );

    }

  }catch(e){
    console.warn("[WATCHDOG] provider failover error", e);
  }

}

async function marketSessionControl(){

  try{

    const r = await httpGetJson(
      `${dashBaseUrlFromEnv(readEnv())}/api/market/session`
    );

    if(!r.ok) return;

    const stateNow = r.json?.state;

    if(stateNow === "CLOSED"){

      if(state.lastMode !== "safe"){
        console.log("[MARKET] closed -> safe mode");
        await restartEngineGracefully("safe", { stopTimeoutMs: 15000 });
      }

    }

    if(stateNow === "OPEN"){

      if(state.lastMode === "safe"){
        console.log("[MARKET] open -> shadow mode");
        await restartEngineGracefully("shadow", { stopTimeoutMs: 15000 });
      }

    }

  }catch(e){
    console.warn("[WATCHDOG] market session error", e);
  }

}

async function modelPromotionWatchdog(){

  try{

    const r = await httpGetJson(
      `${dashBaseUrlFromEnv(readEnv())}/api/models/status`
    );

    if(!r.ok) return;

    const status = r.json || {};

    if(status.promotion_ready){

      console.log("[MODEL] promoting model");

      await httpPostJson(
        `http://${OPERATOR_BIND_HOST}:${OPERATOR_PORT}/api/operator/promote_model`
      );

    }

  }catch(e){
    console.warn("[WATCHDOG] promotion error", e);
  }

}

async function waitForOperatorCheck(checkFn, timeoutMs = 60000, sleepMs = 1500) {
  const startedAt = Date.now();
  let last = null;

  while ((Date.now() - startedAt) < timeoutMs) {
    try {
      last = await checkFn();
      if (last && last.ok) {
        return { ok: true, elapsedMs: Date.now() - startedAt, last };
      }
    } catch (e) {
      last = { ok: false, error: String(e) };
    }

    await sleep(sleepMs);
  }

  return { ok: false, elapsedMs: Date.now() - startedAt, last };
}

async function startDashboardJob(base, name) {
  const r = await httpPostJson(`${base}/api/jobs/start?name=${encodeURIComponent(name)}`, { name }, 30000);
  return {
    ok: !!(r && r.ok && r.json && r.json.ok !== false),
    name,
    result: r ? (r.json || null) : null
  };
}

app.post("/api/operator/guided_bootstrap", async (req, res) => {
  const mode = String((req.body && req.body.mode) || state.lastMode || "safe")
    .trim()
    .toLowerCase();

  const finalMode = (mode === "live" || mode === "shadow") ? mode : "safe";
  const confirmation = requireOperatorConfirmation(
    req,
    res,
    finalMode === "live" ? "operator.guided_bootstrap_live" : "operator.guided_bootstrap",
    { target: `mode:${finalMode}` }
  );
  if (!confirmation) return;
  const steps = [];

  try {
    if (status() !== "RUNNING") {
      const started = startEngine(finalMode);
      steps.push({
        id: "engine_start",
        ok: !!(started && started.ok),
        label: "Start engine",
        detail: started
      });

      if (!started || !started.ok) {
        return sendOperatorPayload(res, { ok: false, mode: finalMode, steps }, 503);
      }
    } else {
      steps.push({
        id: "engine_start",
        ok: true,
        label: "Start engine",
        detail: "engine already running"
      });
    }

    const healthWait = await waitForOperatorCheck(async () => {
      const health = await verifyHealth();
      return { ok: !!(health && health.ok), health };
    }, 45000, 1500);

    steps.push({
      id: "health_wait",
      ok: !!healthWait.ok,
      label: "Verify backend health",
      detail: healthWait.ok ? healthWait.last.health : (healthWait.last || { error: "health_timeout" })
    });

    if (!healthWait.ok) {
      return sendOperatorPayload(res, { ok: false, mode: finalMode, steps }, 503);
    }

    const envObj = readEnv();
    const base = dashBaseUrlFromEnv(envObj);
    const r = await httpPostJson(`${base}/api/operator/bootstrap_pipeline`, {
      mode: finalMode,
      ...operatorConfirmationBody(req, "GUIDED_BOOTSTRAP", {
        actionId: "operator.guided_bootstrap",
        target: `mode:${finalMode}`,
      }),
    }, 180000);

    if (!r || !r.ok) {
      steps.push({
        id: "python_bootstrap_proxy",
        ok: false,
        label: "Startup orchestrator",
        detail: r ? (r.json || r.error || r) : "bootstrap_proxy_failed"
      });
      return sendOperatorPayload(res, { ok: false, mode: finalMode, steps }, 503);
    }

    const body = r.json || {};
    const mergedSteps = steps.concat(Array.isArray(body.steps) ? body.steps : []);

    return sendOperatorPayload(res, {
      ok: !!body.ok,
      mode: finalMode,
      steps: mergedSteps,
      result: body
    }, body && body.ok ? 202 : 503);
  } catch (e) {
    steps.push({
      id: "guided_bootstrap_exception",
      ok: false,
      label: "Guided bootstrap exception",
      detail: String(e)
    });

    return jsonFail(res, "guided_bootstrap_failed", 500, { mode: finalMode, steps, detail: String(e) });
  }
});

// --------------------------------------------------
// LIVE TELEMETRY WEBSOCKET
// --------------------------------------------------
function startTelemetryWebSocket(server){

  const wss = new WebSocket.Server({
    server,
    path: "/ws/operator",
    verifyClient: (info, done) => {
      if (operatorMutationAuthorized(info.req)) {
        done(true);
        return;
      }
      done(false, 403, "operator_forbidden");
    }
  });

  _wsServer = wss;

  wss.on("connection", (ws) => {

    ws.isAlive = true;

    ws.on("pong", () => ws.isAlive = true);

  });

  // heartbeat
  _wsHeartbeatTimer = setInterval(() => {

    wss.clients.forEach(ws => {

      if(!ws.isAlive) return ws.terminate();

      ws.isAlive = false;
      ws.ping();

    });

  }, 30000);

}

function broadcastTelemetry(type, payload){

  if(!_wsServer) return;

  const msg = JSON.stringify({
    type,
    payload
  });

  _wsServer.clients.forEach(ws => {

    if(ws.readyState === WebSocket.OPEN){
      ws.send(msg);
    }

  });

}

_httpServer = app.listen(OPERATOR_PORT, OPERATOR_BIND_HOST, () => {

  startTelemetryWebSocket(_httpServer);

  ensureLogDir();
  console.log(
    `Operator Control Center: http://${OPERATOR_BIND_HOST}:${OPERATOR_PORT}`
  );

  const autoStart = normalizeBool(process.env.OPERATOR_AUTO_START);
  if (autoStart === true && !child) {
    setTimeout(() => {
      const mode = String(process.env.ENGINE_MODE || process.env.OPERATOR_MODE || "safe")
        .trim()
        .toLowerCase();
      const finalMode = (mode === "live" || mode === "shadow") ? mode : "safe";

      try {
        const started = startEngine(finalMode);
        if (!started || started.ok !== true) {
          console.error("[operator] auto-start failed:", started);
        } else {
          console.log(`[operator] auto-start requested mode=${finalMode}`);
        }
      } catch (e) {
        console.error("[operator] auto-start exception:", e);
      }
    }, OPERATOR_AUTO_START_DELAY_MS);
  }
});
