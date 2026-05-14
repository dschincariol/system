#!/usr/bin/env node
"use strict";

const DEFAULT_TIMEOUT_MS = Number(process.env.OPERATOR_AUDIT_TIMEOUT_MS || 12000);

const checks = [
  {
    name: "operator status",
    url: "http://127.0.0.1:4001/api/operator/status",
    required: true,
    maxMs: 1000,
    json: true
  },
  {
    name: "operator readiness",
    url: "http://127.0.0.1:4001/api/operator/readiness",
    required: true,
    maxMs: 5000,
    json: true,
    warnWhenOkFalse: true
  },
  {
    name: "operator bootstrap",
    url: "http://127.0.0.1:4001/api/operator/bootstrapStatus",
    required: true,
    maxMs: 5000,
    json: true
  },
  {
    name: "support snapshot quick",
    url: "http://127.0.0.1:4001/api/operator/snapshot?mode=quick",
    required: true,
    maxMs: 7000,
    json: true
  },
  {
    name: "dashboard health",
    url: "http://127.0.0.1:8000/api/health",
    required: true,
    maxMs: 5000,
    json: true,
    warnWhenOkFalse: true
  },
  {
    name: "data source page",
    url: "http://127.0.0.1:8000/ui/data_sources.html",
    required: true,
    maxMs: 3000,
    json: false
  },
  {
    name: "data source api",
    url: "http://127.0.0.1:8000/api/data_sources",
    required: true,
    maxMs: 3000,
    json: true
  },
  {
    name: "telemetry",
    url: "http://127.0.0.1:8000/api/telemetry",
    required: true,
    maxMs: 3000,
    json: true
  }
];

function summarizeJson(payload) {
  if (!payload || typeof payload !== "object") return {};
  const currentBlocker = payload.currentBlocker && typeof payload.currentBlocker === "object"
    ? (payload.currentBlocker.message || payload.currentBlocker.code || null)
    : null;
  const reasons = Array.isArray(payload.reasons)
    ? payload.reasons
    : (Array.isArray(payload.health?.reasons) ? payload.health.reasons : []);
  return {
    ok: Object.prototype.hasOwnProperty.call(payload, "ok") ? payload.ok : null,
    status: payload.status || payload.state || null,
    ready: Object.prototype.hasOwnProperty.call(payload, "ready") ? payload.ready : null,
    error: payload.error || null,
    currentBlocker,
    reasons
  };
}

async function fetchCheck(check) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  const startedAt = Date.now();
  try {
    const response = await fetch(check.url, {
      cache: "no-store",
      signal: controller.signal
    });
    const text = await response.text();
    let payload = null;
    if (check.json) {
      try {
        payload = JSON.parse(text);
      } catch {
        payload = null;
      }
    }
    const elapsedMs = Date.now() - startedAt;
    const summary = summarizeJson(payload);
    const failures = [];
    const warnings = [];

    if (check.required && response.status >= 400) {
      failures.push(`http_${response.status}`);
    }
    if (elapsedMs > check.maxMs) {
      failures.push(`slow_${elapsedMs}ms_gt_${check.maxMs}ms`);
    }
    if (check.json && !payload) {
      failures.push("invalid_json");
    }
    if (check.warnWhenOkFalse && payload && payload.ok === false) {
      warnings.push(summary.currentBlocker || summary.error || summary.reasons.join(", ") || "ok_false");
    }

    return {
      name: check.name,
      url: check.url,
      httpStatus: response.status,
      elapsedMs,
      bytes: text.length,
      ...summary,
      failures,
      warnings
    };
  } catch (error) {
    return {
      name: check.name,
      url: check.url,
      elapsedMs: Date.now() - startedAt,
      httpStatus: 0,
      ok: null,
      status: null,
      ready: null,
      error: error && error.name === "AbortError" ? "timeout" : String(error),
      currentBlocker: null,
      reasons: [],
      failures: [error && error.name === "AbortError" ? "timeout" : "request_failed"],
      warnings: []
    };
  } finally {
    clearTimeout(timeout);
  }
}

function printResult(row) {
  const state = row.failures.length ? "FAIL" : (row.warnings.length ? "WARN" : "OK");
  const bits = [
    state.padEnd(4),
    row.name.padEnd(23),
    `http=${String(row.httpStatus).padEnd(3)}`,
    `time=${String(row.elapsedMs).padStart(5)}ms`
  ];
  if (row.status) bits.push(`status=${row.status}`);
  if (row.ok === false) bits.push("ok=false");
  if (row.currentBlocker) bits.push(`blocker=${row.currentBlocker}`);
  if (row.reasons && row.reasons.length && !row.currentBlocker) {
    bits.push(`reasons=${row.reasons.slice(0, 4).join(",")}`);
  }
  if (row.failures.length) bits.push(`failures=${row.failures.join(",")}`);
  if (row.warnings.length && !row.currentBlocker && !(row.reasons && row.reasons.length)) {
    bits.push(`warnings=${row.warnings.join(",")}`);
  }
  console.log(bits.join(" | "));
}

(async () => {
  const rows = [];
  for (const check of checks) {
    const row = await fetchCheck(check);
    rows.push(row);
    printResult(row);
  }

  const failed = rows.filter((row) => row.failures.length > 0);
  const warned = rows.filter((row) => row.warnings.length > 0);
  console.log("");
  console.log(`startup_audit=${failed.length ? "FAIL" : "PASS"} failures=${failed.length} warnings=${warned.length}`);
  if (failed.length) {
    process.exitCode = 1;
  }
})();
