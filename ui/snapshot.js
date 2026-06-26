/*
  FILE: ui/snapshot.js

  Snapshot-bundle builder for dashboard and operator support workflows. It
  gathers a fixed set of API responses plus local UI state into a portable
  diagnostic payload for export or triage.
*/

import { apiFetch } from "./api_client.js";

export async function buildSnapshotBundle({
  OPERATOR_MODE = null,
  EXPERT_UNLOCK = false,
  isExecutionDegraded = () => false,
  lastAlerts = [],
  lastHealth = null
} = {}) {

  const endpoints = [
    "/api/operator/status",
    "/api/operator/bootstrap",
    "/api/operator/readiness",
    "/api/health",
    "/api/system/state",
    "/api/execution/barrier",
    "/api/promotion/status",
    "/api/jobs",
    "/api/jobs/history?limit=50",
    "/api/jobs/log?tail=200",
    "/api/alerts",
    "/api/validation",
    "/api/model/diagnostics",
    "/api/db/health"
  ];

  const OPERATOR_BASE =
    window.OPERATOR_BASE ||
    window.location.origin;

  const bundle = {
    ts_iso: new Date().toISOString(),
    ts_ms: Date.now(),
    location: window.location.href,
    userAgent: navigator.userAgent,

    runtime: {
      base_url: window.location.origin,
      operator_base: OPERATOR_BASE,
      endpoints,
      ts: Date.now()
    },

    operator_mode: OPERATOR_MODE,
    expert_unlocked: EXPERT_UNLOCK,
    execution_degraded: isExecutionDegraded(),

    system_stage: "BOOT",
    stage_reason: null,
    stage_ts: Date.now(),

    data_flow_ok: false,
    data_flow_checks: {
      ingestion_ok: false,
      db_ok: false,
      jobs_ok: false
    },

    critical_blockers: [],
    root_cause_candidates: [],
    primary_root_cause: null,

    endpoints: {},

    alerts_snapshot: lastAlerts || [],
    health_snapshot: lastHealth || null,

    ingestion: {
      last_tick_ts: null,
      stale: true,
      provider_connected: null,
      latency_ms: null,
      source: null
    },

    jobs: {
      by_name: {},
      summary: {
        total: 0,
        running: 0,
        dead: 0,
        stale: 0
      },
      last_update_ts: null
    },

    db: {
      path: null,
      exists: null,
      size_bytes: null,
      tables: [],
      row_counts: {},
      integrity: null,
      last_checked_ts: null
    },

    engine: {
      mode: null,
      readiness_blockers: [],
      readiness_ok: null,
      last_error: null,
      last_start_ts: null,
      uptime_ms: null
    },

    console_tail: [],
    errors: [],
    last_errors_by_module: {},
    event_log_tail: []
  };

  for (const ep of endpoints) {
    const startedAt = Date.now();

    try {
      const isOperator = ep.startsWith("/api/operator");
      const url = isOperator ? (OPERATOR_BASE + ep) : ep;

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);

      let res;
      try {
        res = await apiFetch(url, { cache: "no-store", signal: controller.signal });
      } finally {
        clearTimeout(timeoutId);
      }

      let json = null;
      try {
        const txt = await res.text();
        json = txt ? JSON.parse(txt) : null;
      } catch (e) {}

      bundle.endpoints[ep] = {
        ok: res.ok,
        status: res.status,
        latency_ms: Date.now() - startedAt,
        body: json
      };

      // ---- readiness → engine ----
if (ep === "/api/operator/readiness" && json) {
  bundle.engine.mode = json.mode || null;
  bundle.engine.readiness_blockers = json.issues || [];
  bundle.engine.readiness_ok = json.ok === true;
  bundle.engine.last_error = json.lastError || json.error || null;

        if (Array.isArray(json.issues) && json.issues.length > 0) {
          bundle.critical_blockers.push(...json.issues);
        }
      }

      // ---- ingestion ----
if (ep === "/api/system/state" && json?.ingestion) {
  bundle.ingestion.last_tick_ts = json.ingestion.lastTickTs || null;
  bundle.ingestion.provider_connected = json.ingestion.connected || false;
  bundle.ingestion.source = json.ingestion.provider || json.ingestion.source || null;

        if (bundle.ingestion.last_tick_ts) {
          const age = Date.now() - bundle.ingestion.last_tick_ts;
          bundle.ingestion.latency_ms = age;
          bundle.ingestion.stale = age > 5000;

          if (bundle.ingestion.stale) {
            bundle.critical_blockers.push("ingestion_stale");
          }
        }
      }

      // ---- DB ----
      if (ep === "/api/db/health" && json) {
        bundle.db.path = json.db_path || null;
        bundle.db.last_checked_ts = Date.now();
        bundle.db.exists = json.exists === true;
        bundle.db.tables = json.tables || [];
        bundle.db.row_counts = json.row_counts || {};
        bundle.db.size_bytes = json.size_bytes || null;
        bundle.db.integrity = json.integrity || null;
      }

      // ---- JOBS ----
      if (ep === "/api/jobs" && Array.isArray(json)) {
        bundle.jobs.last_update_ts = Date.now();

        for (const j of json) {
          const name = j.name || j.job_name || "unknown";

          const job = {
            running: !!j.running,
            last_heartbeat_ts: j.last_heartbeat_ts || null,
            last_success_ts: j.last_success_ts || null,
            last_error: j.last_error || null,
            restart_count: j.restart_count || 0,
            stale: false
          };

          if (job.last_heartbeat_ts) {
            const age = Date.now() - job.last_heartbeat_ts;
            if (age > 60000) {
              job.stale = true;
              bundle.jobs.summary.stale += 1;
            }
          }

          if (job.running) bundle.jobs.summary.running += 1;
          else bundle.jobs.summary.dead += 1;

          bundle.jobs.summary.total += 1;
          bundle.jobs.by_name[name] = job;
        }
      }

      // ---- HEALTH ----
      if (ep === "/api/health" && json) {
        if (json.ok !== true) {
          bundle.root_cause_candidates.push("health_not_ok");
        }
      }

    } catch (e) {
      const err = String(e);

      bundle.endpoints[ep] = {
        ok: false,
        error: err,
        ts_ms: Date.now()
      };

bundle.errors.push({
  endpoint: ep,
  error: err,
  ts_ms: Date.now()
});

bundle.last_errors_by_module[ep] = err;

      bundle.root_cause_candidates.push(`endpoint_failure:${ep}`);
    }
  }

  // ---- console ----
  const consoleEl = document.getElementById("console");
  if (consoleEl) {
    const lines = (consoleEl.innerText || "").split("\n");
    bundle.console_tail = lines.slice(-300);

    const errorLines = lines.filter(l =>
      l.includes("ERROR") ||
      l.includes("Exception") ||
      l.includes("failed")
    ).slice(-50);

if (errorLines.length > 0) {
  bundle.errors.push({
    source: "console",
    lines: errorLines,
    ts_ms: Date.now()
  });

  bundle.last_errors_by_module.console = errorLines.slice(-1)[0] || null;

  bundle.root_cause_candidates.push("console_errors_detected");
}
  }

  // ---- data flow checks ----
  bundle.data_flow_checks.ingestion_ok =
    bundle.ingestion.stale === false &&
    bundle.ingestion.provider_connected === true;

  bundle.data_flow_checks.db_ok =
    bundle.db.exists === true &&
    bundle.db.tables.length > 0;

  bundle.data_flow_checks.jobs_ok =
    bundle.jobs.summary.dead === 0;

  bundle.data_flow_ok =
    bundle.data_flow_checks.ingestion_ok &&
    bundle.data_flow_checks.db_ok &&
    bundle.data_flow_checks.jobs_ok;

  // ---- stage ----
  if (bundle.execution_degraded) {
    bundle.system_stage = "EXECUTION";
    bundle.stage_reason = "execution_degraded";
  } else if (!bundle.data_flow_checks.ingestion_ok) {
    bundle.system_stage = "INGESTION";
    bundle.stage_reason = "ingestion_not_ready";
    bundle.critical_blockers.push("ingestion_not_ready");
  } else if (!bundle.data_flow_checks.db_ok) {
    bundle.system_stage = "BOOT";
    bundle.stage_reason = "db_not_ready";
    bundle.critical_blockers.push("db_not_ready");
  } else {
    bundle.system_stage = "RUNNING";
    bundle.stage_reason = "nominal";
  }

if (bundle.critical_blockers.length > 0) {
  bundle.primary_root_cause = bundle.critical_blockers[0];
  bundle.root_cause_candidates = [
    ...new Set([
      ...bundle.critical_blockers,
      ...bundle.root_cause_candidates
    ])
  ];
}

bundle.summary = {
  stage: bundle.system_stage,
  stage_reason: bundle.stage_reason,
  primary_root_cause: bundle.primary_root_cause,
  ingestion_ok: bundle.data_flow_checks.ingestion_ok,
  db_ok: bundle.data_flow_checks.db_ok,
  jobs_ok: bundle.data_flow_checks.jobs_ok,
  blockers: bundle.critical_blockers.slice(0, 5)
};

return bundle;
}

export async function copySnapshotBundle(state = {}) {
  const bundle = await buildSnapshotBundle(state);
  const serialized = JSON.stringify(bundle, null, 2);

  let copied = false;
  try {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
      await navigator.clipboard.writeText(serialized);
      copied = true;
    }
  } catch {}

  if (!copied) {
    const textArea = document.createElement("textarea");
    textArea.value = serialized;
    textArea.setAttribute("readonly", "readonly");
    textArea.style.position = "fixed";
    textArea.style.left = "-9999px";
    document.body.appendChild(textArea);
    textArea.select();
    try {
      copied = document.execCommand("copy");
    } catch {}
    document.body.removeChild(textArea);
  }

  return {
    ok: copied,
    bundle,
  };
}
