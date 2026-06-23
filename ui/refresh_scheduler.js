/*
  FILE: ui/refresh_scheduler.js

  Shared browser refresh scheduler for the dashboard. It serializes recurring
  async tasks, pauses work on hidden pages, and provides a single place to
  start or stop the polling cadence used by dashboard modules.

  Scheduling uses a self-rescheduling timeout (not a fixed setInterval) so that:
    - runs never overlap (the next run is scheduled only after the prior one
      finishes), which matters on a higher-latency LAN link, and
    - failing runs back off multiplicatively instead of hammering a struggling
      backend every cycle, recovering immediately once a run succeeds.

  Operators can override the base cadence at runtime via
  ``window.DASHBOARD_REFRESH_MS`` (clamped to a sane floor) without touching
  call sites -- useful for trimming traffic over a remote LAN connection.
*/

let _timerId = null;
let _running = false;
let _tasks = [];
let _intervalMs = 2000;
let _backoffMs = 0;
let _boundVisibility = false;
let _boundPagehide = false;

const _MIN_INTERVAL_MS = 250;
const _MAX_BACKOFF_MS = 30000;

function _resolveBaseIntervalMs(requested) {
  let ms = Number(requested);
  try {
    if (typeof window !== "undefined") {
      const override = Number(window.DASHBOARD_REFRESH_MS);
      if (Number.isFinite(override) && override > 0) ms = override;
    }
  } catch {}
  if (!Number.isFinite(ms) || ms <= 0) ms = 2000;
  return Math.max(_MIN_INTERVAL_MS, ms);
}

async function _runTasks(tasks) {
  if (_running) return;
  if (typeof document !== "undefined" && document.hidden) return;

  _running = true;
  let anyError = false;
  try {
    for (const fn of tasks) {
      try {
        if (typeof fn === "function") {
          await fn();
        }
      } catch (e) {
        anyError = true;
        console.error("refresh task error:", e);
      }
    }
  } finally {
    _running = false;
  }

  // Multiplicative backoff on failure; reset instantly on a clean run so a
  // transient LAN/backend hiccup does not permanently slow the dashboard.
  if (anyError) {
    _backoffMs = _backoffMs ? Math.min(_MAX_BACKOFF_MS, _backoffMs * 2) : _intervalMs;
  } else {
    _backoffMs = 0;
  }
}

function _stopIntervalOnly() {
  if (_timerId) {
    clearTimeout(_timerId);
    _timerId = null;
  }
}

function _scheduleNext() {
  _stopIntervalOnly();

  if (!Array.isArray(_tasks) || _tasks.length === 0) return;
  if (typeof document !== "undefined" && document.hidden) return;

  const delay = Math.max(_MIN_INTERVAL_MS, _intervalMs + _backoffMs);
  _timerId = setTimeout(_tick, delay);
}

async function _tick() {
  await _runTasks(_tasks);
  _scheduleNext();
}

// Backward-compatible alias retained for any external callers/tests.
const _startInterval = _scheduleNext;

function _bindLifecycle() {
  if (typeof document !== "undefined" && !_boundVisibility) {
    _boundVisibility = true;
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) {
        _stopIntervalOnly();
        return;
      }

      void _runTasks(_tasks);
      _scheduleNext();
    });
  }

  if (typeof window !== "undefined" && !_boundPagehide) {
    _boundPagehide = true;
    window.addEventListener("pagehide", () => {
      _stopIntervalOnly();
      _running = false;
    });
  }
}

export function scheduleRefreshTasks(tasks = [], intervalMs = 2000) {
  _tasks = Array.isArray(tasks) ? tasks.slice() : [];
  _intervalMs = _resolveBaseIntervalMs(intervalMs);
  _backoffMs = 0;

  _bindLifecycle();
  _scheduleNext();
}

export function stopRefreshTasks() {
  _stopIntervalOnly();
  _tasks = [];
  _running = false;
  _backoffMs = 0;
}

export { _startInterval };
