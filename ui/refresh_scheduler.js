/*
  FILE: ui/refresh_scheduler.js

  Shared browser refresh scheduler for the dashboard. It serializes recurring
  async tasks, pauses work on hidden pages, and provides a single place to
  start or stop the polling cadence used by dashboard modules.
*/

let _intervalId = null;
let _running = false;
let _tasks = [];
let _intervalMs = 2000;
let _boundVisibility = false;
let _boundPagehide = false;

async function _runTasks(tasks) {
  if (_running) return;
  if (typeof document !== "undefined" && document.hidden) return;

  _running = true;
  try {
    for (const fn of tasks) {
      try {
        if (typeof fn === "function") {
          await fn();
        }
      } catch (e) {
        console.error("refresh task error:", e);
      }
    }
  } finally {
    _running = false;
  }
}

function _stopIntervalOnly() {
  if (_intervalId) {
    clearInterval(_intervalId);
    _intervalId = null;
  }
}

function _startInterval() {
  _stopIntervalOnly();

  if (!Array.isArray(_tasks) || _tasks.length === 0) return;
  if (typeof document !== "undefined" && document.hidden) return;

  _intervalId = setInterval(() => {
    void _runTasks(_tasks);
  }, _intervalMs);
}

function _bindLifecycle() {
  if (typeof document !== "undefined" && !_boundVisibility) {
    _boundVisibility = true;
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) {
        _stopIntervalOnly();
        return;
      }

      void _runTasks(_tasks);
      _startInterval();
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
  _intervalMs = Math.max(250, Number(intervalMs) || 2000);

  _bindLifecycle();
  _startInterval();
}

export function stopRefreshTasks() {
  _stopIntervalOnly();
  _tasks = [];
  _running = false;
}
