/*
  FILE: ui/refresh_runtime.js

  Dashboard refresh-loop coordinator. This module wires the main refresh
  functions into the shared scheduler and ensures the recurring header/alerts
  updates are only started once per page load.
*/

export function startDashboardRefreshScheduler({
  refresh,
  loadAlerts,
  loadSystemStatusHeader,
  pauseFlagGetter,
  getRealtimeState,
  scheduleRefreshTasks
}) {

  if (window.__dashboardRefreshSchedulerStarted) return;

  window.__dashboardRefreshSchedulerStarted = true;
  let lastFullRefreshAt = 0;

  scheduleRefreshTasks([
    async () => {
      if (pauseFlagGetter()) return;
      const realtime = typeof getRealtimeState === "function" ? (getRealtimeState() || {}) : {};
      const lastMessageTs = Number(realtime.lastMessageTs || 0);
      const connected = realtime.connected === true;
      const now = Date.now();
      const recentRealtime = connected && lastMessageTs > 0 && (now - lastMessageTs) <= 8_000;
      const allowBackfill = !recentRealtime || (now - lastFullRefreshAt) >= 12_000;
      if (!allowBackfill) return;
      lastFullRefreshAt = now;
      await refresh();
    },
    async () => {
      if (pauseFlagGetter() || typeof loadAlerts !== "function") return;
      const screen = String(document?.body?.dataset?.dashboardScreen || "").trim().toLowerCase();
      if (screen === "overview" || screen === "operate") return;
      await loadAlerts();
    }
  ], 3500);
}
