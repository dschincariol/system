/*
  Browser-side mirror of the FX 24/5 session calendar.

  FX-04's `engine/data/prices/fx_clock.py` is the canonical session-boundary
  owner. This module expresses the same default week in UTC for presentation:
  open Sunday 22:00 UTC, close Friday 22:00 UTC, with overrides so deployments
  can pin the UI to the backend's configured FX clock.
*/

const WEEK_MS = 7 * 24 * 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;

function clampWeekday(value, fallback) {
  const n = Number.parseInt(value, 10);
  return Number.isInteger(n) && n >= 0 && n <= 6 ? n : fallback;
}

function clampHour(value, fallback) {
  const n = Number.parseInt(value, 10);
  return Number.isInteger(n) && n >= 0 && n <= 23 ? n : fallback;
}

function boundaryMsForWeek(anchorMs, weekday, hourUtc) {
  const date = new Date(anchorMs);
  const midnight = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
  const dayStart = midnight - date.getUTCDay() * DAY_MS;
  return dayStart + weekday * DAY_MS + hourUtc * HOUR_MS;
}

function formatUtcBoundary(weekday, hourUtc) {
  const names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  return `${names[weekday]} ${String(hourUtc).padStart(2, "0")}:00 UTC`;
}

export function fxSessionStatus(nowMs, opts = {}) {
  const now = Number(nowMs);
  if (!Number.isFinite(now)) {
    return { open: false, label: "FX market status unavailable", nextChangeMs: null };
  }

  const openDay = clampWeekday(opts.openWeekdayUtc, 0);
  const openHour = clampHour(opts.openHourUtc, 22);
  const closeDay = clampWeekday(opts.closeWeekdayUtc, 5);
  const closeHour = clampHour(opts.closeHourUtc, 22);
  const openThisWeek = boundaryMsForWeek(now, openDay, openHour);
  const closeThisWeek = boundaryMsForWeek(now, closeDay, closeHour);

  let currentOpen = openThisWeek;
  let currentClose = closeThisWeek;
  if (currentClose <= currentOpen) currentClose += WEEK_MS;
  if (now < currentOpen) {
    currentOpen -= WEEK_MS;
    currentClose -= WEEK_MS;
  }
  if (now >= currentClose) {
    currentOpen += WEEK_MS;
    currentClose += WEEK_MS;
  }

  const open = now >= currentOpen && now < currentClose;
  if (open) {
    return {
      open: true,
      label: `FX market open — closes ${formatUtcBoundary(closeDay, closeHour)}`,
      nextChangeMs: currentClose,
    };
  }

  return {
    open: false,
    label: `FX market closed — opens ${formatUtcBoundary(openDay, openHour)}`,
    nextChangeMs: currentOpen,
  };
}
