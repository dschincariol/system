/*
  Browser-side mirror of the FX 24/5 session calendar.

  FX-04's `engine/data/prices/fx_clock.py` is the canonical session-boundary
  owner. This module mirrors the same default week in America/New_York local
  time for presentation: open Sunday 17:00, close Friday 17:00. UTC overrides
  remain available only for tests or deployments that explicitly pass them.
*/

const WEEK_MS = 7 * 24 * 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
const DEFAULT_TIME_ZONE = "America/New_York";
const DEFAULT_OPEN_WEEKDAY_ET = 0;
const DEFAULT_CLOSE_WEEKDAY_ET = 5;
const DEFAULT_BOUNDARY_HOUR_ET = 17;
const FORMATTERS = new Map();

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

function formatterForTimeZone(timeZone) {
  const key = String(timeZone || DEFAULT_TIME_ZONE);
  if (!FORMATTERS.has(key)) {
    FORMATTERS.set(
      key,
      new Intl.DateTimeFormat("en-US", {
        timeZone: key,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hourCycle: "h23",
      }),
    );
  }
  return FORMATTERS.get(key);
}

function zonedParts(ms, timeZone = DEFAULT_TIME_ZONE) {
  const parts = {};
  for (const part of formatterForTimeZone(timeZone).formatToParts(new Date(ms))) {
    if (part.type !== "literal") parts[part.type] = part.value;
  }
  return {
    year: Number.parseInt(parts.year, 10),
    month: Number.parseInt(parts.month, 10),
    day: Number.parseInt(parts.day, 10),
    hour: Number.parseInt(parts.hour, 10),
    minute: Number.parseInt(parts.minute, 10),
    second: Number.parseInt(parts.second, 10),
  };
}

function localWeekday(parts) {
  return new Date(Date.UTC(parts.year, parts.month - 1, parts.day)).getUTCDay();
}

function localDatePlus(parts, days) {
  const date = new Date(Date.UTC(parts.year, parts.month - 1, parts.day + days));
  return {
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
  };
}

function zonedLocalToUtcMs(year, month, day, hour, timeZone = DEFAULT_TIME_ZONE) {
  const targetAsUtc = Date.UTC(year, month - 1, day, hour, 0, 0);
  let guess = targetAsUtc;
  for (let i = 0; i < 4; i += 1) {
    const parts = zonedParts(guess, timeZone);
    const renderedAsUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
    const delta = targetAsUtc - renderedAsUtc;
    if (delta === 0) break;
    guess += delta;
  }
  return guess;
}

function boundaryMsForLocalWeek(anchorMs, weekday, hourLocal, timeZone = DEFAULT_TIME_ZONE) {
  const parts = zonedParts(anchorMs, timeZone);
  const deltaDays = weekday - localWeekday(parts);
  const target = localDatePlus(parts, deltaDays);
  return zonedLocalToUtcMs(target.year, target.month, target.day, hourLocal, timeZone);
}

function formatLocalBoundary(weekday, hourLocal, timeZone = DEFAULT_TIME_ZONE) {
  const names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  return `${names[weekday]} ${String(hourLocal).padStart(2, "0")}:00 ${timeZone}`;
}

function hasUtcOverride(opts) {
  return (
    opts.openWeekdayUtc !== undefined
    || opts.openHourUtc !== undefined
    || opts.closeWeekdayUtc !== undefined
    || opts.closeHourUtc !== undefined
  );
}

export function fxSessionStatus(nowMs, opts = {}) {
  const now = Number(nowMs);
  if (!Number.isFinite(now)) {
    return { open: false, label: "FX market status unavailable", nextChangeMs: null };
  }

  const utcOverride = hasUtcOverride(opts);
  const timeZone = String(opts.timeZone || DEFAULT_TIME_ZONE);
  const openDay = utcOverride
    ? clampWeekday(opts.openWeekdayUtc, DEFAULT_OPEN_WEEKDAY_ET)
    : clampWeekday(opts.openWeekdayEt, DEFAULT_OPEN_WEEKDAY_ET);
  const openHour = utcOverride
    ? clampHour(opts.openHourUtc, DEFAULT_BOUNDARY_HOUR_ET)
    : clampHour(opts.openHourEt, DEFAULT_BOUNDARY_HOUR_ET);
  const closeDay = utcOverride
    ? clampWeekday(opts.closeWeekdayUtc, DEFAULT_CLOSE_WEEKDAY_ET)
    : clampWeekday(opts.closeWeekdayEt, DEFAULT_CLOSE_WEEKDAY_ET);
  const closeHour = utcOverride
    ? clampHour(opts.closeHourUtc, DEFAULT_BOUNDARY_HOUR_ET)
    : clampHour(opts.closeHourEt, DEFAULT_BOUNDARY_HOUR_ET);
  const openThisWeek = utcOverride
    ? boundaryMsForWeek(now, openDay, openHour)
    : boundaryMsForLocalWeek(now, openDay, openHour, timeZone);
  const closeThisWeek = utcOverride
    ? boundaryMsForWeek(now, closeDay, closeHour)
    : boundaryMsForLocalWeek(now, closeDay, closeHour, timeZone);
  const formatBoundary = utcOverride
    ? formatUtcBoundary
    : (weekday, hour) => formatLocalBoundary(weekday, hour, timeZone);

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
      label: `FX market open - closes ${formatBoundary(closeDay, closeHour)}`,
      nextChangeMs: currentClose,
    };
  }

  return {
    open: false,
    label: `FX market closed - opens ${formatBoundary(openDay, openHour)}`,
    nextChangeMs: currentOpen,
  };
}
