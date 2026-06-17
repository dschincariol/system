export const KILL_SWITCH_CONFIRM_PHRASE = "KILL";
export const KILL_SWITCH_HOLD_MS = 3000;

export function cleanText(value, fallback = "-") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

export function asArray(value) {
  return Array.isArray(value) ? value : [];
}

export function numberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function formatMoney(value) {
  const n = numberOrNull(value);
  if (n === null) return "-";
  const sign = n < 0 ? "-" : "";
  return `${sign}$${Math.abs(n).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

export function formatAge(tsMs, nowMs = Date.now()) {
  const ts = numberOrNull(tsMs);
  if (!ts || ts <= 0) return "age unknown";
  const ageMs = Math.max(0, Number(nowMs) - ts);
  const ageS = Math.floor(ageMs / 1000);
  if (ageS < 60) return `${ageS}s old`;
  const ageM = Math.floor(ageS / 60);
  if (ageM < 60) return `${ageM}m old`;
  const ageH = Math.floor(ageM / 60);
  return `${ageH}h old`;
}

export function normalizeEndpointResult(result) {
  if (!result || typeof result !== "object") {
    return { ok: false, data: null, error: "missing_endpoint_result" };
  }
  if (result.ok === true) return result;
  const data = result.data && typeof result.data === "object" ? result.data : null;
  return {
    ...result,
    ok: false,
    data,
    error: cleanText(result.error || (data && data.error), "endpoint_failed"),
  };
}

export function normalizePnl(payload = {}) {
  const data = payload && typeof payload.data === "object" && !Array.isArray(payload.data) ? payload.data : {};
  const ok = payload && payload.ok === false ? false : true;
  return {
    ok,
    total: payload.total ?? payload.today_pnl ?? payload.day_pnl ?? payload.daily_pnl ?? payload.total_pnl ?? payload.pnl
      ?? data.total ?? data.today_pnl ?? data.day_pnl ?? data.daily_pnl ?? data.total_pnl ?? data.pnl,
    unrealized: payload.unrealized ?? payload.unrealized_pnl ?? data.unrealized ?? data.unrealized_pnl,
    realized: payload.realized ?? payload.realized_pnl ?? data.realized ?? data.realized_pnl,
    ready: ok && (!!(payload.meta && payload.meta.ready) || Object.keys(data).length > 0),
  };
}

function _pnlSeriesSources(payload = {}) {
  const data = payload && typeof payload.data === "object" && !Array.isArray(payload.data) ? payload.data : {};
  return [
    payload.history,
    payload.rows,
    payload.points,
    payload.series,
    payload.pnl,
    payload.snapshots,
    payload.timeline,
    data.history,
    data.rows,
    data.points,
    data.series,
    data.pnl,
    data.snapshots,
    data.timeline,
  ].filter(Array.isArray);
}

function _timestampMs(value) {
  const raw = value && typeof value === "object"
    ? value.ts_ms ?? value.timestamp_ms ?? value.time_ms ?? value.updated_ts_ms ?? value.ts ?? value.timestamp ?? value.time
    : null;
  const numeric = numberOrNull(raw);
  if (numeric !== null) {
    return numeric < 10_000_000_000 ? numeric * 1000 : numeric;
  }
  if (typeof raw === "string") {
    const parsed = Date.parse(raw);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function _pnlValue(value) {
  if (typeof value === "number") return numberOrNull(value);
  if (Array.isArray(value)) {
    return numberOrNull(value[1] ?? value[0]);
  }
  if (!value || typeof value !== "object") return null;
  return numberOrNull(
    value.total
    ?? value.total_pnl
    ?? value.today_pnl
    ?? value.day_pnl
    ?? value.daily_pnl
    ?? value.pnl
    ?? value.value
    ?? value.net_total
  );
}

export function summarizePnlTrend(payload = {}) {
  if (payload && payload.ok === false) {
    return {
      available: false,
      tone: "unavailable",
      text: `PnL trend unavailable: ${cleanText(payload.error, "endpoint_failed")}.`,
    };
  }

  const points = [];
  for (const rows of _pnlSeriesSources(payload)) {
    for (const row of rows) {
      const value = _pnlValue(row);
      if (value === null) continue;
      points.push({ value, tsMs: _timestampMs(row) });
    }
  }

  const ordered = points.some((point) => point.tsMs !== null)
    ? points.slice().sort((a, b) => (a.tsMs ?? 0) - (b.tsMs ?? 0))
    : points;

  if (ordered.length < 2) {
    const pnl = normalizePnl(payload);
    return {
      available: false,
      tone: "unavailable",
      text: pnl.ready
        ? "PnL trend unavailable: /api/pnl returned only the latest snapshot."
        : "PnL trend unavailable: /api/pnl is not ready.",
    };
  }

  const first = ordered[0];
  const last = ordered[ordered.length - 1];
  const delta = last.value - first.value;
  const direction = Math.abs(delta) < 0.005 ? "flat" : (delta > 0 ? "up" : "down");
  const tone = direction === "up" ? "ok" : direction === "down" ? "danger" : "muted";
  return {
    available: true,
    tone,
    points: ordered.length,
    delta,
    direction,
    text: `PnL trend: ${direction} ${formatMoney(Math.abs(delta))} over ${ordered.length} points (${formatMoney(first.value)} to ${formatMoney(last.value)}).`,
  };
}

export function normalizePositionRows(...sources) {
  const rows = [];
  for (const source of sources) {
    const sourceRows = Array.isArray(source)
      ? source
      : Array.isArray(source?.rows)
        ? source.rows
        : Array.isArray(source?.positions)
          ? source.positions
          : [];
    for (const row of sourceRows) {
      if (!row || typeof row !== "object") continue;
      const qty = numberOrNull(row.qty ?? row.quantity ?? row.position_qty) ?? 0;
      if (qty === 0) continue;
      rows.push({
        symbol: cleanText(row.symbol || row.ticker || row.asset, "UNKNOWN").toUpperCase(),
        qty,
        avgPx: numberOrNull(row.avg_px ?? row.avgPrice ?? row.average_price),
        updatedTsMs: numberOrNull(row.updated_ts_ms ?? row.ts_ms),
      });
    }
  }
  const seen = new Set();
  return rows.filter((row) => {
    const key = row.symbol;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function normalizeAlertRows(payload = {}) {
  const rows = asArray(payload.rows)
    .concat(asArray(payload.data))
    .concat(asArray(payload.alerts))
    .concat(asArray(payload.items));
  const severityScore = { CRIT: 4, CRITICAL: 4, WARN: 3, WARNING: 3, INFO: 2, OK: 1 };
  return rows
    .filter((row) => row && typeof row === "object")
    .map((row) => {
      const severity = cleanText(row.severity || row.level || row.kind, "INFO").toUpperCase();
      const status = cleanText(row.status, "active").toLowerCase();
      return {
        id: row.id ?? row.alert_id ?? "",
        severity,
        status,
        symbol: cleanText(row.symbol || row.asset, ""),
        title: cleanText(row.title || row.message || row.reason || row.alert_type, "Alert"),
        tsMs: numberOrNull(row.ts_ms ?? row.created_ts_ms ?? row.updated_ts_ms),
        score: severityScore[severity] || 0,
      };
    })
    .filter((row) => row.status !== "resolved")
    .sort((a, b) => (b.score - a.score) || ((b.tsMs || 0) - (a.tsMs || 0)));
}

export function killSwitchIsActive(payload = {}) {
  const root = payload.kill_switches && typeof payload.kill_switches === "object"
    ? payload.kill_switches
    : payload.data && typeof payload.data === "object"
      ? payload.data
      : payload;
  if (!root || typeof root !== "object") return false;
  if (root.enabled === true) return true;
  if (String(root.state || "").toUpperCase() === "KILL") return true;
  const stateRows = Array.isArray(root.state) ? root.state : [];
  return stateRows.some((row) => Number(row?.enabled || 0) === 1);
}

export function canStartKillSwitchHold({ typedPhrase = "", pending = false } = {}) {
  return String(typedPhrase || "").trim().toUpperCase() === KILL_SWITCH_CONFIRM_PHRASE && !pending;
}

export function canFireKillSwitch({ typedPhrase = "", holdComplete = false, pending = false } = {}) {
  return canStartKillSwitchHold({ typedPhrase, pending }) && holdComplete === true;
}

export function describeEmergencyConsequences(snapshot = {}) {
  const pnl = normalizePnl(snapshot.pnl || {});
  const positions = normalizePositionRows(snapshot.positions, snapshot.broker);
  const status = snapshot.status && typeof snapshot.status === "object" ? snapshot.status : {};
  const killSwitchActive = killSwitchIsActive(snapshot.killSwitches || {});
  const executionAllowed = Boolean(status.execution_allowed ?? status.allowed);
  const lines = [
    "This sends the backend operator emergency stop.",
    "It stops operator jobs, activates the global kill switch, and disarms execution.",
    "It does not expose order entry and does not submit a mobile flatten order.",
    `Open positions visible now: ${positions.length}.`,
    `Live PnL visible now: total ${formatMoney(pnl.total)}, unrealized ${formatMoney(pnl.unrealized)}.`,
    `Execution currently ${executionAllowed ? "allowed" : "blocked"}; kill switch currently ${killSwitchActive ? "active" : "not active"}.`,
  ];
  return lines.join("\n");
}

export function summarizeEmergencyResult(payload = {}) {
  const ok = payload && payload.ok === true;
  const stopped = asArray(payload.operator_stop?.stopped).filter(Boolean);
  const errors = asArray(payload.safety_errors).filter(Boolean);
  const reasons = asArray(payload.reasons).filter(Boolean).slice(-5);
  const lines = [
    ok ? "Emergency stop accepted by backend." : `Emergency stop failed: ${cleanText(payload.error, "request_failed")}`,
    `Status: ${cleanText(payload.status, ok ? "KILL_SWITCH" : "unknown")}`,
  ];
  if (stopped.length) lines.push(`Stopped jobs: ${stopped.join(", ")}`);
  if (errors.length) lines.push(`Safety errors: ${errors.join(", ")}`);
  if (reasons.length) lines.push(`Audit reasons: ${reasons.join(", ")}`);
  return lines.join("\n");
}
