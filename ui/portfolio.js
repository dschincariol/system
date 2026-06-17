/*
  FILE: ui/portfolio.js

  Portfolio panel loader for the dashboard. It fetches current portfolio state,
  open orders, and related metadata, then renders the portfolio tables and
  status pills used by the main browser UI.
*/

import {
  ageMsFromTimestamp,
  escapeHTML,
  formatAgeMs,
  formatDecimal,
  formatPercent,
  fmtTime
} from "./utils.js";
import { renderLineChart } from "./charts.js";
import { applyInlineMetricAnnotation } from "./tooltip.js";

export async function loadPortfolio(fetchJSON) {

  const meta = document.getElementById("portfolioMeta");
  const stateBody = document.getElementById("portfolioStateBody");
  const ordersBody = document.getElementById("portfolioOrdersBody");

  if (!stateBody || !ordersBody) return;

  try {

    const d = await fetchJSON("/api/portfolio");
    const isReady = !!(d && d.ok && d.meta && d.meta.ready);
    const stateRows = Array.isArray(d && d.state) ? d.state : [];
    const orderRows = Array.isArray(d && d.orders) ? d.orders : [];

    if (!d || !d.ok) {

      if (meta) {
        meta.textContent = "error";
        meta.className = "pill crit bad";
      }

      stateBody.innerHTML = `
        <tr class="table-row">
          <td colspan="5" class="metric-meta">(portfolio load failed)</td>
        </tr>
      `;

      ordersBody.innerHTML = `
        <tr class="table-row">
          <td colspan="6" class="metric-meta">(portfolio orders unavailable)</td>
        </tr>
      `;

      return;
    }

    if (meta) {
      if (isReady) {
        meta.textContent = "live";
        meta.className = "pill ok";
      } else {
        meta.textContent = "not ready";
        meta.className = "pill neutral dim status-neutral";
      }
    }

    stateBody.innerHTML = "";

    if (!stateRows.length) {
      stateBody.innerHTML = `
        <tr class="table-row">
          <td colspan="5" class="metric-meta">${isReady ? "(no portfolio state)" : "(portfolio state not ready)"}</td>
        </tr>
      `;
    } else {
      for (const p of stateRows) {

        stateBody.insertAdjacentHTML("beforeend", `
          <tr class="table-row">
            <td class="mono">${p.symbol || ""}</td>
            <td class="mono">${p.side || ""}</td>
            <td class="mono table-cell-num">${Number(p.weight || 0).toFixed(3)}</td>
            <td class="mono metric-meta">${fmtTime(p.opened_ts_ms)}</td>
            <td class="mono metric-meta">${fmtTime(p.updated_ts_ms)}</td>
          </tr>
        `);
      }
    }

    ordersBody.innerHTML = "";

    if (!orderRows.length) {
      ordersBody.innerHTML = `
        <tr class="table-row">
          <td colspan="6" class="metric-meta">${isReady ? "(no portfolio orders)" : "(portfolio orders not ready)"}</td>
        </tr>
      `;
    } else {
      for (const o of orderRows.slice(0, 20)) {

        ordersBody.insertAdjacentHTML("beforeend", `
          <tr class="table-row">
            <td class="mono metric-meta">${fmtTime(o.ts_ms)}</td>
            <td class="mono">${o.symbol || ""}</td>
            <td class="mono">${o.action || ""}</td>
            <td class="mono">${o.from_side || ""} ${Number(o.from_weight || 0).toFixed(3)}</td>
            <td class="mono">${o.to_side || ""} ${Number(o.to_weight || 0).toFixed(3)}</td>
            <td class="mono table-cell-num">${Number(o.delta_weight || 0).toFixed(3)}</td>
          </tr>
        `);
      }
    }

  } catch (e) {

    if (meta) {
      meta.textContent = "load failed";
      meta.className = "pill crit bad";
    }

    stateBody.innerHTML = `
      <tr class="table-row">
        <td colspan="5" class="metric-meta">(portfolio load failed)</td>
      </tr>
    `;

    ordersBody.innerHTML = `
      <tr class="table-row">
        <td colspan="6" class="metric-meta">(portfolio orders unavailable)</td>
      </tr>
    `;

    console.error("loadPortfolio failed", e);
  }
}

export async function loadBroker(fetchJSON) {

  const panel = document.getElementById("brokerPanel");
  const el = document.getElementById("brokerSnapshot");

  if (!panel || !el) return;
  if (panel.style.display === "none") return;

  try {

    const d = await fetchJSON("/api/broker");

    if (!d || !d.ok) {

      el.textContent = d && d.error
        ? d.error
        : "(broker not available)";

      return;
    }

    let out = "";

    out += `equity=${Number(d.account?.equity ?? 1).toFixed(3)} cash=${Number(d.account?.cash ?? 0).toFixed(3)}\n\n`;

    out += "=== POSITIONS ===\n";

    for (const p of (d.positions || [])) {

      out += `${p.symbol} qty=${Number(p.qty).toFixed(6)} avg_px=${Number(p.avg_px).toFixed(4)}\n`;
    }

    out += "\n=== FILLS (latest) ===\n";

    for (const f of (d.fills || []).slice(0, 20)) {

      out += `${fmtTime(f.ts_ms)} ${f.symbol} qty=${Number(f.qty).toFixed(6)} px=${Number(f.px).toFixed(4)} oid=${f.order_id ?? ""}\n`;
    }

    el.textContent = out || "(no broker data)";

  } catch (e) {

    el.textContent = `[error] ${e.message}`;
  }
}

export async function loadEquityDrift(fetchJSON) {

  const panel = document.getElementById("equityDriftPanel");
  const canvas = document.getElementById("equityDriftCanvas");
  const meta = document.getElementById("equityDriftMeta");

  if (!panel || !canvas || !meta) return;
  if (panel.style.display === "none") return;

  try {

    const res = await fetchJSON("/api/equity_drift?limit=500");

    if (!res || !res.ok || !Array.isArray(res.points)) {

      meta.textContent = "n/a";
      meta.className = "pill neutral dim status-neutral";

      renderLineChart(canvas, [], {
        a11yTitle: "Equity drift",
        emptyMessage: "Equity drift data is unavailable.",
        errorMessage: "Equity drift data is unavailable.",
        valueLabel: "drift",
        a11yValueFormatter: (v) => `${(Number(v) * 100).toFixed(2)}%`,
      });

      return;
    }

    const pts = res.points;

    if (!pts.length) {

      meta.textContent = "empty";
      meta.className = "pill neutral dim status-neutral";

      renderLineChart(canvas, [], {
        a11yTitle: "Equity drift",
        emptyMessage: "No equity drift points are available.",
        valueLabel: "drift",
        a11yValueFormatter: (v) => `${(Number(v) * 100).toFixed(2)}%`,
      });

      return;
    }

    meta.textContent = "live";
    meta.className = "pill ok";

    const series =
      pts
        .map((p, index) => {
          const time = p && (p.ts_ms ?? p.time ?? p.t);
          return {
            time: time ?? index + 1,
            xTime: time ?? null,
            value: Number(p && p.diff_equity_pct),
          };
        })
        .filter((p) => Number.isFinite(p.value));
    const ys = series.map((p) => p.value);

    renderLineChart(canvas, ys, {
      xValues: series.map((p) => p.xTime),
      fmtX: (value, index) => {
        const n = Number(value);
        if (Number.isFinite(n) && n > 100_000_000_000) return fmtTime(n);
        if (typeof value === "string" && value.trim()) {
          const parsed = Date.parse(value);
          if (Number.isFinite(parsed) && parsed > 0) return fmtTime(parsed);
        }
        return String(index + 1);
      },
      topLabel: "equity drift (%)",
      a11yTitle: "Equity drift",
      a11ySeries: series,
      a11yTimeKey: "time",
      valueLabel: "drift",
      a11yValueFormatter: (v) => `${(Number(v) * 100).toFixed(2)}%`,
      fmtY: (v) => `${(v * 100).toFixed(2)}%`,
      stroke: "#d29922",
      yMax: Math.max(0.01, Math.max(...ys, 0)),
      yMin: Math.min(-0.01, Math.min(...ys, 0)),
    });

  } catch (e) {

    meta.textContent = "error";
    meta.className = "pill crit bad";

    renderLineChart(canvas, [], {
      a11yTitle: "Equity drift",
      emptyMessage: "Equity drift failed to load.",
      errorMessage: e && e.message ? e.message : "Equity drift failed to load.",
      valueLabel: "drift",
      a11yValueFormatter: (v) => `${(Number(v) * 100).toFixed(2)}%`,
    });
  }
}

function driftSeverityClass(severity) {
  const value = String(severity || "").toUpperCase();
  if (value === "CRIT" || value === "CRITICAL") return "pill crit bad";
  if (value === "WARN") return "pill warn";
  if (value === "OK" || value === "NORMAL") return "pill ok";
  if (value === "STALE") return "pill warn";
  return "pill neutral dim status-neutral";
}

function driftStatusMeta(status = {}) {
  const state = String(status.state || "unknown").toLowerCase();
  const severity = String(status.severity || "UNKNOWN").toUpperCase();
  if (state === "normal") return "No active drift";
  if (state === "active") return `${severity} drift active`;
  if (state === "stale") return "Drift data stale";
  return "Drift attribution unavailable";
}

function driftFormatValue(value, metric = "") {
  const n = Number(value);
  if (!Number.isFinite(n)) return "unavailable";
  const name = String(metric || "").toLowerCase();
  if (name.includes("pct") || name.includes("percent")) return formatPercent(n, 2);
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return formatDecimal(n, Math.abs(n) < 1 ? 4 : 2);
}

function driftContributorSignal(row = {}) {
  const metric = String(row.metric || "");
  const metricValue = row.metric_value;
  const details = row.details && typeof row.details === "object" ? row.details : {};
  if (metric) return `${metric}: ${driftFormatValue(metricValue, metric)}`;
  if (details.drift_score !== undefined) return `drift_score: ${driftFormatValue(details.drift_score)}`;
  return String(row.severity || "INFO");
}

function driftAffectedSummary(affected = {}) {
  const symbols = Array.isArray(affected.symbols) ? affected.symbols : [];
  const models = Array.isArray(affected.models) ? affected.models : [];
  const regimes = Array.isArray(affected.regimes) ? affected.regimes : [];

  const fmtList = (rows, key) => rows
    .slice(0, 8)
    .map((row) => String(row[key] || ""))
    .filter(Boolean)
    .join(", ");

  return [
    {
      label: "Symbols",
      value: symbols.length ? fmtList(symbols, "symbol") : "unavailable",
    },
    {
      label: "Models",
      value: models.length ? fmtList(models, "model") : "unavailable",
    },
    {
      label: "Regimes",
      value: regimes.length ? fmtList(regimes, "regime") : "unavailable",
    },
  ];
}

export function buildDriftExplainerViewModel(payload = {}) {
  const root = payload && typeof payload === "object" ? payload : {};
  const status = root.status && typeof root.status === "object" ? root.status : {};
  const contributors = Array.isArray(root.contributors) ? root.contributors : [];
  const unavailable = Array.isArray(root.unavailable) ? root.unavailable : [];
  const links = Array.isArray(root.related_links) ? root.related_links : [];
  const latestTs = Number(status.latest_ts_ms || root.ts_ms || 0);
  const age = Number.isFinite(latestTs) && latestTs > 0 ? ageMsFromTimestamp(latestTs) : null;

  const rows = contributors.map((row) => {
    const metric = String(row.metric || "");
    const delta = row.delta_pct !== null && row.delta_pct !== undefined
      ? driftFormatValue(row.delta_pct, "pct")
      : driftFormatValue(row.delta_value, metric);
    return {
      severity: String(row.severity || "INFO").toUpperCase(),
      severityClass: driftSeverityClass(row.severity),
      scope: String(row.kind || "dimension"),
      dimension: String(row.dimension || row.label || "unavailable"),
      current: driftFormatValue(row.current_value),
      baseline: driftFormatValue(row.baseline_value),
      delta,
      signal: driftContributorSignal(row),
      source: String(row.source || "unknown"),
      stale: !!row.stale,
      tsText: row.ts_ms ? fmtTime(row.ts_ms) : "unavailable",
    };
  });

  const notes = unavailable.map((item) => ({
    field: String(item && item.field || "unavailable"),
    reason: String(item && item.reason || "No attribution data returned."),
  }));

  if (!rows.length && !notes.length) {
    notes.push({
      field: "contributors",
      reason: "No contributor rows were returned by the drift explainer endpoint.",
    });
  }

  return {
    ok: root.ok !== false,
    state: String(status.state || "unavailable"),
    severity: String(status.severity || "UNKNOWN").toUpperCase(),
    active: !!status.active,
    stale: !!status.stale,
    pillClass: driftSeverityClass(status.severity),
    metaText: driftStatusMeta(status),
    updatedText: latestTs > 0
      ? `${fmtTime(latestTs)} (${formatAgeMs(age)} old)`
      : "updated unavailable",
    summaryText: String(status.reason || "No drift explanation status returned."),
    rows,
    affected: driftAffectedSummary(root.affected || {}),
    notes,
    links,
  };
}

function renderDriftExplainer(payload = {}, error = null) {
  const meta = document.getElementById("driftExplainerMeta");
  const statusEl = document.getElementById("driftExplainerStatus");
  const updatedEl = document.getElementById("driftExplainerUpdated");
  const summaryEl = document.getElementById("driftExplainerSummary");
  const body = document.getElementById("driftExplainerRows");
  const affectedEl = document.getElementById("driftExplainerAffected");
  const unavailableEl = document.getElementById("driftExplainerUnavailable");
  const linksEl = document.getElementById("driftExplainerLinks");
  if (!meta || !statusEl || !updatedEl || !summaryEl || !body) return;

  const vm = error
    ? buildDriftExplainerViewModel({
        ok: false,
        status: {
          state: "unavailable",
          severity: "UNKNOWN",
          reason: `Drift explainer endpoint failed: ${error.message || String(error)}`,
        },
        contributors: [],
        unavailable: [{ field: "endpoint", reason: error.message || String(error) }],
      })
    : buildDriftExplainerViewModel(payload);

  meta.textContent = vm.metaText;
  meta.className = vm.pillClass;
  statusEl.textContent = vm.state;
  statusEl.className = vm.pillClass;
  updatedEl.textContent = vm.updatedText;
  updatedEl.className = vm.stale ? "pill warn mono" : "pill dim mono";
  summaryEl.textContent = vm.summaryText;

  if (!vm.rows.length) {
    body.innerHTML = `
      <tr class="table-row">
        <td colspan="7" class="metric-meta">No contributor attribution available.</td>
      </tr>
    `;
  } else {
    body.innerHTML = vm.rows.map((row) => `
      <tr class="table-row">
        <td><span class="${escapeHTML(row.severityClass)}">${escapeHTML(row.severity)}</span></td>
        <td>${escapeHTML(row.scope)}</td>
        <td>${escapeHTML(row.dimension)}${row.stale ? ' <span class="metric-meta">(stale)</span>' : ""}</td>
        <td class="mono table-cell-num">${escapeHTML(row.current)}</td>
        <td class="mono table-cell-num">${escapeHTML(row.baseline)}</td>
        <td class="mono table-cell-num">${escapeHTML(row.delta)}</td>
        <td><code>${escapeHTML(row.signal)}</code><div class="metric-meta">${escapeHTML(row.source)} ${escapeHTML(row.tsText)}</div></td>
      </tr>
    `).join("");
  }

  if (affectedEl) {
    affectedEl.innerHTML = vm.affected.map((item) => `
      <div class="opsNote"><strong>${escapeHTML(item.label)}:</strong> ${escapeHTML(item.value)}</div>
    `).join("");
  }

  if (unavailableEl) {
    unavailableEl.innerHTML = vm.notes.length
      ? vm.notes.map((item) => `
          <div class="opsNote"><strong>${escapeHTML(item.field)}:</strong> ${escapeHTML(item.reason)}</div>
        `).join("")
      : '<div class="opsNote">All requested attribution fields returned data.</div>';
  }

  if (linksEl) {
    linksEl.innerHTML = vm.links.map((link) => `
      <button class="btn btnSmall" type="button" data-drift-screen="${escapeHTML(link.screen || "explain")}" data-drift-panel="${escapeHTML(link.panel_id || "")}">
        ${escapeHTML(link.label || "Open panel")}
      </button>
    `).join("");
    linksEl.querySelectorAll("[data-drift-panel]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const screen = btn.getAttribute("data-drift-screen") || "explain";
        const panelId = btn.getAttribute("data-drift-panel") || "";
        if (window.__DASHBOARD_COMMANDS__ && typeof window.__DASHBOARD_COMMANDS__.navigateToPanel === "function") {
          window.__DASHBOARD_COMMANDS__.navigateToPanel(screen, panelId);
        } else if (panelId) {
          const target = document.getElementById(panelId);
          if (target && target.scrollIntoView) target.scrollIntoView({ behavior: "smooth", block: "start" });
        }
      });
    });
  }
}

export async function loadDriftExplainer(fetchJSON) {
  const panel = document.getElementById("driftExplainerPanel");
  if (!panel || panel.style.display === "none") return;

  try {
    const payload = await fetchJSON("/api/drift/explainer?top_n=8");
    renderDriftExplainer(payload);
  } catch (e) {
    renderDriftExplainer({}, e);
  }
}

export async function loadEquityReconciliation(fetchJSON) {

  const badge = document.getElementById("eqReconBadge");
  const detail = document.getElementById("eqReconDetail");

  if (!badge) return;

  try {

    const j = await fetchJSON("/api/reconcile/broker_backtest");

    if (!j || !j.ok) {

      badge.textContent = "n/a";
      badge.className = "pill neutral dim status-neutral";

      if (detail) detail.textContent = "";

      return;
    }

    const level = String(j.equity_diff_level || "UNKNOWN").toUpperCase();

    if (j.resolved) {

      badge.textContent = "RESOLVED";
      badge.className = "pill resolved";

    } else if (j.acked) {

      badge.textContent = "ACKED";
      badge.className = "pill acked";

    } else if (level === "CRIT") {

      badge.textContent = "CRIT";
      badge.className = "pill crit";

    } else if (level === "WARN") {

      badge.textContent = "WARN";
      badge.className = "pill warn";

    } else {

      badge.textContent = "OK";
      badge.className = "pill ok";
    }

    if (detail) {

      let msg = j.reason || "";

      if (Number.isFinite(j.diff_equity) && Number.isFinite(j.diff_equity_pct)) {

        msg += ` (Δ=${j.diff_equity.toFixed(2)}, ${(j.diff_equity_pct * 100).toFixed(2)}%)`;
      }

      detail.textContent = msg;
      applyInlineMetricAnnotation(
        detail,
        "equity_diff_level",
        level,
        { prefix: msg ? " · " : "" }
      );
    }

  } catch (e) {

    badge.textContent = "error";
    badge.className = "pill crit bad";

    if (detail) detail.textContent = e.message || String(e);
  }
}
