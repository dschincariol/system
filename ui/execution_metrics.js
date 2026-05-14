/*
  FILE: ui/execution_metrics.js

  Execution-metrics panel loader for the dashboard. This module fetches
  confidence-bucket execution analytics, renders the table, and emits degraded
  execution alerts into the shared toast/alert workflow.
*/

import { detectExecutionDegradation, buildExecutionAlert } from "./execution_degradation.js";
import { setMetricValueAttribute } from "./tooltip.js";

function normalizeConfidenceRows(payload) {
  const sourceRows = Array.isArray(payload && payload.rows)
    ? payload.rows
    : (Array.isArray(payload && payload.buckets) ? payload.buckets : []);

  return sourceRows.map((row) => {
    const avgCost = Number(row && (row.mean_cost ?? row.avg_cost));
    return {
      symbol: String(row && row.symbol || "").trim().toUpperCase(),
      conf_lo: Number(row && row.conf_lo),
      conf_hi: Number(row && row.conf_hi),
      n: Number(row && (row.n ?? row.n_fills ?? 0)),
      mean_cost: Number.isFinite(avgCost) ? avgCost : null,
    };
  });
}

export async function loadExecutionByConfidence(fetchJSON, toast, OPERATOR_MODE) {

  const body = document.getElementById("execByConfBody");
  if (!body) return;

  try {
    const j = await fetchJSON("/api/execution/metrics/by_confidence");
    const rows = normalizeConfidenceRows(j);

    body.innerHTML = "";

    const degradationRows = rows.filter((row) =>
      row.symbol && row.mean_cost != null
    );
    const execAlerts = detectExecutionDegradation(degradationRows, toast);

    for (const a of execAlerts || []) {
      toast(
        `${a.level}: execution degraded for ${a.symbol} (+${(a.worsenPct * 100).toFixed(1)}%)`,
        a.level === "CRIT" ? "bad" : "warn",
        a.level === "CRIT" ? 7000 : 5000
      );

      buildExecutionAlert(a);
    }

    if (!rows.length) {
      body.innerHTML = `
        <tr class="table-row">
          <td colspan="4" class="metric-meta">(no execution data yet)</td>
        </tr>
      `;
      return;
    }

    const maxAbs = Math.max(
      ...rows.map(r => Math.abs(Number(r.mean_cost || 0))),
      1e-9
    );

    for (const r of rows) {

      const lo = Number.isFinite(Number(r.conf_lo)) ? Number(r.conf_lo).toFixed(2) : "—";
      const hi = Number.isFinite(Number(r.conf_hi)) ? Number(r.conf_hi).toFixed(2) : "—";
      const n  = Number(r.n || 0);
      const c  = r.mean_cost;

      const severityPct = c == null
        ? 0
        : Math.min(100, (Math.abs(c) / maxAbs) * 100);

      const tr = document.createElement("tr");
      const rangeTd = document.createElement("td");
      const countTd = document.createElement("td");
      const costTd = document.createElement("td");
      const severityTd = document.createElement("td");
      const severityColor = c != null && c > 0 ? "var(--color-crit)" : "var(--color-ok)";
      const costClass = " table-cell-num";
      tr.className = "table-row";

      rangeTd.className = "mono table-cell-num";
      rangeTd.textContent = `${lo}–${hi}`;

      countTd.className = "mono table-cell-num";
      countTd.textContent = String(n);
      countTd.setAttribute("data-metric", "execution_fill_count");
      setMetricValueAttribute(countTd, n);

      costTd.className = `mono${costClass}`;
      costTd.textContent = Number.isFinite(c)
        ? c.toFixed(6)
        : "—";
      costTd.setAttribute("data-metric", "execution_mean_cost");
      setMetricValueAttribute(costTd, Number.isFinite(c) ? c : null);

      severityTd.innerHTML = `
        <div class="severity-bar">
          <div
            class="severity-bar-fill"
            style="--severity-fill:${severityPct.toFixed(1)}%; --severity-color:${severityColor};"
          ></div>
        </div>
      `;

      tr.appendChild(rangeTd);
      tr.appendChild(countTd);
      tr.appendChild(costTd);
      tr.appendChild(severityTd);

      body.appendChild(tr);
    }

  } catch (e) {

    body.innerHTML = `
      <tr class="table-row">
        <td colspan="4" class="metric-meta">
          error loading execution metrics
        </td>
      </tr>
    `;
  }
}
