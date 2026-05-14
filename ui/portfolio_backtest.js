/*
  FILE: ui/portfolio_backtest.js

  Latest portfolio-backtest panel loader. This module fetches the newest
  backtest run, renders summary metrics, and draws equity/drawdown charts for
  dashboard inspection.
*/

import { renderLineChart } from "./charts.js";
import { _fmtPct } from "./utils.js";

let _lastPortfolioBacktestSummary = null;

export function getLastPortfolioBacktestSummary() {
  return _lastPortfolioBacktestSummary;
}

function _fmtMoney(x) {
  const v = Number(x || 0);
  const s = Math.abs(v) >= 1000 ? v.toFixed(0) : v.toFixed(2);
  return (v < 0 ? "-" : "") + "$" + String(s).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export async function loadPortfolioBacktestLatest(fetchJSON) {

  const meta = document.getElementById("portfolioBtMeta");
  const sumBody = document.getElementById("portfolioBtSummaryBody");
  const cEq = document.getElementById("portfolioEquityCanvas");
  const cDd = document.getElementById("portfolioDdCanvas");

  if (!meta || !sumBody || !cEq || !cDd) return;

  try {

    const res = await fetchJSON("/api/portfolio/backtest/latest");

    if (!res || !res.ok || !res.run) {
      _lastPortfolioBacktestSummary = null;

      meta.textContent = "no runs";
      meta.className = "pill dim";

      sumBody.innerHTML = "";

      renderLineChart(cEq, []);
      renderLineChart(cDd, []);

      return;
    }

    meta.textContent = "ok";
    meta.className = "pill ok";

    const run = res.run || {};
    const metrics = run.metrics || {};
    const pts = Array.isArray(run.points) ? run.points : [];

    const equity = [];

    for (const p of pts) {
      const e = Number(p && p.equity);
      if (!Number.isFinite(e)) continue;
      equity.push(e);
    }

    const dd = [];
    let maxDd = 0;

    for (const p of pts) {

      const d = Number(p && p.drawdown);

      if (!Number.isFinite(d)) continue;

      dd.push(d);

      if (d < maxDd) maxDd = d;
    }

    renderLineChart(cEq, equity, {
      topLabel: "equity",
      fmtY: (v) => Number(v).toFixed(3),
      stroke: "#2ea043",
    });

    renderLineChart(cDd, dd, {
      topLabel: "drawdown",
      fmtY: (v) => _fmtPct(v),
      stroke: "#ff6b6b",
      yMax: 0,
    });

    const startTs = Number(run.start_ts_ms);
    const endTs = Number(run.end_ts_ms);

    const windowStr =
      (Number.isFinite(startTs) && Number.isFinite(endTs))
        ? `${new Date(startTs).toLocaleDateString()} → ${new Date(endTs).toLocaleDateString()}`
        : "—";

    const totalReturn =
      (equity.length >= 2)
        ? ((equity[equity.length - 1] / equity[0]) - 1.0)
        : (Number.isFinite(metrics.total_return) ? Number(metrics.total_return) : NaN);

    const sharpe = Number.isFinite(metrics.sharpe_simple) ? Number(metrics.sharpe_simple) : NaN;
    const sortino = Number.isFinite(metrics.sortino_simple) ? Number(metrics.sortino_simple) : NaN;
    const calmar = Number.isFinite(metrics.calmar_simple) ? Number(metrics.calmar_simple) : NaN;
    const turnover = Number.isFinite(metrics.turnover_avg) ? Number(metrics.turnover_avg) : NaN;

    const trades =
      Number.isFinite(metrics.steps_used) ? Number(metrics.steps_used) : NaN;

    _lastPortfolioBacktestSummary = {
      totalReturn: Number.isFinite(totalReturn) ? totalReturn : null,
      maxDrawdown: Number.isFinite(maxDd) ? maxDd : null,
      sharpe: Number.isFinite(sharpe) ? sharpe : null,
      sortino: Number.isFinite(sortino) ? sortino : null,
      calmar: Number.isFinite(calmar) ? calmar : null,
      latestEquity: equity.length ? equity[equity.length - 1] : null,
    };

    sumBody.innerHTML = "";

    sumBody.insertAdjacentHTML("beforeend", `
      <tr>
        <td class="small">${windowStr}</td>
        <td class="mono">${Number.isFinite(totalReturn) ? _fmtPct(totalReturn) : "?"}</td>
        <td class="mono">${_fmtPct(maxDd)}</td>
        <td class="mono">
          S=${Number.isFinite(sharpe) ? sharpe.toFixed(2) : "?"}
          &nbsp;So=${Number.isFinite(sortino) ? sortino.toFixed(2) : "?"}
          &nbsp;C=${Number.isFinite(calmar) ? calmar.toFixed(2) : "?"}
        </td>
        <td class="mono">
          n=${Number.isFinite(trades) ? trades : "?"}
          &nbsp;τ=${Number.isFinite(turnover) ? turnover.toFixed(3) : "?"}
        </td>
      </tr>
    `);

  } catch (e) {
    _lastPortfolioBacktestSummary = null;

    meta.textContent = "error";
    meta.className = "pill bad";

    sumBody.innerHTML = "";

    renderLineChart(cEq, []);
    renderLineChart(cDd, []);
  }
}
