/*
  FILE: ui/market_stress.js

  Market-stress panel loader for the dashboard. It fetches the normalized
  stress snapshot and renders a compact badge, timestamp, and structured detail
  view for operator monitoring.
*/

import { applyInlineMetricAnnotation, setMetricValueAttribute } from "./tooltip.js";

function resetMarketStressUI({
  badge,
  updated,
  body,
  raw,
  message = "(market stress unavailable)",
} = {}) {
  if (badge) {
    badge.className = "pill dim";
    badge.textContent = "—";
    setMetricValueAttribute(badge, null);
  }

  const hdr = document.getElementById("marketStressHeader");
  if (hdr) {
    hdr.className = "pill dim";
    hdr.textContent = "Stress: —";
  }

  if (updated) {
    updated.textContent = "—";
    setMetricValueAttribute(updated, null);
  }
  if (body) {
    body.innerHTML = `
      <tr class="table-row">
        <td colspan="3" class="metric-meta">(market stress unavailable)</td>
      </tr>
    `;
  }
  if (raw) raw.textContent = message;
  try {
    const setPanelState = typeof window !== "undefined" ? window.__setDashboardPanelState__ : null;
    if (typeof setPanelState === "function") {
      setPanelState("marketStressPanel", {
        state: "error",
        reason: String(message || "(market stress unavailable)"),
      });
    }
  } catch {}

  const stripStress = document.getElementById("tStress");
  if (stripStress) {
    stripStress.textContent = "Market Stress —";
    setMetricValueAttribute(stripStress, null);
  }
}

export async function loadMarketStress(fetchJSON) {
  const badge = document.getElementById("marketStressBadge");
  const updated = document.getElementById("marketStressUpdated");
  const body = document.getElementById("marketStressBody");
  const raw = document.getElementById("marketStressRaw");

  if (!badge || !updated || !body || !raw) return;

  try {
    const j = await fetchJSON("/api/market_stress");
    if (!j || !j.ok || !j.stress) {
      window.__LAST_MARKET_STRESS__ = null;
      resetMarketStressUI({
        badge,
        updated,
        body,
        raw,
        message: j && j.error ? String(j.error) : "(market stress unavailable)",
      });
      return;
    }

    window.__LAST_MARKET_STRESS__ = j;

    const s = j.stress || {};
    const score = Number(s.stress_score ?? 0);
    const ts_ms = Number(s.ts_ms ?? 0);

    let cls = "pill ok";
    if (score >= 0.75) cls = "pill bad";
    else if (score >= 0.55) cls = "pill warn";

    badge.className = cls;
    badge.textContent = Number.isFinite(score) ? score.toFixed(3) : "—";
    setMetricValueAttribute(badge, Number.isFinite(score) ? score : null);
    applyInlineMetricAnnotation(
      badge,
      "market_stress_score",
      Number.isFinite(score) ? score : null
    );

    const hdr = document.getElementById("marketStressHeader");
    if (hdr) {
      hdr.className = cls;
      hdr.textContent = Number.isFinite(score)
        ? `Stress: ${score.toFixed(2)}`
        : "Stress: —";
    }

    if (Number.isFinite(ts_ms) && ts_ms > 0) {
      updated.textContent = new Date(ts_ms).toLocaleString();
      setMetricValueAttribute(updated, ts_ms);
    } else {
      updated.textContent = "—";
      setMetricValueAttribute(updated, null);
    }

    const stripStress = document.getElementById("tStress");
    if (stripStress) {
      stripStress.textContent = Number.isFinite(score)
        ? `Market Stress ${score.toFixed(2)}`
        : "Market Stress —";
      setMetricValueAttribute(stripStress, Number.isFinite(score) ? score : null);
      applyInlineMetricAnnotation(
        stripStress,
        "market_stress_score",
        Number.isFinite(score) ? score : null
      );
    }

    const rows = [
      { label: "VIX", metricKey: "vix", value: s.vix, zMetricKey: "z_vix", zValue: s.z_vix },
      { label: "VVIX", metricKey: "vvix", value: s.vvix, zMetricKey: "z_vvix", zValue: s.z_vvix },
      { label: "MOVE", metricKey: "move", value: s.move, zMetricKey: "z_move", zValue: s.z_move },
      { label: "VIX1D/VIX", metricKey: "vix1d_over_vix", value: s.vix1d_over_vix, zMetricKey: null, zValue: null },
      { label: "VIX9D/VIX", metricKey: "vix9d_over_vix", value: s.vix9d_over_vix, zMetricKey: null, zValue: null },
      { label: "VIX3M/VIX", metricKey: "vix3m_over_vix", value: s.vix3m_over_vix, zMetricKey: null, zValue: null },
      { label: "Term Z", metricKey: "z_term", value: null, zMetricKey: "z_term", zValue: s.z_term },
      { label: "Credit LQD/HYG", metricKey: "credit_lqd_over_hyg", value: s.credit_lqd_over_hyg, zMetricKey: "z_credit", zValue: s.z_credit },
      { label: "Rates TLT/SHY", metricKey: "rates_tlt_over_shy", value: s.rates_tlt_over_shy, zMetricKey: "z_rates", zValue: s.z_rates },
    ];

    body.innerHTML = "";

    for (const row of rows) {
      const tr = document.createElement("tr");
      const nameTd = document.createElement("td");
      const valueTd = document.createElement("td");
      const zTd = document.createElement("td");

      const v =
        row.value === null || row.value === undefined || !Number.isFinite(Number(row.value))
          ? "—"
          : Number(row.value).toFixed(4);

      const zz =
        row.zValue === null || row.zValue === undefined || !Number.isFinite(Number(row.zValue))
          ? "—"
          : Number(row.zValue).toFixed(3);

      nameTd.textContent = row.label;
      if (row.metricKey) {
        nameTd.setAttribute("data-metric", row.metricKey);
      }

      valueTd.className = "mono";
      valueTd.textContent = v;
      if (row.metricKey) {
        valueTd.setAttribute("data-metric", row.metricKey);
        setMetricValueAttribute(
          valueTd,
          Number.isFinite(Number(row.value)) ? Number(row.value) : null
        );
      }

      zTd.className = "mono";
      zTd.textContent = zz;
      if (row.zMetricKey) {
        zTd.setAttribute("data-metric", row.zMetricKey);
        setMetricValueAttribute(
          zTd,
          Number.isFinite(Number(row.zValue)) ? Number(row.zValue) : null
        );
        applyInlineMetricAnnotation(
          zTd,
          row.zMetricKey,
          Number.isFinite(Number(row.zValue)) ? Number(row.zValue) : null
        );
      }

      tr.appendChild(nameTd);
      tr.appendChild(valueTd);
      tr.appendChild(zTd);

      body.appendChild(tr);
    }

    raw.textContent = JSON.stringify(s, null, 2);
    try {
      const setPanelState = typeof window !== "undefined" ? window.__setDashboardPanelState__ : null;
      if (typeof setPanelState === "function") {
        const ageMs = Number.isFinite(ts_ms) && ts_ms > 0 ? Math.max(0, Date.now() - ts_ms) : null;
        setPanelState("marketStressPanel", {
          state: ageMs != null && ageMs >= 300_000 ? "stale" : "fresh",
          reason: Number.isFinite(score)
            ? `Stress score ${score.toFixed(3)} ${ageMs == null ? "without a timestamp" : `• backend ${Math.round(ageMs / 1000)}s old`}`
            : "Market stress snapshot is available without a usable score.",
        });
      }
    } catch {}

  } catch (e) {
    window.__LAST_MARKET_STRESS__ = null;
    resetMarketStressUI({
      badge,
      updated,
      body,
      raw,
      message: e && e.message ? e.message : String(e),
    });
  }
}

export async function loadMarketStressHistory(fetchJSON) {

  const canvas = document.getElementById("marketStressSparkline");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  try {
    const j = await fetchJSON("/api/market_stress_history");
    const w = canvas.width;
    const h = canvas.height;

    ctx.clearRect(0, 0, w, h);
    if (!j || !j.ok || !Array.isArray(j.series)) return;

    const ys = j.series.map(p => Number(p.stress_score || 0));
    if (ys.length < 2) return;

    const min = 0;
    const max = 1;

    ctx.beginPath();
    ctx.strokeStyle = "#aaa";

    ys.forEach((v, i) => {
      const x = (i / (ys.length - 1)) * w;
      const y = h - ((v - min) / (max - min)) * h;

      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });

    ctx.stroke();

  } catch {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
  }
}
