"use strict";

/*
  ui/bullet_bars.js

  Accessible bullet bars for operator risk headroom. The module is DOM-light:
  view-model functions are pure, and renderBulletBars only writes into a caller
  supplied mount element.
*/

const DEFAULT_BANDS = Object.freeze([
  Object.freeze({ key: "ok", label: "OK", start: 0.0, end: 0.75 }),
  Object.freeze({ key: "watch", label: "Watch", start: 0.75, end: 1.0 }),
  Object.freeze({ key: "over", label: "Over", start: 1.0, end: 1.25 }),
]);
const TRACK_MAX_RATIO = 1.25;

export const DEFAULT_RISK_CAPS = Object.freeze({
  gross: 1.0,
  net: 0.6,
  drawdown: 0.06,
  vol: 0.02,
});

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function numOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function pickNumber(...values) {
  for (const value of values) {
    const n = numOrNull(value);
    if (n != null) return n;
  }
  return null;
}

function escapeHTML(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function clamp(value, lo, hi) {
  const n = Number(value);
  if (!Number.isFinite(n)) return lo;
  return Math.max(lo, Math.min(hi, n));
}

function formatPercent(value, digits = 1) {
  const n = numOrNull(value);
  if (n == null) return "unavailable";
  return `${(n * 100).toFixed(digits)}%`;
}

function formatSignedPercent(value, digits = 1) {
  const n = numOrNull(value);
  if (n == null) return "unavailable";
  const pct = n * 100;
  if (pct > 0) return `+${pct.toFixed(digits)}%`;
  return `${pct.toFixed(digits)}%`;
}

function latestRiskRow(portfolioRisk) {
  const history = asArray(asObject(portfolioRisk).history);
  return asObject(history[0]);
}

function riskSummary(portfolioRisk) {
  return asObject(asObject(portfolioRisk).summary);
}

function riskInfo(portfolioRisk) {
  return asObject(asObject(portfolioRisk).info);
}

function riskCapsFromPortfolio(portfolioRisk) {
  const info = riskInfo(portfolioRisk);
  const summary = riskSummary(portfolioRisk);
  const caps = asObject(asObject(portfolioRisk).caps);
  const volTarget = asObject(asObject(portfolioRisk).vol_target);

  return {
    gross: pickNumber(caps.gross, caps.max_gross, info.cap_max_gross, DEFAULT_RISK_CAPS.gross),
    net: pickNumber(caps.net, caps.max_net, info.cap_max_net, DEFAULT_RISK_CAPS.net),
    drawdown: pickNumber(
      caps.drawdown,
      caps.drawdown_throttle_start,
      info.dd_throttle_start,
      summary.dd_throttle_start,
      DEFAULT_RISK_CAPS.drawdown
    ),
    vol: pickNumber(
      caps.vol,
      caps.vol_target,
      volTarget.target_vol,
      info.portfolio_vol_effective_target,
      info.portfolio_vol_target,
      summary.portfolio_vol_target,
      DEFAULT_RISK_CAPS.vol
    ),
    volHardBlock: pickNumber(caps.vol_hard_block, info.portfolio_vol_hard_block),
    drawdownHardBlock: pickNumber(caps.drawdown_hard_block, info.dd_hard_block),
  };
}

function riskCapsFromCanonicalRisk(risk) {
  const item = asObject(risk);
  const caps = asObject(item.caps);
  const volTarget = asObject(item.vol_target);
  return {
    gross: pickNumber(caps.gross, caps.max_gross),
    net: pickNumber(caps.net, caps.max_net),
    drawdown: pickNumber(caps.drawdown, caps.drawdown_throttle_start),
    vol: pickNumber(caps.vol, caps.vol_target, volTarget.target_vol),
    volHardBlock: pickNumber(caps.vol_hard_block),
    drawdownHardBlock: pickNumber(caps.drawdown_hard_block),
  };
}

export function buildBulletBarViewModel(config = {}) {
  const label = String(config.label || "Risk").trim() || "Risk";
  const value = numOrNull(config.value);
  const cap = numOrNull(config.cap);
  const useAbs = config.useAbs !== false;
  const displayValue = value == null ? null : (useAbs ? Math.abs(value) : value);
  const usableCap = cap != null && cap > 0 ? cap : null;
  const ratio = displayValue != null && usableCap != null ? displayValue / usableCap : null;
  const blocked = !!config.blocked;
  const missing = value == null || usableCap == null;

  let tone = "unavailable";
  let statusWord = "Unavailable";
  if (!missing) {
    if (blocked) {
      tone = "blocked";
      statusWord = "Blocked";
    } else if (ratio > 1.0) {
      tone = "over";
      statusWord = "Over cap";
    } else if (ratio >= 0.85) {
      tone = "watch";
      statusWord = "Watch";
    } else {
      tone = "ok";
      statusWord = "OK";
    }
  }

  const valueFormatter = typeof config.valueFormatter === "function" ? config.valueFormatter : formatPercent;
  const capFormatter = typeof config.capFormatter === "function" ? config.capFormatter : valueFormatter;
  const valueText = value == null ? "unavailable" : valueFormatter(value);
  const capText = usableCap == null ? "unavailable" : capFormatter(usableCap);
  const fallbackText = missing
    ? `${label}: data unavailable.`
    : `${label}: ${valueText} against cap ${capText}; ${statusWord}.`;

  return {
    id: String(config.id || label.toLowerCase().replace(/[^a-z0-9]+/g, "-")).replace(/^-|-$/g, ""),
    label,
    value,
    displayValue,
    cap: usableCap,
    ratio,
    valueText,
    capText,
    statusWord,
    tone,
    blocked,
    missing,
    source: String(config.source || ""),
    fallbackText: String(config.fallbackText || fallbackText),
    fillPct: ratio == null ? 0 : clamp((ratio / TRACK_MAX_RATIO) * 100, 0, 100),
    capPct: (1 / TRACK_MAX_RATIO) * 100,
    bands: asArray(config.bands).length ? asArray(config.bands) : DEFAULT_BANDS,
  };
}

export function buildRiskHeadroomViewModel({
  uiMetrics = null,
  portfolioRisk = null,
  riskSummary: legacyRiskSummary = null,
  blocked = null,
  source = "",
} = {}) {
  const metrics = asObject(uiMetrics);
  const canonicalExposure = asObject(metrics.exposure);
  const canonicalRisk = asObject(metrics.risk);
  const riskPortfolio = asObject(portfolioRisk);
  const legacy = asObject(legacyRiskSummary);
  const latest = latestRiskRow(riskPortfolio);
  const summary = riskSummary(riskPortfolio);
  const info = riskInfo(riskPortfolio);
  const portfolioCaps = riskCapsFromPortfolio(riskPortfolio);
  const canonicalCaps = riskCapsFromCanonicalRisk(canonicalRisk);
  const caps = {
    gross: pickNumber(canonicalCaps.gross, portfolioCaps.gross, DEFAULT_RISK_CAPS.gross),
    net: pickNumber(canonicalCaps.net, portfolioCaps.net, DEFAULT_RISK_CAPS.net),
    drawdown: pickNumber(canonicalCaps.drawdown, portfolioCaps.drawdown, DEFAULT_RISK_CAPS.drawdown),
    vol: pickNumber(canonicalCaps.vol, portfolioCaps.vol, DEFAULT_RISK_CAPS.vol),
    volHardBlock: pickNumber(canonicalCaps.volHardBlock, portfolioCaps.volHardBlock),
    drawdownHardBlock: pickNumber(canonicalCaps.drawdownHardBlock, portfolioCaps.drawdownHardBlock),
  };

  const useCanonical = !!metrics.ok;
  const gross = useCanonical
    ? pickNumber(canonicalExposure.gross, canonicalExposure.gross_exposure)
    : pickNumber(legacy.gross_exposure, latest.gross, summary.gross, summary.final_gross, info.final_gross);
  const net = useCanonical
    ? pickNumber(canonicalExposure.net, canonicalExposure.net_exposure)
    : pickNumber(legacy.net_exposure, latest.net, summary.net, summary.final_net, info.final_net);
  const drawdown = useCanonical
    ? pickNumber(canonicalRisk.max_drawdown_pct, canonicalRisk.drawdown)
    : pickNumber(legacy.max_drawdown_pct, latest.drawdown, summary.drawdown, info.drawdown);
  const vol = pickNumber(canonicalRisk.vol_proxy, latest.vol_proxy, summary.portfolio_vol_proxy, info.portfolio_vol_proxy);
  const effectiveBlocked = blocked == null
    ? !!(canonicalRisk.blocked === true || riskPortfolio.blocked === true)
    : !!blocked;
  const sourceLabel = source || (useCanonical ? "/api/ui/metrics" : "/api/risk/portfolio");

  const bars = [
    buildBulletBarViewModel({
      id: "gross-exposure",
      label: "Gross Exposure",
      value: gross,
      cap: caps.gross,
      blocked: effectiveBlocked,
      source: sourceLabel,
      valueFormatter: formatPercent,
    }),
    buildBulletBarViewModel({
      id: "net-exposure",
      label: "Net Exposure",
      value: net,
      cap: caps.net,
      blocked: effectiveBlocked,
      source: sourceLabel,
      valueFormatter: formatSignedPercent,
      capFormatter: formatPercent,
    }),
    buildBulletBarViewModel({
      id: "vol-proxy",
      label: "Vol Proxy / Target",
      value: vol,
      cap: caps.vol,
      blocked: effectiveBlocked,
      source: "/api/risk/portfolio",
      valueFormatter: formatPercent,
    }),
    buildBulletBarViewModel({
      id: "drawdown",
      label: "Drawdown",
      value: drawdown,
      cap: caps.drawdown,
      blocked: effectiveBlocked,
      source: sourceLabel,
      valueFormatter: formatPercent,
    }),
  ];

  return {
    ok: bars.some((bar) => !bar.missing),
    blocked: effectiveBlocked,
    caps,
    bars,
    fallbackText: bars.map((bar) => bar.fallbackText).join(" "),
  };
}

export function renderBulletBars(mount, model) {
  if (!mount) return;
  const bars = asArray(asObject(model).bars);
  if (!bars.length) {
    mount.innerHTML = '<div class="bulletBarsFallback">Risk headroom unavailable.</div>';
    return;
  }

  mount.innerHTML = `
    <div class="bulletBars" role="list" aria-label="Risk headroom against configured caps">
      ${bars.map(renderBulletBar).join("")}
    </div>
  `;
}

function renderBulletBar(bar) {
  const item = asObject(bar);
  const bands = asArray(item.bands).length ? asArray(item.bands) : DEFAULT_BANDS;
  const tone = String(item.tone || "unavailable");
  const label = escapeHTML(item.label || "Risk");
  const status = escapeHTML(item.statusWord || "Unavailable");
  const fallback = escapeHTML(item.fallbackText || `${label}: unavailable.`);
  const valueText = escapeHTML(item.valueText || "unavailable");
  const capText = escapeHTML(item.capText || "unavailable");
  const fillPct = clamp(item.fillPct, 0, 125).toFixed(2);
  const capPct = clamp(item.capPct ?? 100, 0, 125).toFixed(2);

  return `
    <div class="bulletBar bulletBar-${escapeHTML(tone)}" role="listitem" aria-label="${fallback}">
      <div class="bulletBarHeader">
        <span class="bulletBarLabel">${label}</span>
        <span class="bulletBarStatus">${status}</span>
      </div>
      <div class="bulletBarTrack" aria-hidden="true">
        <div class="bulletBarBands">
          ${bands.map(renderBand).join("")}
        </div>
        <div class="bulletBarFill" style="width:${fillPct}%;"></div>
        <div class="bulletBarCap" style="left:${capPct}%;"></div>
      </div>
      <div class="bulletBarMeta">
        <span>${valueText}</span>
        <span>cap ${capText}</span>
      </div>
      <div class="sr-only">${fallback}</div>
    </div>
  `;
}

function renderBand(band) {
  const item = asObject(band);
  const start = clamp(item.start, 0, TRACK_MAX_RATIO);
  const end = clamp(item.end, start, TRACK_MAX_RATIO);
  const left = (start / TRACK_MAX_RATIO) * 100;
  const width = ((end - start) / TRACK_MAX_RATIO) * 100;
  return `<span class="bulletBarBand bulletBarBand-${escapeHTML(item.key || "band")}" title="${escapeHTML(item.label || "")}" style="left:${left.toFixed(2)}%; width:${width.toFixed(2)}%;"></span>`;
}
