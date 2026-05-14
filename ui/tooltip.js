/*
  FILE: ui/tooltip.js

  Shared delegated metric tooltip support for dashboard surfaces that expose
  `data-metric` and optional `data-metric-value` attributes.
*/

import { getMetricDefinition, classifyMetricValue } from "./metric_glossary.js";

const TOOLTIP_ID = "metricTooltip";
const TOOLTIP_STYLE_ID = "metricTooltipStyles";

let _initialized = false;
let _root = null;
let _tooltipEl = null;
let _activeTarget = null;
let _observer = null;

function ensureTooltipStyles() {
  if (document.getElementById(TOOLTIP_STYLE_ID)) return;

  const style = document.createElement("style");
  style.id = TOOLTIP_STYLE_ID;
  style.textContent = `
    #${TOOLTIP_ID} {
      position: fixed;
      z-index: 2147483647;
      max-width: 320px;
      padding: 10px 12px;
      border: 1px solid rgba(240, 246, 252, 0.16);
      border-radius: 8px;
      background: rgba(13, 17, 23, 0.96);
      color: #f0f6fc;
      box-shadow: 0 12px 28px rgba(0, 0, 0, 0.38);
      pointer-events: none;
      font-size: 12px;
      line-height: 1.45;
      white-space: normal;
    }
    #${TOOLTIP_ID}[hidden] {
      display: none;
    }
    #${TOOLTIP_ID} .metricTooltipLabel {
      display: block;
      margin: 0 0 4px;
      font-weight: 700;
    }
    #${TOOLTIP_ID} .metricTooltipText + .metricTooltipText {
      margin-top: 4px;
    }
    .metricInlineAnnotation {
      margin-left: 4px;
      color: var(--muted);
      font-size: 0.9em;
      font-weight: 400;
      white-space: nowrap;
    }
  `;
  document.head.appendChild(style);
}

function ensureTooltipEl() {
  if (_tooltipEl && _tooltipEl.isConnected) return _tooltipEl;

  ensureTooltipStyles();

  const el = document.createElement("div");
  el.id = TOOLTIP_ID;
  el.setAttribute("role", "tooltip");
  el.hidden = true;
  document.body.appendChild(el);
  _tooltipEl = el;
  return el;
}

function findMetricTarget(node) {
  if (!node || !node.closest) return null;
  const hit = node.closest("[data-metric]");
  if (!hit) return null;
  if (_root && _root !== document && !_root.contains(hit)) return null;
  return hit;
}

function rangeText(def) {
  const parts = [];
  if (def && def.normalRange) parts.push(`Normal: ${def.normalRange}`);
  if (def && def.warningRange) parts.push(`Watch: ${def.warningRange}`);
  return parts.join(" | ");
}

function appendTooltipLine(parent, className, text) {
  if (!text) return;
  const line = document.createElement("div");
  line.className = className;
  line.textContent = text;
  parent.appendChild(line);
}

function getMetricValue(target) {
  if (!target || !target.hasAttribute("data-metric-value")) return null;
  const raw = target.getAttribute("data-metric-value");
  return raw === null || raw === "" ? null : raw;
}

function suppressNativeTitle(target) {
  if (!target || !target.hasAttribute("title")) return;
  if (!target.hasAttribute("data-tooltip-native-title")) {
    target.setAttribute("data-tooltip-native-title", target.getAttribute("title") || "");
  }
  target.removeAttribute("title");
}

function restoreNativeTitle(target) {
  if (!target || !target.hasAttribute("data-tooltip-native-title")) return;
  const title = target.getAttribute("data-tooltip-native-title");
  if (title) target.setAttribute("title", title);
  target.removeAttribute("data-tooltip-native-title");
}

function applyAriaDescription(target) {
  if (!target) return;
  if (!target.hasAttribute("data-tooltip-prev-describedby")) {
    target.setAttribute("data-tooltip-prev-describedby", target.getAttribute("aria-describedby") || "");
  }
  target.setAttribute("aria-describedby", TOOLTIP_ID);
}

function restoreAriaDescription(target) {
  if (!target || !target.hasAttribute("data-tooltip-prev-describedby")) return;
  const prev = target.getAttribute("data-tooltip-prev-describedby");
  if (prev) target.setAttribute("aria-describedby", prev);
  else target.removeAttribute("aria-describedby");
  target.removeAttribute("data-tooltip-prev-describedby");
}

function hideTooltip() {
  if (_activeTarget) {
    restoreNativeTitle(_activeTarget);
    restoreAriaDescription(_activeTarget);
  }
  _activeTarget = null;
  if (_tooltipEl) {
    _tooltipEl.hidden = true;
    _tooltipEl.replaceChildren();
  }
}

function buildTooltipContent(target) {
  const def = getMetricDefinition(target && target.getAttribute("data-metric"));
  if (!def) return false;

  const tooltip = ensureTooltipEl();
  tooltip.replaceChildren();

  appendTooltipLine(tooltip, "metricTooltipLabel", def.label);
  appendTooltipLine(tooltip, "metricTooltipText", def.shortHelp);

  const summary = rangeText(def);
  if (summary) appendTooltipLine(tooltip, "metricTooltipText", summary);

  const rawValue = getMetricValue(target);
  if (rawValue !== null) {
    const classification = classifyMetricValue(def.key, rawValue);
    if (classification && classification !== "unknown") {
      appendTooltipLine(
        tooltip,
        "metricTooltipText",
        `Status: ${classification.toUpperCase()}`
      );
    }
  }

  return tooltip.childNodes.length > 0;
}

function positionTooltip(target, event = null) {
  const tooltip = ensureTooltipEl();
  if (tooltip.hidden) return;

  let left = 0;
  let top = 0;

  if (event && Number.isFinite(event.clientX) && Number.isFinite(event.clientY)) {
    left = event.clientX + 14;
    top = event.clientY + 16;
  } else if (target && target.getBoundingClientRect) {
    const rect = target.getBoundingClientRect();
    left = rect.left + 8;
    top = rect.bottom + 10;
  }

  tooltip.style.left = "0px";
  tooltip.style.top = "0px";
  tooltip.hidden = false;
  tooltip.style.visibility = "hidden";

  const width = tooltip.offsetWidth || 320;
  const height = tooltip.offsetHeight || 80;
  const maxLeft = Math.max(8, window.innerWidth - width - 8);
  const maxTop = Math.max(8, window.innerHeight - height - 8);

  if (left > maxLeft) left = maxLeft;
  if (top > maxTop) {
    top = event && Number.isFinite(event.clientY)
      ? Math.max(8, event.clientY - height - 14)
      : Math.max(8, top - height - 24);
  }

  tooltip.style.left = `${Math.max(8, left)}px`;
  tooltip.style.top = `${Math.max(8, top)}px`;
  tooltip.style.visibility = "visible";
}

function showTooltip(target, event = null) {
  if (!target || !target.isConnected) {
    hideTooltip();
    return;
  }

  if (_activeTarget === target) {
    positionTooltip(target, event);
    return;
  }

  hideTooltip();

  if (!buildTooltipContent(target)) {
    hideTooltip();
    return;
  }

  _activeTarget = target;
  suppressNativeTitle(target);
  applyAriaDescription(target);
  positionTooltip(target, event);
}

function handleMouseOver(event) {
  const target = findMetricTarget(event.target);
  if (!target) return;
  showTooltip(target, event);
}

function handleMouseMove(event) {
  if (!_activeTarget) return;
  const target = findMetricTarget(event.target);
  if (target === _activeTarget) {
    positionTooltip(_activeTarget, event);
  }
}

function handleMouseOut(event) {
  if (!_activeTarget) return;

  const target = findMetricTarget(event.target);
  if (target !== _activeTarget) return;

  const related = event.relatedTarget;
  if (related && _activeTarget.contains(related)) return;

  hideTooltip();
}

function handleFocusIn(event) {
  const target = findMetricTarget(event.target);
  if (!target) return;
  showTooltip(target, null);
}

function handleFocusOut(event) {
  if (!_activeTarget) return;
  const related = event.relatedTarget;
  if (related && _activeTarget.contains(related)) return;
  hideTooltip();
}

function handleKeyDown(event) {
  if (event && event.key === "Escape") {
    hideTooltip();
  }
}

function handleViewportChange() {
  if (!_activeTarget) return;
  if (!_activeTarget.isConnected) {
    hideTooltip();
    return;
  }
  positionTooltip(_activeTarget, null);
}

function metricInlineAnnotationText(key, value) {
  const def = getMetricDefinition(key);
  if (!def) return "";
  const classification = classifyMetricValue(def.key, value);
  if (classification === "normal") return "normal";
  if (classification === "warning") return "elevated";
  if (classification === "critical") return "outside range";
  return "";
}

function getInlineAnnotationNode(el) {
  const last = el && el.lastElementChild;
  if (!last || !last.classList || !last.classList.contains("metricInlineAnnotation")) {
    return null;
  }
  return last;
}

export function setMetricValueAttribute(el, value) {
  if (!el || typeof el.setAttribute !== "function") return;
  const invalidNumber = typeof value === "number" && !Number.isFinite(value);
  if (value === undefined || value === null || value === "" || invalidNumber) {
    el.removeAttribute("data-metric-value");
    return;
  }
  el.setAttribute("data-metric-value", String(value));
}

export function applyInlineMetricAnnotation(el, key, value, { prefix = " · " } = {}) {
  if (!el || typeof el.appendChild !== "function") return;

  const label = metricInlineAnnotationText(key, value);
  const existing = getInlineAnnotationNode(el);

  if (!label) {
    if (existing && existing.parentNode) existing.parentNode.removeChild(existing);
    return;
  }

  ensureTooltipStyles();

  const note = existing || document.createElement("span");
  if (!existing) {
    note.className = "metric-meta metricInlineAnnotation";
    note.setAttribute("aria-hidden", "true");
    el.appendChild(note);
  }
  note.textContent = `${prefix}${label}`;
}

export function initMetricTooltips({ root = document } = {}) {
  if (_initialized) return;

  _root = root;
  ensureTooltipEl();

  root.addEventListener("mouseover", handleMouseOver);
  root.addEventListener("mousemove", handleMouseMove);
  root.addEventListener("mouseout", handleMouseOut);
  root.addEventListener("focusin", handleFocusIn);
  root.addEventListener("focusout", handleFocusOut);
  root.addEventListener("keydown", handleKeyDown);
  root.addEventListener("pointerdown", hideTooltip, true);
  window.addEventListener("scroll", handleViewportChange, true);
  window.addEventListener("resize", handleViewportChange);

  if (document.body && !_observer) {
    _observer = new MutationObserver(() => {
      if (_activeTarget && !_activeTarget.isConnected) {
        hideTooltip();
      }
    });
    _observer.observe(document.body, {
      childList: true,
      subtree: true,
    });
  }

  _initialized = true;
}
