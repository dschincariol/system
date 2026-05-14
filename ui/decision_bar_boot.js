/*
  FILE: ui/decision_bar_boot.js

  Bootstrap helpers for the extracted decision bar. This module wires shared
  dashboard state into the decision-bar runtime and keeps that setup logic out
  of the main controller file.
*/

import { initDecisionBarEngine } from "./decision_bar.js";

export function initDecisionBarRuntime({
  getLastAlerts,
  getLastHealth,
  getLastSystemState,
  getLastExecutionBarrier,
  getLastPromotionStatus,
  isExecutionDegraded,
  updateDecisionHeader
}) {

  if (typeof getLastAlerts !== "function") return;

  try {

    window.__decisionBarEngine = {
      getLastAlerts,
      getLastHealth,
      getLastSystemState,
      getLastExecutionBarrier,
      getLastPromotionStatus,
      isExecutionDegraded
    };

    initDecisionBarEngine(window.__decisionBarEngine);
    updateDecisionHeader("boot");

  } catch (e) {
    console.warn("decision bar init failed", e);
  }
}
