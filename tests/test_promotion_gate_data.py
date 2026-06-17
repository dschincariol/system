from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_promotion_gate_response_shape_includes_comparison_checklist_and_safe_actions():
    from engine.api.api_governance import build_promotion_gate_data

    now_ms = int(time.time() * 1000)
    gate = build_promotion_gate_data(
        status={
            "enabled": True,
            "allowed": False,
            "updated_ts_ms": now_ms,
            "reason": {
                "promotion_enabled_env": True,
                "promotion_enabled_db": "1",
                "blockers": ["cooldown"],
                "last_promo_ts_ms": now_ms - 60_000,
                "cooldown_s": 3600,
                "crit_alerts": 0,
                "equity_drift_available": True,
                "equity_drift_crit_points": 0,
                "max_drift_ratio": 0.08,
                "model_pnl_snapshot": {"embed_regressor": 12.5},
                "statistical_gate": {"enabled": True},
                "cpcv_gate": {"enabled": True},
            },
        },
        registry_rows=[
            {
                "model_name": "embed_regressor",
                "model_kind": "ridge_v1",
                "model_ts_ms": 101,
                "stage": "champion",
                "regime": "global",
                "created_ts_ms": now_ms - 300_000,
                "updated_ts_ms": now_ms - 200_000,
                "metrics": {"rmse": 1.2, "directional_acc": 0.54, "sharpe": 0.8, "max_drawdown": 0.12},
            },
            {
                "model_name": "embed_regressor",
                "model_kind": "ridge_v2",
                "model_ts_ms": 102,
                "stage": "challenger",
                "regime": "global",
                "created_ts_ms": now_ms - 100_000,
                "updated_ts_ms": now_ms - 90_000,
                "metrics": {"rmse": 1.0, "directional_acc": 0.59, "turnover": 0.2},
            },
            {
                "model_name": "embed_regressor",
                "model_kind": "ridge_v0",
                "model_ts_ms": 99,
                "stage": "retired",
                "regime": "global",
                "created_ts_ms": now_ms - 500_000,
                "metrics": {"rmse": 1.4},
            },
        ],
        replay_status={"fresh": True, "status": "fresh", "ts_ms": now_ms},
        replay_validation={"models": {"ridge_v2": {"approved": True}}},
        shadow_scores=[{"model_name": "ridge_v2", "score": 0.7}],
    )

    assert gate["ok"] is True
    assert gate["champion"]["model_kind"] == "ridge_v1"
    assert gate["challenger"]["model_kind"] == "ridge_v2"
    assert gate["actions"]["rollback"]["available"] is True
    assert gate["actions"]["rollback"]["requires_justification"] is True
    assert gate["actions"]["rollback"]["required_confirm"] == "ROLLBACK_CHAMPION"
    assert gate["actions"]["force_promote"]["available"] is False

    metrics = {row["key"]: row for row in gate["comparison_metrics"]}
    assert metrics["rmse"]["champion"] == 1.2
    assert metrics["rmse"]["challenger"] == 1.0
    assert metrics["rmse"]["gate_threshold"]["operator"] == "<="
    assert metrics["n_eval"]["gate_threshold"]["operator"] == ">="
    assert metrics["drawdown"]["champion"] == 0.12
    assert metrics["turnover"]["challenger"] == 0.2

    checklist = {row["key"]: row for row in gate["checklist"]}
    assert checklist["cooldown"]["state"] == "fail"
    assert checklist["crit_alerts"]["state"] == "pass"
    assert checklist["replay_validation"]["state"] == "pass"
    assert checklist["statistical_gate"]["state"] == "unavailable"


def test_rollback_requires_justification_after_confirmation():
    from engine.api.api_governance import api_post_rollback

    response = api_post_rollback(None, {"confirm": "ROLLBACK_CHAMPION"})

    assert response == {
        "ok": False,
        "error": "justification_required",
        "min_length": 12,
        "http_status": 422,
    }


def test_rollback_audit_reason_includes_operator_justification(monkeypatch):
    from engine.api import api_governance
    from engine import model_registry
    from engine.strategy import promotion_audit

    before = {"model_kind": "ridge_v2", "model_ts_ms": 102}
    after = {"model_kind": "ridge_v1", "model_ts_ms": 101}
    seen = {}

    monkeypatch.setattr(model_registry, "get_stage_latest", lambda *args, **kwargs: before)
    monkeypatch.setattr(model_registry, "rollback_champion", lambda *args, **kwargs: after)
    monkeypatch.setattr(promotion_audit, "audit", lambda **kwargs: seen.update(kwargs))

    response = api_governance.api_post_rollback(
        None,
        {
            "confirm": "ROLLBACK_CHAMPION",
            "justification": "risk metrics deteriorated materially",
            "preview": {"action": "rollback"},
        },
    )

    assert response["ok"] is True
    assert response["audit_justification_recorded"] is True
    assert seen["action"] == "rollback"
    assert seen["reason"]["justification"] == "risk metrics deteriorated materially"
    assert seen["reason"]["preview"] == {"action": "rollback"}


def test_promotion_gate_ui_helpers_render_states_and_justification_payload():
    script = r"""
import assert from "node:assert/strict";
import {
  buildPromotionComparisonBarViewModel,
  buildPromotionActionPayload,
  buildRollbackConsequencePreview,
  formatGateState,
  renderGateStateBadge,
  validatePromotionActionInput
} from "./ui/promotion_gate.mjs";

assert.equal(formatGateState("pass"), "PASS");
assert.equal(formatGateState("fail"), "FAIL");
assert.equal(formatGateState("unavailable"), "Not available");
assert.match(renderGateStateBadge("fail"), /crit/);

const invalid = validatePromotionActionInput({ justification: "too short" });
assert.equal(invalid.ok, false);
assert.equal(invalid.error, "justification_required");

const payload = buildPromotionActionPayload({
  action: "rollback",
  confirm: "ROLLBACK_CHAMPION",
  justification: "risk deterioration exceeded rollback threshold",
  preview: { action: "rollback" },
});
assert.equal(payload.justification, "risk deterioration exceeded rollback threshold");
assert.equal(payload.confirm, "ROLLBACK_CHAMPION");
assert.deepEqual(payload.preview, { action: "rollback" });

const preview = buildRollbackConsequencePreview({
  model_name: "embed_regressor",
  regime: "global",
  champion: { model_kind: "ridge_v2", model_ts_ms: 102 },
  rollback_target: { model_kind: "ridge_v1", model_ts_ms: 101 },
  actions: { rollback: { available: true, preview: { action: "rollback" } } },
});
assert.match(preview, /Current champion: ridge_v2 @ 102/);
assert.match(preview, /Rollback target: ridge_v1 @ 101/);

const comparison = buildPromotionComparisonBarViewModel({
  model_name: "embed_regressor",
  regime: "global",
  status: { enabled: true, allowed: false },
  champion: { updated_ts_ms: 1_000 },
  challenger: { updated_ts_ms: 1_000 },
  comparison_metrics: [
    {
      key: "rmse",
      label: "RMSE",
      direction: "lower",
      champion: 1.2,
      challenger: 1.0,
      delta: -0.2,
      gate_threshold: { value: 1.1, operator: "<=", source: "unit" },
      p_value: 0.01,
    },
    {
      key: "turnover",
      label: "Turnover",
      direction: "lower",
      champion: 0.2,
      challenger: 0.5,
      stale: true,
    },
  ],
}, { nowMs: 10_000, staleAfterMs: 5_000 });

assert.equal(comparison.summaryState, "fail");
assert.equal(comparison.bars[0].decision.state, "pass");
assert.equal(comparison.bars[0].thresholdLabel, "Gate <= 1.100");
assert.equal(comparison.bars[0].significance.state, "pass");
assert.equal(comparison.bars[0].championPct, 100);
assert.equal(Math.round(comparison.bars[0].challengerPct), 83);
assert.equal(Math.round(comparison.bars[0].thresholdPct), 92);
assert.equal(comparison.bars[1].decision.state, "fail");
assert.equal(comparison.bars[1].stale, true);
"""
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr or result.stdout
