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
                "deconfounded_signal_validation": {
                    "passed": False,
                    "status": "weak_incremental_effect",
                    "coefficient": 0.0,
                    "residual_ic": 0.0,
                },
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
    assert gate["model_card_preview"]["intended_use"]
    assert gate["gate_state_snapshot"]["checklist"]
    assert gate["source_citations"]
    assert any(row["key"].startswith("temporal_eval") for row in gate["staleness_badges"])

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
    assert checklist["deconfounded_signal_validation"]["state"] == "fail"
    assert checklist["deconfounded_signal_validation"]["observed"]["status"] == "weak_incremental_effect"


def test_decision_snapshot_serializes_model_card_gate_state_and_citations():
    from engine.strategy.model_decision_snapshot import enrich_decision_reason

    now_ms = int(time.time() * 1000)
    reason = enrich_decision_reason(
        {"justification": "metrics were stale before rollback"},
        action="rollback",
        actor="risk-owner",
        model_name="embed_regressor",
        from_kind="ridge_v2",
        from_ts_ms=102,
        to_kind="ridge_v1",
        to_ts_ms=101,
        regime="global",
        gate_snapshot={
            "source": "unit-test",
            "status": {
                "allowed": False,
                "blockers": ["execution_degradation"],
                "updated_ts_ms": now_ms,
            },
            "champion": {
                "model_name": "embed_regressor",
                "model_kind": "ridge_v2",
                "model_ts_ms": 102,
                "updated_ts_ms": 1,
                "metrics": {"rmse": 1.3},
            },
            "rollback_target": {
                "model_name": "embed_regressor",
                "model_kind": "ridge_v1",
                "model_ts_ms": 101,
                "updated_ts_ms": now_ms,
                "metrics": {"rmse": 1.1},
            },
            "comparison_metrics": [{"key": "rmse", "champion": 1.3, "challenger": 1.1}],
            "checklist": [{"key": "cpcv_gate", "state": "fail"}],
            "validation": {"replay_status": {"fresh": False, "status": "stale", "ts_ms": 1}},
        },
        confirmation={
            "action_id": "promotion.rollback",
            "confirmation_token": "ROLLBACK_CHAMPION",
        },
        decision_ts_ms=now_ms,
    )

    card = reason["model_card_snapshot"]
    gate_state = reason["gate_state_at_decision"]
    assert card["schema_version"] == 1
    assert card["action"] == "rollback"
    assert card["owner"] == "risk-owner"
    assert card["metrics"]["from_model"]["model_kind"] == "ridge_v2"
    assert card["metrics"]["to_model"]["model_kind"] == "ridge_v1"
    assert card["comparison_to_champion"][0]["key"] == "rmse"
    assert card["source_citations"]
    assert gate_state["checklist"][0]["key"] == "cpcv_gate"
    assert any(row["key"] == "replay_stale" for row in gate_state["staleness_badges"])
    assert any(row["key"] == "execution_degradation" for row in gate_state["staleness_badges"])
    assert reason["confirmation"]["action_id"] == "promotion.rollback"


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
    monkeypatch.setattr(
        api_governance,
        "_current_gate_snapshot_for_audit",
        lambda *args, **kwargs: {
            "source": "unit-test",
            "status": {"allowed": False, "blockers": ["execution_degradation"], "updated_ts_ms": 123},
            "champion": {"model_kind": "ridge_v2", "model_ts_ms": 102, "updated_ts_ms": 123},
            "rollback_target": {"model_kind": "ridge_v1", "model_ts_ms": 101, "updated_ts_ms": 123},
            "comparison_metrics": [{"key": "rmse", "champion": 1.4, "challenger": 1.1}],
            "checklist": [{"key": "cpcv_gate", "state": "fail"}],
            "validation": {"replay_status": {"fresh": False, "status": "stale", "ts_ms": 1}},
        },
    )

    response = api_governance.api_post_rollback(
        None,
        {
            "confirm": "ROLLBACK_CHAMPION",
            "confirmation_token": "ROLLBACK_CHAMPION",
            "action_id": "promotion.rollback",
            "confirmation_method": "typed_phrase",
            "justification": "risk metrics deteriorated materially",
            "preview": {"action": "rollback"},
            "request_id": "req-123",
        },
    )

    assert response["ok"] is True
    assert response["audit_justification_recorded"] is True
    assert seen["action"] == "rollback"
    assert seen["reason"]["justification"] == "risk metrics deteriorated materially"
    assert seen["reason"]["preview"] == {"action": "rollback"}
    assert seen["reason"]["confirmation"]["action_id"] == "promotion.rollback"
    assert seen["reason"]["confirmation"]["confirmation_token"] == "ROLLBACK_CHAMPION"
    assert seen["reason"]["confirmation"]["request_id"] == "req-123"
    assert seen["reason"]["model_card_snapshot"]["metrics"]["to_model"]["model_kind"] == "ridge_v1"
    assert seen["reason"]["gate_state_at_decision"]["checklist"][0]["key"] == "cpcv_gate"
    assert any(row["key"] == "replay_stale" for row in seen["reason"]["staleness_badges"])


def test_promotion_gate_ui_helpers_render_states_and_justification_payload():
    script = r"""
import assert from "node:assert/strict";
import {
  buildPromotionComparisonBarViewModel,
  buildPromotionActionPayload,
  buildRollbackConsequencePreview,
  formatGateState,
  normalizePromotionAuditRow,
  renderPromotionAuditRows,
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
  gateSnapshot: { source: "unit-test" },
});
assert.equal(payload.justification, "risk deterioration exceeded rollback threshold");
assert.equal(payload.confirm, "ROLLBACK_CHAMPION");
assert.deepEqual(payload.preview, { action: "rollback" });
assert.equal(payload.gate_snapshot.source, "unit-test");

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

const auditRow = normalizePromotionAuditRow({
  ts_ms: 123,
  actor: "manual",
  action: "rollback",
  model_name: "embed_regressor",
  regime: "global",
  reason: {
    justification: "risk metrics stale",
    confirmation: { action_id: "promotion.rollback", confirmation_token: "ROLLBACK_CHAMPION" },
  },
  model_card_snapshot: {
    owner: "risk-owner",
    intended_use: "governed trading predictions",
    data_window: { start_ts_ms: 100, end_ts_ms: 120 },
    metrics: {
      from_model: { model_kind: "ridge_v2" },
      to_model: { model_kind: "ridge_v1" },
    },
    caveats: ["Replay evidence stale"],
  },
  gate_state_at_decision: {
    checklist: [{ key: "cpcv_gate", state: "fail" }],
    staleness_badges: [{ key: "replay_stale", label: "Replay evidence stale", state: "stale", source: "runtime_meta", ts_ms: 1 }],
    source_citations: [{ source: "model_registry", label: "champion", ts_ms: 100 }],
  },
  source_citations: [{ source: "promotion_guard.status", label: "guard", ts_ms: 123 }],
});
assert.equal(auditRow.confirmation.action_id, "promotion.rollback");
assert.equal(auditRow.badges[0].key, "replay_stale");

const auditHtml = renderPromotionAuditRows([auditRow]);
assert.match(auditHtml, /ridge_v2 -&gt; ridge_v1/);
assert.match(auditHtml, /governed trading predictions/);
assert.match(auditHtml, /Replay evidence stale: stale/);
assert.match(auditHtml, /promotion_guard.status/);
assert.match(auditHtml, /promotion.rollback \/ ROLLBACK_CHAMPION/);
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
