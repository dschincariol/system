from __future__ import annotations

import importlib
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _connect() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


def _configure_dashboard_token_file(monkeypatch, tmp_path: Path) -> None:
    token_file = tmp_path / "dashboard_api_token"
    token_file.write_text("dashboard-governance-test-token-1234567890", encoding="utf-8")
    token_file.chmod(0o600)
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)
    monkeypatch.setenv("DASHBOARD_API_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("TRADING_SECRET_POLICY_REPO_ROOT", str(tmp_path))


def _patch_monitoring(monkeypatch, *, now_ms: int) -> None:
    from engine.strategy import production_monitoring

    monkeypatch.setattr(
        production_monitoring,
        "get_latest_production_monitoring_snapshot",
        lambda limit=80: {
            "ok": True,
            "updated_ts_ms": now_ms,
            "status": {
                "state": "normal",
                "severity": "OK",
                "active": False,
                "latest_ts_ms": now_ms,
                "metric_count": 2,
                "alert_count": 0,
            },
            "metrics": [
                {
                    "metric_name": "calibration_ece",
                    "ts_ms": now_ms,
                    "state": "ok",
                    "severity": "OK",
                    "sample_n": 120,
                },
                {
                    "metric_name": "shadow_live_disagreement",
                    "ts_ms": now_ms,
                    "state": "ok",
                    "severity": "OK",
                    "sample_n": 80,
                },
            ],
            "signals": [],
        },
    )


def test_governance_evidence_marks_stale_ope_and_missing_sources_as_blockers(monkeypatch):
    from engine.api.governance_evidence import build_governance_evidence_summary
    from engine.strategy import promotion_guard

    now_ms = int(time.time() * 1000)
    stale_ts = now_ms - (91 * 24 * 60 * 60 * 1000)
    con = _connect()
    con.executescript(
        """
        CREATE TABLE risk_state(key TEXT PRIMARY KEY, value TEXT, updated_ts_ms INTEGER);
        INSERT INTO risk_state(key, value, updated_ts_ms) VALUES ('promotion_enabled', '1', 1000);

        CREATE TABLE policy_ope_evidence(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          candidate_key TEXT,
          model_id TEXT,
          model_name TEXT NOT NULL,
          candidate_type TEXT NOT NULL,
          candidate_version TEXT,
          symbol TEXT,
          horizon_s INTEGER NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          policy_value REAL,
          standard_error REAL,
          ci_lower REAL,
          ci_upper REAL,
          n_obs INTEGER NOT NULL DEFAULT 0,
          effective_n REAL NOT NULL DEFAULT 0.0,
          support REAL NOT NULL DEFAULT 0.0,
          max_importance_weight REAL NOT NULL DEFAULT 0.0,
          confidence_z REAL NOT NULL DEFAULT 0.0,
          decision TEXT NOT NULL,
          reason TEXT NOT NULL,
          config_json TEXT NOT NULL DEFAULT '{}',
          diagnostics_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE runtime_meta(key TEXT PRIMARY KEY, value TEXT);
        """
    )
    con.execute(
        """
        INSERT INTO policy_ope_evidence(
          ts_ms, candidate_key, model_id, model_name, candidate_type,
          policy_value, standard_error, ci_lower, ci_upper, n_obs,
          effective_n, support, max_importance_weight, confidence_z,
          decision, reason, diagnostics_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            stale_ts,
            "policy:v1",
            "policy-v1",
            "sizing_policy_v1",
            "sizing_policy",
            0.12,
            0.01,
            0.08,
            0.16,
            200,
            140.0,
            0.93,
            4.0,
            1.96,
            "pass",
            "pass",
            json.dumps({"passed": True, "blockers": []}),
        ),
    )

    monkeypatch.setattr(promotion_guard, "promotion_allowed", lambda: (True, {"blockers": []}))
    _patch_monitoring(monkeypatch, now_ms=now_ms)

    payload = build_governance_evidence_summary(con=con, now_ms=now_ms)

    evidence = {row["key"]: row for row in payload["evidence"]}
    assert payload["state"] == "block"
    assert evidence["ope_gate"]["state"] == "block"
    assert evidence["ope_gate"]["freshness"] == "stale"
    assert evidence["ope_gate"]["source_artifact"].startswith("policy_ope_evidence#")
    assert evidence["experiment_ledger"]["state"] == "block"
    assert evidence["net_after_cost_labels"]["remediation"].startswith("Run label materialization")
    assert any(row["key"] == "ope_gate" for row in payload["blockers"])
    assert payload["authority"]["mode"] == "read_only_governance_evidence"


def test_generated_candidate_provenance_returns_exact_blockers():
    from engine.api.governance_evidence import build_generated_candidate_provenance

    con = _connect()
    con.executescript(
        """
        CREATE TABLE experiment_ledger(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER NOT NULL,
          candidate_key TEXT NOT NULL,
          candidate_name TEXT,
          candidate_version TEXT,
          candidate_type TEXT NOT NULL,
          source TEXT NOT NULL,
          parent_candidate_key TEXT,
          model_name TEXT,
          model_family TEXT,
          feature_ids_json TEXT NOT NULL DEFAULT '[]',
          prompt_hash TEXT,
          model_hash TEXT,
          search_space_json TEXT NOT NULL DEFAULT '{}',
          trial_budget INTEGER NOT NULL DEFAULT 0,
          trial_count INTEGER NOT NULL DEFAULT 0,
          cpcv_json TEXT NOT NULL DEFAULT '{}',
          pbo REAL,
          dsr REAL,
          fdr_json TEXT NOT NULL DEFAULT '{}',
          redundancy_json TEXT NOT NULL DEFAULT '{}',
          evidence_json TEXT NOT NULL DEFAULT '{}',
          promotion_decision TEXT NOT NULL DEFAULT 'pending',
          status TEXT NOT NULL DEFAULT 'recorded',
          diagnostics_json TEXT NOT NULL DEFAULT '{}'
        );
        """
    )
    con.execute(
        """
        INSERT INTO experiment_ledger(
          ts, candidate_key, candidate_name, candidate_version, candidate_type,
          source, feature_ids_json, trial_budget, trial_count, promotion_decision
        )
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            2000,
            "llm_factor:abc",
            "llm_factor",
            "abc",
            "alpha",
            "llm_factor",
            json.dumps(["factor.llm.abc"]),
            0,
            0,
            "pending",
        ),
    )

    payload = build_generated_candidate_provenance(con=con)

    assert payload["state"] == "block"
    assert payload["rows"][0]["source_artifact"] == "experiment_ledger#1"
    assert "trial_budget_missing" in payload["rows"][0]["blockers"]
    assert "ledger_decision_not_passing" in payload["blockers"][0]["blockers"]


def test_shadow_capital_payload_is_registered_and_masks_sensitive_components(monkeypatch, tmp_path):
    from engine.api.governance_evidence import build_shadow_capital_evidence

    now_ms = int(time.time() * 1000)
    con = _connect()
    con.executescript(
        """
        CREATE TABLE shadow_capital_scores(
          ts_ms INTEGER NOT NULL,
          window_s INTEGER NOT NULL,
          regime TEXT NOT NULL DEFAULT 'global',
          model_name TEXT NOT NULL,
          n INTEGER NOT NULL DEFAULT 0,
          rmse REAL,
          dir_acc REAL,
          net_rmse REAL,
          slippage_bps_mean REAL,
          slippage_bps_std REAL,
          drawdown_proxy REAL,
          cap_eff REAL,
          realized_pnl REAL,
          unrealized_pnl REAL,
          total_pnl REAL,
          score REAL NOT NULL,
          components_json TEXT,
          PRIMARY KEY(model_name, window_s, regime)
        );
        """
    )
    con.execute(
        """
        INSERT INTO shadow_capital_scores(
          ts_ms, window_s, regime, model_name, n, realized_pnl,
          unrealized_pnl, total_pnl, score, components_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_ms,
            86400,
            "global",
            "challenger_a",
            42,
            5.0,
            1.0,
            6.0,
            4.5,
            json.dumps({"realized_pnl": 5.0, "broker_account_id": "abc", "token": "secret"}),
        ),
    )

    payload = build_shadow_capital_evidence(con=con, now_ms=now_ms)

    assert payload["masking"]["applied"] is True
    assert payload["rows"][0]["model_name"] == "challenger_a"
    assert payload["rows"][0]["components"] == {"realized_pnl": 5.0}

    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_INIT_ON_STARTUP", "0")
    monkeypatch.setenv("ENGINE_PRIMARY_BOOTSTRAP_DONE", "1")
    _configure_dashboard_token_file(monkeypatch, tmp_path)
    dashboard_server = importlib.import_module("dashboard_server")
    route_index = {(str(route["method"]), str(route["path"])): route for route in dashboard_server.ROUTE_SPECS}
    for path in (
        "/api/governance/evidence",
        "/api/governance/evidence/promotion_blockers",
        "/api/governance/evidence/generated_candidates",
        "/api/governance/evidence/shadow_capital",
        "/api/governance/shadow_capital/scores",
    ):
        assert ("GET", path) in route_index
        handler_name = route_index[("GET", path)]["handler"]
        assert callable(dashboard_server.API_HANDLERS[handler_name])


def test_governance_evidence_ui_renderer_outputs_required_columns():
    script = r"""
import assert from "node:assert/strict";
import {
  renderGovernanceEvidenceCenter,
  summarizeGovernanceEvidence
} from "./ui/promotion_gate.mjs";

const payload = {
  state: "block",
  authority: { mode: "read_only_governance_evidence" },
  evidence: [{
    key: "ope_gate",
    label: "OPE gate",
    state: "block",
    freshness: "stale",
    sample_count: 10,
    last_update_ts_ms: 1700000000000,
    source_artifact: "policy_ope_evidence#7",
    remediation: "Run OPE."
  }],
  blockers: [{
    key: "ope_gate",
    label: "OPE gate",
    state: "block",
    freshness: "stale",
    source_artifact: "policy_ope_evidence#7",
    remediation: "Run OPE."
  }],
  shadow_capital: {
    rows: [{ model_name: "m1", score: 1.25, n: 20, total_pnl: 4, ts_ms: 1700000000000 }],
    evidence: { state: "pass" }
  }
};

const summary = summarizeGovernanceEvidence(payload);
assert.equal(summary.state, "block");
assert.equal(summary.blockerCount, 1);

const target = { innerHTML: "" };
renderGovernanceEvidenceCenter(target, payload);
assert.match(target.innerHTML, /Evidence/);
assert.match(target.innerHTML, /Freshness/);
assert.match(target.innerHTML, /Samples/);
assert.match(target.innerHTML, /Source/);
assert.match(target.innerHTML, /Remediation/);
assert.match(target.innerHTML, /policy_ope_evidence#7/);
assert.match(target.innerHTML, /Shadow capital/);
"""
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr or result.stdout
